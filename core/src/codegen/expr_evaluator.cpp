/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2023. All rights reserved.
 * Description: Expression evaluator
 */
#include "expr_evaluator.h"
#include "vector/lazy_vector.h"

namespace omniruntime::codegen {
void GetData(VectorBatch &vecBatch, intptr_t valueAddrs[], intptr_t nullAddrs[], intptr_t offsetAddrs[],
    intptr_t dictionaries[])
{
    intptr_t valuesAddress;
    intptr_t dictVecAddress;
    int32_t vectorCount = vecBatch.GetVectorCount();
    for (int32_t i = 0; i < vectorCount; i++) {
        Vector *colVec = vecBatch.GetVector(i);
        if (colVec->GetEncoding() == OMNI_VEC_ENCODING_LAZY) {
            colVec = dynamic_cast<LazyVector *>(colVec)->GetLoadedVector();
        }
        dictVecAddress = 0;
        valuesAddress = 0;
        if (colVec->GetEncoding() == OMNI_VEC_ENCODING_DICTIONARY) {
            dictVecAddress = reinterpret_cast<intptr_t>(reinterpret_cast<void *>(colVec));
        } else {
            valuesAddress = VectorHelper::GetValuesAddr(colVec);
        }

        // data handling
        dictionaries[i] = dictVecAddress;
        valueAddrs[i] = valuesAddress;

        // nulls handling
        nullAddrs[i] = VectorHelper::GetNullsAddr(colVec);

        // offsets handling
        offsetAddrs[i] = VectorHelper::GetOffsetsAddr(colVec);
    }
}

Filter::Filter(const Expr &expression, const DataTypes &inputDataTypes, OverflowConfig *overflowConfig)
{
#ifdef DEBUG
    std::cout << "String expression in Filter: " << std::endl;
    ExprPrinter printExprTree;
    expression.Accept(printExprTree);
    std::cout << std::endl;
#endif
    intptr_t f;
    if (!ConfigUtil::IsEnableBatchExprEvaluate()) {
        this->codeGen = std::make_unique<FilterCodeGen>("filterFunc", expression, overflowConfig);
        f = this->codeGen->GetFunction(inputDataTypes);
    } else {
        this->batchCodeGen = std::make_unique<BatchFilterCodeGen>("filterFunc", expression, overflowConfig);
        f = this->batchCodeGen->GetFunction();
    }
    if (f == 0) {
        this->isSupported = false;
        this->apply = nullptr;
    } else {
        this->isSupported = true;
        void *function = &f;
        this->apply = *static_cast<FilterFunc *>(function);
    }
}

bool Projection::Initialize(bool filter, const DataTypes &inputDataTypes, OverflowConfig *overflowConfig)
{
    // short-circuit logic for column projections
    // no need to go through codegen
    if (expr->GetType() == ExprType::FIELD_E) {
        auto fieldExpr = dynamic_cast<const FieldExpr *>(expr);
        this->isColumnProjection = true;
        this->columnProjectionIndex = fieldExpr->colVal;
        return true;
    }
    intptr_t f;
    if (!ConfigUtil::IsEnableBatchExprEvaluate()) {
        this->codeGen = std::make_unique<ProjectionCodeGen>("proj_func", *(this->expr), filter, overflowConfig);
        f = this->codeGen->GetFunction(inputDataTypes);
    } else {
        this->batchCodeGen =
            std::make_unique<BatchProjectionCodeGen>("proj_func", *(this->expr), filter, overflowConfig);
        f = this->batchCodeGen->GetFunction();
    }

    if (f == 0) {
        return false;
    }

    void *function = &f;
    auto cfunction = static_cast<ProjFunc *>(function);
    this->projector = *cfunction;
    return true;
}

Projection::Projection(const Expr &expr, bool filter, DataTypePtr outType, const DataTypes &inputDataTypes,
    OverflowConfig *overflowConfig)
    : expr(&expr), outType(std::move(outType)), projector(nullptr)
{
#ifdef DEBUG
    std::cout << "Expression in projection:" << std::endl;
    ExprPrinter printExprTree;
    expr.Accept(printExprTree);
    std::cout << std::endl;
#endif
    bool initialized = this->Initialize(filter, inputDataTypes, overflowConfig);
    if (!initialized) {
        this->isSupported = false;
    }
}

Vector *GenerateOutVec(DataTypeId outTypeId, VectorAllocator *vecAllocator, int32_t numSelectedRows,
    const Projection *projection)
{
    switch (outTypeId) {
        case OMNI_DATE32:
        case OMNI_INT:
            return new IntVector(vecAllocator, numSelectedRows);
        case OMNI_LONG:
        case OMNI_DECIMAL64:
            return new LongVector(vecAllocator, numSelectedRows);
        case OMNI_DOUBLE:
            return new DoubleVector(vecAllocator, numSelectedRows);
        case OMNI_CHAR:
        case OMNI_VARCHAR:
            return new VarcharVector(vecAllocator, numSelectedRows);
        case OMNI_DECIMAL128:
            return new Decimal128Vector(vecAllocator, numSelectedRows);
        case OMNI_BOOLEAN:
            return new BooleanVector(vecAllocator, numSelectedRows);
        default: {
            LogError("No such data type %d", outTypeId);
            return nullptr;
        }
    }
}

Vector *Projection::Project(VectorAllocator *vecAllocator, VectorBatch *vecBatch, int32_t selectedRows[],
    int32_t numSelectedRows, int64_t *valueAddrs, int64_t *nullAddrs, int64_t *offsetAddrs, ExecutionContext *context,
    int64_t *dictionaryVectors) const
{
    if (this->isColumnProjection) {
        Vector *colVec = vecBatch->GetVector(this->columnProjectionIndex);
        if (colVec->GetEncoding() == OMNI_VEC_ENCODING_LAZY) {
            colVec = dynamic_cast<LazyVector *>(colVec)->GetLoadedVector();
        }
        if (numSelectedRows != 0 && numSelectedRows == vecBatch->GetRowCount()) {
            return colVec->Slice(0, numSelectedRows);
        }

        if (selectedRows != nullptr && numSelectedRows != 0) {
            if (colVec->GetEncoding() == OMNI_VEC_ENCODING_DICTIONARY) {
                return dynamic_cast<DictionaryVector *>(colVec)->ExtractDictionary(selectedRows, numSelectedRows);
            } else {
                return colVec->CopyPositions(selectedRows, 0, numSelectedRows);
            }
        }
    }

    DataTypeId outTypeId = this->GetOutputType().GetId();
    Vector *outVec = GenerateOutVec(outTypeId, vecAllocator, numSelectedRows, this);
    if (outVec == nullptr) {
        return outVec;
    }

    if (outTypeId == OMNI_VARCHAR || outTypeId == OMNI_CHAR) {
        ProjectHelperVarWidth(*vecBatch, valueAddrs, nullAddrs, offsetAddrs, outVec, numSelectedRows, selectedRows,
            context, dictionaryVectors);
    } else {
        ProjectHelperFixedWidth(*vecBatch, valueAddrs, nullAddrs, offsetAddrs, outVec, numSelectedRows, selectedRows,
            context, dictionaryVectors);
    }
    context->GetArena()->Reset();
    // fixme::true means projected vec has null in it, when project supports whether the vec of the output flag is null
    // or not, it is obtained directly from the project flag
    outVec->SetNullFlag(true);
    return outVec;
}

void Projection::ProjectHelperVarWidth(omniruntime::vec::VectorBatch &vecBatch, int64_t *valueAddrs, int64_t *nullAddrs,
    int64_t *offsetAddrs, omniruntime::vec::Vector *outVec, int32_t numSelectedRows, int32_t selectedRows[],
    ExecutionContext *context, int64_t *dictionaryVectors) const
{
    ((int32_t *)outVec->GetValueOffsets())[0] = 0;
    GetProjector()(valueAddrs, vecBatch.GetRowCount(), reinterpret_cast<int64_t>(outVec), selectedRows, numSelectedRows,
        nullAddrs, offsetAddrs, reinterpret_cast<bool *>(outVec->GetValueNulls()),
        reinterpret_cast<int32_t *>(outVec->GetValueOffsets()), reinterpret_cast<int64_t>(context), dictionaryVectors);
}

void Projection::ProjectHelperFixedWidth(omniruntime::vec::VectorBatch &vecBatch, int64_t *valueAddrs,
    int64_t *nullAddrs, int64_t *offsetAddrs, omniruntime::vec::Vector *outVec, int32_t numSelectedRows,
    int32_t selectedRows[], ExecutionContext *context, int64_t *dictionaryVectors) const
{
    GetProjector()(valueAddrs, vecBatch.GetRowCount(), reinterpret_cast<int64_t>(outVec->GetValues()), selectedRows,
        numSelectedRows, nullAddrs, offsetAddrs, reinterpret_cast<bool *>(outVec->GetValueNulls()),
        reinterpret_cast<int32_t *>(outVec->GetValueOffsets()), reinterpret_cast<int64_t>(context), dictionaryVectors);
}

Vector *Projection::Project(VectorAllocator *vecAllocator, VectorBatch *vecBatch, int64_t *valueAddrs,
    int64_t *nullAddrs, int64_t *offsetAddrs, ExecutionContext *context, int64_t *dictionaryVectors) const
{
    return this->Project(vecAllocator, vecBatch, nullptr, vecBatch->GetRowCount(), valueAddrs, nullAddrs, offsetAddrs,
        context, dictionaryVectors);
}

ExpressionEvaluator::ExpressionEvaluator(Expr *filterExpression, const std::vector<Expr *> &projectionExprs,
    const DataTypes &inputDataTypes, OverflowConfig *ofConfig)
    : inputTypes(const_cast<DataTypes &>(inputDataTypes))
{
    hasFilter = true;
    filterExpr = filterExpression;
    for (auto &projectionExpr : projectionExprs) {
        projExprs.emplace_back(projectionExpr);
    }
    overflowConfig = std::make_unique<OverflowConfig>(*ofConfig);
    projectVecCount = static_cast<int32_t>(projectionExprs.size());
}

ExpressionEvaluator::ExpressionEvaluator(const std::vector<Expr *> &projectionExprs, const DataTypes &inputDataTypes,
    OverflowConfig *ofConfig)
    : inputTypes(const_cast<DataTypes &>(inputDataTypes))
{
    hasFilter = false;
    for (auto &projectionExpr : projectionExprs) {
        projExprs.emplace_back(projectionExpr);
    }
    overflowConfig = std::make_unique<OverflowConfig>(*ofConfig);
    projectVecCount = static_cast<int32_t>(projectionExprs.size());
}

bool ExpressionEvaluator::IsSupportedExpr() const
{
    return isSupportedExpr;
}

VectorBatch *ExpressionEvaluator::Evaluate(VectorBatch *vecBatch, ExecutionContext *context,
    VectorAllocator *vecAllocator)
{
    const int vectorCount = vecBatch->GetVectorCount();
    intptr_t valueAddrs[vectorCount];
    intptr_t nullAddrs[vectorCount];
    intptr_t offsetAddrs[vectorCount];
    intptr_t dictionaries[vectorCount];
    GetData(*vecBatch, valueAddrs, nullAddrs, offsetAddrs, dictionaries);
    if (hasFilter) {
        return ProcessFilterAndProject(vecBatch, context, vecAllocator, valueAddrs, nullAddrs, offsetAddrs,
            dictionaries);
    } else {
        return ProcessProject(vecBatch, context, vecAllocator, valueAddrs, nullAddrs, offsetAddrs, dictionaries);
    }
}

void ExpressionEvaluator::FilterFuncGeneration()
{
    filter = std::make_unique<Filter>(*filterExpr, GetInputDataTypes(), overflowConfig.get());
    if (!this->filter->IsSupported()) {
        this->isSupportedExpr = false;
    }
    for (auto &projExpr : projExprs) {
        auto projection = std::make_unique<Projection>(*projExpr, true, projExpr->GetReturnType(), GetInputDataTypes(),
            overflowConfig.get());
        if (!projection->IsSupported()) {
            this->isSupportedExpr = false;
            break;
        }
        projections.emplace_back(move(projection));
    }
}

void ExpressionEvaluator::ProjectFuncGeneration()
{
    for (auto &projExpr : projExprs) {
        auto projection = std::make_unique<Projection>(*projExpr, false, projExpr->GetReturnType(), GetInputDataTypes(),
            overflowConfig.get());
        if (!projection->IsSupported()) {
            this->isSupportedExpr = false;
            break;
        }
        projections.emplace_back(move(projection));
    }
}

VectorBatch *ExpressionEvaluator::ProcessProject(VectorBatch *vecBatch, ExecutionContext *context,
    VectorAllocator *vecAllocator, intptr_t *valueAddrs, intptr_t *nullAddrs, intptr_t *offsetAddrs,
    intptr_t *dictionaries)
{
    auto rowCount = vecBatch->GetRowCount();
    auto projectedVecs = new VectorBatch(projectVecCount, rowCount);
    for (int32_t i = 0; i < projectVecCount; i++) {
        Vector *outCol =
            projections[i]->Project(vecAllocator, vecBatch, valueAddrs, nullAddrs, offsetAddrs, context, dictionaries);
        if (context->HasError()) {
            delete outCol;
            for (int32_t j = 0; j < i; j++) {
                delete projectedVecs->GetVector(j);
            }
            delete projectedVecs;
            VectorHelper::FreeVecBatch(vecBatch);
            context->GetArena()->Reset();
            std::string errorMessage = context->GetError();
            throw OmniException("OPERATOR_RUNTIME_ERROR", errorMessage);
        }
        projectedVecs->SetVector(i, outCol);
    }
    VectorHelper::FreeVecBatch(vecBatch);
    context->GetArena()->Reset();
    return projectedVecs;
}

VectorBatch *ExpressionEvaluator::ProcessFilterAndProject(VectorBatch *vecBatch, ExecutionContext *context,
    VectorAllocator *vecAllocator, intptr_t *valueAddrs, intptr_t *nullAddrs, intptr_t *offsetAddrs,
    intptr_t *dictionaries)
{
    const int rowCount = vecBatch->GetRowCount();
    auto selectedRows = new int32_t[rowCount];
    int32_t numSelectedRows = filter->GetFilterFunc()(valueAddrs, rowCount, selectedRows, nullAddrs, offsetAddrs,
        reinterpret_cast<int64_t>(context), dictionaries);
    if (context->HasError()) {
        context->GetArena()->Reset();
        delete[] selectedRows;
        VectorHelper::FreeVecBatch(vecBatch);
        std::string errorMessage = context->GetError();
        throw OmniException("OPERATOR_RUNTIME_ERROR", errorMessage);
    }
    if (numSelectedRows <= 0) {
        context->GetArena()->Reset();
        delete[] selectedRows;
        VectorHelper::FreeVecBatch(vecBatch);
        return nullptr;
    }

    auto projectedVecs = new VectorBatch(projectVecCount, numSelectedRows);
    for (int32_t i = 0; i < projectVecCount; i++) {
        Vector *col = projections[i]->Project(vecAllocator, vecBatch, selectedRows, numSelectedRows, valueAddrs,
            nullAddrs, offsetAddrs, context, dictionaries);
        if (context->HasError()) {
            delete col;
            for (int32_t j = 0; j < i; j++) {
                delete projectedVecs->GetVector(j);
            }
            delete projectedVecs;
            delete[] selectedRows;
            VectorHelper::FreeVecBatch(vecBatch);
            context->GetArena()->Reset();

            std::string errorMessage = context->GetError();
            throw OmniException("OPERATOR_RUNTIME_ERROR", errorMessage);
        }
        projectedVecs->SetVector(i, col);
    }

    delete[] selectedRows;
    VectorHelper::FreeVecBatch(vecBatch);
    context->GetArena()->Reset();
    return projectedVecs;
}
}