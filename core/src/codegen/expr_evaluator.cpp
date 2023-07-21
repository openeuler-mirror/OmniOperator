/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2023. All rights reserved.
 * Description: Expression evaluator
 */
#include "expr_evaluator.h"

namespace omniruntime::codegen {
int64_t GetRawAddr(const DataTypes &types, int32_t i, BaseVector *colVec)
{
    switch (types.GetIds()[i]) {
        case OMNI_INT:
        case OMNI_DATE32:
            return reinterpret_cast<int64_t>(
                unsafe::UnsafeVector::GetRawValues(reinterpret_cast<Vector<int32_t> *>(colVec)));
        case OMNI_SHORT:
            return reinterpret_cast<int64_t>(
                unsafe::UnsafeVector::GetRawValues(reinterpret_cast<Vector<int16_t> *>(colVec)));
        case OMNI_LONG:
        case OMNI_DECIMAL64:
            return reinterpret_cast<int64_t>(
                unsafe::UnsafeVector::GetRawValues(reinterpret_cast<Vector<int64_t> *>(colVec)));
        case OMNI_DOUBLE:
            return reinterpret_cast<int64_t>(
                unsafe::UnsafeVector::GetRawValues(reinterpret_cast<Vector<double> *>(colVec)));
        case OMNI_BOOLEAN:
            return reinterpret_cast<int64_t>(
                unsafe::UnsafeVector::GetRawValues(reinterpret_cast<Vector<bool> *>(colVec)));
        case OMNI_DECIMAL128:
            return reinterpret_cast<int64_t>(
                unsafe::UnsafeVector::GetRawValues(reinterpret_cast<Vector<Decimal128> *>(colVec)));
        case OMNI_VARCHAR:
        case OMNI_CHAR:
            return reinterpret_cast<int64_t>(unsafe::UnsafeStringVector::GetValues(
                reinterpret_cast<Vector<LargeStringContainer<std::string_view>> *>(colVec)));
        default:
            LogError("Do not support such vector type %d", types.GetIds()[i]);
            return 0;
    }
}

void GetAddr(VectorBatch &vecBatch, intptr_t valueAddrs[], intptr_t nullAddrs[], intptr_t offsetAddrs[],
    intptr_t dictionaries[], const DataTypes &types)
{
    intptr_t valuesAddress;
    intptr_t dictVecAddress;
    int32_t vectorCount = vecBatch.GetVectorCount();
    for (int32_t i = 0; i < vectorCount; i++) {
        auto colVec = vecBatch.Get(i);
        dictVecAddress = 0;
        valuesAddress = 0;
        if (colVec->GetEncoding() == OMNI_DICTIONARY) {
            dictVecAddress = reinterpret_cast<intptr_t>(reinterpret_cast<void *>(colVec));
        } else {
            valuesAddress = GetRawAddr(types, i, colVec);
        }

        // data handling
        dictionaries[i] = dictVecAddress;
        valueAddrs[i] = valuesAddress;

        // nulls handling
        nullAddrs[i] = reinterpret_cast<intptr_t>(unsafe::UnsafeBaseVector::GetNulls(colVec));

        // offsets handling
        offsetAddrs[i] = reinterpret_cast<intptr_t>(VectorHelper::UnsafeGetOffsetsAddr(colVec));
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
        auto fieldExpr = static_cast<const FieldExpr *>(expr);
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

BaseVector *Projection::Project(VectorBatch *vecBatch, int32_t selectedRows[], int32_t numSelectedRows,
    int64_t *valueAddrs, int64_t *nullAddrs, int64_t *offsetAddrs, ExecutionContext *context,
    int64_t *dictionaryVectors, const int32_t *typeIds) const
{
    if (this->isColumnProjection) {
        return ColumnProjectionProxy(vecBatch, selectedRows, numSelectedRows, typeIds);
    }

    DataTypeId outTypeId = this->GetOutputType().GetId();
    BaseVector *outVec = nullptr;

    if (outTypeId == OMNI_VARCHAR || outTypeId == OMNI_CHAR) {
        ProjectHelperVarWidth(*vecBatch, valueAddrs, nullAddrs, offsetAddrs, &outVec, numSelectedRows, selectedRows,
            context, dictionaryVectors, outTypeId);
    } else {
        ProjectHelperFixedWidth(*vecBatch, valueAddrs, nullAddrs, offsetAddrs, &outVec, numSelectedRows, selectedRows,
            context, dictionaryVectors, outTypeId);
    }
    context->GetArena()->Reset();
    return outVec;
}

template <typename T>
BaseVector *Projection::ColumnProjectionFlatVectorCopyPositionsHelper(const int32_t *selectedRows,
    int32_t numSelectedRows, BaseVector *colVec) const
{
    return reinterpret_cast<Vector<T> *>(colVec)->CopyPositions(selectedRows, 0, numSelectedRows);
}

template <typename T>
BaseVector *Projection::ColumnProjectionDictionaryVectorCopyPositionsHelper(const int32_t *selectedRows,
    int32_t numSelectedRows, BaseVector *colVec) const
{
    return reinterpret_cast<Vector<DictionaryContainer<T>> *>(colVec)->CopyPositions(selectedRows, 0, numSelectedRows);
}

template <typename T>
BaseVector *Projection::ColumnProjectionFlatVectorSliceHelper(int32_t numSelectedRows,
    BaseVector *colVec) const
{
    return reinterpret_cast<Vector<T> *>(colVec)->Slice(0, numSelectedRows);
}

template <typename T>
BaseVector *Projection::ColumnProjectionDictionaryVectorSliceHelper(int32_t numSelectedRows,
    BaseVector *colVec) const
{
    return reinterpret_cast<Vector<DictionaryContainer<T>> *>(colVec)->Slice(0, numSelectedRows);
}

void Projection::ProjectHelperVarWidth(VectorBatch &vecBatch, int64_t *valueAddrs, int64_t *nullAddrs,
    int64_t *offsetAddrs, BaseVector **outVec, int32_t numSelectedRows, int32_t selectedRows[],
    ExecutionContext *context, int64_t *dictionaryVectors, DataTypeId &outTypeId) const
{
    *outVec = new Vector<LargeStringContainer<std::string_view>>(numSelectedRows);
    this->projector(valueAddrs, vecBatch.GetRowCount(), reinterpret_cast<int64_t>(*outVec),
        selectedRows, numSelectedRows, nullAddrs, offsetAddrs, unsafe::UnsafeBaseVector::GetNulls(*outVec),
        nullptr, reinterpret_cast<int64_t>(context), dictionaryVectors);
}

void Projection::ProjectHelperFixedWidth(VectorBatch &vecBatch, int64_t *valueAddrs, int64_t *nullAddrs,
    int64_t *offsetAddrs, BaseVector **outVec, int32_t numSelectedRows, int32_t selectedRows[],
    ExecutionContext *context, int64_t *dictionaryVectors, DataTypeId &outTypeId) const
{
    int64_t outValueAddr;
    switch (outTypeId) {
        case type::OMNI_INT:
        case type::OMNI_DATE32:
            *outVec = new Vector<int32_t>(numSelectedRows);
            outValueAddr = reinterpret_cast<int64_t>(
                unsafe::UnsafeVector::GetRawValues(reinterpret_cast<Vector<int32_t> *>(*outVec)));
            break;
        case type::OMNI_SHORT:
            *outVec = new Vector<int16_t>(numSelectedRows);
            outValueAddr = reinterpret_cast<int64_t>(
                unsafe::UnsafeVector::GetRawValues(reinterpret_cast<Vector<int16_t> *>(*outVec)));
            break;
        case type::OMNI_LONG:
        case type::OMNI_DECIMAL64:
            *outVec = new Vector<int64_t>(numSelectedRows);
            outValueAddr = reinterpret_cast<int64_t>(
                unsafe::UnsafeVector::GetRawValues(reinterpret_cast<Vector<int64_t> *>(*outVec)));
            break;
        case type::OMNI_DOUBLE:
            *outVec = new Vector<double>(numSelectedRows);
            outValueAddr = reinterpret_cast<int64_t>(
                unsafe::UnsafeVector::GetRawValues(reinterpret_cast<Vector<double> *>(*outVec)));
            break;
        case type::OMNI_BOOLEAN:
            *outVec = new Vector<bool>(numSelectedRows);
            outValueAddr = reinterpret_cast<int64_t>(
                unsafe::UnsafeVector::GetRawValues(reinterpret_cast<Vector<bool> *>(*outVec)));
            break;
        case type::OMNI_DECIMAL128:
            *outVec = new Vector<Decimal128>(numSelectedRows);
            outValueAddr = reinterpret_cast<int64_t>(
                unsafe::UnsafeVector::GetRawValues(reinterpret_cast<Vector<type::Decimal128> *>(*outVec)));
            break;
        default:
            LogError("Do not support such vector type %d", outTypeId);
            outValueAddr = 0;
    }

    this->projector(valueAddrs, vecBatch.GetRowCount(), outValueAddr, selectedRows,
        numSelectedRows, nullAddrs, offsetAddrs, unsafe::UnsafeBaseVector::GetNulls(*outVec), nullptr,
        reinterpret_cast<int64_t>(context), dictionaryVectors);
}

BaseVector *Projection::Project(VectorBatch *vecBatch, int64_t *valueAddrs, int64_t *nullAddrs,
    int64_t *offsetAddrs, ExecutionContext *context, int64_t *dictionaryVectors, const int32_t *typeIds) const
{
    return this->Project(vecBatch, nullptr, vecBatch->GetRowCount(), valueAddrs, nullAddrs,
        offsetAddrs, context, dictionaryVectors, typeIds);
}

BaseVector *Projection::ColumnProjectionProxy(VectorBatch *vecBatch, int32_t selectedRows[],
    int32_t numSelectedRows, const int32_t *typeIds) const
{
    switch (typeIds[columnProjectionIndex]) {
        case OMNI_INT:
        case OMNI_DATE32:
            return ColumnProjectionHelper<int32_t>(vecBatch, selectedRows, numSelectedRows);
        case OMNI_SHORT:
            return ColumnProjectionHelper<int16_t>(vecBatch, selectedRows, numSelectedRows);
        case OMNI_LONG:
        case OMNI_DECIMAL64:
            return ColumnProjectionHelper<int64_t>(vecBatch, selectedRows, numSelectedRows);
        case OMNI_DOUBLE:
            return ColumnProjectionHelper<double>(vecBatch, selectedRows, numSelectedRows);
        case OMNI_BOOLEAN:
            return ColumnProjectionHelper<bool>(vecBatch, selectedRows, numSelectedRows);
        case OMNI_DECIMAL128:
            return ColumnProjectionHelper<Decimal128>(vecBatch, selectedRows, numSelectedRows);
        case OMNI_VARCHAR:
        case OMNI_CHAR:
            return ColumnProjectionVarCharVectorHelper<std::string_view>(vecBatch, selectedRows, numSelectedRows);
        default:
            LogError("Do not support such vector type %d", typeIds[columnProjectionIndex]);
            return nullptr;
    }
}

template <typename T>
BaseVector *Projection::ColumnProjectionHelper(VectorBatch *vecBatch, const int32_t *selectedRows,
    int32_t numSelectedRows) const
{
    auto colVec = vecBatch->Get(this->columnProjectionIndex);
    auto rowCnt = vecBatch->GetRowCount();
    if (numSelectedRows != 0 && numSelectedRows == rowCnt) {
        if (colVec->GetEncoding() == OMNI_DICTIONARY) {
            return ColumnProjectionDictionaryVectorSliceHelper<T>(numSelectedRows, colVec);
        } else {
            return ColumnProjectionFlatVectorSliceHelper<T>(numSelectedRows, colVec);
        }
    }

    if (selectedRows != nullptr && numSelectedRows != 0) {
        if (colVec->GetEncoding() == OMNI_DICTIONARY) {
            return ColumnProjectionDictionaryVectorCopyPositionsHelper<T>(selectedRows, numSelectedRows, colVec);
        } else {
            return ColumnProjectionFlatVectorCopyPositionsHelper<T>(selectedRows, numSelectedRows, colVec);
        }
    }
    return nullptr;
}

template <typename T>
BaseVector *Projection::ColumnProjectionVarCharVectorHelper(VectorBatch *vecBatch,
    const int32_t *selectedRows, int32_t numSelectedRows) const
{
    auto colVec = vecBatch->Get(this->columnProjectionIndex);
    auto rowCnt = vecBatch->GetRowCount();
    if (numSelectedRows != 0 && numSelectedRows == rowCnt) {
        if (colVec->GetEncoding() == OMNI_DICTIONARY) {
            return ColumnProjectionDictionaryVectorSliceHelper<T>(numSelectedRows, colVec);
        } else {
            return ColumnProjectionFlatVectorSliceHelper<LargeStringContainer<std::string_view>>(numSelectedRows,
                colVec);
        }
    }

    if (selectedRows != nullptr && numSelectedRows != 0) {
        if (colVec->GetEncoding() == OMNI_DICTIONARY) {
            return ColumnProjectionDictionaryVectorCopyPositionsHelper<T>(selectedRows, numSelectedRows, colVec);
        } else {
            return ColumnProjectionFlatVectorCopyPositionsHelper<LargeStringContainer<std::string_view>>(selectedRows,
                numSelectedRows, colVec);
        }
    }
    return nullptr;
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

    for (int i = 0; isSupportedExpr && i < projectVecCount; ++i) {
        outputTypes.emplace_back(projExprs[i]->GetReturnType());
    }
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

    for (int i = 0; isSupportedExpr && i < projectVecCount; ++i) {
        outputTypes.emplace_back(projExprs[i]->GetReturnType());
    }
}

bool ExpressionEvaluator::IsSupportedExpr() const
{
    return isSupportedExpr;
}

VectorBatch *ExpressionEvaluator::Evaluate(VectorBatch *vecBatch, ExecutionContext *context)
{
    const int vectorCount = vecBatch->GetVectorCount();
    intptr_t valueAddrs[vectorCount];
    intptr_t nullAddrs[vectorCount];
    intptr_t offsetAddrs[vectorCount];
    intptr_t dictionaries[vectorCount];
    GetAddr(*vecBatch, valueAddrs, nullAddrs, offsetAddrs, dictionaries, this->inputTypes);
    if (hasFilter) {
        return ProcessFilterAndProject(vecBatch, context, valueAddrs, nullAddrs, offsetAddrs, dictionaries);
    } else {
        return ProcessProject(vecBatch, context, valueAddrs, nullAddrs, offsetAddrs, dictionaries);
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

VectorBatch *ExpressionEvaluator::ProcessProject(VectorBatch *vecBatch, ExecutionContext *context, intptr_t *valueAddrs,
    intptr_t *nullAddrs, intptr_t *offsetAddrs, intptr_t *dictionaries)
{
    auto rowCount = vecBatch->GetRowCount();
    auto projectedVecs = new VectorBatch(rowCount);
    for (int32_t i = 0; i < projectVecCount; i++) {
        BaseVector *outCol = projections[i]->Project(vecBatch, valueAddrs, nullAddrs, offsetAddrs,
            context, dictionaries, GetInputDataTypes().GetIds());
        if (context->HasError()) {
            VectorHelper::FreeVecBatch(projectedVecs);
            VectorHelper::FreeVecBatch(vecBatch);
            context->GetArena()->Reset();
            std::string errorMessage = context->GetError();
            throw OmniException("OPERATOR_RUNTIME_ERROR", errorMessage);
        }
        projectedVecs->Append(outCol);
    }
    VectorHelper::FreeVecBatch(vecBatch);
    context->GetArena()->Reset();
    return projectedVecs;
}

VectorBatch *ExpressionEvaluator::ProcessFilterAndProject(VectorBatch *vecBatch, ExecutionContext *context,
    intptr_t *valueAddrs, intptr_t *nullAddrs, intptr_t *offsetAddrs, intptr_t *dictionaries)
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

    auto projectedVecs = new VectorBatch(numSelectedRows);
    for (int32_t i = 0; i < projectVecCount; i++) {
        BaseVector *col = projections[i]->Project(vecBatch, selectedRows, numSelectedRows, valueAddrs,
            nullAddrs, offsetAddrs, context, dictionaries, GetInputDataTypes().GetIds());
        if (context->HasError()) {
            VectorHelper::FreeVecBatch(projectedVecs);
            delete[] selectedRows;
            VectorHelper::FreeVecBatch(vecBatch);
            context->GetArena()->Reset();

            std::string errorMessage = context->GetError();
            throw OmniException("OPERATOR_RUNTIME_ERROR", errorMessage);
        }
        projectedVecs->Append(col);
    }

    delete[] selectedRows;
    VectorHelper::FreeVecBatch(vecBatch);
    context->GetArena()->Reset();
    return projectedVecs;
}
}