/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Projection operator source file
 */
#include "projection.h"
#include "vector/vector_helper.h"
#include "expression/jsonparser/jsonparser.h"
#include "util/config_util.h"

using namespace std;
using namespace omniruntime::vec;
using namespace omniruntime::expressions;

namespace omniruntime {
namespace op {
RowProjection::RowProjection(const Expr &expression) : codegen(nullptr), expression(&expression) {}

RowProjection::~RowProjection()
{
    this->codegen.reset();
}

// Return nullptr if expression is unsupported
RowProjFunc RowProjection::Create(OverflowConfig *overflowConfig)
{
    if (this->expression == nullptr) {
        return nullptr;
    }
#ifdef DEBUG
    std::cout << "RowProjection: " << std::endl;
    ExprPrinter p;
    this->expression->Accept(p);
    std::cout << std::endl;
#endif
    this->codegen = ProjectionCodeGen::Create("single_row_project", *this->expression, false, overflowConfig);
    int64_t fPtr = this->codegen->GetExpressionEvaluator();
    if (fPtr == 0) {
        return nullptr;
    }

    void *refFunc = &fPtr;
    auto castedRef = static_cast<RowProjFunc *>(refFunc);
    return *castedRef;
}

// Return INVALIDDATAD if expression is unsupported
DataTypePtr RowProjection::GetReturnType()
{
    if (this->expression == nullptr) {
        return std::make_shared<InvalidDataType>();
    }
    return this->expression->GetReturnType();
}

bool RowProjection::IsColumnProjection()
{
    return this->expression != nullptr && this->expression->GetType() == ExprType::FIELD_E;
}

int RowProjection::GetIndexIfColumnProjection()
{
    if (!IsColumnProjection()) {
        return -1;
    }
    return static_cast<const FieldExpr *>(this->expression)->colVal;
}

bool Projection::Initialize(bool filter, OverflowConfig *overflowConfig)
{
    // short-circuit logic for column projections
    // no need to go through codegen
    if (expr->GetType() == ExprType::FIELD_E) {
        auto fieldExpr = static_cast<const FieldExpr *>(expr);
        this->isColumnProjection = true;
        this->columnProjectionIndex = fieldExpr->colVal;
        return true;
    }
    int64_t f;
    if (!ConfigUtil::IsEnableBatchExprEvaluate()) {
        this->codegen = ProjectionCodeGen::Create("proj_func", *(this->expr), filter, overflowConfig);
        f = this->codegen->GetFunction();
    } else {
        this->batchCodegen = BatchProjectionCodeGen::Create("proj_func", *(this->expr), filter, overflowConfig);
        f = this->batchCodegen->GetFunction();
    }

    if (f == 0) {
        return false;
    }

    void *function = &f;
    auto cfunction = static_cast<ProjFunc *>(function);
    this->projector = *cfunction;
    return true;
}

bool Projection::IsSupported()
{
    return this->isSupported;
}

Projection::Projection(const expressions::Expr &expr, bool filter, DataTypePtr outType, OverflowConfig *overflowConfig)
    : expr(&expr), outType(std::move(outType)), projector(nullptr)
{
#ifdef DEBUG
    std::cout << "Expression in projection:" << std::endl;
    ExprPrinter printExprTree;
    expr.Accept(printExprTree);
    std::cout << std::endl;
#endif
    bool initialized = this->Initialize(filter, overflowConfig);
    if (!initialized) {
        this->isSupported = false;
    }
}

// Helper function to return data, null bitmap, offsets in vecBatch
void GetProjData(VectorBatch &vecBatch, int64_t valueAddrs[], int64_t nullAddrs[], int64_t offsetAddrs[],
    int64_t dictionaries[])
{
    int64_t valuesAddress;
    int64_t dictVecAddress;
    int32_t vectorCount = vecBatch.GetVectorCount();
    for (int32_t i = 0; i < vectorCount; i++) {
        Vector *colVec = vecBatch.GetVector(i);
        if (colVec->GetEncoding() == OMNI_VEC_ENCODING_LAZY) {
            colVec = static_cast<LazyVector *>(colVec)->GetLoadedVector();
        }
        dictVecAddress = 0;
        valuesAddress = 0;
        if (colVec->GetEncoding() == OMNI_VEC_ENCODING_DICTIONARY) {
            dictVecAddress = reinterpret_cast<int64_t>(reinterpret_cast<void *>(colVec));
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

Vector *Projection::Project(VectorAllocator *vecAllocator, VectorBatch *vecBatch, int32_t selectedRows[],
    int32_t numSelectedRows, int64_t *valueAddrs, int64_t *nullAddrs, int64_t *offsetAddrs, ExecutionContext *context,
    int64_t *dictionaryVectors) const
{
    // short-circuit logic for column projections
    if (this->isColumnProjection) {
        // if no row gets filtered or without a filter
        // we can just slice the whole vector
        Vector *colVec = vecBatch->GetVector(this->columnProjectionIndex);
        if (colVec->GetEncoding() == OMNI_VEC_ENCODING_LAZY) {
            colVec = static_cast<LazyVector *>(colVec)->GetLoadedVector();
        }
        if (numSelectedRows != 0 && numSelectedRows == vecBatch->GetRowCount()) {
            return colVec->Slice(0, numSelectedRows);
        }
        // if some rows get filtered,
        // we can just copy the original vector
        if (selectedRows != nullptr && numSelectedRows != 0) {
            if (colVec->GetEncoding() == OMNI_VEC_ENCODING_DICTIONARY) {
                return static_cast<DictionaryVector *>(colVec)->ExtractDictionary(selectedRows, numSelectedRows);
            } else {
                return colVec->CopyPositions(selectedRows, 0, numSelectedRows);
            }
        }
    }

    DataTypeId outTypeId = this->GetOutputType().GetId();
    Vector *outVec = nullptr;
    int32_t avgStringLength = 200;
    switch (outTypeId) {
        case type::OMNI_DATE32:
        case type::OMNI_INT:
            outVec = new IntVector(vecAllocator, numSelectedRows);
            break;
        case type::OMNI_LONG:
        case type::OMNI_DECIMAL64:
            outVec = new LongVector(vecAllocator, numSelectedRows);
            break;
        case type::OMNI_DOUBLE:
            outVec = new DoubleVector(vecAllocator, numSelectedRows);
            break;
        case type::OMNI_CHAR:
            outVec = new VarcharVector(vecAllocator,
                numSelectedRows * static_cast<CharDataType &>(this->GetOutputType()).GetWidth(), numSelectedRows);
            break;
        case type::OMNI_VARCHAR:
            outVec = new VarcharVector(vecAllocator, numSelectedRows * avgStringLength, numSelectedRows);
            break;
        case type::OMNI_DECIMAL128:
            outVec = new Decimal128Vector(vecAllocator, numSelectedRows);
            break;
        case type::OMNI_BOOLEAN:
            outVec = new BooleanVector(vecAllocator, numSelectedRows);
            break;
        default: {
            LogError("No such data type %d", outTypeId);
            return outVec;
        }
    }

    Vector *projectedVec = nullptr;
    if (outTypeId == OMNI_VARCHAR || outTypeId == OMNI_CHAR) {
        projectedVec = ProjectHelperVarWidth(*vecBatch, valueAddrs, nullAddrs, offsetAddrs, outVec, numSelectedRows,
            selectedRows, context, dictionaryVectors);
    } else {
        projectedVec = ProjectHelperFixedWidth(*vecBatch, valueAddrs, nullAddrs, offsetAddrs, outVec, numSelectedRows,
            selectedRows, context, dictionaryVectors);
    }
    context->GetArena()->Reset();
    // fixme::true means projected vec has null in it, when project supports whether the vec of the output flag is null
    // or not, it is obtained directly from the project flag
    projectedVec->SetNullFlag(true);
    return projectedVec;
}

omniruntime::vec::Vector *Projection::ProjectHelperVarWidth(omniruntime::vec::VectorBatch &vecBatch,
    int64_t *valueAddrs, int64_t *nullAddrs, int64_t *offsetAddrs, omniruntime::vec::Vector *outVec,
    int32_t numSelectedRows, int32_t selectedRows[], ExecutionContext *context, int64_t *dictionaryVectors) const
{
    // using projector
    ((int32_t *)outVec->GetValueOffsets())[0] = 0;
    this->projector(valueAddrs, vecBatch.GetRowCount(), reinterpret_cast<int64_t>(outVec), selectedRows,
        numSelectedRows, nullAddrs, offsetAddrs, reinterpret_cast<bool *>(outVec->GetValueNulls()),
        reinterpret_cast<int32_t *>(outVec->GetValueOffsets()), reinterpret_cast<int64_t>(context), dictionaryVectors);
    return outVec;
}

omniruntime::vec::Vector *Projection::ProjectHelperFixedWidth(omniruntime::vec::VectorBatch &vecBatch,
    int64_t *valueAddrs, int64_t *nullAddrs, int64_t *offsetAddrs, omniruntime::vec::Vector *outVec,
    int32_t numSelectedRows, int32_t selectedRows[], ExecutionContext *context, int64_t *dictionaryVectors) const
{
    this->projector(valueAddrs, vecBatch.GetRowCount(), reinterpret_cast<int64_t>(outVec->GetValues()), selectedRows,
        numSelectedRows, nullAddrs, offsetAddrs, reinterpret_cast<bool *>(outVec->GetValueNulls()),
        reinterpret_cast<int32_t *>(outVec->GetValueOffsets()), reinterpret_cast<int64_t>(context), dictionaryVectors);
    return outVec;
}

Vector *Projection::Project(VectorAllocator *vecAllocator, VectorBatch *vecBatch, int64_t *valueAddrs,
    int64_t *nullAddrs, int64_t *offsetAddrs, ExecutionContext *context, int64_t *dictionaryVectors) const
{
    return this->Project(vecAllocator, vecBatch, nullptr, vecBatch->GetRowCount(), valueAddrs, nullAddrs, offsetAddrs,
        context, dictionaryVectors);
}

int32_t ProjectionOperator::AddInput(VectorBatch *vecBatch)
{
    const int vectorCount = vecBatch->GetVectorCount();
    int64_t valueAddrs[vectorCount];
    int64_t nullAddrs[vectorCount];
    int64_t offsetAddrs[vectorCount];
    int64_t dictionaries[vectorCount];
    auto resultRowCount = vecBatch->GetRowCount();

    // contents of bitmap are modified in getProjData method
    GetProjData(*vecBatch, valueAddrs, nullAddrs, offsetAddrs, dictionaries);

    auto nProj = static_cast<int32_t>(proj.size());
    auto outBatch = new VectorBatch(nProj, resultRowCount);
    for (int32_t i = 0; i < nProj; i++) {
        Vector *outCol =
            proj[i]->Project(vecAllocator, vecBatch, valueAddrs, nullAddrs, offsetAddrs, context, dictionaries);
        if (context->HasError()) {
            delete outCol;
            for (int32_t j = 0; j < i; j++) {
                delete outBatch->GetVector(j);
            }
            delete outBatch;
            VectorHelper::FreeVecBatch(vecBatch);
            context->GetArena()->Reset();
            string errorMessage = context->GetError();
            throw OmniException("OPERATOR_RUNTIME_ERROR", errorMessage);
        }
        outBatch->SetVector(i, outCol);
    }
    this->mutated = outBatch;
    VectorHelper::FreeVecBatch(vecBatch);
    context->GetArena()->Reset();
    return resultRowCount;
}

int32_t ProjectionOperator::GetOutput(std::vector<VectorBatch *> &data)
{
    if (this->mutated == nullptr) {
        return -1;
    }
    int rowCount = this->mutated->GetRowCount();
    data.push_back(this->mutated);
    this->mutated = nullptr;
    return rowCount;
}

OmniStatus ProjectionOperator::Close()
{
    if (mutated != nullptr) {
        VectorHelper::FreeVecBatch(mutated);
        mutated = nullptr;
    }
    return OMNI_STATUS_NORMAL;
}

ProjectionOperatorFactory::ProjectionOperatorFactory(const std::vector<omniruntime::expressions::Expr *> &exprs,
    int32_t nProj, const DataTypes &inputTypes, int32_t nCols, OverflowConfig *overflowConfig)
    : inputTypes(inputTypes), nCols(nCols), nProj(nProj)
{
    for (auto expr : exprs) {
        auto projection = std::make_unique<Projection>(*expr, false, expr->GetReturnType(), overflowConfig);
        if (!projection->IsSupported()) {
            this->isSupported = false;
            break;
        }
        this->proj.push_back(move(projection));
    }
}

ProjectionOperatorFactory::~ProjectionOperatorFactory()
{
    for (auto &projection : proj) {
        projection.reset();
    }
    proj.clear();
}

omniruntime::op::Operator *ProjectionOperatorFactory::CreateOperator()
{
    return new ProjectionOperator(proj, new ExecutionContext());
}

bool ProjectionOperatorFactory::IsSupported()
{
    return this->isSupported;
}
}
}