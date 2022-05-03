/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Projection operator source file
 */
#include "projection.h"
#include "vector/vector_helper.h"
#include "expression/jsonparser/jsonparser.h"

using namespace std;
using namespace omniruntime::vec;
using namespace omniruntime::expressions;

namespace omniruntime {
namespace op {
RowProjection::RowProjection(const Expr &expression) : codegen(nullptr), expression(&expression) {}

RowProjection::~RowProjection()
{
    delete this->expression;
    this->codegen.reset();
}

// Return nullptr if expression is unsupported
RowProjFunc RowProjection::Create()
{
    if (this->expression == nullptr) {
        return nullptr;
    }
    this->codegen = ProjectionCodeGen::Create("single_row_project", *this->expression, false);
    int64_t fPtr = this->codegen->GetExpressionEvaluator();
    if (fPtr == 0) {
        return nullptr;
    }

    void *refFunc = &fPtr;
    auto castedRef = static_cast<RowProjFunc *>(refFunc);
    return *castedRef;
}

// Return INVALIDDATAD if expression is unsupported
DataType RowProjection::GetReturnType()
{
    if (this->expression == nullptr) {
        return DataType(OMNI_INVALID);
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

bool Projection::Initialize(bool filter)
{
    // short-circuit logic for column projections
    // no need to go through codegen
    if (expr->GetType() == ExprType::FIELD_E) {
        auto fieldExpr = static_cast<const FieldExpr *>(expr);
        this->isColumnProjection = true;
        this->columnProjectionIndex = fieldExpr->colVal;
        return true;
    }

    this->codegen = ProjectionCodeGen::Create("proj_func", *(this->expr), filter);
    auto f = this->codegen->GetFunction();
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

Projection::Projection(DataTypes &inputTypes, int32_t nCols, const Expr &expr, bool filter)
    : inputTypes(inputTypes), nCols(nCols), expr(&expr)
{
    this->inputTypeIds = const_cast<int32_t *>(this->inputTypes.GetIds());

    bool initialized = this->Initialize(filter);
    if (!initialized) {
        this->isSupported = false;
    }
#ifdef DEBUG
    if (initialized) {
        std::cout << "Expression in projection:" << std::endl;
        ExprPrinter printExprTree;
        expr.Accept(printExprTree);
        std::cout << std::endl;
    }
#endif
}

// Helper function to return data, null bitmap, offsets in vecBatch
std::vector<int64_t> GetProjData(VectorBatch &vecBatch, int64_t bitmap[], int64_t offsetsAddrs[],
    std::vector<Vector *> &dictionaryVecs, int vectorCount, int64_t dictionaries[])
{
    std::vector<int64_t> data;
    int64_t valuesAddress;
    int64_t dictVecAddress;

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
        data.push_back(valuesAddress);

        // bitmap handling
        bitmap[i] = VectorHelper::GetNullsAddr(colVec);

        // offsets handling
        offsetsAddrs[i] = VectorHelper::GetOffsetsAddr(colVec);
    }

    return data;
}

Vector *Projection::Project(VectorAllocator *vecAllocator, VectorBatch *vecBatch, int32_t selectedRows[],
    int32_t numSelectedRows, vector<int64_t> const & vecData, int64_t *bitmap, int64_t *offsets,
    ExecutionContext *context, int64_t *dictionaryVectors) const
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

    DataTypeId outTypeId = expr->GetReturnTypeId();
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
        case type::OMNI_VARCHAR:
        case type::OMNI_CHAR:
            // Must set capacity appropriately (to do)
            // capacity = numSelectedRows * 50 cannot handle vectors with average string length over 50
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
            break;
        }
    }

    Vector *projectedVec = nullptr;
    if (outTypeId == OMNI_VARCHAR || outTypeId == OMNI_CHAR) {
        projectedVec = ProjectHelperVarWidth(*vecBatch, vecData, bitmap, offsets, outVec, numSelectedRows, selectedRows,
            context, dictionaryVectors);
    } else {
        projectedVec = ProjectHelperFixedWidth(*vecBatch, vecData, bitmap, offsets, outVec, numSelectedRows,
            selectedRows, context, dictionaryVectors);
    }
    context->GetArena()->Reset();
    return projectedVec;
}

omniruntime::vec::Vector *Projection::ProjectHelperVarWidth(omniruntime::vec::VectorBatch &vecBatch,
    std::vector<int64_t> const & vecData, int64_t *bitmap, int64_t *offsets, omniruntime::vec::Vector *outVec,
    int32_t numSelectedRows, int32_t selectedRows[], ExecutionContext *context, int64_t *dictionaryVectors) const
{
    // using projector
    ((int32_t *)outVec->GetValueOffsets())[0] = 0;
    this->projector(vecData.data(), vecBatch.GetRowCount(), reinterpret_cast<int64_t>(outVec), selectedRows,
        numSelectedRows, bitmap, offsets, reinterpret_cast<bool *>(outVec->GetValueNulls()),
        reinterpret_cast<int32_t *>(outVec->GetValueOffsets()), reinterpret_cast<int64_t>(context), dictionaryVectors);
    return outVec;
}

omniruntime::vec::Vector *Projection::ProjectHelperFixedWidth(omniruntime::vec::VectorBatch &vecBatch,
    std::vector<int64_t> const & vecData, int64_t *bitmap, int64_t *offsets, omniruntime::vec::Vector *outVec,
    int32_t numSelectedRows, int32_t selectedRows[], ExecutionContext *context, int64_t *dictionaryVectors) const
{
    this->projector(vecData.data(), vecBatch.GetRowCount(), reinterpret_cast<int64_t>(outVec->GetValues()),
        selectedRows, numSelectedRows, bitmap, offsets, reinterpret_cast<bool *>(outVec->GetValueNulls()),
        reinterpret_cast<int32_t *>(outVec->GetValueOffsets()), reinterpret_cast<int64_t>(context), dictionaryVectors);
    return outVec;
}

Vector *Projection::Project(VectorAllocator *vecAllocator, VectorBatch *vecBatch, vector<int64_t> const & vecData,
    int64_t *bitmap, int64_t *offsets, ExecutionContext *context, int64_t *dictionaryVectors) const
{
    return this->Project(vecAllocator, vecBatch, nullptr, vecBatch->GetRowCount(), vecData, bitmap, offsets, context,
        dictionaryVectors);
}

int32_t ProjectionOperator::AddInput(VectorBatch *vecBatch)
{
    const int vectorCount = vecBatch->GetVectorCount();
    int64_t offsets[vectorCount];
    int64_t bitmap[vectorCount];
    int64_t dictionaries[vectorCount];
    auto resultRowCount = vecBatch->GetRowCount();

    // when the dictionary vector is processed it will be restored to an original vector
    // needs to be released
    vector<Vector *> dictionaryVecs;

    // contents of bitmap are modified in getProjData method
    std::vector<int64_t> vecData = GetProjData(*vecBatch, bitmap, offsets, dictionaryVecs, vectorCount, dictionaries);

    auto outBatch = new VectorBatch(nProj, resultRowCount);
    for (int32_t i = 0; i < nProj; i++) {
        Vector *outCol = proj[i]->Project(vecAllocator, vecBatch, vecData, bitmap, offsets, context, dictionaries);
        outBatch->SetVector(i, outCol);
    }
    this->mutated = outBatch;
    vecData.clear();
    for (auto &dictionaryVec : dictionaryVecs) {
        delete dictionaryVec;
    }
    VectorHelper::FreeVecBatch(vecBatch);
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
ProjectionOperatorFactory::ProjectionOperatorFactory(const std::vector<Expr *> &exprs, int32_t nProj,
    DataTypes &inputTypes, int32_t nCols)
    : inputTypes(inputTypes), nCols(nCols), nProj(nProj)
{
    this->inputTypeIds = const_cast<int32_t *>(this->inputTypes.GetIds());
    this->SetJitContext(nullptr);
    for (int32_t i = 0; i < nProj; i++) {
        auto projection = std::make_unique<Projection>(inputTypes, nCols, *(exprs.at(i)), false);
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
    return new ProjectionOperator(proj, inputTypeIds, nCols, nProj, new ExecutionContext());
}

bool ProjectionOperatorFactory::IsSupported()
{
    return this->isSupported;
}
}
}