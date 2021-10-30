/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Projection operator source file
 */
#include "projection.h"
#include "../../vector/vector_helper.h"

using namespace std;
using namespace omniruntime::op;
using namespace omniruntime::vec;
using namespace omniruntime::expressions;

using Uint8vec = std::vector<uint8_t>;
namespace omniruntime {
namespace op {
RowProjection::RowProjection(std::string &expression, std::vector<DataType> &inputTypes)
    : codegen(nullptr), expression(nullptr)
{
    Parser parser;
    this->expression =
        parser.ParseRowExpression(expression, reinterpret_cast<int32_t *>(inputTypes.data()), inputTypes.size());
}

RowProjection::~RowProjection()
{
    delete this->expression;
    this->codegen.reset();
}

// Return nullptr if expression is unsupported
RowProjFunc RowProjection::Create(std::vector<DataType> &inputTypes)
{
    if (this->expression == nullptr) {
        return nullptr;
    }
    this->codegen = std::make_unique<ProjectionCodeGen>("single_row_project", *this->expression, inputTypes, false);
    int64_t fPtr = this->codegen->GetExpressionEvaluator();
    void *refFunc = &fPtr;
    auto castedRef = static_cast<RowProjFunc *>(refFunc);
    return *castedRef;
}

// Return INVALIDDATAD if expression is unsupported
DataType RowProjection::GetReturnType()
{
    if (this->expression == nullptr) {
        return INVALIDDATAD;
    }
    return this->expression->GetExprDataType();
}

bool RowProjection::IsColumnProjection()
{
    return this->expression != nullptr && this->expression->GetType() == ExprType::DATA_E &&
        static_cast<DataExpr *>(this->expression)->isColumn;
}

int RowProjection::GetIndexIfColumnProjection()
{
    if (!IsColumnProjection()) {
        return -1;
    }
    return static_cast<DataExpr *>(this->expression)->colVal;
}
}
}

void GetColumnsInExpr(Expr *expr, set<int32_t>& columnsInExpression, bool isRoot)
{
    switch (expr->GetType()) {
        case ExprType::DATA_E: {
            auto dataExpr = static_cast<DataExpr *>(expr);
            if (dataExpr->isColumn && !isRoot) {
                columnsInExpression.insert(dataExpr->colVal);
            }
            break;
        }
        case ExprType::BINARY_E: {
            auto binaryExpr = static_cast<BinaryExpr *>(expr);
            GetColumnsInExpr(binaryExpr->left, columnsInExpression, false);
            GetColumnsInExpr(binaryExpr->right, columnsInExpression, false);
            break;
        }
        case UNARY_E: {
            auto unaryExpr = static_cast<UnaryExpr *>(expr);
            GetColumnsInExpr(unaryExpr->exp, columnsInExpression, false);
            break;
        }
        case IN_E: {
            auto inExpr = static_cast<InExpr *>(expr);
            for (auto arg : inExpr->arguments) {
                GetColumnsInExpr(arg, columnsInExpression, false);
            }
            break;
        }
        case BETWEEN_E: {
            auto betweenExpr = static_cast<BetweenExpr *>(expr);
            GetColumnsInExpr(betweenExpr->value, columnsInExpression, false);
            GetColumnsInExpr(betweenExpr->lowerBound, columnsInExpression, false);
            GetColumnsInExpr(betweenExpr->upperBound, columnsInExpression, false);
            break;
        }
        case IF_E: {
            auto ifExpr = static_cast<IfExpr *>(expr);
            GetColumnsInExpr(ifExpr->condition, columnsInExpression, false);
            GetColumnsInExpr(ifExpr->trueExpr, columnsInExpression, false);
            GetColumnsInExpr(ifExpr->falseExpr, columnsInExpression, false);
            break;
        }
        case COALESCE_E: {
            auto coalesceExpr = static_cast<CoalesceExpr *>(expr);
            GetColumnsInExpr(coalesceExpr->value1, columnsInExpression, false);
            GetColumnsInExpr(coalesceExpr->value2, columnsInExpression, false);
            break;
        }
        case IS_NULL_E: {
            auto isNullExpr = static_cast<IsNullExpr *>(expr);
            GetColumnsInExpr(isNullExpr->value, columnsInExpression, false);
            break;
        }
        case FUNC_E: {
            auto funcExpr = static_cast<FuncExpr *>(expr);
            for (auto arg : funcExpr->arguments) {
                GetColumnsInExpr(arg, columnsInExpression, false);
            }
            break;
        }
        case INVALID_E:
        default:
            break;
    }
}

bool Projection::Initialize(bool filter)
{
    std::vector<DataType> dataTypes;
    dataTypes.reserve(nCols);
    for (int32_t i = 0; i < nCols; i++) {
        dataTypes.push_back(expressions::ColTypeTrans(inputTypes[i]));
    }

    // short-circuit logic for column projections
    // no need to go through codegen
    if (expr->GetType() == DATA_E) {
        auto dataExpr = static_cast<DataExpr *>(expr);
        if (dataExpr->isColumn) {
            this->isColumnProjection = true;
            this->columnProjectionIndex = dataExpr->colVal;
            return true;
        }
    }

    this->codegen = std::make_unique<ProjectionCodeGen>("proj_func", *(this->expr), dataTypes, filter);

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

Projection::Projection(int32_t inputTypes[], int32_t nCols, const std::string &expr, bool filter)
    : inputTypes(inputTypes), nCols(nCols)
{
    Parser parser;
    this->expr = parser.ParseRowExpression(expr, inputTypes, nCols);
    if (this->expr == nullptr) {
        this->isSupported = false;
    }

    bool initialized = this->Initialize(filter);
    if (!initialized) {
        this->isSupported = false;
    }
}

Projection::Projection(int32_t inputTypes[], int32_t nCols, Expr &expr, bool filter)
    : inputTypes(inputTypes), nCols(nCols), expr(&expr)
{
    bool initialized = this->Initialize(filter);
    if (!initialized) {
        this->isSupported = false;
    }
}

void GetProjDecimal128Data(Vector *col, std::vector<int64_t> &data, uint32_t nRows)
{
    int32_t longs = 2;
    auto *values = static_cast<int64_t *>(col->GetValues());
    // create new vector to store addresses of rows
    unique_ptr<vec64> vcData = make_unique<vec64>();
    int32_t positionOffset = col->GetPositionOffset();

    for (int32_t row = 0; row < nRows; row++) {
        int64_t *index = &((values)[(positionOffset + row) * longs]);
        vcData->push_back(reinterpret_cast<int64_t>(index));
    }
    // data handling
    data.push_back(reinterpret_cast<int64_t>(vcData.release()->data()));
}

// Helper function to return data, null bitmap, offsets in vecBatch
std::vector<int64_t> GetProjData(VectorBatch &vecBatch, int64_t bitmap[], int64_t offsetsAddrs[],
    std::vector<Vector *> &dictionaryVecs, int vectorCount)
{
    std::vector<int64_t> data;

    for (int32_t i = 0; i < vectorCount; i++) {
        Vector *colVec = vecBatch.GetVector(i);
        // handle dictionary vec
        if (colVec->GetTypeId() == OMNI_VEC_TYPE_DICTIONARY) {
            colVec = static_cast<DictionaryVector *>(colVec)->ExtractDictionary();
            dictionaryVecs.push_back(colVec);
        }
        if (colVec->GetTypeId() == OMNI_VEC_TYPE_DECIMAL128) {
            GetProjDecimal128Data(colVec, data, vecBatch.GetRowCount());
        } else {
            // data handling
            data.push_back(reinterpret_cast<int64_t>(colVec->GetValues()));
        }
        // bitmap handling
        bitmap[i] = reinterpret_cast<int64_t>(colVec->GetValueNulls());

        // offsets handling
        offsetsAddrs[i] = reinterpret_cast<int64_t>(colVec->GetValueOffsets());
    }

    return data;
}

Vector *Projection::Project(VectorAllocator *vecAllocator, VectorBatch *vecBatch, int32_t selectedRows[],
    int32_t numSelectedRows, vector<int64_t> const &vecData, int64_t *bitmap, int64_t *offsets,
    ExecutionContext *context) const
{
    // short-circuit logic for column projections
    if (this->isColumnProjection) {
        // if no row gets filtered or without a filter
        // we can just slice the whole vector
        Vector *colVec = vecBatch->GetVector(this->columnProjectionIndex);
        if (numSelectedRows != 0 && numSelectedRows == vecBatch->GetRowCount()) {
            return colVec->Slice(0, numSelectedRows);
        }
        // if some rows get filtered,
        // we can just copy the original vector
        if (selectedRows != nullptr && numSelectedRows != 0) {
            if (colVec->GetTypeId() == OMNI_VEC_TYPE_DICTIONARY) {
                return static_cast<DictionaryVector *>(colVec)->ExtractDictionary(selectedRows, numSelectedRows);
            } else {
                return colVec->CopyPositions(selectedRows, 0, numSelectedRows);
            }
        }
    }

    DataType outType = expr->GetExprDataType();
    std::unique_ptr<Vector> outVec;
    int32_t avgStringLength = 200;
    switch (outType) {
        case INT32D:
            outVec = std::make_unique<IntVector>(vecAllocator, numSelectedRows);
            break;
        case INT64D:
            outVec = std::make_unique<LongVector>(vecAllocator, numSelectedRows);
            break;
        case DOUBLED:
            outVec = std::make_unique<DoubleVector>(vecAllocator, numSelectedRows);
            break;
        case STRINGD:
            // Must set capacity appropriately (to do)
            // capacity = numSelectedRows * 50 cannot handle vectors with average string length over 50
            outVec = std::make_unique<VarcharVector>(vecAllocator, numSelectedRows * avgStringLength, numSelectedRows);
            break;
        case DECIMAL128D:
            outVec = std::make_unique<Decimal128Vector>(vecAllocator, numSelectedRows);
            break;
        default: {
            LogError("No such data type %d", outType);
            break;
        }
    }
    const int vecSize = outVec->GetSize();
    bool newNullValues[vecSize];
    int32_t newLengthValues[vecSize];
    Vector *projectedVec = nullptr;
    if (outType == STRINGD) {
        projectedVec = ProjectHelperVarWidth(
            *vecBatch, vecData, bitmap, offsets, outVec.release(),
            numSelectedRows, selectedRows, newNullValues, newLengthValues, context);
    } else {
        projectedVec = ProjectHelperFixedWidth(
            *vecBatch, vecData, bitmap, offsets, outVec.release(),
            numSelectedRows, selectedRows, newNullValues, newLengthValues, context);
    }
    context->getArena()->Reset();
    return projectedVec;
}

omniruntime::vec::Vector *Projection::ProjectHelperVarWidth(omniruntime::vec::VectorBatch &vecBatch,
    std::vector<int64_t> const &vecData, int64_t *bitmap, int64_t *offsets,
    omniruntime::vec::Vector *outVec, int32_t numSelectedRows, int32_t selectedRows[],
    bool *newNullValues, int32_t *newLengthValues, ExecutionContext *context) const
{
    // using projector
    vector<int64_t> oVec(numSelectedRows);
    auto ov = oVec.data();
    void *vecVals = &ov;
    auto cvecVals = static_cast<int64_t *>(vecVals);
    this->projector(vecData.data(), vecBatch.GetRowCount(),
        *cvecVals, selectedRows, numSelectedRows, bitmap, offsets, newNullValues, newLengthValues,
        reinterpret_cast<int64_t>(context));

    auto *outVarcharVec = static_cast<VarcharVector *>(outVec);
    for (int i = 0; i < numSelectedRows; i++) {
        if (newNullValues[i]) {
            outVarcharVec->SetValueNull(i);
            continue;
        }
        auto charArr = reinterpret_cast<uint8_t *>(ov[i]);

        outVarcharVec->SetValue(i, charArr, newLengthValues[i]);
    }

    return outVec;
}

omniruntime::vec::Vector *Projection::ProjectHelperFixedWidth(omniruntime::vec::VectorBatch &vecBatch,
    std::vector<int64_t> const &vecData, int64_t *bitmap, int64_t *offsets,
    omniruntime::vec::Vector *outVec, int32_t numSelectedRows, int32_t selectedRows[],
    bool *newNullValues, int32_t *newLengthValues, ExecutionContext *context) const
{
    if (outVec->GetTypeId() == OMNI_VEC_TYPE_DECIMAL128) {
        vector<int64_t> oVec(numSelectedRows);
        auto ov = oVec.data();
        void *vecVals = &ov;
        auto cvecVals = static_cast<int64_t *>(vecVals);
        this->projector(vecData.data(), vecBatch.GetRowCount(), *cvecVals,
                        selectedRows, numSelectedRows, bitmap, offsets, newNullValues, newLengthValues,
                        reinterpret_cast<int64_t>(context));
        auto *outDecimal128Vec = static_cast<Decimal128Vector *>(outVec);
        for (int i = 0; i < numSelectedRows; i++) {
            auto *value = reinterpret_cast<int64_t *>(ov[i]);
            outDecimal128Vec->SetValue(i, Decimal128(*(value + 1), *value));
        }
    } else {
        auto ov = outVec->GetValues();
        void *vecVals = &ov;
        auto cvecVals = static_cast<int64_t *>(vecVals);
        int32_t nReturned = this->projector(vecData.data(), vecBatch.GetRowCount(), *cvecVals,
            selectedRows, numSelectedRows, bitmap, offsets, newNullValues, newLengthValues,
            reinterpret_cast<int64_t>(context));
    }

    // set null
    for (int i = 0; i < numSelectedRows; i++) {
        if (newNullValues[i]) {
            outVec->SetValueNull(i);
        }
    }
    return outVec;
}

Vector *Projection::Project(VectorAllocator *vecAllocator, VectorBatch *vecBatch,
    vector<int64_t> const &vecData, int64_t *bitmap, int64_t *offsets, ExecutionContext *context) const
{
    return this->Project(vecAllocator, vecBatch, nullptr, vecBatch->GetRowCount(), vecData, bitmap, offsets, context);
}

int32_t ProjectionOperator::AddInput(VectorBatch *vecBatch)
{
    const int vectorCount = vecBatch->GetVectorCount();
    int64_t offsets[vectorCount];
    int64_t bitmap[vectorCount];

    // when the dictionary vector is processed it will be restored to an original vector
    // needs to be released
    vector<Vector *> dictionaryVecs;

    // contents of bitmap are modified in getProjData method
    std::vector<int64_t> vecData = GetProjData(*vecBatch, bitmap, offsets, dictionaryVecs, vectorCount);

    auto outBatch = std::make_unique<VectorBatch>(nProj);
    for (int32_t i = 0; i < nProj; i++) {
        Vector *outCol = proj[i]->Project(vecAllocator, vecBatch, vecData, bitmap, offsets, context);
        outBatch->SetVector(i, outCol);
    }
    this->mutated = outBatch.release();
    vecData.clear();
    for (auto &dictionaryVec : dictionaryVecs) {
        delete dictionaryVec;
    }

    return vecBatch->GetRowCount();
}

int32_t ProjectionOperator::GetOutput(std::vector<VectorBatch *> &data)
{
    if (this->mutated == nullptr) {
        return -1;
    }
    int rowCount = this->mutated->GetRowCount();
    data.push_back(this->mutated);
    FreeDecimalArrays();
    this->mutated = nullptr;
    return rowCount;
}

ProjectionOperatorFactory::ProjectionOperatorFactory(std::string expressions[], int32_t nProj, int32_t inputTypes[],
    int32_t nCols)
    : inputTypes(inputTypes), nCols(nCols), nProj(nProj)
{
    this->SetJitContext(nullptr);
    for (int32_t i = 0; i < nProj; i++) {
        auto projection = std::make_unique<Projection>(inputTypes, nCols, expressions[i], false);
        if (!projection->IsSupported()) {
            this->isSupported = false;
            break;
        }
        this->proj.push_back(move(projection));
    }
}


ProjectionOperatorFactory::ProjectionOperatorFactory(Expr *exprs[], int32_t nProj, int32_t inputTypes[], int32_t nCols)
    : inputTypes(inputTypes), nCols(nCols), nProj(nProj)
{
    this->SetJitContext(nullptr);
    for (int32_t i = 0; i < nProj; i++) {
        this->proj.push_back(std::make_unique<Projection>(inputTypes, nCols, *(exprs[i]), false));
    }
}

ProjectionOperatorFactory::~ProjectionOperatorFactory()
{
    for (auto &projection : this->proj) {
        projection.reset();
    }
    this->proj.clear();
}

omniruntime::op::Operator *ProjectionOperatorFactory::CreateOperator()
{
    auto projectionOperator =
        std::make_unique<ProjectionOperator>(this->proj, this->inputTypes, this->nCols, this->nProj,
                                             new ExecutionContext());
    return projectionOperator.release();
}

bool ProjectionOperatorFactory::IsSupported()
{
    return this->isSupported;
}