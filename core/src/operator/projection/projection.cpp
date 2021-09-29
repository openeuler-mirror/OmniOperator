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

RowProjFunc RowProjection::Create(std::vector<DataType> &inputTypes)
{
    this->codegen = std::make_unique<ProjectionCodeGen>("single_row_project", *this->expression, inputTypes, false);
    int64_t fPtr = this->codegen->GetExpressionEvaluator();
    void *refFunc = &fPtr;
    auto castedRef = static_cast<RowProjFunc *>(refFunc);
    return *castedRef;
}

DataType RowProjection::GetReturnType()
{
    return this->expression->GetExprDataType();
}

bool RowProjection::IsColumnProjection()
{
    return this->expression->GetType() == ExprType::DATA_E && static_cast<DataExpr *>(this->expression)->isColumn;
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

Projection::Projection(int32_t inputTypes[], int32_t nCols, const std::string &expr, bool filter)
    : inputTypes(inputTypes), nCols(nCols)
{
    Parser parser;
    this->expr = parser.ParseRowExpression(expr, inputTypes, nCols);
    std::vector<DataType> dataTypes;
    dataTypes.reserve(nCols);
    for (int32_t i = 0; i < nCols; i++) {
        dataTypes.push_back(expressions::ColTypeTrans(inputTypes[i]));
    }
    this->codegen = std::make_unique<ProjectionCodeGen>("proj_func", *(this->expr), dataTypes, filter);

    auto f = this->codegen->GetFunction();
    void *function = &f;
    auto cfunction = static_cast<ProjFunc *>(function);
    this->projector = *cfunction;
}

Projection::Projection(int32_t inputTypes[], int32_t nCols, Expr &expr, bool filter)
    : inputTypes(inputTypes), nCols(nCols), expr(&expr)
{
    std::vector<DataType> dataTypes;
    dataTypes.reserve(nCols);
    for (int32_t i = 0; i < nCols; i++) {
        dataTypes.push_back(expressions::ColTypeTrans(inputTypes[i]));
    }
    this->codegen = std::make_unique<ProjectionCodeGen>("proj_func", *(this->expr), dataTypes, filter);

    auto f = this->codegen->GetFunction();
    void *function = &f;
    auto cfunction = static_cast<ProjFunc *>(function);
    this->projector = *cfunction;
}

unique_ptr<vector<uint8_t>> GetProjDataHelper(const uint8_t actualChar[], int32_t len)
{
    auto accStr = make_unique<Uint8vec>(len + 1);
    for (int32_t k = 0; k < len; k++) {
        (*accStr)[k] = actualChar[k];
    }
    (*accStr)[len] = '\0';
    return move(accStr);
}

void GetProjVarcharData(Vector *col, vector<unique_ptr<vector<int64_t>>> &vcdataVec,
                        vector<unique_ptr<vector<uint8_t>>> &stringvalVec, std::vector<int64_t> &data, uint32_t nRows)
{
    auto *vcVec = static_cast<omniruntime::vec::VarcharVector *>(col);
    // Create array to hold addresses
    unique_ptr<vec64> vcData = make_unique<vec64>();

    for (int32_t j = 0; j < nRows; j++) {
        // get data
        uint8_t *actualChar = nullptr;
        int32_t len = vcVec->GetValue(j, &actualChar);

        // len is -1 only when the value is null
        // treat it as an empty string for now, need to handle null value properly
        if (len < 0) {
            len = 0;
        }

        // Truncate the resulting string
        unique_ptr<Uint8vec> accStr = GetProjDataHelper(actualChar, len);

        actualChar = accStr->data();

        // add to vector so it can be freed later
        stringvalVec.push_back(move(accStr));

        // add to subarray of data
        auto ac = actualChar;
        void *accChar = &ac;
        auto caccChar = static_cast<int64_t *>(accChar);
        vcData->push_back(*caccChar);
    }
    // data handling
    auto dc = vcData->data();
    void *dataCol = &dc;
    auto cdataCol = static_cast<int64_t *>(dataCol);
    data.push_back(*cdataCol);

    vcdataVec.push_back(move(vcData));
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

// Helper function to return an array of data
// Modifies bitmap array, also adds to vcdataVec and stringvalVec so that the values can be freed
std::vector<int64_t> GetProjData(VectorBatch &vecBatch, std::vector<unique_ptr<std::vector<int64_t>>> &vcdataVec,
    vector<unique_ptr<vector<uint8_t>>> &stringvalVec, int64_t bitmap[], std::vector<Vector *> &dictionaryVecs)
{
    uint32_t nCols = vecBatch.GetVectorCount();
    std::vector<int64_t> data;

    for (int32_t i = 0; i < nCols; i++) {
        Vector *colVec = vecBatch.GetVector(i);
        // handle dictionary vec
        if (colVec->GetTypeId() == OMNI_VEC_TYPE_DICTIONARY) {
            colVec = static_cast<DictionaryVector *>(colVec)->ExtractDictionary();
            dictionaryVecs.push_back(colVec);
        }
        // varchar vec GetValues is different from the rest
        if (colVec->GetTypeId() == omniruntime::vec::OMNI_VEC_TYPE_VARCHAR) {
            GetProjVarcharData(colVec, vcdataVec, stringvalVec, data, vecBatch.GetRowCount());
        } else if (colVec->GetTypeId() == OMNI_VEC_TYPE_DECIMAL128) {
            GetProjDecimal128Data(colVec, data, vecBatch.GetRowCount());
        } else {
            // data handling
            auto dc = colVec->GetValues();
            void *dataCol = &dc;
            auto cdataCol = static_cast<int64_t *>(dataCol);
            data.push_back(*cdataCol);
        }
        // bitmap handling
        auto bc = colVec->GetValueNulls();
        void *bitmapCol = &bc;
        auto cbitmapCol = static_cast<int64_t *>(bitmapCol);
        bitmap[i] = *cbitmapCol;
    }

    return data;
}

Vector *Projection::Project(VectorAllocator *vecAllocator, VectorBatch *vecBatch, int32_t selectedRows[],
    int32_t numSelectedRows) const
{
    if (numSelectedRows != 0 && numSelectedRows == vecBatch->GetRowCount() && expr->GetType() == ExprType::DATA_E) {
        auto *dEx = static_cast<DataExpr *>(expr);
        if (dEx->isColumn) {
            return vecBatch->GetVector(dEx->colVal)->Slice(0, numSelectedRows);
        }
    } else if (selectedRows != nullptr && numSelectedRows != 0 && expr->GetType() == ExprType::DATA_E) {
        auto *dEx = static_cast<DataExpr *>(expr);

        // TODO: optimize branches and extract common functions
        if (dEx->isColumn) {
            Vector *colVec = vecBatch->GetVector(dEx->colVal);
            if (colVec->GetTypeId() == OMNI_VEC_TYPE_DICTIONARY) {
                return static_cast<DictionaryVector *>(colVec)->ExtractDictionary(selectedRows, numSelectedRows);
            } else {
                return colVec->CopyPositions(selectedRows, 0, numSelectedRows);
            }
        }
    }
    DataType outType = expr->GetExprDataType();
    std::unique_ptr<Vector> outVec;
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
            outVec = std::make_unique<VarcharVector>(vecAllocator, numSelectedRows * 200, numSelectedRows);
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
    Vector *projectedVec = nullptr;
    if (outType == STRINGD) {
        projectedVec = ProjectHelperVarWidth(
            *vecBatch, outVec.release(), numSelectedRows, selectedRows, *vecAllocator, newNullValues);
    } else {
        projectedVec = ProjectHelperFixedWidth(
            *vecBatch, outVec.release(), numSelectedRows, selectedRows, *vecAllocator, newNullValues);
    }
    return projectedVec;
}

omniruntime::vec::Vector *Projection::ProjectHelperVarWidth(omniruntime::vec::VectorBatch &vecBatch,
    omniruntime::vec::Vector *outVec, int32_t numSelectedRows,
    int32_t selectedRows[], omniruntime::vec::VectorAllocator &va, bool *newNullValues) const
{
    // Contains arrays with addresses for varchar vecs
    std::vector<unique_ptr<std::vector<int64_t>>> vcdataVec;
    // Contains all strings created in VarcharVector::GetValue method which need to be freed
    vector<unique_ptr<vector<uint8_t>>> stringvalVec;

    vector<int64_t> bitmap(vecBatch.GetVectorCount());

    // when the dictionary vector is processed it will be restored to an original vector
    // needs to be released
    vector<Vector *> dictionaryVecs;

    // contents of bitmap are modified in getProjData method
    std::vector<int64_t> data = GetProjData(vecBatch, vcdataVec, stringvalVec, bitmap.data(), dictionaryVecs);

    // using projector
    vector<int64_t> oVec(numSelectedRows);
    auto ov = oVec.data();
    void *vecVals = &ov;
    auto cvecVals = static_cast<int64_t *>(vecVals);
    this->projector(data.data(), vecBatch.GetRowCount(),
        *cvecVals, selectedRows, numSelectedRows, bitmap.data(), newNullValues);

    auto *outVarcharVec = static_cast<VarcharVector *>(outVec);
    for (int i = 0; i < numSelectedRows; i++) {
        if (newNullValues[i]) {
            outVarcharVec->SetValueNull(i);
            continue;
        }
        auto charArr = reinterpret_cast<uint8_t *>(ov[i]);

        int j = 0;
        while (charArr[j] != '\0') {
            j++;
        }
        outVarcharVec->SetValue(i, charArr, j);
    }

    for (auto &dictionaryVec : dictionaryVecs) {
        delete dictionaryVec;
    }
    data.clear();

    return outVec;
}

omniruntime::vec::Vector *Projection::ProjectHelperFixedWidth(omniruntime::vec::VectorBatch &vecBatch,
    omniruntime::vec::Vector *outVec, int32_t numSelectedRows,
    int32_t selectedRows[], omniruntime::vec::VectorAllocator &va, bool *newNullValues) const
{
    // Contains arrays with addresses for varchar vecs
    std::vector<unique_ptr<std::vector<int64_t>>> vcdataVec;
    // Contains all strings created in VarcharVector::GetValue method which need to be freed
    vector<unique_ptr<vector<uint8_t>>> stringvalVec;

    vector<int64_t> bitmap(vecBatch.GetVectorCount());

    // when the dictionary vector is processed it will be restored to an original vector
    // needs to be released
    vector<Vector *> dictionaryVecs;

    // contents of bitmap are modified in getProjData method
    std::vector<int64_t> data = GetProjData(vecBatch, vcdataVec, stringvalVec, bitmap.data(), dictionaryVecs);

    if (outVec->GetTypeId() == OMNI_VEC_TYPE_DECIMAL128) {
        vector<int64_t> oVec(numSelectedRows);
        auto ov = oVec.data();
        void *vecVals = &ov;
        auto cvecVals = static_cast<int64_t *>(vecVals);
        this->projector(data.data(), vecBatch.GetRowCount(), *cvecVals,
                        selectedRows, numSelectedRows, bitmap.data(), newNullValues);
        auto *outDecimal128Vec = static_cast<Decimal128Vector *>(outVec);
        for (int i = 0; i < numSelectedRows; i++) {
            int64_t *value = reinterpret_cast<int64_t *>(ov[i]);
            outDecimal128Vec->SetValue(i, Decimal128(*(value + 1), *value));
        }
    } else {
        auto ov = outVec->GetValues();
        void *vecVals = &ov;
        auto cvecVals = static_cast<int64_t *>(vecVals);
        int32_t nReturned = this->projector(data.data(), vecBatch.GetRowCount(), *cvecVals,
                                            selectedRows, numSelectedRows, bitmap.data(), newNullValues);
    }

    data.clear();
    for (auto &dictionaryVec : dictionaryVecs) {
        delete dictionaryVec;
    }

    // set null
    for (int i = 0; i < numSelectedRows; i++) {
        if (newNullValues[i]) {
            outVec->SetValueNull(i);
        }
    }
    return outVec;
}

Vector *Projection::Project(VectorAllocator *vecAllocator, VectorBatch *vecBatch) const
{
    return this->Project(vecAllocator, vecBatch, nullptr, vecBatch->GetRowCount());
}

int32_t ProjectionOperator::AddInput(VectorBatch *vecBatch)
{
    auto outBatch = std::make_unique<VectorBatch>(nProj);
    for (int32_t i = 0; i < nProj; i++) {
        Vector *outCol = proj[i]->Project(vecAllocator, vecBatch);
        outBatch->SetVector(i, outCol);
    }
    this->mutated = outBatch.release();
    return vecBatch->GetRowCount();
}

int32_t ProjectionOperator::GetOutput(std::vector<VectorBatch *> &data)
{
    if (this->mutated == nullptr) {
        return -1;
    }
    int rowCount = this->mutated->GetRowCount();
    data.push_back(this->mutated);
    FreeStrings();
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
        this->proj.push_back(std::make_unique<Projection>(inputTypes, nCols, expressions[i], false));
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
        std::make_unique<ProjectionOperator>(this->proj, this->inputTypes, this->nCols, this->nProj);
    return projectionOperator.release();
}