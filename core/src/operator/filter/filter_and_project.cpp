/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: FilterAndProject operator source file
 */
#include "filter_and_project.h"
#include "filter_compiler.h"
#include "../../vector/vector_helper.h"

namespace omniruntime {
namespace op {
using namespace omniruntime::vec;
using namespace omniruntime::expressions;
using namespace std;

using Uint8vec = std::vector<uint8_t>;
RowFilter::RowFilter(std::string &expression, std::vector<expressions::DataType> &inputTypes)
    : codegen(nullptr), expression(nullptr)
{
    Parser parser;
    this->expression = parser.ParseRowExpression(
        expression, reinterpret_cast<int32_t *>(inputTypes.data()), inputTypes.size());
}

RowFilter::~RowFilter()
{
    delete this->expression;
    this->codegen.reset();
}

// Return nullptr if expression is unsupported
RowFilterFunc RowFilter::Create(std::vector<DataType> &inputTypes)
{
    this->codegen = std::make_unique<FilterCodeGen>("single_row_filter", *this->expression, inputTypes);
    int64_t fAddr = this->codegen->GetExpressionEvaluator();
    void *refFunc = &fAddr;
    auto castedRef = static_cast<RowFilterFunc *>(refFunc);
    return *castedRef;
}

FilterAndProjectOperatorFactory::FilterAndProjectOperatorFactory(std::string expression, int32_t *inputTypes,
    int32_t vecCount, int32_t projectIndex[], int32_t projectVecCount)
{
    this->inputTypes = inputTypes;
    this->vecCount = vecCount;
    this->projectIndex = projectIndex;
    this->projectVecCount = projectVecCount;
    this->SetJitContext(nullptr);

    Expr *parsedExpr = nullptr;

    Parser parserObject;
    parsedExpr = parserObject.ParseRowExpression(expression, inputTypes, vecCount);
    if (parsedExpr != nullptr) {
        this->isSupportedExpr = true;
        unique_ptr<Compiler> compiler = make_unique<Compiler>(*parsedExpr, inputTypes, vecCount);
        this->filter = compiler->Compile();

        for (int32_t i = 0; i < this->projectVecCount; i++) {
            auto exp = make_unique<DataExpr>();
            exp->isColumn = true;
            exp->colVal = this->projectIndex[i];
            exp->dataType = ColTypeTrans(inputTypes[projectIndex[i]]);
            projections.push_back(make_unique<Projection>(inputTypes, vecCount, *(exp.release()), true));
        }
    } else {
        this->isSupportedExpr = false;
    }
}


FilterAndProjectOperatorFactory::~FilterAndProjectOperatorFactory()
{
    this->filter.reset();
    for (auto &projection : this->projections) {
        projection.reset();
    }
    this->projections.clear();
}

Operator *FilterAndProjectOperatorFactory::CreateOperator()
{
    auto filterAndProjectOperator = make_unique<FilterAndProjectOperator>(this->filter, this->inputTypes,
        this->vecCount, this->projections, this->projectVecCount);
    return filterAndProjectOperator.release();
}

int32_t FilterAndProjectOperator::AddInput(VectorBatch *vecBatch)
{
    const int rowCount = vecBatch->GetRowCount();
    int32_t selectedRows[rowCount];
    int32_t numSelectedRows = this->filter->DoFilter(vecBatch, selectedRows, rowCount);
    if (numSelectedRows <= 0) {
        return 0;
    }
    auto projectedData = make_unique<VectorBatch>(this->projectVecCount);
    for (int32_t i = 0; i < this->projectVecCount; i++) {
        // vecData and bitmap won't be used for filter projection
        Vector *col = this->projections[i]->Project(
            this->vecAllocator, vecBatch, selectedRows, numSelectedRows, vector<int64_t> {}, vector<int64_t> {}, vector<int64_t> {});
        projectedData->SetVector(i, col);
    }
    this->projectedVecs = std::move(projectedData);
    return numSelectedRows;
}

int32_t FilterAndProjectOperator::GetOutput(std::vector<VectorBatch *> &data)
{
    if (this->projectedVecs == nullptr) {
        return 0;
    }

    int rowCount = this->projectedVecs->GetRowCount();
    data.push_back(this->projectedVecs.release());

    // need to cleanup memory in old vecBatches
    FreeStrings();
    FreeDecimalArrays();
    return rowCount;
}

Filter::Filter(unique_ptr<FilterCodeGen> codeGen, Expr &expr)
{
    this->codeGen = std::move(codeGen);
    this->expr = &expr;

    auto f = this->codeGen->GetFunction();
    void *function = &f;
    auto cfunction = static_cast<FilterFunc *>(function);
    this->func = *cfunction;
}

unique_ptr<vector<uint8_t>> GetDataHelper(uint8_t actualChar[], int32_t len)
{
    auto accStr = make_unique<Uint8vec>(len + 1);
    for (int32_t k = 0; k < len; k++) {
        (*accStr)[k] = actualChar[k];
    }
    (*accStr)[len] = '\0';
    return move(accStr);
}

void GetVarcharData(Vector *col, vector<unique_ptr<vector<int64_t>>> &vcdataVec,
                    vector<unique_ptr<vector<uint8_t>>> &stringvalVec, std::vector<int64_t> &data, uint32_t nRows)
{
    auto vcVec = static_cast<VarcharVector *>(col);
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
        unique_ptr<Uint8vec> accStr = GetDataHelper(actualChar, len);
        actualChar = accStr->data();

        // add to vector so it can be freed later
        stringvalVec.push_back(move(accStr));

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

void GetDecimal128Data(Vector *col, std::vector<int64_t> &data, uint32_t nRows)
{
    int32_t longs = 2;
    int64_t *values = reinterpret_cast<int64_t *>(col->GetValues());
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
std::vector<int64_t> GetData(VectorBatch *&vecBatch, vector<unique_ptr<vector<int64_t>>> &vcdataVec,
    vector<unique_ptr<vector<uint8_t>>> &stringvalVec, int64_t bitmap[],
    int64_t offsetsAddrs[], std::vector<omniruntime::vec::Vector *> &dictionaryVecs)
{
    uint32_t nCols = vecBatch->GetVectorCount();
    std::vector<int64_t> data;

    for (int32_t i = 0; i < nCols; i++) {
        omniruntime::vec::Vector *colVec = vecBatch->GetVector(i);
        // handle dictionary vec
        if (colVec->GetTypeId() == omniruntime::vec::OMNI_VEC_TYPE_DICTIONARY) {
            colVec = static_cast<DictionaryVector *>(colVec)->ExtractDictionary();
            dictionaryVecs.push_back(colVec);
        }
        // varchar vec GetValues is different from the rest
        if (colVec->GetTypeId() == OMNI_VEC_TYPE_VARCHAR) {
            GetVarcharData(colVec, vcdataVec, stringvalVec, data, vecBatch->GetRowCount());
        } else if (colVec->GetTypeId() == OMNI_VEC_TYPE_DECIMAL128) {
            GetDecimal128Data(colVec, data, vecBatch->GetRowCount());
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

        // offsets handling
        auto offsets = colVec->GetValueOffsets();
        void *columnOffsets = &offsets;
        offsetsAddrs[i] = *static_cast<int64_t *>(columnOffsets);
    }

    return data;
}

int32_t Filter::DoFilter(VectorBatch *&vecBatch, int32_t selectedRows[], int rowCount) const
{
    // Contains arrays with addresses for varchar vecs
    vector<unique_ptr<vector<int64_t>>> vcdataVec;
    // Contains all strings created in VarcharVector::GetValue method which need to be freed
    vector<unique_ptr<vector<uint8_t>>> stringvalVec;

    vector<int64_t> bitmap(vecBatch->GetVectorCount());
    vector<int64_t> offsets(vecBatch->GetVectorCount());

    // when the dictionary vector is processed it will be restored to an original vector
    // needs to be released
    vector<Vector *> dictionaryVecs;

    // contents of bitmap are appropriately modified in GetData
    std::vector<int64_t> data = GetData(vecBatch, vcdataVec, stringvalVec, bitmap.data(), offsets.data(), dictionaryVecs);
    int32_t ret = this->func(data.data(), rowCount, selectedRows, bitmap.data(), offsets.data());

    for (auto &dictionaryVec : dictionaryVecs) {
        delete dictionaryVec;
    }
    data.clear();

    return ret;
}
} // end of op
} // end of omniruntime
