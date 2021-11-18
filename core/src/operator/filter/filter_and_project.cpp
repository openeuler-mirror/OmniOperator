/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: FilterAndProject operator source file
 */
#include "filter_and_project.h"
#include "../../vector/vector_helper.h"

namespace omniruntime {
namespace op {
using namespace omniruntime::vec;
using namespace omniruntime::expressions;
using namespace omniruntime::mem;
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
    this->codegen = std::make_unique<FilterCodeGen>("single_row_filter", *this->expression);
    int64_t fAddr = this->codegen->GetExpressionEvaluator();
    void *refFunc = &fAddr;
    auto castedRef = static_cast<RowFilterFunc *>(refFunc);
    return *castedRef;
}

FilterAndProjectOperatorFactory::FilterAndProjectOperatorFactory(std::string expression, int32_t *inputVecTypes,
    int32_t inputVecCount, std::string projectExprs[], int32_t projectVecCount)
{
    this->inputVecTypes = inputVecTypes;
    this->inputVecCount = inputVecCount;
    this->projectVecCount = projectVecCount;
    this->SetJitContext(nullptr);

    Expr *parsedExpr = nullptr;

    Parser parserObject;
    parsedExpr = parserObject.ParseRowExpression(expression, inputVecTypes, inputVecCount);
#ifdef DEBUG
    std::cout << "String expression in Filter: " << expression << std::endl;
    ExprPrinter printExprTree;
    parsedExpr->Accept(printExprTree);
#endif
    if (parsedExpr != nullptr) {
        this->isSupportedExpr = true;

        this->filter = make_unique<Filter>(*parsedExpr, inputVecTypes, inputVecCount);

        for (int32_t i = 0; i < this->projectVecCount; i++) {
            projections.push_back(make_unique<Projection>(inputVecTypes, inputVecCount, projectExprs[i], true));
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
    auto filterAndProjectOperator = make_unique<FilterAndProjectOperator>(this->filter, this->inputVecTypes,
        this->inputVecCount, this->projections, this->projectVecCount, new ExecutionContext());
    return filterAndProjectOperator.release();
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

// Helper function to return data, null bitmap, offsets in vecBatch
std::vector<int64_t> GetData(VectorBatch *&vecBatch, int64_t bitmap[], int64_t offsetsAddrs[],
    std::vector<omniruntime::vec::Vector *> &dictionaryVecs, int32_t vectorCount, int64_t dictionaries[])
{
    std::vector<int64_t> data;

    for (int32_t i = 0; i < vectorCount; i++) {
        omniruntime::vec::Vector *colVec = vecBatch->GetVector(i);
        // handle dictionary vec
        if (colVec->GetTypeId() == omniruntime::vec::OMNI_VEC_TYPE_DICTIONARY) {
            dictionaries[i] = reinterpret_cast<int64_t>(colVec);
            data.push_back(0);
        } else if (colVec->GetTypeId() == OMNI_VEC_TYPE_DECIMAL128) {
            GetDecimal128Data(colVec, data, vecBatch->GetRowCount());
            dictionaries[i] = 0;
        } else {
            // data handling
            data.push_back(reinterpret_cast<int64_t>(colVec->GetValues()));
            dictionaries[i] = 0;
        }
        // bitmap handling
        bitmap[i] = reinterpret_cast<int64_t>(colVec->GetValueNulls());

        // offsets handling
        offsetsAddrs[i] = reinterpret_cast<int64_t>(colVec->GetValueOffsets());
    }

    return data;
}

int32_t FilterAndProjectOperator::AddInput(VectorBatch *vecBatch)
{
    const int rowCount = vecBatch->GetRowCount();
    const int vectorCount = vecBatch->GetVectorCount();
    int32_t *selectedRows = new int32_t[rowCount];
    int64_t bitmap[vectorCount];
    int64_t offsets[vectorCount];
    int64_t dictionaries[vectorCount];

    // when the dictionary vector is processed it will be restored to an original vector
    // needs to be released
    vector<Vector *> dictionaryVecs; // not used

    std::vector<int64_t> data = GetData(vecBatch, bitmap, offsets, dictionaryVecs, vectorCount, dictionaries);

    int32_t numSelectedRows = this->filter->Apply(data.data(), rowCount, selectedRows, bitmap, offsets,
                                                  reinterpret_cast<int64_t>(context), dictionaries);

    if (numSelectedRows <= 0) {
        return 0;
    }
    auto projectedData = make_unique<VectorBatch>(this->projectVecCount);

    for (int32_t i = 0; i < this->projectVecCount; i++) {
        // vecData and bitmap won't be used for filter projection
        Vector *col = this->projections[i]->Project(
            this->vecAllocator, vecBatch, selectedRows, numSelectedRows, data, bitmap, offsets, context, dictionaries);
        projectedData->SetVector(i, col);
    }
    this->projectedVecs = std::move(projectedData);

    for (auto &dictionaryVec : dictionaryVecs) {
        delete dictionaryVec;
    }
    data.clear();
    delete[] selectedRows;
    context->getArena()->Reset();
    return numSelectedRows;
}

int32_t FilterAndProjectOperator::GetOutput(std::vector<VectorBatch *> &data)
{
    if (this->projectedVecs == nullptr) {
        return 0;
    }

    int rowCount = this->projectedVecs->GetRowCount();
    data.push_back(this->projectedVecs.release());

    return rowCount;
}

Filter::Filter(expressions::Expr &expression, int32_t inputVecTypes[], int32_t inputVecCount)
{
    vector<DataType> dataTypes;
    dataTypes.reserve(inputVecCount);
    for (int32_t i = 0; i < inputVecCount; i++) {
        dataTypes.push_back(expressions::ColTypeTrans(inputVecTypes[i]));
    }
    auto codeGenObj = make_unique<FilterCodeGen>("filterFunc", expression);

    this->codeGen = std::move(codeGenObj);
    this->expr = &expression;

    auto f = this->codeGen->GetFunction();
    void *function = &f;
    this->Apply = *static_cast<FilterFunc *>(function);
}

} // end of op
} // end of omniruntime
