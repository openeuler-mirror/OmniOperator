/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: FilterAndProject operator source file
 */
#include "filter_and_project.h"
#include "vector/vector_helper.h"
#include "expression/jsonparser/jsonparser.h"

namespace omniruntime {
namespace op {
using namespace omniruntime::vec;
using namespace omniruntime::expressions;
using namespace omniruntime::mem;
using namespace std;

SimpleFilter::SimpleFilter(const Expr &expression)
    : codegen(nullptr),
      expression(&expression),
      func(nullptr),
      initialized(false)
{
    resultLength = new int(0);
    isResultNull = new bool(false);
}

SimpleFilter::~SimpleFilter()
{
    delete this->isResultNull;
    delete this->resultLength;
    this->codegen.reset();
}

bool SimpleFilter::Initialize()
{
    if (this->expression == nullptr) {
        LogWarn("Unable to parse expression for simple filter");
        return false;
    }

    if (this->expression->GetReturnTypeId() != OMNI_BOOLEAN) {
        LogWarn("Filter expression can only return boolean, current type: %d", this->expression->GetReturnTypeId());
        return false;
    }

    this->codegen = RowExpressionCodeGen::Create("simple_row_expr_eval", *this->expression);
    if (this->codegen == nullptr) {
        LogWarn("Unable to generate function for simple filter");
        return false;
    }

    int64_t fAddr = this->codegen->GetFunction();
    if (fAddr == 0) {
        LogWarn("Unable to generate function for simple filter");
        return false;
    }

    void *refFunc = &fAddr;
    this->func = *static_cast<SimpleRowExprEvalFunc *>(refFunc);
    this->initialized = true;
    return true;
}

set<int32_t> SimpleFilter::GetVectorIndexes()
{
    if (!this->initialized) {
        LogWarn("SimpleFilter not initialized or failed to initialize.");
        return set<int32_t> {};
    }
    return this->codegen->vectorIndexes;
}

bool SimpleFilter::Evaluate(int64_t *values, bool *isNulls, int32_t *lengths, int64_t executionContext)
{
    auto result =  this->func(values, isNulls, lengths, this->isResultNull, this->resultLength, executionContext);
    return *this->isResultNull? false : result;
}

FilterAndProjectOperatorFactory::FilterAndProjectOperatorFactory(Expr *parsedExpr, DataTypes &inputDataTypes,
    int32_t inputVecCount, const std::vector<Expr *> &projectExprs, int32_t projectVecCount)
    : inputDataTypes(inputDataTypes), inputVecCount(inputVecCount), projectVecCount(projectVecCount)
{
    this->SetJitContext(nullptr);
#ifdef DEBUG
    std::cout << "String expression in Filter: " << expression << std::endl;
    ExprPrinter printExprTree;
    parsedExpr->Accept(printExprTree);
    std::cout << std::endl;
#endif
    this->filter = make_unique<Filter>(*parsedExpr);
    if (!this->filter->isSupported) {
        this->isSupportedExpr = false;
    }

    for (int32_t i = 0; i < this->projectVecCount; i++) {
        auto projection = make_unique<Projection>(*(projectExprs[i]), true, projectExprs[i]->GetReturnTypeId());
        if (!projection->IsSupported()) {
            this->isSupportedExpr = false;
            break;
        }
        this->projections.push_back(move(projection));
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
    return new FilterAndProjectOperator(filter, this->inputDataTypes.GetIds(), inputVecCount, projections,
        projectVecCount, new ExecutionContext());
}

// Helper function to return data, null bitmap, offsets in vecBatch
void GetData(VectorBatch &vecBatch, int64_t valueAddrs[], int64_t nullAddrs[], int64_t offsetAddrs[],
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

int32_t FilterAndProjectOperator::AddInput(VectorBatch *vecBatch)
{
    const int vectorCount = vecBatch->GetVectorCount();
    int64_t valueAddrs[vectorCount];
    int64_t nullAddrs[vectorCount];
    int64_t offsetAddrs[vectorCount];
    int64_t dictionaries[vectorCount];

    // when the dictionary vector is processed it will be restored to an original vector
    // needs to be released
    GetData(*vecBatch, valueAddrs, nullAddrs, offsetAddrs, dictionaries);

    const int rowCount = vecBatch->GetRowCount();
    auto *selectedRows = new int32_t[rowCount];
    int32_t numSelectedRows = this->filter->apply(valueAddrs, rowCount, selectedRows, nullAddrs, offsetAddrs,
        reinterpret_cast<int64_t>(context), dictionaries);
    if (context->HasError()) {
        // resource cleanup
        delete[] selectedRows;
        context->GetArena()->Reset();
        VectorHelper::FreeVecBatch(vecBatch);
        string errorMessage = context->GetError();
        throw OmniException("OPERATOR_RUNTIME_ERROR", errorMessage);
    }
    if (numSelectedRows <= 0) {
        delete[] selectedRows;
        context->GetArena()->Reset();
        VectorHelper::FreeVecBatch(vecBatch);
        return 0;
    }

    projectedVecs = new VectorBatch(this->projectVecCount, numSelectedRows);
    for (int32_t i = 0; i < this->projectVecCount; i++) {
        // vecData and bitmap won't be used for filter projection
        Vector *col = this->projections[i]->Project(this->vecAllocator, vecBatch, selectedRows, numSelectedRows,
            valueAddrs, nullAddrs, offsetAddrs, context, dictionaries);
        if (context->HasError()) {
            // resource cleanup
            for (int32_t j = 0; j < i; j++) {
                delete projectedVecs->GetVector(j);
            }
            delete projectedVecs;
            projectedVecs = nullptr;
            VectorHelper::FreeVecBatch(vecBatch);
            delete[] selectedRows;
            context->GetArena()->Reset();

            string errorMessage = context->GetError();
            throw OmniException("OPERATOR_RUNTIME_ERROR", errorMessage);
        }
        projectedVecs->SetVector(i, col);
    }

    VectorHelper::FreeVecBatch(vecBatch);
    delete[] selectedRows;
    context->GetArena()->Reset();
    return numSelectedRows;
}

int32_t FilterAndProjectOperator::GetOutput(std::vector<VectorBatch *> &data)
{
    if (this->projectedVecs == nullptr) {
        return 0;
    }

    int rowCount = this->projectedVecs->GetRowCount();
    data.push_back(this->projectedVecs);
    this->projectedVecs = nullptr;

    return rowCount;
}

Filter::Filter(const expressions::Expr &expression)
    : codeGen(FilterCodeGen::Create("filterFunc", expression)), expr(&expression)
{
    auto f = this->codeGen->GetFunction();
    if (f == 0) {
        this->isSupported = false;
        this->apply = nullptr;
    } else {
        this->isSupported = true;
        void *function = &f;
        this->apply = *static_cast<FilterFunc *>(function);
    }
}

OmniStatus FilterAndProjectOperator::Close()
{
    if (projectedVecs != nullptr) {
        VectorHelper::FreeVecBatch(projectedVecs);
        projectedVecs = nullptr;
    }
    return OMNI_STATUS_NORMAL;
}
} // end of op
} // end of omniruntime
