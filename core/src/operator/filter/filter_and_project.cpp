/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: FilterAndProject operator source file
 */
#include "filter_and_project.h"
#include "vector/vector_helper.h"
#include "expression/jsonparser/jsonparser.h"
#include "util/config_util.h"

namespace omniruntime {
namespace op {
using namespace omniruntime::vec;
using namespace omniruntime::expressions;
using namespace omniruntime::mem;
using namespace std;

SimpleFilter::SimpleFilter(const Expr &expression)
    : codegen(nullptr), expression(&expression), func(nullptr), initialized(false)
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

bool SimpleFilter::Initialize(OverflowConfig *overflowConfig)
{
    if (this->expression == nullptr) {
        LogWarn("Unable to parse expression for simple filter");
        return false;
    }

    if (this->expression->GetReturnTypeId() != OMNI_BOOLEAN) {
        LogWarn("Filter expression can only return boolean, current type: %d", this->expression->GetReturnTypeId());
        return false;
    }

    this->codegen = std::make_unique<SimpleFilterCodeGen>("simple_row_expr_eval", *this->expression, overflowConfig);
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
        return set<int32_t>{};
    }
    return this->codegen->vectorIndexes;
}

bool SimpleFilter::Evaluate(int64_t *values, bool *isNulls, int32_t *lengths, int64_t executionContext)
{
    auto result = this->func(values, isNulls, lengths, this->isResultNull, this->resultLength, executionContext);
    return !*this->isResultNull && result;
}

Operator *FilterAndProjectOperatorFactory::CreateOperator()
{
    return new FilterAndProjectOperator(new ExecutionContext(), this->exprEvaluator);
}

int32_t FilterAndProjectOperator::AddInput(VectorBatch *vecBatch)
{
    projectedVecs = this->exprEvaluator->Evaluate(vecBatch, this->context);
    return 0;
}

int32_t FilterAndProjectOperator::GetOutput(VectorBatch **outputVecBatch)
{
    if (this->projectedVecs == nullptr) {
        return 0;
    }

    int rowCount = this->projectedVecs->GetRowCount();
    *outputVecBatch = this->projectedVecs;
    this->projectedVecs = nullptr;

    return rowCount;
}

OmniStatus FilterAndProjectOperator::Close()
{
    if (projectedVecs != nullptr) {
        VectorHelper::FreeVecBatch(projectedVecs);
        projectedVecs = nullptr;
    }
    return OMNI_STATUS_NORMAL;
}

/**
 * Process one row for fusion operator
 * @param valueAddrs contains value address of each column.
 * @param inputLens contains null or length of each column. inputLens[i] == -1 means i-th value is null; inputLens[i] >=
 * 0 represents the i-th values's length.
 * @param outValueAddrs contains output value address of each projection.
 * @param outLens contains null or length of each projection.
 * @return true(filter pass) or false(filter fail).
 */
bool FilterAndProjectOperator::ProcessRow(int64_t valueAddrs[], const int32_t inputLens[], int64_t outValueAddrs[],
    int32_t outLens[])
{
    auto vecCount = exprEvaluator->GetInputDataTypes().GetSize();
    auto dictsAddrs = new int64_t[vecCount];
    auto offsetsAddrs = new int64_t[vecCount];
    auto nullsAddrs = new int64_t[vecCount];
    for (int i = 0; i < vecCount; ++i) {
        dictsAddrs[i] = 0; // Spark's TableScan will not produce dictionary.
        auto null = new bool[1];
        nullsAddrs[i] = reinterpret_cast<int64_t>(null);
        auto offset = new int32_t[2]; // offset[1] - offset[0] = length
        offsetsAddrs[i] = reinterpret_cast<int64_t>(offset);
    }

    // Construct nullsAddrs and offsetsAddrs from inputLens
    for (int i = 0; i < vecCount; ++i) {
        if (inputLens[i] == -1) {
            reinterpret_cast<bool *>(nullsAddrs[i])[0] = true;
            reinterpret_cast<int32_t *>(offsetsAddrs[i])[0] = 0;
            reinterpret_cast<int32_t *>(offsetsAddrs[i])[1] = 0;
        } else {
            reinterpret_cast<bool *>(nullsAddrs[i])[0] = false;
            reinterpret_cast<int32_t *>(offsetsAddrs[i])[0] = 0;
            reinterpret_cast<int32_t *>(offsetsAddrs[i])[1] = inputLens[i];
        }
    }

    const int rowCount = 1;
    int32_t selectedRows[rowCount];
    int32_t numSelectedRows = exprEvaluator->GetFilterFunc()(valueAddrs, rowCount, selectedRows, nullsAddrs,
        offsetsAddrs, reinterpret_cast<int64_t>(context), dictsAddrs);

    if (context->HasError()) {
        context->GetArena()->Reset();
        for (int i = 0; i < vecCount; ++i) {
            delete[] reinterpret_cast<bool *>(nullsAddrs[i]);
            delete[] reinterpret_cast<int32_t *>(offsetsAddrs[i]);
        }
        delete[] dictsAddrs;
        delete[] nullsAddrs;
        delete[] offsetsAddrs;
        string errorMessage = context->GetError();
        throw OmniException("OPERATOR_RUNTIME_ERROR", errorMessage);
    }

    if (numSelectedRows <= 0) {
        context->GetArena()->Reset();
        for (int i = 0; i < vecCount; ++i) {
            delete[] reinterpret_cast<bool *>(nullsAddrs[i]);
            delete[] reinterpret_cast<int32_t *>(offsetsAddrs[i]);
        }
        delete[] dictsAddrs;
        delete[] nullsAddrs;
        delete[] offsetsAddrs;
        return false;
    }

    for (int32_t i = 0; i < exprEvaluator->GetProjectVecCount(); i++) {
        auto &projections = exprEvaluator->GetProjections();
        if (projections[i]->IsColumnProjection()) {
            outValueAddrs[i] = valueAddrs[projections[i]->GetColumnProjectionIndex()];
            outLens[i] = inputLens[projections[i]->GetColumnProjectionIndex()];
        } else {
            context->GetArena()->Reset();
            for (int i = 0; i < vecCount; ++i) {
                delete[] reinterpret_cast<bool *>(nullsAddrs[i]);
                delete[] reinterpret_cast<int32_t *>(offsetsAddrs[i]);
            }
            delete[] dictsAddrs;
            delete[] nullsAddrs;
            delete[] offsetsAddrs;
            string errorMessage = "Fusion filter only supports raw column projection!";
            throw OmniException("OPERATOR_RUNTIME_ERROR", errorMessage);
        }
    }

    context->GetArena()->Reset();
    for (int i = 0; i < vecCount; ++i) {
        delete[] reinterpret_cast<bool *>(nullsAddrs[i]);
        delete[] reinterpret_cast<int32_t *>(offsetsAddrs[i]);
    }
    delete[] dictsAddrs;
    delete[] nullsAddrs;
    delete[] offsetsAddrs;
    return true;
}
} // end of op
} // end of omniruntime
