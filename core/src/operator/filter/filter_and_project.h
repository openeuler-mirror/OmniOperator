/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: FilterAndProject operator header
 */
#ifndef __FILTER_H__
#define __FILTER_H__

#include <memory>
#include <vector>

#include "../../vector/vector_batch.h"
#include "../operator_factory.h"
#include "../operator.h"
#include "../projection/projection.h"
#include "../../codegen/filter_codegen.h"
#include "../../common/expressions.h"
#include "../execution_context.h"

using vec64 = std::vector<int64_t>;
using FilterFunc = int32_t (*)(int64_t *, int32_t, int32_t *, int64_t *, int64_t *, int64_t, int64_t *);

namespace omniruntime {
namespace op {
/**
 * vector value addresses
 * vector null value addresses
 * vector offsets addresses
 * row index
 */
using RowFilterFunc = bool (*)(int64_t *, int64_t *, int64_t *, int32_t, int64_t);

class RowFilter {
public:
    RowFilter(std::string &expression, std::vector<expressions::DataType> &inputType);
    ~RowFilter();
    RowFilterFunc Create(std::vector<expressions::DataType> &inputTypes);

private:
    std::unique_ptr<FilterCodeGen> codegen = nullptr;
    expressions::Expr *expression;
};

class Filter {
public:
    Filter(expressions::Expr &expression, int32_t inputVecTypes[], int32_t inputVecCount);
    ~Filter()
    {
        this->codeGen.reset();
        delete this->expr;
    }

    FilterFunc Apply;

private:
    std::unique_ptr<FilterCodeGen> codeGen;
    expressions::Expr *expr;
    // Filter function is retrieved from FilterCodeGen
    // arguments to func are (data, numSelectedRows, rowCount, bitmap, offsets, allocator)
    // data: 2D array containing vector values
    // selectedRows: array of row numbers which pass the Filter; is modified in func
    // rowCount: number of rows in data
    // bitmap: 2d boolean array where bitmap[col][row] is true if data[row][col] is null
    // value offsets
    // address to an allocator
};

class FilterAndProjectOperator : public Operator {
public:
    FilterAndProjectOperator(std::unique_ptr<Filter> const & filter, int32_t inputTypes[], int32_t vecCount,
        const std::vector<std::unique_ptr<Projection>> &projections, int32_t projectVecCount, ExecutionContext *context)
        : filter(filter),
          inputTypes(inputTypes),
          vecCount(vecCount),
          projections(projections),
          projectVecCount(projectVecCount),
          projectedVecs(nullptr),
          context(context)
    {}

    ~FilterAndProjectOperator() override
    {
        delete context;
    }

    int32_t AddInput(omniruntime::vec::VectorBatch *vecBatch) override;

    int32_t GetOutput(std::vector<omniruntime::vec::VectorBatch *> &data) override;

    int32_t *GetSourceTypes() override
    {
        return this->inputTypes;
    }

private:
    const std::unique_ptr<Filter> &filter;
    const std::vector<std::unique_ptr<Projection>> &projections;
    int32_t projectVecCount;
    int32_t *inputTypes;
    int32_t vecCount;
    ExecutionContext *context;
    std::unique_ptr<omniruntime::vec::VectorBatch> projectedVecs;
};

class FilterAndProjectOperatorFactory : public OperatorFactory {
public:
    FilterAndProjectOperatorFactory(std::string expression, int32_t inputVecTypes[], int32_t inputVecCount,
                                    std::string projections[], int32_t projectVecCount);

    ~FilterAndProjectOperatorFactory() override;

    Operator *CreateOperator() override;

    bool isSupportedExpr;

private:
    std::string expression;
    int32_t *inputVecTypes;
    int32_t inputVecCount;
    int32_t projectVecCount;
    std::unique_ptr<Filter> filter;
    std::vector<std::unique_ptr<Projection>> projections;
};
} // end of op
} // end of omniruntime
#endif