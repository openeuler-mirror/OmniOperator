/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: FilterAndProject operator header
 */
#ifndef __FILTER_AND_PROJECT_H__
#define __FILTER_AND_PROJECT_H__

#include <memory>
#include <vector>

#include "type/data_types.h"
#include "operator/operator.h"
#include "operator/operator_factory.h"
#include "vector/vector_batch.h"
#include "operator/projection/projection.h"
#include "codegen/filter_codegen.h"
#include "expression/expressions.h"
#include "codegen/row_expression_codegen.h"
#include "operator/execution_context.h"

using FilterFunc = int32_t (*)(int64_t *, int32_t, int32_t *, int64_t *, int64_t *, int64_t, int64_t *);

namespace omniruntime {
namespace op {
/**
 * vector value addresses
 * vector null value addresses
 * vector offsets addresses
 * row index
 */
using RowFilterFunc = bool (*)(int64_t *, int64_t *, int64_t *, int32_t, int64_t, int64_t *);
using SimpleRowExprEvalFunc = bool (*)(int64_t *, bool *, int32_t *, bool *, int32_t *, int64_t);

class RowFilter {
public:
    explicit RowFilter(const omniruntime::expressions::Expr &expr);
    ~RowFilter();
    RowFilterFunc Create();

private:
    std::unique_ptr<FilterCodeGen> codegen = nullptr;
    const expressions::Expr *expression;
};

/**
 * Simple Filter that can be evaluated given expression,
 * all the involved value references and types
 */
class SimpleFilter {
public:
    /* *
     * Simple Filter constructor
     *
     * @param expression the filter expression, must return evaluates to boolean type
     * @param inputType types for all involved values
     */
    explicit SimpleFilter(const omniruntime::expressions::Expr &expression);

    ~SimpleFilter();

    /* *
     * Initialize the filter, this method must be called after the construct
     *
     * @return if the expression and types can be supported or not
     */
    bool Initialize();

    /* *
     * Get all the vector indexes used in the expression
     *
     * @return set including the indexes, or empty set if filter not supported or initialized
     */
    std::set<int32_t> GetVectorIndexes();

    /* *
     * Evaluate the filter
     *
     * To make it consistent and simplify the evaluation logic, please make sure
     * index of the value, isNull and length matches with the index in expression
     * for example:
     * and(less_than(#0, 100), greater_than(#5, 5))
     * The input array size should be at least 6, and there should be values at
     * index 0 and index 6, for other indexes the values can simply be default value
     * such as 0 for address, false for boolean, 0 for length.
     *
     * @param values array of value addresses that will be used for evaluation
     * @param isNull array of booleans indicating if each value is null
     * @param lengths array of lengths for varchar type values, 0 for other types
     * @param the execution context
     * @return true if the data matches the expression, false if it doesn't match
     */
    bool Evaluate(int64_t *values, bool *isNulls, int32_t *lengths, int64_t executionContext);

private:
    std::unique_ptr<ExpressionCodeGen> codegen;
    const expressions::Expr *expression;
    SimpleRowExprEvalFunc func;
    bool *isResultNull;
    int32_t *resultLength;
    bool initialized;
};

class Filter {
public:
    explicit Filter(const expressions::Expr &expression);
    ~Filter()
    {
        this->codeGen.reset();
        delete this->expr;
    }
    bool isSupported;

    FilterFunc apply;

private:
    std::unique_ptr<FilterCodeGen> codeGen;
    const expressions::Expr *expr;
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
    FilterAndProjectOperator(std::unique_ptr<Filter> const & filter, int32_t const * inputDataTypes, int32_t vecCount,
        const std::vector<std::unique_ptr<Projection>> &projections, int32_t projectVecCount, ExecutionContext *context)
        : filter(filter),
          projections(projections),
          projectVecCount(projectVecCount),
          inputTypes(inputDataTypes),
          vecCount(vecCount),
          projectedVecs(nullptr)
    {
        this->context = context;
        this->context->GetArena()->SetAllocator(vecAllocator);
    }

    ~FilterAndProjectOperator() override
    {
        delete context;
    }

    int32_t AddInput(omniruntime::vec::VectorBatch *vecBatch) override;

    int32_t GetOutput(std::vector<omniruntime::vec::VectorBatch *> &data) override;

    OmniStatus Close() override;

private:
    const std::unique_ptr<Filter> &filter;
    const std::vector<std::unique_ptr<Projection>> &projections;
    int32_t projectVecCount;
    const int32_t *inputTypes;
    int32_t vecCount;
    omniruntime::vec::VectorBatch *projectedVecs;
};

class FilterAndProjectOperatorFactory : public OperatorFactory {
public:
    FilterAndProjectOperatorFactory(omniruntime::expressions::Expr *parsedExpr, DataTypes &inputDataTypes,
        int32_t inputVecCount, const std::vector<omniruntime::expressions::Expr *> &projections,
        int32_t projectVecCount);

    ~FilterAndProjectOperatorFactory() override;

    Operator *CreateOperator() override;

    bool isSupportedExpr = true;

private:
    std::string expression;
    DataTypes inputDataTypes;
    int32_t inputVecCount;
    int32_t projectVecCount;
    std::unique_ptr<Filter> filter;
    std::vector<std::unique_ptr<Projection>> projections;
};
} // end of op
} // end of omniruntime
#endif