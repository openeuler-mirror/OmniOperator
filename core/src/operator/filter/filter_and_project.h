/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: FilterAndProject operator header
 */
#ifndef __FILTER_AND_PROJECT_H__
#define __FILTER_AND_PROJECT_H__

#include <memory>
#include <vector>

#include "codegen/expr_evaluator.h"
#include "codegen/simple_filter_codegen.h"
#include "expression/expressions.h"
#include "operator/config/operator_config.h"
#include "operator/execution_context.h"
#include "operator/operator.h"
#include "operator/operator_factory.h"
#include "operator/status.h"
#include "plannode/planNode.h"
#include "type/data_types.h"
#include "util/config/QueryConfig.h"
#include "vector/vector_batch.h"

namespace omniruntime {
namespace op {
using namespace omniruntime::codegen;
using SimpleRowExprEvalFunc = bool (*)(int64_t *, bool *, int32_t *, bool *, int32_t *, int64_t);

/**
 * Simple Filter that can be evaluated given expression,
 * all the involved value references and types
 */
class SimpleFilter {
public:
    /* *
     * Simple Filter constructor
     * @param expression the filter expression, must return evaluates to boolean type
     */
    explicit SimpleFilter(const omniruntime::expressions::Expr &expression);

    ~SimpleFilter();

    SimpleFilter(const SimpleFilter &simpleFilter);

    SimpleFilter &operator=(const SimpleFilter &simpleFilter) = delete;

    /* *
     * Initialize the filter, this method must be called after the construct
     * @return if the expression and types can be supported or not
     */
    bool Initialize(OverflowConfig *overflowConfig);

    /* *
     * Get all the vector indexes used in the expression
     * @return set including the indexes, or empty set if filter not supported or initialized
     */
    std::set<int32_t> &GetVectorIndexes();

    /* *
     * Evaluate the filter
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

    const expressions::Expr *GetExpression() { return this->expression; }

    /* *
     * short-circuit logic for column filter, no need to go through codegen
     * @return is short-circuit
     */
    bool IsColumnFilter() { return isColumnFilter; }

private:
    std::shared_ptr<codegen::SimpleFilterCodeGen> codegen;
    const expressions::Expr *expression;
    SimpleRowExprEvalFunc func;
    bool *isResultNull;
    int32_t *resultLength;
    bool initialized;
    bool isColumnFilter = false;
};

OperatorFactory *CreateFilterOperatorFactory(
    std::shared_ptr<const FilterNode> filterNode, const config::QueryConfig &queryConfig);

class FilterAndProjectOperator : public Operator {
public:
    explicit FilterAndProjectOperator(std::shared_ptr<ExpressionEvaluator> &exprEvaluator)
        : projectedVecs(nullptr), exprEvaluator(exprEvaluator)
    {
        SetOperatorName(metricsNameFilter);
    }

    ~FilterAndProjectOperator() override = default;

    int32_t AddInput(omniruntime::vec::VectorBatch *vecBatch) override;

    int32_t GetOutput(omniruntime::vec::VectorBatch **outputVecBatch) override;

    bool ProcessRow(int64_t valueAddrs[], const int32_t inputLens[], int64_t outValueAddrs[], int32_t outLens[]);

    OmniStatus Close() override;

private:
    omniruntime::mem::AlignedBuffer<int32_t> selectedRowsBuffer;
    omniruntime::vec::VectorBatch *projectedVecs;
    std::shared_ptr<ExpressionEvaluator> &exprEvaluator;
};

class FilterAndProjectOperatorFactory : public OperatorFactory {
public:
    explicit FilterAndProjectOperatorFactory(std::shared_ptr<ExpressionEvaluator> &&exprEvaluator)
        : exprEvaluator(std::move(exprEvaluator))
    {
        this->exprEvaluator->FilterFuncGeneration();
    }

    ~FilterAndProjectOperatorFactory() override = default;

    Operator *CreateOperator() override;

private:
    std::shared_ptr<ExpressionEvaluator> exprEvaluator;
};
} // namespace op
} // namespace omniruntime
#endif
