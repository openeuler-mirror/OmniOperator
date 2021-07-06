#ifndef __FILTER_H__
#define __FILTER_H__

#include <stdio.h>
#include <chrono>
#include <vector>

#include "../operator_factory.h"
#include "../operator.h"
#include "../projection/projection.h"
#include "../../util/debug.h"
#include "../../codegen/filter_codegen.h"
#include "../../common/expressions.h"
#include "../../common/parser/parser.h"
#include "../../codegen/llvm_codegen.h"

using namespace omniruntime::expressions;

namespace omniruntime {
namespace op {
    
class Filter
{
public:
    Filter(FilterCodeGen* codegen, Expr* expr);
    ~Filter() {delete this->codeGen; delete this->expr;}
    int32_t filter(VectorBatch *vecBatch, int32_t *selectedRows);
private:
    FilterCodeGen *codeGen;
    Expr* expr;
    int32_t (*func)(int64_t*, int32_t, int32_t*);
};

class FilterAndProjectOperator : public Operator
{
public:
    FilterAndProjectOperator(Filter *filter, int32_t *inputTypes, int32_t vecCount, Projection** projections, int32_t projectVecCount)
        : filter(filter), inputTypes(inputTypes), vecCount(vecCount), projections(projections), projectVecCount(projectVecCount)
    {
    }

    int32_t addInput(VectorBatch *vecBatch) override;

    int32_t getOutput(std::vector<VectorBatch*>& data) override;

    int32_t getVecCount() { return this->vecCount; }

    int32_t *getSourceTypes() override { return this->inputTypes; }

    // void close() override { delete this; }

    private:
    Filter *filter;
    Projection** projections;
    int32_t projectVecCount;
    int32_t *inputTypes;
    int32_t vecCount;
    VectorBatch *projectedVecs;
};

class FilterAndProjectOperatorFactory : public OperatorFactory
{
public:
    FilterAndProjectOperatorFactory(std::string expression, int32_t *inputTypes, int32_t vecCount, int32_t *projectIndex, int32_t projectVecCount);

    ~FilterAndProjectOperatorFactory() override;

    Operator* createOperator() override;

private:
    std::string expression;
    int32_t *inputTypes;
    int32_t vecCount;
    int32_t *projectIndex;
    int32_t projectVecCount;
    Filter *filter;
    Projection** projections;
};
} // end of op
} // end of omniruntime
#endif