#ifndef __FILTER_H__
#define __FILTER_H__

#include "../native_base.h"
#include "../../vector/table.h"
#include "../../util/debug.h"

typedef bool (*jit_evaluateExpression)(Table* table, int32_t);

class Filter
{
public:
    Filter(jit_evaluateExpression evaluater);
    int32_t filter(Table *table, int32_t rowNumber, int32_t *selectedRows);

private:
    jit_evaluateExpression evaluater;
};

class NativeOmniFilterOperator : public NativeOmniOperator
{
public:
    NativeOmniFilterOperator(Filter *filter, int32_t *inputTypes, int32_t vecCount, int32_t *projectIndex, int32_t projectVecCount)
        : filter(filter), inputTypes(inputTypes), vecCount(vecCount), projectIndex(projectIndex), projectVecCount(projectVecCount)
    {
    }

    int32_t addInput(Table* data, int32_t rowCount) override;

    int32_t getOutput(std::vector<Table*>& data) override;

    int32_t addInput(Table** data, int32_t* rowCount, int32_t pageCount) override
    {
        return 0;
    }

    int32_t getVecCount() { return this->vecCount; }

    int32_t *getSourceTypes() { return this->inputTypes; }

    private:
    Filter *filter;
    int32_t *inputTypes;
    int32_t vecCount;
    int32_t *projectIndex;
    int32_t projectVecCount;
    Table *projectedVecs;
};

class NativeOmniFilterOperatorFactory : public NativeOmniOperatorFactory
{
public:
    NativeOmniFilterOperatorFactory(std::string expression, int32_t *inputTypes, int32_t vecCount, int32_t *projectIndex, int32_t projectVecCount)
        : expression(expression), inputTypes(inputTypes), vecCount(vecCount), projectIndex(projectIndex), projectVecCount(projectVecCount)
    {
    }

    ~NativeOmniFilterOperatorFactory() override {};

    NativeOmniOperator* createOmniOperator() override;

private:
    std::string expression;
    int32_t *inputTypes;
    int32_t vecCount;
    int32_t *projectIndex;
    int32_t projectVecCount;
};

#endif