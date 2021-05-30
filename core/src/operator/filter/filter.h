#ifndef __FILTER_H__
#define __FILTER_H__

#include "../native_base.h"
#include "../../vector/table.h"
#include "../../util/debug.h"
#include "../../codegen/llvm_codegen.h"


class Filter
{
public:
    Filter(LLVMCodeGen* codegen, Expr* expr);
    int32_t filter(Table *table, int32_t rowNumber, int32_t *selectedRows);

private:
    LLVMCodeGen *codeGen;
    Expr* expr;
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

    int32_t *getSourceTypes() override { return this->inputTypes; }

    void close() override { delete this; }

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
    NativeOmniFilterOperatorFactory(std::string expression, int32_t *inputTypes, int32_t vecCount, int32_t *projectIndex, int32_t projectVecCount);

    ~NativeOmniFilterOperatorFactory() override;

    NativeOmniOperator* createOmniOperator() override;

private:
    std::string expression;
    int32_t *inputTypes;
    int32_t vecCount;
    int32_t *projectIndex;
    int32_t projectVecCount;
    Filter *filter;
};

#endif