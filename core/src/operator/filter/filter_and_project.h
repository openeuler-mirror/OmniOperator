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

namespace omniruntime {
namespace op {
    
class Filter
{
public:
    Filter(FilterCodeGen* codegen, expressions::Expr* expr);
    ~Filter() {delete this->codeGen; delete this->expr;}
    int32_t filter(omniruntime::vec::VectorBatch* &vecBatch, int32_t *selectedRows);
private:
    FilterCodeGen *codeGen;
    expressions::Expr* expr;
    int64_t* getData(omniruntime::vec::VectorBatch* &vecBatch, vector<int64_t *> &vcdataVec,
        vector<char *> &stringvalVec, bool* bitmap);
    
    // filter function is retrieved from FilterCodeGen
    // func(data, numSelectedRows, rowCount, bitmap)
    // data: 2D array containing vector values
    // selectedRows: array of row numbers which pass the filter; is modified in func
    // rowCount: number of rows in data
    // bitmap: boolean array where bitmap[numCols * row + col] is true if data[row][col] is null
    int32_t (*func)(int64_t*, int32_t, int32_t*, bool*);
};

class FilterAndProjectOperator : public Operator
{
public:
    FilterAndProjectOperator(Filter *filter, int32_t *inputTypes, int32_t vecCount, Projection** projections, int32_t projectVecCount)
        : filter(filter), inputTypes(inputTypes), vecCount(vecCount), projections(projections), projectVecCount(projectVecCount)
    {
    }

    ~FilterAndProjectOperator() {
        for (int i = 0; i < this->projectVecCount; i++) {
            delete this->projections[i];
        }
        delete[] this->projections;
    }

    int32_t AddInput(omniruntime::vec::VectorBatch *vecBatch) override;

    int32_t GetOutput(std::vector<omniruntime::vec::VectorBatch*>& data) override;

    int32_t getVecCount() { return this->vecCount; }

    int32_t *GetSourceTypes() override { return this->inputTypes; }

    // void Close() override { delete this; }

    private:
    Filter *filter;
    Projection** projections;
    int32_t projectVecCount;
    int32_t *inputTypes;
    int32_t vecCount;
    omniruntime::vec::VectorBatch *projectedVecs;
};

class FilterAndProjectOperatorFactory : public OperatorFactory
{
public:
    FilterAndProjectOperatorFactory(std::string expression, int32_t *inputTypes, int32_t vecCount, int32_t *projectIndex, int32_t projectVecCount);

    ~FilterAndProjectOperatorFactory() override;

    Operator* CreateOperator() override;

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