/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: FilterAndProject operator header
 */
#ifndef __FILTER_H__
#define __FILTER_H__

#include <chrono>
#include <memory>
#include <vector>

#include "../../vector/vector_batch.h"
#include "../operator_factory.h"
#include "../operator.h"
#include "../projection/projection.h"
#include "../../codegen/filter_codegen.h"
#include "../../common/expressions.h"

namespace omniruntime {
namespace op {
class Filter {
public:
    Filter(std::unique_ptr<FilterCodeGen> codegen, expressions::Expr *expr);
    ~Filter()
    {
        this->codeGen.reset();
        delete this->expr;
    }
    int32_t DoFilter(omniruntime::vec::VectorBatch *&vecBatch, int32_t *selectedRows, int rowCount) const;

private:
    std::unique_ptr<FilterCodeGen> codeGen;
    expressions::Expr *expr;
    std::vector<int64_t> GetData(omniruntime::vec::VectorBatch *&vecBatch, std::vector<std::vector<int64_t>> &vcdataVec,
        std::vector<char *> &stringvalVec, bool *bitmap) const;
    // Filter function is retrieved from FilterCodeGen
    // func(data, numSelectedRows, rowCount, bitmap)
    // data: 2D array containing vector values
    // selectedRows: array of row numbers which pass the Filter; is modified in func
    // rowCount: number of rows in data
    // bitmap: boolean array where bitmap[numCols * row + col] is true if data[row][col] is null
    int32_t (*func)(int64_t *, int32_t, int32_t *, bool *);
};

class FilterAndProjectOperator : public Operator {
public:
    FilterAndProjectOperator(std::unique_ptr<Filter> const & filter, int32_t *inputTypes, int32_t vecCount,
        const std::vector<std::unique_ptr<Projection>> &projections, int32_t projectVecCount)
        : filter(filter),
          inputTypes(inputTypes),
          vecCount(vecCount),
          projections(projections),
          projectVecCount(projectVecCount),
          projectedVecs(nullptr)
    {}

    ~FilterAndProjectOperator() override = default;

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
    std::unique_ptr<omniruntime::vec::VectorBatch> projectedVecs;
};

class FilterAndProjectOperatorFactory : public OperatorFactory {
public:
    FilterAndProjectOperatorFactory(std::string expression, int32_t *inputTypes, int32_t vecCount,
        int32_t *projectIndex, int32_t projectVecCount);

    ~FilterAndProjectOperatorFactory() override;

    Operator *CreateOperator() override;

private:
    std::string expression;
    int32_t *inputTypes;
    int32_t vecCount;
    int32_t *projectIndex;
    int32_t projectVecCount;
    std::unique_ptr<Filter> filter;
    std::vector<std::unique_ptr<Projection>> projections;
};
} // end of op
} // end of omniruntime
#endif