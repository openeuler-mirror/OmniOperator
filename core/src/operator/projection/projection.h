/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Projection operator header
 */
#ifndef __PROJECTION_H__
#define __PROJECTION_H__

#include <vector>
#include <memory>

#include "../../codegen/projection_codegen.h"
#include "../operator_factory.h"
#include "../operator.h"
#include "../../vector/vector_common.h"
#include "../../vector/vector_allocator_manager.h"

namespace omniruntime {
namespace op {
class Projection {
public:
    Projection(int32_t *inputTypes, int32_t nCols, std::string expr, bool filter);
    Projection(int32_t *inputTypes, int32_t nCols, Expr *expr, bool filter);
    ~Projection()
    {
        delete this->expr;
        this->codegen.reset();
    }
    omniruntime::vec::Vector *Project(omniruntime::vec::VectorBatch *vecBatch, int32_t *selectedRows,
        int32_t numSelectedRows) const;
    omniruntime::vec::Vector *Project(omniruntime::vec::VectorBatch *vecBatch) const;
    DataType GetOutputType() const
    {
        return this->expr->GetExprDataType();
    }

private:
    int32_t *inputTypes;
    int32_t nCols;
    Expr *expr;
    std::unique_ptr<ProjectionCodeGen> codegen;
    std::vector<int64_t> GetData(omniruntime::vec::VectorBatch *&vecBatch, std::vector<std::vector<int64_t>> &vcdataVec,
        std::vector<char *> &stringvalVec, bool *bitmap) const;

    // projector function is retrieved from ProjectionCodeGen
    // projector(data, rowCount, selectedRows, numSelectedRows, bitmap)
    // data: 2D array containing vector values
    // rowCount: number of rows in data
    // selectedRows: array of row numbers which pass the Filter
    // numSelectedRows: number of rows which pass the Filter
    // bitmap: boolean array where bitmap[numCols * row + col] is true if data[row][col] is null
    int32_t (*projector)(int64_t *, int32_t, int64_t, int32_t *, int32_t, bool *);
};

class ProjectionOperator : public Operator {
public:
    ProjectionOperator(std::vector<std::unique_ptr<Projection>> const & proj, int32_t *inputTypes, int32_t nCols,
        int32_t nProj)
        : proj(proj), nCols(nCols), nProj(nProj)
    {
        this->sourceTypes = inputTypes;
        this->mutated = nullptr;
    }
    ~ProjectionOperator() override = default;

    int32_t AddInput(omniruntime::vec::VectorBatch *vecBatch) override;
    int32_t GetOutput(std::vector<omniruntime::vec::VectorBatch *> &ret) override;

private:
    const std::vector<std::unique_ptr<Projection>> &proj;
    int32_t nCols;
    int32_t nProj;
    omniruntime::vec::VectorBatch *mutated;
};

class ProjectionOperatorFactory : public OperatorFactory {
public:
    ProjectionOperatorFactory(std::string const *expression, int32_t nProj, int32_t *inputTypes, int32_t nCols);
    ProjectionOperatorFactory(Expr **exprs, int32_t nProj, int32_t *inputTypes, int32_t nCols);
    ~ProjectionOperatorFactory() override;
    omniruntime::op::Operator *CreateOperator() override;

private:
    int32_t *inputTypes;
    int32_t nCols;
    std::vector<std::unique_ptr<Projection>> proj;
    int32_t nProj;
};
}
}

#endif