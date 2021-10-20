/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Projection operator header
 */
#ifndef __PROJECTION_H__
#define __PROJECTION_H__

#include <vector>
#include "../../codegen/projection_codegen.h"
#include "../operator_factory.h"
#include "../operator.h"
#include "../../vector/vector_common.h"
#include "../../vector/vector_allocator_factory.h"
#include "../../common/expressions.h"
#include "projection.h"

using vec64 = std::vector<int64_t>;
using ProjFunc = int32_t (*)(int64_t const *, int32_t, int64_t, int32_t *, int32_t,
    int64_t const *, int64_t const *, bool *, int32_t *);

namespace omniruntime {
namespace op {
using namespace vec;
using RowProjFunc = void *(*)(int64_t *, int64_t *, int64_t *, int32_t, int64_t*);

class RowProjection {
public:
    RowProjection(std::string &expression, std::vector<expressions::DataType> &inputType);
    ~RowProjection();
    RowProjFunc Create(std::vector<expressions::DataType> &inputTypes);
    expressions::DataType GetReturnType();
    bool IsColumnProjection();
    int GetIndexIfColumnProjection();

private:
    std::unique_ptr<ProjectionCodeGen> codegen = nullptr;
    expressions::Expr *expression;
};

class Projection {
public:
    Projection(int32_t *inputTypes, int32_t nCols, const std::string &expr, bool filter);
    Projection(int32_t *inputTypes, int32_t nCols, expressions::Expr &expr, bool filter);
    ~Projection()
    {
        delete this->expr;
        this->codegen.reset();
    }
    bool IsSupported();

    omniruntime::vec::Vector *ProjectHelperFixedWidth(omniruntime::vec::VectorBatch &vecBatch,
        std::vector<int64_t> const &vecData, std::vector<int64_t> const &bitmap,
        std::vector<int64_t> const &offsets, omniruntime::vec::Vector *outVec,
        int32_t numSelectedRows, int32_t selectedRows[], bool *newNullValues, int32_t *newLengthValues) const;
    omniruntime::vec::Vector *ProjectHelperVarWidth(omniruntime::vec::VectorBatch &vecBatch,
        std::vector<int64_t> const &vecData, std::vector<int64_t> const &bitmap,
        std::vector<int64_t> const &offsets, omniruntime::vec::Vector *outVec,
        int32_t numSelectedRows, int32_t selectedRows[], bool *newNullValues, int32_t *newLengthValues) const;

    Vector *Project(VectorAllocator *vecAllocator, VectorBatch *vecBatch, int32_t selectedRows[],
        int32_t numSelectedRows, std::vector<int64_t> const &vecData,
        std::vector<int64_t> const &bitmap, std::vector<int64_t> const &offsets) const;

    Vector *Project(VectorAllocator *vectorAllocator, VectorBatch *vecBatch,
        std::vector<int64_t> const &vecData, std::vector<int64_t> const &bitmap,
        std::vector<int64_t> const &offsets) const;

    omniruntime::expressions::DataType GetOutputType() const
    {
        return this->expr->GetExprDataType();
    }

private:
    int32_t *inputTypes;
    int32_t nCols;
    omniruntime::expressions::Expr *expr;
    std::unique_ptr<ProjectionCodeGen> codegen { nullptr };
    bool isSupported = true;

    // projector function is retrieved from ProjectionCodeGen
    // projector(data, rowCount, selectedRows, numSelectedRows, bitmap)
    // data: 2D array containing vector values
    // rowCount: number of rows in data
    // selectedRows: array of row numbers which pass the filter
    // numSelectedRows: number of rows which pass the filter
    // bitmap: 2D boolean array where bitmap[col][row] is true if data[row][col] is null
    ProjFunc projector;

    bool Initialize(bool filter);
};

class ProjectionOperator : public Operator {
public:
    ProjectionOperator(std::vector<std::unique_ptr<Projection>> const & proj, int32_t inputTypes[], int32_t nCols,
        int32_t nProj)
        : proj(proj), nCols(nCols), nProj(nProj)
    {
        this->sourceTypes = inputTypes;
        this->mutated = nullptr;
    }
    ~ProjectionOperator() override = default;

    int32_t AddInput(VectorBatch *vecBatch) override;
    int32_t GetOutput(std::vector<VectorBatch *> &ret) override;
    ;

private:
    const std::vector<std::unique_ptr<Projection>> &proj;
    int32_t nCols = 0;
    int32_t nProj = 0;
    VectorBatch *mutated = nullptr;
};

class ProjectionOperatorFactory : public OperatorFactory {
public:
    ProjectionOperatorFactory(std::string expression[], int32_t nProj, int32_t inputTypes[], int32_t nCols);
    ProjectionOperatorFactory(omniruntime::expressions::Expr *exprs[], int32_t nProj, int32_t inputTypes[],
        int32_t nCols);
    ~ProjectionOperatorFactory() override;
    omniruntime::op::Operator *CreateOperator() override;
    bool IsSupported();

private:
    int32_t *inputTypes;
    int32_t nCols;
    std::vector<std::unique_ptr<Projection>> proj;
    int32_t nProj;
    bool isSupported = true;
};
}
}

#endif