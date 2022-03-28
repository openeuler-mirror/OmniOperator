/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Projection operator header
 */
#ifndef __PROJECTION_H__
#define __PROJECTION_H__

#include <vector>
#include "codegen/projection_codegen.h"
#include "operator/operator_factory.h"
#include "operator/operator.h"
#include "vector/vector_common.h"
#include "type/data_types.h"
#include "expression/expressions.h"
#include "projection.h"
#include "operator/execution_context.h"

using vec64 = std::vector<int64_t>;
using ProjFunc = int32_t (*)(int64_t const *, int32_t, int64_t, int32_t *, int32_t, int64_t const *, int64_t const *,
    bool *, int32_t *, int64_t, int64_t *);

namespace omniruntime {
namespace op {
using namespace vec;
/**
 * vector value addresses
 * vector null value addresses
 * vector offsets addresses
 * row index * int pointer to return length of varchar result
 * address of ExecutionContext
 * dictionary vector addresses
 * boolean pointer to return if results is null
 */
using RowProjFunc = void *(*)(int64_t *, int64_t *, int64_t *, int32_t, int32_t *, int64_t, int64_t *, bool *);

class RowProjection {
public:
    explicit RowProjection(const omniruntime::expressions::Expr &expression);
    ~RowProjection();
    RowProjFunc Create();
    DataType GetReturnType();
    bool IsColumnProjection();
    int GetIndexIfColumnProjection();

private:
    std::unique_ptr<ProjectionCodeGen> codegen = nullptr;
    const expressions::Expr *expression;
};

class Projection {
public:
    Projection(DataTypes &inputTypes, int32_t nCols, const expressions::Expr &expr, bool filter);
    ~Projection()
    {
        delete this->expr;
        this->codegen.reset();
    }
    bool IsSupported();

    omniruntime::vec::Vector *ProjectHelperFixedWidth(omniruntime::vec::VectorBatch &vecBatch,
        std::vector<int64_t> const & vecData, int64_t *bitmap, int64_t *offsets, omniruntime::vec::Vector *outVec,
        int32_t numSelectedRows, int32_t selectedRows[], ExecutionContext *context, int64_t *dictionaryVectors) const;

    omniruntime::vec::Vector *ProjectHelperVarWidth(omniruntime::vec::VectorBatch &vecBatch,
        std::vector<int64_t> const & vecData, int64_t *bitmap, int64_t *offsets, omniruntime::vec::Vector *outVec,
        int32_t numSelectedRows, int32_t selectedRows[], ExecutionContext *context, int64_t *dictionaryVectors) const;

    Vector *Project(VectorAllocator *vecAllocator, VectorBatch *vecBatch, int32_t selectedRows[],
        int32_t numSelectedRows, std::vector<int64_t> const & vecData, int64_t *bitmap, int64_t *offset,
        ExecutionContext *context, int64_t *dictionaryVectors) const;

    Vector *Project(VectorAllocator *vectorAllocator, VectorBatch *vecBatch, std::vector<int64_t> const & vecData,
        int64_t *bitmap, int64_t *offsets, ExecutionContext *context, int64_t *dictionaryVectors) const;

    omniruntime::type::DataType GetOutputType() const
    {
        return this->expr->GetReturnType();
    }

private:
    int32_t *inputTypeIds;
    DataTypes inputTypes;
    int32_t nCols;
    const omniruntime::expressions::Expr *expr;
    std::unique_ptr<ProjectionCodeGen> codegen { nullptr };
    bool isSupported = true;
    bool isColumnProjection = false;
    int columnProjectionIndex = -1;

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
    explicit ProjectionOperator(std::vector<std::unique_ptr<Projection>> const & proj, int32_t inputTypes[],
        int32_t nCols, int32_t nProj, ExecutionContext *context)
        : proj(proj), nCols(nCols), nProj(nProj), context(context)
    {
        this->sourceTypes = inputTypes;
        this->mutated = nullptr;
        this->context->getArena()->SetAllocator(vecAllocator);
    }

    ~ProjectionOperator() override
    {
        delete context;
    }

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
    ProjectionOperatorFactory(const std::vector<omniruntime::expressions::Expr *> &exprs, int32_t nProj,
        DataTypes &inputTypes, int32_t nCols);

    ~ProjectionOperatorFactory() override;
    omniruntime::op::Operator *CreateOperator() override;
    bool IsSupported();

private:
    int32_t *inputTypeIds;
    DataTypes inputTypes;
    int32_t nCols;
    std::vector<std::unique_ptr<Projection>> proj;
    int32_t nProj;
    bool isSupported = true;
    int8_t parseFormat;
};
}
}

#endif