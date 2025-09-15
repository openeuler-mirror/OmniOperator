/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2023. All rights reserved.
 * Description: Expression evaluator
 */
#ifndef OMNI_RUNTIME_EXPR_EVALUATOR_H
#define OMNI_RUNTIME_EXPR_EVALUATOR_H

#include "operator/execution_context.h"
#include "codegen/filter_codegen.h"
#include "codegen/batch_filter_codegen.h"
#include "codegen/projection_codegen.h"
#include "codegen/batch_projection_codegen.h"
#include "util/config_util.h"
#include "vector/vector_helper.h"

namespace omniruntime::codegen {
using namespace omniruntime::expressions;
using namespace omniruntime::op;
using namespace omniruntime::vec;
using namespace omniruntime::type;
using namespace omniruntime::exception;

using FilterFunc = int32_t (*)(int64_t *, int32_t, int32_t *, int64_t *, int64_t *, int64_t, int64_t *);
using ProjFunc = int32_t (*)(int64_t const *, int32_t, int64_t, int32_t *, int32_t, int64_t const *, int64_t const *,
    int32_t *, int32_t *, int64_t, int64_t *);

typedef struct LiteralValue {
    bool isNull = false;
    union Value {
        bool boolVal;
        int16_t shortVal;
        int32_t intVal;
        int64_t longVal;
        double doubleVal;
        Decimal128 decimal128Val;
        std::string_view stringVal;

        Value() {}
    } value;
    LiteralValue() {}
} LiteralValue;

void GetAddr(VectorBatch &vecBatch, intptr_t valueAddrs[], intptr_t nullAddrs[], intptr_t offsetAddrs[],
    intptr_t dictionaries[], const DataTypes &types);

class Filter {
public:
    explicit Filter(const Expr &expression, const DataTypes &inputDataTypes, OverflowConfig *overflowConfig);

    ~Filter() = default;

    FilterFunc GetFilterFunc()
    {
        return apply;
    }

    bool IsSupported()
    {
        return isSupported;
    }

private:
    std::unique_ptr<FilterCodeGen> codeGen;
    std::unique_ptr<BatchFilterCodeGen> batchCodeGen;
    bool isSupported;
    FilterFunc apply;
};

class Projection {
public:
    Projection(const Expr &expr, bool filter, DataTypePtr outType, const DataTypes &inputDataTypes,
        OverflowConfig *overflowConfig);

    ~Projection() = default;

    void ProjectHelperFixedWidth(VectorBatch &vecBatch, int64_t *valueAddrs, int64_t *nullAddrs, int64_t *offsetAddrs,
        BaseVector *outVec, int32_t numSelectedRows, int32_t selectedRows[], ExecutionContext *context,
        int64_t *dictionaryVectors, DataTypeId &typeIds) const;

    void ProjectHelperVarWidth(VectorBatch &vecBatch, int64_t *valueAddrs, int64_t *nullAddrs, int64_t *offsetAddrs,
        BaseVector *outVec, int32_t numSelectedRows, int32_t selectedRows[], ExecutionContext *context,
        int64_t *dictionaryVectors, DataTypeId &typeIds) const;

    BaseVector *Project(VectorBatch *vecBatch, int32_t selectedRows[], int32_t numSelectedRows, int64_t *valueAddrs,
        int64_t *nullAddrs, int64_t *offsetAddrs, ExecutionContext *context, int64_t *dictionaryVectors,
        const int32_t *typeIds);

    BaseVector *Project(VectorBatch *vecBatch, int64_t *valueAddrs, int64_t *nullAddrs, int64_t *offsetAddrs,
        ExecutionContext *context, int64_t *dictionaryVectors, const int32_t *typeIds);

    omniruntime::type::DataType &GetOutputType() const
    {
        return *(this->outType);
    }

    ProjFunc GetProjector() const
    {
        return projector;
    }

    bool IsSupported()
    {
        return isSupported;
    }

    bool IsColumnProjection() const
    {
        return isColumnProjection;
    }

    int GetColumnProjectionIndex() const
    {
        return columnProjectionIndex;
    }

private:
    const omniruntime::expressions::Expr *expr;
    std::unique_ptr<ProjectionCodeGen> codeGen { nullptr };
    std::unique_ptr<BatchProjectionCodeGen> batchCodeGen { nullptr };
    bool isSupported = true;
    bool isColumnProjection = false;
    int columnProjectionIndex = -1;
    bool isConstantProjection = false;
    LiteralValue literalVal;
    DataTypePtr outType;
    ProjFunc projector;

    bool Initialize(bool filter, const DataTypes &inputDataTypes, OverflowConfig *overflowConfig);
    BaseVector *ColumnProjectionProxy(VectorBatch *vecBatch, int32_t selectedRows[], int32_t numSelectedRows,
        const int32_t *typeIds) const;

    template <typename T>
    BaseVector *ColumnProjectionHelper(VectorBatch *vecBatch, const int32_t selectedRows[],
        int32_t numSelectedRows) const;

    template <typename T>
    BaseVector *ColumnProjectionVarCharVectorHelper(VectorBatch *vecBatch, const int32_t *selectedRows,
        int32_t numSelectedRows) const;

    template <typename T>
    BaseVector *ColumnProjectionFlatVectorSliceHelper(int32_t numSelectedRows, BaseVector *colVec) const;

    template <typename T>
    BaseVector *ColumnProjectionDictionaryVectorSliceHelper(int32_t numSelectedRows, BaseVector *colVec) const;

    template <typename T>
    BaseVector *ColumnProjectionDictionaryVectorCopyPositionsHelper(const int32_t *selectedRows,
        int32_t numSelectedRows, BaseVector *colVec) const;

    template <typename T>
    BaseVector *ColumnProjectionFlatVectorCopyPositionsHelper(const int32_t *selectedRows, int32_t numSelectedRows,
        BaseVector *colVec) const;

    bool SetLiteralValue(const LiteralExpr *literalExpr);

    bool NullColumnProjection(ExecutionContext *context, BaseVector *outVec);

    bool ConstantColumnProjection(ExecutionContext *context, BaseVector *outVec);

    template <typename T> void SetConstantValues(T &value, BaseVector *outVec);
};

class ExpressionEvaluator {
public:
    ExpressionEvaluator(Expr *filterExpression, const std::vector<Expr *> &projectionExprs,
        const DataTypes &inputDataTypes, OverflowConfig *ofConfig);

    ExpressionEvaluator(const std::vector<Expr *> &projectionExprs, const DataTypes &inputDataTypes,
        OverflowConfig *ofConfig);

    ~ExpressionEvaluator()
    {
        if (filterExpr) {
            delete filterExpr;
        }
        for (size_t i = 0; i < projExprs.size(); ++i) {
            delete projExprs[i];
        }
        projExprs.clear();
    }

    DataTypes &GetInputDataTypes()
    {
        return inputTypes;
    }

    std::vector<type::DataTypePtr> &GetOutputDataTypes()
    {
        return outputTypes;
    }

    FilterFunc GetFilterFunc()
    {
        return filter->GetFilterFunc();
    }

    std::vector<std::unique_ptr<Projection>> &GetProjections()
    {
        return projections;
    }

    int32_t GetProjectVecCount()
    {
        return projectVecCount;
    }

    VectorBatch *Evaluate(VectorBatch *vecBatch, ExecutionContext *context,
        AlignedBuffer<int32_t> *selectedRowsBuffer = nullptr);

    bool IsSupportedExpr() const;

    void FilterFuncGeneration();

    void ProjectFuncGeneration();

private:
    Expr *filterExpr = nullptr;
    std::vector<Expr *> projExprs;
    int32_t projectVecCount = 0;
    std::unique_ptr<OverflowConfig> overflowConfig;
    bool isSupportedExpr = true;
    bool hasFilter = false;
    DataTypes inputTypes;
    std::vector<type::DataTypePtr> outputTypes;

    std::unique_ptr<Filter> filter;
    std::vector<std::unique_ptr<Projection>> projections;

    VectorBatch *ProcessFilterAndProject(VectorBatch *vecBatch, ExecutionContext *context,
        AlignedBuffer<int32_t> *selectedRowsBuffer, intptr_t *valueAddrs, intptr_t *nullAddrs, intptr_t *offsetAddrs,
        intptr_t *dictionaries);

    VectorBatch *ProcessProject(VectorBatch *vecBatch, ExecutionContext *context, intptr_t *valueAddrs,
        intptr_t *nullAddrs, intptr_t *offsetAddrs, intptr_t *dictionaries);
};
}
#endif // OMNI_RUNTIME_EXPR_EVALUATOR_H
