/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: visitor class for expressions
 */

#pragma once

#include <folly/container/F14Map.h>
#include <folly/Range.h>
#include "CastHooks.h"
#include "vectorization/VectorFunction.h"
#include "vectorization/SelectivityVector.h"
#include "expression/expressions.h"

namespace omniruntime::vectorization {
using namespace omniruntime::type;
using namespace omniruntime::op;
using namespace omniruntime::vec;
using namespace omniruntime::expressions;

constexpr folly::StringPiece kCast = "cast";
constexpr folly::StringPiece kTryCast = "try_cast";

/// Custom operator for casts from and to custom types.
class CastOperator {
public:
    virtual ~CastOperator() = default;

    /// Determines whether the cast operator supports casting to the custom type
    /// from the other type.
    virtual bool isSupportedFromType(const DataTypePtr &other) const = 0;

    /// Determines whether the cast operator supports casting from the custom type
    /// to the other type.
    virtual bool isSupportedToType(const DataTypePtr &other) const = 0;

    /// Casts an input vector to the custom type. This function should not throw
    /// when processing input rows, but report errors via context.setError().
    /// @param input The flat or constant input vector
    /// @param context The context
    /// @param rows Non-null rows of input
    /// @param resultType The result type.
    /// @param result The result vector of the custom type
    virtual void castTo(BaseVector *input, ExecutionContext &context, const SelectivityVector &rows,
        const DataTypePtr &resultType, VectorPtr &result) const = 0;

    virtual void castTo(BaseVector *input, ExecutionContext &context, const SelectivityVector &rows,
        const DataTypePtr &resultType, VectorPtr &result, const std::shared_ptr<CastHooks> & /* hooks */) const
    {
        castTo(input, context, rows, resultType, result);
    }

    /// Casts a vector of the custom type to another type. This function should
    /// not throw when processing input rows, but report errors via
    /// context.setError().
    /// @param input The flat or constant input vector
    /// @param context The context
    /// @param rows Non-null rows of input
    /// @param resultType The result type
    /// @param result The result vector of the destination type
    virtual void castFrom(BaseVector *input, ExecutionContext &context, const SelectivityVector &rows,
        const DataTypePtr &resultType, VectorPtr &result) const = 0;
};

using CastOperatorPtr = std::shared_ptr<const CastOperator>;

class CastExpr : public VectorFunction {
public:
    CastExpr(const DataTypePtr &fromType, const DataTypePtr &toType, bool nullOnFailure,
        std::shared_ptr<CastHooks> hooks)
        : fromType_(fromType), toType_(toType), hooks_(std::move(hooks)), nullOnFailure_(nullOnFailure) {}

    void Apply(std::stack<vec::BaseVector *> &args, const type::DataTypePtr &outputType, vec::BaseVector *&result,
        op::ExecutionContext *context) const override;

private:
    /// Apply the cast after generating the input vectors
    /// @param rows The list of rows being processed
    /// @param input The input vector to be casted
    /// @param context The context
    /// @param fromType the input type
    /// @param toType the target type
    /// @param result The result vector
    void apply(const SelectivityVector &rows, BaseVector *input, ExecutionContext &context, const DataTypePtr &fromType,
        const DataTypePtr &toType, VectorPtr &result);

    VectorPtr applyMap(const SelectivityVector &rows, const MapVector *input, ExecutionContext &context,
        const MapType &fromType, const MapType &toType);

    VectorPtr applyArray(const SelectivityVector &rows, const ArrayVector *input, ExecutionContext &context,
        const ArrayType &fromType, const ArrayType &toType);

    VectorPtr applyRow(const SelectivityVector &rows, const RowVector *input, ExecutionContext &context,
        const RowType &fromType, const DataTypePtr &toType);

    /// Apply the cast between decimal vectors.
    /// @param rows Non-null rows of the input vector.
    /// @param input The input decimal vector. It is guaranteed to be flat or
    /// constant.
    template <typename ToDecimalType>
    VectorPtr applyDecimal(const SelectivityVector &rows, BaseVector *input, ExecutionContext &context,
        const DataTypePtr &fromType, const DataTypePtr &toType);

    // Apply the cast to a vector after vector encodings being peeled off. The
    // input vector is guaranteed to be flat or constant.
    void applyPeeled(const SelectivityVector &rows, BaseVector *input, ExecutionContext &context,
        const DataTypePtr &fromType, const DataTypePtr &toType, VectorPtr &result);

    template <typename Func>
    void applyToSelectedNoThrowLocal(ExecutionContext &context, const SelectivityVector &rows, BaseVector *result,
        Func &&func);

    template <DataTypeId ToKind, DataTypeId FromKind, typename TPolicy>
    void applyCastKernel(vector_size_t row, ExecutionContext &context, BaseVector *input, BaseVector *result);

    VectorPtr castFromDate(const SelectivityVector &rows, BaseVector *input, ExecutionContext &context,
        const DataTypePtr &toType);

    VectorPtr castToDate(const SelectivityVector &rows, BaseVector *input, ExecutionContext &context,
        const DataTypePtr &fromType);

    VectorPtr castFromIntervalDayTime(const SelectivityVector &rows, BaseVector *input, ExecutionContext &context,
        const DataTypePtr &toType);

    template <typename TInput, typename TOutput>
    void applyDecimalCastKernel(const SelectivityVector &rows, BaseVector *input, ExecutionContext &context,
        const DataTypePtr &fromType, const DataTypePtr &toType, VectorPtr &castResult);

    template <typename TInput, typename TOutput>
    void applyIntToDecimalCastKernel(const SelectivityVector &rows, BaseVector *input, ExecutionContext &context,
        const DataTypePtr &toType, VectorPtr &castResult);

    template <typename TInput>
    VectorPtr applyIntToBinaryCast(const SelectivityVector &rows, ExecutionContext &context, BaseVector *input);

    template <typename TInput, typename TOutput>
    void applyFloatingPointToDecimalCastKernel(const SelectivityVector &rows, BaseVector *input,
        ExecutionContext &context, const DataTypePtr &toType, VectorPtr &castResult);

    template <typename T>
    void applyVarcharToDecimalCastKernel(const SelectivityVector &rows, BaseVector *input, ExecutionContext &context,
        const DataTypePtr &toType, VectorPtr &castResult);

    template <typename FromNativeType, DataTypeId ToKind>
    VectorPtr applyDecimalToFloatCast(const SelectivityVector &rows, BaseVector *input, ExecutionContext &context,
        const DataTypePtr &fromType, const DataTypePtr &toType);

    template <typename FromNativeType, DataTypeId ToKind>
    VectorPtr applyDecimalToIntegralCast(const SelectivityVector &rows, BaseVector *input, ExecutionContext &context,
        const DataTypePtr &fromType, const DataTypePtr &toType);

    template <typename FromNativeType>
    VectorPtr applyDecimalToBooleanCast(const SelectivityVector &rows, BaseVector *input, ExecutionContext &context);

    template <typename FromNativeType>
    VectorPtr applyDecimalToPrimitiveCast(const SelectivityVector &rows, BaseVector *input, ExecutionContext &context,
        const DataTypePtr &fromType, const DataTypePtr &toType);

    template <DataTypeId ToKind, DataTypeId FromKind>
    void applyCastPrimitives(const SelectivityVector &rows, ExecutionContext &context, BaseVector *input,
        VectorPtr &result);

    template <typename FromNativeType>
    VectorPtr applyDecimalToVarcharCast(const SelectivityVector &rows, BaseVector *input, ExecutionContext &context,
        const DataTypePtr &fromType);

    template <DataTypeId ToKind>
    void applyCastPrimitivesDispatch(const DataTypePtr &fromType, const DataTypePtr &toType,
        const SelectivityVector &rows, ExecutionContext &context, BaseVector *input, VectorPtr &result);

    VectorPtr applyTimestampToVarcharCast(const DataTypePtr &toType, const SelectivityVector &rows,
        ExecutionContext &context, BaseVector *input);

    bool nullOnFailure() const
    {
        return nullOnFailure_;
    }

    bool setNullInResultAtError() const
    {
        return nullOnFailure();
    }

    CastOperatorPtr getCastOperator(const DataTypePtr &type);

    // Custom cast operators for to and from top-level as well as nested types.
    folly::F14FastMap<std::string, CastOperatorPtr> castOperators_;

    bool nullOnFailure_;
    std::shared_ptr<CastHooks> hooks_;

    bool inTopLevel = false;
    DataTypePtr fromType_;
    DataTypePtr toType_;
};

class CastCallToSpecialForm {
public:
    DataTypePtr resolveType(const std::vector<DataTypePtr> &argTypes);

    std::shared_ptr<Expr> constructSpecialForm(const DataTypePtr &type,
        std::vector<std::shared_ptr<Expr>> &&compiledChildren, bool trackCpuUsage, const config::QueryConfig &config);
};

class TryCastCallToSpecialForm {
public:
    DataTypePtr resolveType(const std::vector<DataTypePtr> &argTypes);

    std::shared_ptr<Expr> constructSpecialForm(const DataTypePtr &type,
        std::vector<std::shared_ptr<Expr>> &&compiledChildren, bool trackCpuUsage, const config::QueryConfig &config);
};
}

#include "CastExpr-inl.h"
