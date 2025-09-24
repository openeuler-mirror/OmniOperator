/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: visitor class for expressions
 */

#pragma once
#include "vectorization/VectorFunction.h"
#include "vectorization/SelectivityVector.h"

namespace omniruntime::vectorization {
using namespace omniruntime::type;
using namespace omniruntime::vec;
using namespace omniruntime::op;

/// Generic subscript/element_at implementation for both array and map data
/// types.
///
/// Provides four template parameters to configure the behavior:
/// - allowNegativeIndices: if allowed, negative indices accesses elements
/// from
///   last to the first; otherwise, throws.
/// - nullOnNegativeIndices: returns NULL for negative indices instead of the
///   behavior described above.
/// - allowOutOfBound: if allowed, returns NULL for out of bound accesses; if
///   false, throws an exception.
/// - indexStartsAtOne: whether indices start at zero or one.
class SubscriptImpl : public VectorFunction {
public:
    explicit SubscriptImpl() {}

    void apply(std::stack<VectorPtr> &args, const DataTypePtr &outputType, BaseVector *result,
        ExecutionContext *context) const override
    {
        std::vector<VectorPtr> inputs;

        VectorPtr localResult;
        auto arg = args.top();
        inputs.push_back(arg);
        args.pop();
        inputs.push_back(args.top());
        args.pop();

        auto size = arg->GetSize();
        auto nullBits = reinterpret_cast<uint64_t *>(unsafe::UnsafeBaseVector::GetNulls(arg));
        auto rows = SelectivityVector(size);
        rows.setFromBitsNegate(nullBits, size);
        switch (arg->GetTypeId()) {
            case OMNI_ARRAY:
                localResult = applyArray(rows, inputs, context);
                break;
            default: OMNI_THROW("SubscriptImpl error:", "Not support type!");
        }
    }

    VectorPtr applyArray(const SelectivityVector &rows, std::vector<VectorPtr> &args, ExecutionContext *context) const
    {
        auto arrayArg = args[0];
        auto indexArg = args[1];

        switch (indexArg->GetTypeId()) {
            case OMNI_INT:
                return applyArrayTyped<int32_t>(rows, arrayArg, indexArg, context);
            case OMNI_LONG:
                return applyArrayTyped<int64_t>(rows, arrayArg, indexArg, context);
            default: OMNI_THROW("Unsupported type for element_at index {}", std::to_string(indexArg->GetTypeId()));
        }
    }

    template <typename I>
    VectorPtr applyArrayTyped(const SelectivityVector &rows, const VectorPtr &arrayArg, const VectorPtr &indexArg,
        ExecutionContext *context) const {}
};
}
