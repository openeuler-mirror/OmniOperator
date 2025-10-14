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

    void apply(std::stack<VectorPtr> &args, const DataTypePtr &outputType, BaseVector *&result,
        ExecutionContext *context) const override
    {
        std::vector<VectorPtr> inputs;
        inputs.push_back(args.top());
        args.pop();
        inputs.push_back(args.top());
        args.pop();

        auto size = inputs[1]->GetSize();
        auto nullBits = reinterpret_cast<uint64_t *>(unsafe::UnsafeBaseVector::GetNulls(inputs[1]));
        auto rows = SelectivityVector(size);
        rows.setFromBitsNegate(nullBits, size);
        switch (inputs[1]->GetTypeId()) {
            case OMNI_ARRAY:
                result = applyArray(rows, inputs, context);
                break;
            default: OMNI_THROW("SubscriptImpl error:", "Not support type!");
        }
    }

    VectorPtr applyArray(const SelectivityVector &rows, std::vector<VectorPtr> &args, ExecutionContext *context) const
    {
        auto arrayArg = args[1];
        auto indexArg = args[0];

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
        ExecutionContext *context) const
    {
        auto rowSize = arrayArg->GetSize();
        int32_t dicIndex[rowSize];
        memset(dicIndex, -1, sizeof(dicIndex));
        auto arrayVector = dynamic_cast<ArrayVector *>(arrayArg);

        I index = 0;
        if (auto indexVector = dynamic_cast<ConstVector<I> *>(indexArg)) {
            index = indexVector->GetConstValue();
        } else {
            OMNI_THROW("Runtime Error:", "Index only supported const type!");
        }
        auto offset = arrayVector->GetOffsets();
        rows.applyToSelected([&](auto row) {
            if (offset[row] + index < offset[row + 1]) {
                dicIndex[row] = offset[row] + index;
            } else {
                dicIndex[row] = -1;
            }
        });

        auto rawVector = reinterpret_cast<Vector<int32_t> *>(arrayVector->GetElementVector().get())->Slice(0, rowSize);
        auto result = VectorHelper::CreateDictionaryVector(dicIndex, rowSize, rawVector, rawVector->GetTypeId());
        for (auto i = 0; i < rowSize; i++) {
            if (dicIndex[i] == -1) {
                result->SetNull(i);
            }
        }
        return result;
    }
};
}
