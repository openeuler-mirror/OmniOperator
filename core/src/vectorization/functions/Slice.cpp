/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: Slice function implementation
 */

#include "Slice.h"
#include "vector/array_vector.h"
#include "vector/vector.h"
#include "vector/vector_helper.h"
#include "util/compiler_util.h"

namespace omniruntime::vectorization {
using namespace omniruntime::type;
using namespace omniruntime::vec;
using namespace omniruntime::op;

void SliceImpl::Apply(std::stack<BaseVector *> &args, const DataTypePtr &outputType, BaseVector *&result,
    ExecutionContext *context) const
{
    std::vector<BaseVector *> inputs;
    inputs.push_back(args.top());  // length
    args.pop();
    inputs.push_back(args.top());  // start
    args.pop();
    inputs.push_back(args.top());  // array
    args.pop();

    auto size = inputs[2]->GetSize();
    auto nullBits = reinterpret_cast<uint64_t *>(unsafe::UnsafeBaseVector::GetNulls(inputs[2]));
    auto rows = SelectivityVector(size);
    rows.setFromBitsNegate(nullBits, size);

    // Check if start and length arguments are the same type
    if (inputs[1]->GetTypeId() != inputs[0]->GetTypeId()) {
        OMNI_THROW("Slice error:", "Start and length arguments must have the same type");
    }

    switch (inputs[1]->GetTypeId()) {
        case OMNI_INT:
            result = applySliceTyped<int32_t>(rows, inputs[2], inputs[1], inputs[0], context);
            break;
        case OMNI_LONG:
            result = applySliceTyped<int64_t>(rows, inputs[2], inputs[1], inputs[0], context);
            break;
        default:
            OMNI_THROW("Slice error:", "Unsupported type for start/length arguments: {}", 
                std::to_string(inputs[1]->GetTypeId()));
    }

    for (auto input : inputs) {
        if (input != nullptr) {
            delete input;
        }
    }
}

template <typename I>
BaseVector *SliceImpl::applySliceTyped(const SelectivityVector &rows, BaseVector *arrayArg, BaseVector *startArg,
    BaseVector *lengthArg, ExecutionContext *context) const
{
    auto arrayVector = dynamic_cast<ArrayVector *>(arrayArg);
    if (arrayVector == nullptr) {
        OMNI_THROW("Slice error:", "First argument must be an array");
    }

    auto rowSize = arrayVector->GetSize();
    auto offsets = arrayVector->GetOffsets();
    auto elementVector = arrayVector->GetElementVector();

    // Create result array vector
    auto result = new ArrayVector(rowSize);
    auto resultOffsets = result->GetOffsets();

    // Helper function to adjust start index (1-based to 0-based, handle negative indices)
    auto adjustStart = [](I start, int64_t arraySize) -> int64_t {
        // Spark semantics: start is 1-based, 0 is invalid
        if (start == 0) {
            OMNI_THROW("Slice error:", "Array indices start at 1, got 0");
        }
        // Convert 1-based to 0-based
        if (start > 0) {
            start = start - 1;
        }
        // Handle negative indices (wrap around)
        if (start < 0) {
            start = arraySize + start;
        }
        return static_cast<int64_t>(start);
    };

    // Get start and length values (assuming const for now)
    I startValue = 0;
    I lengthValue = 0;

    if (auto startConst = dynamic_cast<ConstVector<I> *>(startArg)) {
        startValue = startConst->GetConstValue();
    } else {
        OMNI_THROW("Slice error:", "Start argument must be constant");
    }

    if (auto lengthConst = dynamic_cast<ConstVector<I> *>(lengthArg)) {
        lengthValue = lengthConst->GetConstValue();
    } else {
        OMNI_THROW("Slice error:", "Length argument must be constant");
    }

    // Validate length
    if (lengthValue < 0) {
        OMNI_THROW("Slice error:", "Length argument must be non-negative, got {}", lengthValue);
    }

    // Zero-copy implementation: reuse the original elementVector and only adjust offsets
    // This is similar to Velox's implementation which achieves zero-copy by reusing
    // base vector and adjusting rawOffsets and rawSizes vectors
    
    rows.applyToSelected([&](auto row) {
        // Get array size for this row
        int64_t arraySize = offsets[row + 1] - offsets[row];
        
        // Adjust start index
        int64_t adjustedStart = adjustStart(startValue, arraySize);
        
        // Calculate actual start and end positions in the element vector
        int64_t elementStart = offsets[row] + adjustedStart;
        int64_t elementEnd = offsets[row + 1];
        
        // Calculate actual length (don't exceed array bounds)
        int64_t actualLength = std::min(static_cast<int64_t>(lengthValue), elementEnd - elementStart);
        
        // Copy null bits if the original array was null
        if (arrayVector->IsNull(row)) {
            result->SetNull(row);
            resultOffsets[row] = elementStart;  // Use original position
            resultOffsets[row + 1] = elementStart;  // Empty array (size = 0)
            return;
        }
        
        // If start is out of bounds or length is 0, return empty array (not NULL)
        if (adjustedStart < 0 || adjustedStart >= arraySize || actualLength <= 0) {
            resultOffsets[row] = elementStart;  // Use original position
            resultOffsets[row + 1] = elementStart;  // Empty array (size = 0)
            return;
        }
        
        // Set offsets to point to the sliced range in the original elementVector
        // This achieves zero-copy by reusing the original elementVector
        resultOffsets[row] = elementStart;
        resultOffsets[row + 1] = elementStart + actualLength;
    });
    
    // Reuse the original elementVector for zero-copy
    // The offsets above point to the correct positions in the original elementVector
    result->SetElementVector(elementVector);

    return result;
}

// Explicit template instantiation
template BaseVector *SliceImpl::applySliceTyped<int32_t>(const SelectivityVector &rows, BaseVector *arrayArg,
    BaseVector *startArg, BaseVector *lengthArg, ExecutionContext *context) const;

template BaseVector *SliceImpl::applySliceTyped<int64_t>(const SelectivityVector &rows, BaseVector *arrayArg,
    BaseVector *startArg, BaseVector *lengthArg, ExecutionContext *context) const;
}