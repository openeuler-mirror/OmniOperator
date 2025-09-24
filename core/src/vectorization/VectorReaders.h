/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: visitor class for expressions
 */

#pragma once

#include <optional>

#include "vector/unsafe_vector.h"
#include "vector/vector.h"

namespace omniruntime {
using namespace omniruntime::vec;
// ConstantVectorReader and FlatVectorReader are optimized for primitive types
// in constant or flat encoded vectors.  They operate directly on the vector's
// content avoiding the need to go through the expensive decoding process.
template <typename T>
struct ConstantVectorReader {
    std::optional<T> value;

    explicit ConstantVectorReader(T inValue)
    {
        value = inValue;
    }

    T operator[](int32_t) const
    {
        return value.value();
    }
};

template <typename T>
struct FlatVectorReader {
    const T *values;
    BaseVector *vector;

    explicit FlatVectorReader(BaseVector *vector)
        : values(unsafe::UnsafeVector::GetRawValues(reinterpret_cast<Vector<T> *>(vector))), vector(vector) {}

    ~FlatVectorReader()
    {
        if (!vector->GetIsField()) {
            delete vector;
        }
    }

    T operator[](int32_t offset) const
    {
        return values[offset];
    }

    T readNullFree(int32_t offset) const
    {
        return operator[](offset);
    }

    bool containsNull(int32_t startIndex, int32_t endIndex) const
    {
        for (auto index = startIndex; index < endIndex; ++index) {
            if (containsNull(index)) {
                return true;
            }
        }
        return false;
    }
};

template <typename T>
struct DicVectorReader {
    const T *values;
    vec::BaseVector *vector;

    explicit DicVectorReader(vec::BaseVector *vector)
        : values(vec::unsafe::UnsafeVector::GetRawValues(reinterpret_cast<vec::Vector<T> *>(vector))), vector(vector) {}

    T operator[](int32_t offset) const
    {
        return values[offset];
    }

    T readNullFree(int32_t offset) const
    {
        return operator[](offset);
    }

    bool containsNull(int32_t startIndex, int32_t endIndex) const
    {
        for (auto index = startIndex; index < endIndex; ++index) {
            if (containsNull(index)) {
                return true;
            }
        }
        return false;
    }
};
}
