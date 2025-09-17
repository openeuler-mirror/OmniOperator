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

    bool containsNull(int32_t index) const
    {
        return vector->IsNull(index);
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

template <typename T>
struct ConstVectorReader {
    T value;
    BaseVector *vector;

    explicit ConstVectorReader(BaseVector *vector)
        : vector(vector)
    {
        value = reinterpret_cast<ConstVector<T> *>(vector)->GetConstValue();
    }

    ~ConstVectorReader()
    {
        if (!vector->GetIsField()) {
            delete vector;
        }
    }

    T operator[](int32_t offset) const
    {
        return value;
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

struct StringVectorReader {
    Vector<LargeStringContainer<std::string_view>> *vector;

    explicit StringVectorReader(BaseVector *vec)
    {
        vector = dynamic_cast<Vector<LargeStringContainer<std::string_view>> *>(vec);
        if (vector == nullptr) {
            throw std::invalid_argument("StringVectorReader: Input BaseVector pointer type does not match.");
        }
    }

    std::string_view operator[](int32_t offset) const
    {
        if (vector == nullptr || offset >= vector->GetSize() || vector->IsNull(offset)) {
            return std::string_view();
        }
        auto value = vector->GetValue(offset);
        return value;
    }

    bool containsNull(int32_t offset) const
    {
        return vector == nullptr || offset >= vector->GetSize() || vector->IsNull(offset);
    }

    int32_t size() const
    {
        return vector ? vector->GetSize() : 0;
    }
};
}
