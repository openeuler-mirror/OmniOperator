/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: visitor class for expressions
 */

#pragma once

#include <optional>

#include "vector/unsafe_vector.h"
#include "vector/vector.h"
#include "vector/array_vector.h"
#include "vector/map_vector.h"
#include "SimpleFunctionApi.h"
#include "UdfTypeResolver.h"

namespace omniruntime::vectorization {
using namespace omniruntime::vec;

template <typename T>
struct VectorReader {
    using exec_in_t = T;

    explicit VectorReader(BaseVector *decoded) : decoded_(decoded) {}

    explicit VectorReader(const VectorReader<T> &) = delete;

    VectorReader<T> &operator=(const VectorReader<T> &) = delete;

    exec_in_t operator[](size_t offset) const
    {
        if (decoded_->GetEncoding() == OMNI_FLAT) {
            return static_cast<Vector<exec_in_t> *>(decoded_)->GetValue(offset);
        } else {
            OMNI_THROW("Runtime error:", "VectorReader error!");
        }
    }

    bool mayHaveNulls() const
    {
        return decoded_->HasNull();
    }

    // These functions can be used to check if any elements in a given row are
    // NULL. They are not especially fast, so they should only be used when
    // necessary, and other options, e.g. calling mayHaveNullsRecursive() on the
    // vector, have already been exhausted.
    inline bool containsNull(int32_t index) const
    {
        return decoded_->IsNull(index);
    }

    bool containsNull(int32_t startIndex, int32_t endIndex) const
    {
        // Note: This can be optimized for the special case where the underlying
        // vector is flat using bit operations on the nulls buffer.
        for (auto index = startIndex; index < endIndex; ++index) {
            if (containsNull(index)) {
                return true;
            }
        }

        return false;
    }

    // Scalars don't have children, so this is a no-op.
    void setChildrenMayHaveNulls() {}

    BaseVector *decoded_;
};

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

    FlatVectorReader() = default;

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

template <bool nullable, typename V>
class ArrayView;
struct StringVectorReader;

template <typename V>
struct VectorReader<Array<V>> {
    using exec_in_t = ArrayView<true, V>;
    using child_reader_t = std::conditional_t<std::is_same_v<V, std::string_view>, StringVectorReader, FlatVectorReader<
        V>>;

    // TODO:
    explicit VectorReader(BaseVector *decoded)
        : vector_(dynamic_cast<ArrayVector *>(decoded)), childReader_(child_reader_t(vector_->GetElementVector().get()))
    {
        offsets_ = vector_->GetOffsets();
    }

    exec_in_t operator[](size_t offset) const
    {
        return {&childReader_, offsets_[offset], offsets_[offset + 1] - offsets_[offset]};
    }

    void setChildrenMayHaveNulls()
    {
        childReader_.setChildrenMayHaveNulls();
        valuesMayHaveNulls_ = childReader_.mayHaveNullsRecursive();
    }

    ArrayVector *vector_;
    int64_t *offsets_;
    child_reader_t childReader_;
    std::optional<bool> valuesMayHaveNulls_;
};

template <typename T>
struct DicVectorReader {
    Vector<DictionaryContainer<T>> *vector;

    explicit DicVectorReader(BaseVector *vector)
        : vector(reinterpret_cast<Vector<DictionaryContainer<T>> *>(vector)) {}

    ~DicVectorReader()
    {
        if (!vector->GetIsField()) {
            delete vector;
        }
    }

    T operator[](int32_t offset) const
    {
        return vector->GetValue(offset);
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

struct MapVectorReader {
    vec::MapVector *vector;

    explicit MapVectorReader(BaseVector *vec)
    {
        vector = dynamic_cast<vec::MapVector *>(vec);
        if (vector == nullptr) {
            throw std::invalid_argument("MapVectorReader: Input BaseVector pointer type does not match.");
        }
    }

    int64_t GetSize(int32_t offset) const
    {
        if (vector == nullptr || offset >= GetVectorSize() || vector->IsNull(offset)) {
            return 0;
        }
        return vector->GetSize(offset);
    }

    int64_t GetOffset(int32_t offset) const
    {
        if (vector == nullptr || offset >= GetVectorSize()) {
            return 0;
        }
        return vector->GetOffset(offset);
    }

    const std::shared_ptr<BaseVector> GetKeyVector() const
    {
        return vector->GetKeyVector();
    }

    const std::shared_ptr<BaseVector> GetValueVector() const
    {
        return vector->GetValueVector();
    }

    bool containsNull(int32_t offset) const
    {
        return vector == nullptr || offset >= GetVectorSize() || vector->IsNull(offset);
    }

    int32_t size() const
    {
        return GetVectorSize();
    }

private:
    int32_t GetVectorSize() const
    {
        return vector ? vector->vec::BaseVector::GetSize() : 0;
    }
};
}
