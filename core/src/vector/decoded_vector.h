/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
 * Description: DecodedVector - unwrap dictionary/constant encodings into flat values + null bitmap
 */
#ifndef OMNI_RUNTIME_DECODED_VECTOR_H
#define OMNI_RUNTIME_DECODED_VECTOR_H

#include <type_traits>
#include "vector/vector.h"
#include "vector/unsafe_vector.h"
#include "util/compiler_util.h"

namespace omniruntime::vec {

/// Layout after decoding: flat values are directly accessible, nulls are a bitmap.
/// Dictionary and constant encodings are resolved once at decode time so the hot
/// path runs with zero encoding branches.
enum class DVecLayout {
    Flat,       // original flat vector, zero-cost wrap
    Constant,   // constant value broadcast to all rows
    Dictionary, // dictionary: ids[] points into dictValues[]
};

class DecodedVector {
public:
    DecodedVector() = default;

    /// Decode a BaseVector into flat values + null bitmap.
    /// For Flat vectors this is a zero-cost wrap (pointers only).
    /// For Constant vectors the single value is stored and all indices map to it.
    /// For Dictionary vectors the dict values and ids are cached.
    void Decode(BaseVector *vector, int32_t rowCount)
    {
        this->vector = vector;
        this->rowCount = rowCount;
        this->nulls = unsafe::UnsafeBaseVector::GetNulls(vector);
        this->hasNull = vector->HasNull();

        auto encoding = vector->GetEncoding();
        if (encoding == OMNI_ENCODING_CONST) {
            this->layout = DVecLayout::Constant;
        } else if (encoding == OMNI_DICTIONARY) {
            this->layout = DVecLayout::Dictionary;
            this->dictIds = unsafe::UnsafeDictionaryVector::GetIds(
                static_cast<Vector<DictionaryContainer<int64_t>>*>(vector));
        } else {
            this->layout = DVecLayout::Flat;
        }
    }

    /// For fixed-width types: get the raw flat value pointer.
    /// For Flat: returns the vector's values buffer directly.
    /// For Dictionary: returns the dictionary's values buffer (use with ids).
    ///   For string_view types this returns nullptr; use GetValue() instead.
    /// For Constant: returns nullptr, use GetConstValue<T>() instead.
    template <typename T>
    ALWAYS_INLINE const T *FlatValues() const
    {
        if (layout == DVecLayout::Flat) {
            return unsafe::UnsafeVector::GetRawValues(
                static_cast<Vector<T> *>(vector));
        }
        if (layout == DVecLayout::Dictionary) {
            if constexpr (std::is_same_v<T, std::string_view>) {
                return nullptr;
            } else {
                return GetDictValues<T>();
            }
        }
        return nullptr;
    }

    template <typename T>
    ALWAYS_INLINE T GetConstValue() const
    {
        return static_cast<ConstVector<T> *>(vector)->GetConstValue();
    }

    /// For dictionary layout: get the index array (already offset-adjusted).
    ALWAYS_INLINE const int32_t *Ids() const
    {
        if (layout == DVecLayout::Dictionary) {
            return static_cast<const int32_t *>(dictIds);
        }
        return nullptr;
    }

    ALWAYS_INLINE DVecLayout GetLayout() const { return layout; }
    ALWAYS_INLINE BaseVector *Base() const { return vector; }
    ALWAYS_INLINE int32_t RowCount() const { return rowCount; }
    ALWAYS_INLINE const uint8_t *Nulls() const { return nulls; }
    ALWAYS_INLINE bool HasNull() const { return hasNull; }
    ALWAYS_INLINE DataTypeId GetTypeId() const { return vector->GetTypeId(); }

    /// Check null at a given row index.
    ALWAYS_INLINE bool IsNull(int32_t idx) const
    {
        if (!hasNull) return false;
        return BitUtil::IsBitSet(reinterpret_cast<const uint64_t*>(nulls), idx);
    }

    /// Get value at index for fixed-width types. Handles all layouts.
    /// For string_view + Dictionary: falls back to dictionary GetValue (LargeStringContainer).
    template <typename T>
    ALWAYS_INLINE T GetValue(int32_t idx) const
    {
        if (layout == DVecLayout::Flat) {
            return unsafe::UnsafeVector::GetRawValues(
                static_cast<Vector<T> *>(vector))[idx];
        }
        if (layout == DVecLayout::Constant) {
            return static_cast<ConstVector<T> *>(vector)->GetConstValue();
        }
        // Dictionary
        if constexpr (std::is_same_v<T, std::string_view>) {
            return static_cast<Vector<DictionaryContainer<T>> *>(vector)->GetValue(idx);
        } else {
            const T *dictVals = GetDictValues<T>();
            const int32_t *ids = static_cast<const int32_t *>(dictIds);
            return dictVals[ids[idx]];
        }
    }

private:
    template <typename T>
    ALWAYS_INLINE const T *GetDictValues() const
    {
        auto *dicVec = static_cast<Vector<DictionaryContainer<T>> *>(vector);
        return unsafe::UnsafeDictionaryVector::GetDictionary(dicVec);
    }

    BaseVector *vector = nullptr;
    int32_t rowCount = 0;
    const uint8_t *nulls = nullptr;
    const void *dictIds = nullptr;
    DVecLayout layout = DVecLayout::Flat;
    bool hasNull = false;
};

} // namespace omniruntime::vec

#endif // OMNI_RUNTIME_DECODED_VECTOR_H
