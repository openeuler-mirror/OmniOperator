/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

#ifndef __FIXED_WIDTH_VECTOR_OPERATOR_H__
#define __FIXED_WIDTH_VECTOR_OPERATOR_H__

#include <type/data_type.h>
#include <huawei_secure_c/include/securec.h>

#include "vector.h"
#include "type/decimal128.h"
#include "type/data_type.h"
#include "dictionary_vector.h"

namespace omniruntime {
namespace vec {
template <DataTypeId TYPE_ID> class FixedWidthVector : public Vector {
    using T = typename NativeType<TYPE_ID>::type;
    using FixedWidthVectorImpl = FixedWidthVector<TYPE_ID>;

public:
    FixedWidthVector(VectorAllocator *allocator, int size) : Vector(allocator, BYTES * size, size, TYPE_ID) {}

    FixedWidthVector(Vector *vector, int size, int positionOffset) : Vector(vector, size, positionOffset) {}

    ~FixedWidthVector() override {}

    const T ALWAYS_INLINE GetValue(int index) const
    {
        return reinterpret_cast<T *>(valuesAddress)[index + positionOffset];
    }

    void ALWAYS_INLINE SetValue(int index, const T &value)
    {
        (reinterpret_cast<T *>(valuesAddress))[index] = value;
    }

    void SetValues(int startIndex, const void *values, int length)
    {
        if (!reference->IsWritable() || startIndex + length > size) {
            LogError("vector is not writable(%d) or out of range(needed size:%d, real size:%d).",
                reference->IsWritable(), startIndex + length, size);
            return;
        }
        T *startAddr = reinterpret_cast<T *>(valuesAddress);
        errno_t ret = memcpy_s(startAddr + startIndex, capacityInBytes, values, length * BYTES);
        if (ret != EOK) {
            LogError("memory copy failed.");
        }
    }

    FixedWidthVectorImpl *Slice(int startIndex, int length) override
    {
        if (startIndex + length > size) {
            LogError("slice vector out of range(needed size:%d, real size:%d).", startIndex + length, size);
            return nullptr;
        }
        return new FixedWidthVectorImpl(this, length, startIndex);
    }

    FixedWidthVectorImpl *CopyPositions(const int *positions, int offset, int length) override
    {
        FixedWidthVectorImpl *vector = new FixedWidthVectorImpl(GetAllocator(), length);
        for (int i = 0; i < length; ++i) {
            int position = positions[offset + i];
            vector->SetValue(i, GetValue(position));
            vector->SetValueNull(i, IsValueNull(position));
        }
        return vector;
    }

    FixedWidthVectorImpl *CopyRegion(int startIndex, int length) override
    {
        if (startIndex + length > size) {
            LogError("copy region out of range(needed size:%d, real size:%d).", startIndex + length, size);
            return nullptr;
        }
        FixedWidthVectorImpl *vector = new FixedWidthVectorImpl(GetAllocator(), length);
        vector->SetValues(0, static_cast<T *>(valuesAddress) + startIndex + this->positionOffset, length);
        vector->SetValueNulls(0, static_cast<bool *>(valueNullsAddress) + startIndex + this->positionOffset, length);
        return vector;
    }

    void Append(Vector *other, int startIndex, int length) override
    {
        if (startIndex + length > size) {
            LogError("append vector out of range(needed size:%d, real size:%d).", startIndex + length, size);
            return;
        }

        if (other->GetEncoding() != OMNI_VEC_ENCODING_DICTIONARY) {
            int32_t otherPositionOffset = other->GetPositionOffset();
            void *otherValues = static_cast<T *>(other->GetValues()) + otherPositionOffset;
            bool *otherValueNulls = static_cast<bool *>(other->GetValueNulls()) + otherPositionOffset;
            SetValues(startIndex, otherValues, length);
            SetValueNulls(startIndex, otherValueNulls, length);
        } else {
            DictionaryVector *src = static_cast<DictionaryVector *>(other);
            int32_t originalIds[length];
            FixedWidthVectorImpl *dictionary =
                static_cast<FixedWidthVectorImpl *>(src->ExtractDictionaryAndIds(0, length, originalIds));
            for (int32_t i = 0; i < length; i++) {
                if (dictionary->IsValueNull(originalIds[i])) {
                    SetValueNull(startIndex + i);
                } else {
                    SetValue(startIndex + i, dictionary->GetValue(originalIds[i]));
                }
            }
        }
    }

protected:
    static const int BYTES = sizeof(T);
};

using BooleanVector = FixedWidthVector<type::OMNI_BOOLEAN>;
using IntVector = FixedWidthVector<type::OMNI_INT>;
using ShortVector = FixedWidthVector<type::OMNI_SHORT>;
using LongVector = FixedWidthVector<type::OMNI_LONG>;
using DoubleVector = FixedWidthVector<type::OMNI_DOUBLE>;
using Decimal128Vector = FixedWidthVector<type::OMNI_DECIMAL128>;
}
}
#endif // __FIXED_WIDTH_VECTOR_OPERATOR_H__
