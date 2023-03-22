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
    FixedWidthVector(VectorAllocator *allocator, int size);

    FixedWidthVector(Vector *vector, int size, int positionOffset);

    ~FixedWidthVector() override = default;

    T ALWAYS_INLINE GetValue(int index) const
    {
        return reinterpret_cast<T *>(valuesAddress)[index + positionOffset];
    }

    void ALWAYS_INLINE SetValue(int index, const T &value)
    {
        (reinterpret_cast<T *>(valuesAddress))[index] = value;
    }

    void SetValues(int startIndex, const void *values, int length);

    FixedWidthVectorImpl *Slice(int startIndex, int length) override;

    FixedWidthVectorImpl *CopyPositions(const int *positions, int offset, int length) override;

    FixedWidthVectorImpl *CopyRegion(int startIndex, int length) override;

    void Append(Vector *other, int startIndex, int length) override;
    /* *
     * The serialization format : 1-byte null flag + width bytes data.
     * @param rowId
     * @param pos
     * @return
     */
    StringRef SerializeValue(size_t rowId, mem::SimpleArenaAllocator &arenaAllocator, const uint8_t *&begin) override;

    const uint8_t *DeserializeValueIntoThis(size_t rowId, const uint8_t *pos) override;

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
