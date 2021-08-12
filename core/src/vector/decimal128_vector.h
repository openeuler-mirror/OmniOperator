/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

#ifndef OMNI_RUNTIME_DECIMAL128_Decimal128Vector_H
#define OMNI_RUNTIME_DECIMAL128_Decimal128Vector_H

#include "vector.h"
#include "decimal128.h"

namespace omniruntime {
namespace vec {
class Decimal128Vector : public Vector {
public:
    Decimal128Vector(VectorAllocator *allocator, int32_t size);

    // inline for high performance.
    Decimal128 GetValue(int32_t index)
    {
        int64_t *value = &(reinterpret_cast<int64_t *>(valuesAddress)[(index + positionOffset) * 2]);
        return Decimal128(*value, *(value + 1));
    }

    // inline for high performance.
    void SetValue(int32_t index, Decimal128 value)
    {
        reinterpret_cast<int64_t *>(valuesAddress)[index * 2] = value.HighBits();
        reinterpret_cast<int64_t *>(valuesAddress)[index * 2 + 1] = value.LowBits();
    }

    // inline for high performance.
    void SetValue(int32_t index, int64_t highBits, uint64_t lowBits)
    {
        reinterpret_cast<int64_t *>(valuesAddress)[index * 2] = highBits;
        reinterpret_cast<int64_t *>(valuesAddress)[index * 2 + 1] = lowBits;
    }

    void SetValues(int32_t startIndex, const int64_t *values, int32_t length);

    Decimal128Vector *Slice(int32_t positionOffset, int32_t length) override;

    Decimal128Vector *CopyPositions(const int32_t *positions, int32_t offset, int32_t length) override;

    Decimal128Vector *CopyRegion(int32_t positionOffset, int32_t length) override;

    void Append(Vector *other, int32_t positionOffset, int32_t length) override;

    ~Decimal128Vector() {}

    static const int DECIMAL128_TYPE_WIDTH = 2;

private:
    Decimal128Vector(Decimal128Vector *Decimal128Vector, int32_t size, int32_t positionOffset)
        : Vector(Decimal128Vector, size, positionOffset) {};

    static constexpr int32_t BYTES = sizeof(int64_t) + sizeof(int64_t);
};
}
}

#endif // OMNI_RUNTIME_DECIMAL128_Decimal128Vector_H
