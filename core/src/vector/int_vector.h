/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
#ifndef __INT_VECTOR__H__
#define __INT_VECTOR__H__

#include "fixed_width_vector.h"

namespace omniruntime {
namespace vec {
class IntVector : public FixedWidthVector<int32_t> {
public:
    IntVector(VectorAllocator *allocator, int size);

    // inline for high performance.
    int32_t ALWAYS_INLINE GetValue(int index)
    {
        return ((int32_t *)valuesAddress)[index + positionOffset];
    }

    // inline for high performance.
    void ALWAYS_INLINE SetValue(int index, int32_t value)
    {
        ((int32_t *)valuesAddress)[index] = value;
    }

    void SetValues(int startIndex, const int32_t *values, int length) override;

    IntVector *Slice(int positionOffset, int length) override;

    IntVector *CopyPositions(const int *positions, int offset, int length) override;

    IntVector *CopyRegion(int positionOffset, int length) override;

    void Append(Vector *other, int positionOffset, int length) override;

    ~IntVector() {}

private:
    IntVector(IntVector *vector, int size, int positionOffset) : FixedWidthVector(vector, size, positionOffset) {};

    static const int BYTES = sizeof(int32_t);
};
} // namespace vec
} // namespace omniruntime
#endif // __INT_VECTOR__H__