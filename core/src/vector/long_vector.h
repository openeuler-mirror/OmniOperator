/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
#ifndef __LONG_VECTOR__H__
#define __LONG_VECTOR__H__

#include "fixed_width_vector.h"
#include <limits>

namespace omniruntime {
namespace vec {
class LongVector : public FixedWidthVector<int64_t> {
public:
    LongVector(omniruntime::vec::VectorAllocator *allocator, int size);

    // inline for high performance.
    int64_t GetValue(int index)
    {
        if (index >= size) {
            return -1;
        }
        return reinterpret_cast<int64_t *>(valuesAddress)[index + positionOffset];
    }

    // inline for high performance.
    void SetValue(int index, int64_t value)
    {
        reinterpret_cast<int64_t *>(valuesAddress)[index] = value;
    }

    void SetValues(int startIndex, const int64_t *values, int length) override;

    LongVector *Slice(int positionOffset, int length) override;

    LongVector *CopyPositions(const int *positions, int offset, int length) override;

    LongVector *CopyRegion(int positionOffset, int length) override;

    void Append(Vector *other, int positionOffset, int length) override;

    ~LongVector() {}

private:
    LongVector(LongVector *vector, int size, int positionOffset) : FixedWidthVector(vector, size, positionOffset) {};

    static const int BYTES = sizeof(int64_t);
};
} // namespace vec
} // namespace omniruntime
#endif // __LONG_VECTOR__H__
