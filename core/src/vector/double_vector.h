/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
#ifndef __DOUBLE_VECTOR__H__
#define __DOUBLE_VECTOR__H__

#include "fixed_width_vector.h"
#include "../util/compiler_util.h"

namespace omniruntime {
namespace vec {
class DoubleVector : public FixedWidthVector<double> {
public:
    DoubleVector(VectorAllocator *allocator, int size);

    // inline for high performance.
    double ALWAYS_INLINE GetValue(int index)
    {
        return reinterpret_cast<double *>(valuesAddress)[index + positionOffset];
    }

    // inline for high performance.
    void ALWAYS_INLINE SetValue(int index, double value)
    {
        reinterpret_cast<double *>(valuesAddress)[index] = value;
    }

    void SetValues(int startIndex, const double *values, int length) override;

    DoubleVector *Slice(int positionOffset, int length) override;

    DoubleVector *CopyPositions(const int *positions, int offset, int length) override;

    DoubleVector *CopyRegion(int positionOffset, int length) override;

    void Append(Vector *other, int positionOffset, int length) override;

    ~DoubleVector() {}

private:
    DoubleVector(DoubleVector *vector, int size, int positionOffset)
        : FixedWidthVector(vector, size, positionOffset) {};

    static const int BYTES = sizeof(double);
};
} // namespace vec
} // namespace omniruntime
#endif // __DOUBLE_VECTOR__H__