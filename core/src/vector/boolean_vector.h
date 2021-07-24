/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
#ifndef __BOOLEAN_VECTOR__H__
#define __BOOLEAN_VECTOR__H__

#include "fixed_width_vector.h"

namespace omniruntime {
namespace vec {
class BooleanVector : public FixedWidthVector<bool> {
public:
    BooleanVector(VectorAllocator *allocator, int size);

    bool GetValue(int index) const
    {
        return reinterpret_cast<bool *>(valuesAddress)[index + positionOffset];
    }

    void SetValue(int index, bool value)
    {
        if (value) {
            (reinterpret_cast<bool *>(valuesAddress))[index] = true;
        } else {
            (reinterpret_cast<bool *>(valuesAddress))[index] = false;
        }
    }

    void SetValues(int startIndex, const bool *values, int length) override;

    BooleanVector *Slice(int positionOffset, int length) override;

    BooleanVector *CopyPositions(const int *positions, int offset, int length) override;

    BooleanVector *CopyRegion(int positionOffset, int length) override;

    void Append(Vector *other, int positionOffset, int length) override;

    ~BooleanVector() {}

private:
    BooleanVector(BooleanVector *vector, int size, int positionOffset)
        : FixedWidthVector(vector, size, positionOffset) {};

    static const int BYTES = 0;
};
} // namespace vec
} // namespace omniruntime
#endif // __BOOLEAN_VECTOR__H__