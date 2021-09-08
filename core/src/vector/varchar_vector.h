/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
#ifndef __VARCHAR_VECTOR__H__
#define __VARCHAR_VECTOR__H__

#include "variable_width_vector.h"

namespace omniruntime {
namespace vec {
class VarcharVector : public VariableWidthVector<uint8_t *> {
public:
    VarcharVector(VectorAllocator *allocator, int capacityInBytes, int size);

    int ALWAYS_INLINE GetValue(int index, uint8_t **dst)
    {
        int actualIndex = index + positionOffset;
        int startOffset = GetValueOffset(actualIndex);
        int dataLen = GetValueOffset(actualIndex + 1) - startOffset;
        if (dataLen < 0) {
            return -1;
        }
        *dst = static_cast<uint8_t *>(valuesAddress) + startOffset;
        return dataLen;
    }

    void ALWAYS_INLINE SetValue(int index, const uint8_t *value, int length)
    {
        FillSlots(index);
        SetData(index, value, 0, length);
        lastOffsetPosition = index;
    }

    VarcharVector *Slice(int index, int length) override;

    VarcharVector *CopyPositions(const int *positions, int offset, int length) override;

    VarcharVector *CopyRegion(int positionOffset, int length) override;

    void Append(Vector *other, int positionOffset, int bytes) override;

    ~VarcharVector() {}

private:
    VarcharVector(VarcharVector *vector, int size, int positionOffset)
        : VariableWidthVector(vector, size, positionOffset) {};

    void GetData(int index, uint8_t *dst, int start, int length);

    void SetData(int index, const uint8_t *data, int start, int length);

    void FillSlots(int index);

    int lastOffsetPosition = -1;
};
}
}
#endif // __VARCHAR_VECTOR__H__