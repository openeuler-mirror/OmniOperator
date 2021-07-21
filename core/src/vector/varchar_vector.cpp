/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
#include "debug.h"
#include "varchar_vector.h"
#include <cstring>

VarcharVector::VarcharVector(VectorAllocator *allocator, int capacityInBytes, int size)
    : VariableWidthVector(allocator, capacityInBytes, size, OMNI_VEC_TYPE_VARCHAR)
{}

VarcharVector *VarcharVector::Slice(int positionOffset, int length)
{
    return new VarcharVector(this, length, positionOffset);
}

VarcharVector *VarcharVector::CopyPositions(const int *positions, int offset, int length)
{
    if (length > size) {
        return nullptr;
    }
    int totalDataLen = 0;
    for (int i = 0; i < length; i++) {
        int position = positions[offset + i] + positionOffset;
        totalDataLen += GetValueOffset(position + 1) - GetValueOffset(position);
    }
    VarcharVector *vector = new VarcharVector(GetAllocator(), totalDataLen, length);
    for (int i = 0; i < length; i++) {
        int position = positions[offset + i] + positionOffset;
        int startOffset = GetValueOffset(position);
        int dataLen = GetValueOffset(position + 1) - startOffset;
        char *data = reinterpret_cast<char *>(valuesAddress);
        vector->SetValue(i, data + startOffset, dataLen);
        vector->SetValueNulls(i, ((bool *)valueNullsAddress) + position, 1);
        data = nullptr;
    }
    return vector;
}

VarcharVector *VarcharVector::CopyRegion(int positionOffset, int length)
{
    if (positionOffset + length > size) {
        return nullptr;
    }

    int newPosition = positionOffset + this->positionOffset;
    int startOffset = GetValueOffset(newPosition);
    int totalDataLen = GetValueOffset(newPosition + length) - GetValueOffset(newPosition);

    VarcharVector *vector = new VarcharVector(GetAllocator(), totalDataLen, length);
    errno_t ret = memcpy_s(reinterpret_cast<char *>(vector->GetValues()), totalDataLen, 
        (reinterpret_cast<char *>(valuesAddress)) + startOffset,
        totalDataLen);
    if (ret != EOK) {
        delete vector;
        return nullptr;
    }

    vector->SetValueNulls(0, (bool *)valueNullsAddress + positionOffset + this->positionOffset, length);

    // copy offset
    int32_t *offsets = reinterpret_cast<int32_t *>(vector->GetValueOffsets());
    for (int32_t i = 1; i <= length; i++) {
        offsets[i] = GetValueOffset(newPosition + i) - GetValueOffset(newPosition);
    }
    return vector;
}

void VarcharVector::GetData(int startOffset, char *dst, int start, int length)
{
    if (dst == nullptr || start + length > capacityInBytes) {
        return;
    }
    char *data = reinterpret_cast<char *>(valuesAddress);
    errno_t ret = memcpy_s(dst + start, capacityInBytes, data + startOffset, length);
    if (ret != EOK) {
        std::cerr << "get data failed in varchar vector." << std::endl;
    }
    data = nullptr;
}

void VarcharVector::SetData(int index, const char *value, int start, int length)
{
    if (value == nullptr) {
        return;
    }
    int startOffset = GetValueOffset(index);
    if (startOffset + length > capacityInBytes) {
        return;
    }
    SetValueOffset(index + 1, startOffset + length);
    char *data = reinterpret_cast<char *>(valuesAddress);
    errno_t ret = memcpy_s(data + startOffset, capacityInBytes, value + start, length);
    if (ret != EOK) {
        std::cerr << "set data failed in varchar vector." << std::endl;
    }
    data = nullptr;
}

void VarcharVector::FillSlots(int index)
{
    for (int i = lastOffsetPosition + 1; i < index; i++) {
        SetData(i, nullptr, 0, 0);
    }
    lastOffsetPosition = index - 1;
}
void VarcharVector::Append(Vector *other, int positionOffset, int length) {}