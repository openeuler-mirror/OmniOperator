/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
#include "debug.h"
#include "varchar_vector.h"
#include "dictionary_vector.h"

namespace omniruntime {
namespace vec {
VarcharVector::VarcharVector(VectorAllocator *allocator, int capacityInBytes, int size)
    : VariableWidthVector(allocator, capacityInBytes, size, VarcharVecType::Instance())
{}

VarcharVector *VarcharVector::Slice(int positionOffset, int length)
{
    return new VarcharVector(this, length, positionOffset);
}

VarcharVector *VarcharVector::CopyPositions(const int *positions, int offset, int length)
{
    int totalDataLen = 0;
    for (int i = 0; i < length; i++) {
        if (IsValueNull(positions[offset + i])) {
            continue;
        }
        int position = positions[offset + i] + positionOffset;
        totalDataLen += GetValueOffset(position + 1) - GetValueOffset(position);
    }
    auto *vector = new VarcharVector(GetAllocator(), totalDataLen, length);
    for (int i = 0; i < length; i++) {
        if (IsValueNull(positions[offset + i])) {
            vector->SetValueNull(i);
            continue;
        }
        int position = positions[offset + i] + positionOffset;
        int startOffset = GetValueOffset(position);
        int dataLen = GetValueOffset(position + 1) - startOffset;
        uint8_t *data = reinterpret_cast<uint8_t *>(valuesAddress);
        vector->SetValue(i, data + startOffset, dataLen);
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

    auto *vector = new VarcharVector(GetAllocator(), totalDataLen, length);

    // copy offset
    auto *offsets = reinterpret_cast<int32_t *>(vector->GetValueOffsets());
    for (int32_t i = 1; i <= length; i++) {
        offsets[i] = GetValueOffset(newPosition + i) - GetValueOffset(newPosition);
    }

    // copy nulls
    vector->SetValueNulls(0, static_cast<bool *>(valueNullsAddress) + positionOffset + this->positionOffset, length);

    // copy data
    errno_t ret = EOK;
    if (totalDataLen > 0) {
        ret = memcpy_s(reinterpret_cast<uint8_t *>(vector->GetValues()), totalDataLen,
            (reinterpret_cast<uint8_t *>(valuesAddress)) + startOffset, totalDataLen);
    }

    if (ret != EOK) {
        std::cerr << "copy region failed in varchar vector:" << ret << std::endl;
        delete vector;
        return nullptr;
    }
    return vector;
}

void VarcharVector::SetData(int index, const uint8_t *value, int start, int dataLen)
{
    int startOffset = GetValueOffset(index);
    if (startOffset + dataLen > capacityInBytes) {
        return;
    }
    SetValueOffset(index + 1, startOffset + dataLen);

    // empty vector or empty string no need to copy data
    if (value == nullptr || dataLen == 0 || capacityInBytes == 0) {
        return;
    }
    uint8_t *data = reinterpret_cast<uint8_t *>(valuesAddress);
    errno_t ret = memcpy_s(data + startOffset, capacityInBytes, value + start, dataLen);
    if (ret != EOK) {
        std::cerr << "set data failed in varchar vector." << ret << std::endl;
    }
    data = nullptr;
}

void VarcharVector::Append(Vector *other, int positionOffset, int length)
{
    if (positionOffset + length > size) {
        return;
    }

    uint8_t *value = nullptr;
    int32_t valueLen = 0;
    if (other->GetTypeId() != OMNI_VEC_TYPE_DICTIONARY) {
        VarcharVector *src = static_cast<VarcharVector *>(other);
        for (int32_t i = 0; i < length; i++) {
            if (other->IsValueNull(i)) {
                SetValueNull(positionOffset + i);
            } else {
                valueLen = src->GetValue(i, &value);
                SetValue(positionOffset + i, value, valueLen);
            }
        }
    } else {
        DictionaryVector *src = static_cast<DictionaryVector *>(other);
        int32_t originalIds[length];
        VarcharVector *dictionary = static_cast<VarcharVector *>(src->ExtractDictionaryAndIds(0, length, originalIds));
        for (int32_t i = 0; i < length; i++) {
            if (dictionary->IsValueNull(originalIds[i])) {
                SetValueNull(positionOffset + i);
            } else {
                valueLen = dictionary->GetValue(originalIds[i], &value);
                SetValue(positionOffset + i, value, valueLen);
            }
        }
    }
}
}
}
