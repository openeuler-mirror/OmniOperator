/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 *
 */
#ifndef OMNI_RUNTIME_BIT_MAP_H
#define OMNI_RUNTIME_BIT_MAP_H

#include <iostream>

namespace omniruntime {
namespace BitMap {
static const int32_t SIZE_OF_LONG = sizeof(uint64_t);
static int32_t BitCountLong(uint64_t value)
{
    value = value - ((value >> 1)) & 0x5555555555555555L;
    value = (value & 0x3333333333333333L) + ((value >> 2) & 0x3333333333333333L);
    value = (value + (value >> 4)) & 0x0f0f0f0f0f0f0f0fL;
    value = value + (value >> 8);
    value = value + (value >> 16);
    value = value + (value >> 32);
    return (static_cast<uint32_t>(value)) & 0x7f;
}

static int32_t ComputeBitCount(const uint8_t *data, int32_t bitOffset, int32_t size)
{
    int32_t count = 0;
    int32_t popLen = size / SIZE_OF_LONG;
    auto *align = reinterpret_cast<const uint64_t *>(data + bitOffset);
    int32_t length = size;
    int32_t offset = 0;
    for (int32_t i = 0; i < popLen; i++) {
        count += BitCountLong(align[i]);
        length -= SIZE_OF_LONG;
        offset += SIZE_OF_LONG;
    }

    // handle tail
    while (length > 0) {
        if (data[bitOffset + offset] == 1) {
            count++;
        }
        offset++;
        length--;
    }
    return count;
}
} // namespace bitmap
} // namespace omniruntime
#endif // OMNI_RUNTIME_BIT_MAP_H