/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 *
 */
#ifndef OMNI_RUNTIME_BIT_MAP_H
#define OMNI_RUNTIME_BIT_MAP_H

#include <iostream>

namespace omniruntime {
class BitMap {
    static constexpr int32_t SIZE_OF_LONG = sizeof(uint64_t);

public:
    static inline int32_t BitCountLong(uint64_t value)
    {
        // step1:find the sum of every 2 bits of binary corresponding to the value
        value = value - ((value >> 1) & 0x5555555555555555L);
        // step2:find the sum of every 4 bits of binary corresponding to the value in step 1
        value = (value & 0x3333333333333333L) + ((value >> 2) & 0x3333333333333333L);
        // step3:find the sum of every 8 bits of binary corresponding to the value in step 2
        value = (value + (value >> 4)) & 0x0f0f0f0f0f0f0f0fL;
        // step4:find the sum of every 16 bits of binary corresponding to the value in step 3
        value = value + (value >> 8);
        // step5:find the sum of every 32 bits of binary corresponding to the value in step 4
        value = value + (value >> 16);
        // step6:find the sum of every 64 bits of binary corresponding to the value in step 5
        value = value + (value >> 32);
        // last:the number of 1 will not exceed 64,just take the last 7 bits
        return (static_cast<uint32_t>(value)) & 0x7f;
    }

    static inline int32_t BitCountInt(uint32_t value)
    {
        value = value - ((value >> 1) & 0x55555555);
        value = (value & 0x33333333) + ((value >> 2) & 0x33333333);
        value = (value + (value >> 4)) & 0x0f0f0f0f;
        value = value + (value >> 8);
        value = value + (value >> 16);
        return (static_cast<uint32_t>(value)) & 0x3f;
    }

    static inline int32_t ComputeBitCount(const uint8_t *data, int32_t bitOffset, int32_t size)
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
}; // class bitmap
} // namespace omniruntime
#endif // OMNI_RUNTIME_BIT_MAP_H