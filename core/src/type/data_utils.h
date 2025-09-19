/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 * Description: data utils
 */
#ifndef OMNI_RUNTIME_DATA_UTILS_H
#define OMNI_RUNTIME_DATA_UTILS_H
#include <iostream>
#include <cstring>

namespace omniruntime {
namespace type {
class DataUtils {
public:
    static ALWAYS_INLINE int32_t BitCount(int32_t num)
    {
        num = num - ((static_cast<uint32_t>(num) >> 1) & 0x55555555);
        num = (num & 0x33333333) + ((static_cast<uint32_t>(num) >> 2) & 0x33333333);
        num = (num + (static_cast<uint32_t>(num) >> 4)) & 0x0f0f0f0f;
        num = num + (static_cast<uint32_t>(num) >> 8);
        num = num + (static_cast<uint32_t>(num) >> 16);
        return num & 0x3f;
    }

    static ALWAYS_INLINE int32_t BitLengthForInt(int32_t num)
    {
        if (num == 0)
            return 32;
        int len = 1;
        if (static_cast<uint32_t>(num) >> 16 == 0) {
            len += 16;
            num <<= 16;
        }
        if (static_cast<uint32_t>(num) >> 24 == 0) {
            len += 8;
            num <<= 8;
        }
        if (static_cast<uint32_t>(num) >> 28 == 0) {
            len += 4;
            num <<= 4;
        }
        if (static_cast<uint32_t>(num) >> 30 == 0) {
            len += 2;
            num <<= 2;
        }
        len -= static_cast<uint32_t>(num) >> 31;
        return 32 - len;
    }

    static ALWAYS_INLINE int32_t BitLength(int32_t array[], int32_t len, bool isNegative)
    {
        if (len == 0) {
            return 0;
        }
        int32_t magBitLength = ((len - 1) << 5) + BitLengthForInt(array[0]);
        if (isNegative) {
            auto pow2 = BitCount(array[0]) == 1;
            for (int32_t i = 1; i < len && pow2; i++) {
                pow2 = array[i] == 0;
            }
            return (pow2 ? magBitLength - 1 : magBitLength);
        } else {
            return magBitLength;
        }
    }
};
}
}

#endif // OMNI_RUNTIME_DATA_UTILS_H
