/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 * Description: decimal128 utils
 */

#ifndef OMNI_RUNTIME_DECIMAL128_UTILS_H
#define OMNI_RUNTIME_DECIMAL128_UTILS_H
#include "operator/hash_util.h"
#include "data_utils.h"

namespace omniruntime {
namespace type {
class Decimal128Utils {
public:
    static inline __uint128_t StrToUint128_t(const char *s)
    {
        const char *p = s;
        __uint128_t val = 0;

        if (*p == '-' || *p == '+') {
            p++;
        }
        while (*p >= '0' && *p <= '9') {
            val = (10 * val) + (*p - '0');
            p++;
        }
        __uint128_t signBit = 1;
        if (*s == '-') {
            val = val | (signBit << 127);
        }
        return val;
    }

    static inline std::string Uint128_tToStr(__uint128_t num)
    {
        std::string str;
        do {
            int digit = num % 10;
            str = std::to_string(digit) + str;
            num = (num - digit) / 10;
        } while (num != 0);
        return str;
    }

    static int8_t *Decimal128ToBytes(int64_t highBits, uint64_t lowBits, int32_t *byteLen)
    {
        bool isNegative = highBits < 0;
        highBits = isNegative ? omniruntime::op::HashUtil::UnpackUnsignedLong(highBits) : highBits;
        int32_t int32Array[] = {static_cast<int32_t>(highBits >> 32), static_cast<int32_t>(highBits),
                                static_cast<int32_t>(lowBits >> 32), static_cast<int32_t>(lowBits)};
        auto bitLength = omniruntime::type::DataUtils::BitLength(int32Array, 4, isNegative);
        *byteLen = bitLength / 8 + 1;
        auto bytes = new int8_t[*byteLen];
        int32_t firstNonzeroIntNum;
        if (static_cast<int32_t>(lowBits) != 0) {
            firstNonzeroIntNum = 0;
        } else if (static_cast<int32_t>(lowBits >> 32) != 0) {
            firstNonzeroIntNum = 1;
        } else if (static_cast<int32_t>(highBits) != 0) {
            firstNonzeroIntNum = 2;
        } else if (static_cast<int32_t>(highBits >> 32) != 0) {
            firstNonzeroIntNum = 3;
        } else {
            firstNonzeroIntNum = 4;
        }

        for (int32_t i = *byteLen - 1, bytesCopyied = 4, nextInt = 0, intIndex = 3; i >= 0; i--) {
            if (bytesCopyied == 4 && intIndex >= 0) {
                nextInt = (!isNegative) ?
                    int32Array[intIndex--] :
                    ((3 - intIndex) <= firstNonzeroIntNum ? -int32Array[intIndex--] : ~int32Array[intIndex--]);
                bytesCopyied = 1;
            } else {
                nextInt = static_cast<uint32_t>(nextInt) >> 8;
                bytesCopyied++;
            }
            bytes[i] = static_cast<int8_t>(nextInt);
        }
        return bytes;
    }
};
}
}

#endif // OMNI_RUNTIME_DECIMAL128_UTILS_H
