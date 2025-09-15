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
        if (*s == '-') {
            val = -val;
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

    static int8_t *Decimal128ToBytes(int64_t highBits, uint64_t lowBits, int32_t &byteLen)
    {
        bool isNegative = highBits < 0;
        highBits = isNegative ? omniruntime::op::HashUtil::UnpackUnsignedLong(highBits) : highBits;
        int32_t tmpArray[] = { static_cast<int32_t>(highBits >> 32), static_cast<int32_t>(highBits),
                               static_cast<int32_t>(lowBits >> 32), static_cast<int32_t>(lowBits) };
        auto tmpLen = sizeof(tmpArray) / sizeof(tmpArray[0]);
        int32_t int32Array[tmpLen];

        int32_t len = 0;
        int32_t idx2 = 0;
        for (int idx1 = 0; idx1 < tmpLen; idx1++) {
            auto val = tmpArray[idx1];
            if (val != 0) {
                len = tmpLen - idx1;
                while (idx1 < tmpLen) {
                    int32Array[idx2++] = tmpArray[idx1];
                    idx1++;
                }
                break;
            }
        }

        auto bitLength = omniruntime::type::DataUtils::BitLength(int32Array, len, isNegative);

        byteLen = bitLength / 8 + 1;
        auto bytes = new int8_t[byteLen];
        int32_t firstNonZeroReverseIndex = 0;
        for (auto idx = len - 1; idx >= 0; idx--) {
            if (int32Array[idx] != 0) {
                firstNonZeroReverseIndex = len - 1 - idx;
                break;
            }
        }

        for (int32_t i = byteLen - 1, bytesCopyied = 4, nextInt = 0, intIndex = len - 1; i >= 0; i--) {
            if (bytesCopyied == 4 && intIndex >= 0) {
                nextInt = (!isNegative) ? int32Array[intIndex--] :
                                          ((len - 1 - intIndex) <= firstNonZeroReverseIndex ? -int32Array[intIndex--] :
                                                                                              ~int32Array[intIndex--]);
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
