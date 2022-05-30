/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 * Description: decimal128 utils
 */

#ifndef OMNI_RUNTIME_DECIMAL128_UTILS_H
#define OMNI_RUNTIME_DECIMAL128_UTILS_H

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
};
}
}

#endif // OMNI_RUNTIME_DECIMAL128_UTILS_H
