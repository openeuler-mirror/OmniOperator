/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: DecimalOperations
 */

#ifndef OMNI_RUNTIME_DECIMALOPERATIONS_H
#define OMNI_RUNTIME_DECIMALOPERATIONS_H


#include <cstdint>
#include <iostream>
#include <vector/fixed_width_vector.h>

#include "decimal_base.h"
#include "decimal128.h"

namespace omniruntime {
namespace vec {
static Decimal128 OVERFLOW_MULTIPLIER(0x8000000000000000, 0x0000000000000000);
class DecimalOperations {
public:
    DecimalOperations(int64_t high_bits, uint64_t low_bits) = delete;
    ~DecimalOperations() = delete;

    static long AddWithOverflow(Decimal128 &left, Decimal128 &right, Decimal128 &result);

    // decimal and overflow is encoded and decoded in continuous memory
    static void DecodeSumDecimal(void *ptr, Decimal128 &val, int64_t &overflow);
    static void EncodeSumDecimal(void *ptr, const Decimal128 &val, const int64_t &overflow);

    // decimal and overflow is encoded in continuous memory
    static void DecodeAvgDecimal(void *ptr, Decimal128 &val, int64_t &overflow, int64_t &count);
    static void EncodeAvgDecimal(void *ptr, const Decimal128 &val, const int64_t &overflow, const int64_t &count);

    static long AddUnsignedReturnOverflow(const Decimal128 &decimal128, const Decimal128 &decimal1281,
        Decimal128 &decimal1282, bool negative);

    static void SetToZero(Decimal128 &decimal128);

    static int CompareAbsolute(Decimal128 &left, Decimal128 &right);

    static void SubtractUnsigned(Decimal128 &left, Decimal128 &right, Decimal128 &result, bool resultNegative);

    static void Pack(Decimal128 &decimal128, uint64_t low, int64_t high, bool negative);

    static bool UnsignedIsSmaller(uint64_t first, uint64_t second);

    static bool IsNegative(Decimal128 &left);

    static int64_t GetLong(int64_t bits);

    static int CompareUnsigned(int64_t x, int64_t y);

    static int Compare(int64_t x, int64_t y);

    static Decimal128 UnscaledDecimal(int64_t unscaledValue);
};
}
}

#endif // OMNI_RUNTIME_DECIMALOPERATIONS_H
