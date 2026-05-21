/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2026. All rights reserved.
 * Description: Murmur3 Hash function
 */
#include "util/compiler_util.h"
#include "type/decimal128_utils.h"
#include "operator/util/mm3_util.h"
#include "murmur3_hash.h"

namespace omniruntime::codegen::function {
static const int COMBINE_HASH_VALUE = 31;

extern "C" DLLEXPORT int32_t Mm3Int8(int8_t val, bool isValNull, int32_t seed, bool isSeedNull)
{
    if (isSeedNull) {
        seed = 0;
    }
    if (isValNull) {
        return seed;
    }

    return static_cast<int32_t>(
       op::HashInt(static_cast<uint32_t>(static_cast<int32_t>(val)), static_cast<uint32_t>(seed)));
}

extern "C" DLLEXPORT int32_t Mm3Int16(int16_t val, bool isValNull, int32_t seed, bool isSeedNull)
{
    if (isSeedNull) {
        seed = 0;
    }
    if (isValNull) {
        return seed;
    }

    return static_cast<int32_t>(
       op::HashInt(static_cast<uint32_t>(static_cast<int32_t>(val)), static_cast<uint32_t>(seed)));
}

extern "C" DLLEXPORT int32_t Mm3Int32(int32_t val, bool isValNull, int32_t seed, bool isSeedNull)
{
    if (isSeedNull) {
        seed = 0;
    }
    if (isValNull) {
        return seed;
    }

    return static_cast<int32_t>(op::HashInt(static_cast<uint32_t>(val * !isValNull), static_cast<uint32_t>(seed)));
}

extern "C" DLLEXPORT int32_t Mm3Int64(int64_t val, bool isValNull, int32_t seed, bool isSeedNull)
{
    if (isSeedNull) {
        seed = 0;
    }
    if (isValNull) {
        return seed;
    }

    return static_cast<int32_t>(op::HashLong(static_cast<uint64_t>(val * !isValNull), static_cast<uint32_t>(seed)));
}

extern "C" DLLEXPORT int32_t Mm3String1(std::string_view val, bool isValNull, int32_t seed, bool isSeedNull)
{
    std::string str(val);
    return Mm3String(str.data(), str.length(), isValNull, seed, isSeedNull);
}

extern "C" DLLEXPORT int32_t Mm3String(char *val, int32_t valLen, bool isValNull, int32_t seed, bool isSeedNull)
{
    if (isSeedNull) {
        seed = 0;
    }
    if (isValNull) {
        return seed;
    }

    valLen = valLen * !isValNull;
    return static_cast<int32_t>(op::HashUnsafeBytes(val, static_cast<uint32_t>(valLen), static_cast<uint32_t>(seed)));
}

extern "C" DLLEXPORT int32_t Mm3Float(float val, bool isValNull, int32_t seed, bool isSeedNull)
{
    union {
        uint32_t lVal;
        float dVal;
    } uVal = { 0 };
    uVal.dVal = val * !isValNull;
    if (isSeedNull) {
        seed = 0;
    }
    if (isValNull) {
        return seed;
    }

    return static_cast<int32_t>(op::HashInt(uVal.lVal, static_cast<uint32_t>(seed)));
}

extern "C" DLLEXPORT int32_t Mm3Double(double val, bool isValNull, int32_t seed, bool isSeedNull)
{
    union {
        uint64_t lVal;
        double dVal;
    } uVal = { 0 };
    uVal.dVal = val * !isValNull;
    if (isSeedNull) {
        seed = 0;
    }
    if (isValNull) {
        return seed;
    }

    return static_cast<int32_t>(op::HashLong(uVal.lVal, static_cast<uint32_t>(seed)));
}

extern "C" DLLEXPORT int32_t Mm3Decimal64(int64_t val, int32_t precision, int32_t scale, bool isValNull, int32_t seed,
    bool isSeedNull)
{
    if (isSeedNull) {
        seed = 0;
    }
    if (isValNull) {
        return seed;
    }

    return static_cast<int32_t>(op::HashLong(val * !isValNull, seed));
}

extern "C" DLLEXPORT int32_t Mm3Decimal128(int64_t xHigh, uint64_t xLow, int32_t precision, int32_t scale,
    bool isValNull, int32_t seed, bool isSeedNull)
{
    if (isSeedNull) {
        seed = 0;
    }
    if (isValNull) {
        return seed;
    }

    int32_t byteLen = 0;
    auto bytes = type::Decimal128Utils::Decimal128ToBytes(xHigh, xLow, byteLen);
    auto result = static_cast<int32_t>(op::HashUnsafeBytes(reinterpret_cast<char *>(bytes), byteLen, seed));
    delete[] bytes;
    return result;
}

extern "C" DLLEXPORT int32_t Mm3Boolean(bool val, bool isValNull, int32_t seed, bool isSeedNull)
{
    if (isSeedNull) {
        seed = 0;
    }
    if (isValNull) {
        return seed;
    }

    uint32_t intVal = val ? 1 : 0;
    return static_cast<int32_t>(op::HashInt(static_cast<uint32_t>(intVal * !isValNull), static_cast<uint32_t>(seed)));
}

extern "C" DLLEXPORT int64_t CombineHash(int64_t prevHashVal, bool isPrevHashValNull, int64_t val, bool isValNull)
{
    if (isPrevHashValNull) {
        prevHashVal = 0;
    }
    if (isValNull) {
        val = 0;
    }
    return COMBINE_HASH_VALUE * prevHashVal + val;
}
}
