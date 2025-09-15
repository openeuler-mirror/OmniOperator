/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * Description: batch mmh3 functions implementation
 */
#include "codegen/functions/murmur3_hash.h"
#include "type/decimal128_utils.h"
#include "batch_murmur3_hash.h"

namespace omniruntime::codegen::function {
static const int COMBINE_HASH_VALUE = 31;

extern "C" DLLEXPORT void BatchMm3Int32(int32_t *val, bool *isValNull, int32_t *seed, bool *isSeedNull, bool *resIsNull,
    int32_t *output, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; ++i) {
        seed[i] = isSeedNull[i] ? 0 : seed[i];
        output[i] = static_cast<int32_t>(
            HashInt(static_cast<uint32_t>(val[i] * !isValNull[i]), static_cast<uint32_t>(seed[i])));
    }
}

extern "C" DLLEXPORT void BatchMm3Int64(int64_t *val, bool *isValNull, int32_t *seed, bool *isSeedNull, bool *resIsNull,
    int32_t *output, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; ++i) {
        seed[i] = isSeedNull[i] ? 0 : seed[i];
        output[i] = static_cast<int32_t>(
            HashLong(static_cast<uint64_t>(val[i] * !isValNull[i]), static_cast<uint32_t>(seed[i])));
    }
}

extern "C" DLLEXPORT void BatchMm3String(uint8_t **val, int32_t *valLen, bool *isValNull, int32_t *seed,
    bool *isSeedNull, bool *resIsNull, int32_t *output, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; ++i) {
        seed[i] = isSeedNull[i] ? 0 : seed[i];
        valLen[i] = valLen[i] * !isValNull[i];
        output[i] = static_cast<int32_t>(HashUnsafeBytes(reinterpret_cast<char *>(val[i]),
            static_cast<uint32_t>(valLen[i]), static_cast<uint32_t>(seed[i])));
    }
}

extern "C" DLLEXPORT void BatchMm3Double(double *val, bool *isValNull, int32_t *seed, bool *isSeedNull, bool *resIsNull,
    int32_t *output, int32_t rowCnt)
{
    union {
        uint64_t lVal;
        double dVal;
    } uVal = { 0 };

    for (int i = 0; i < rowCnt; ++i) {
        uVal.dVal = val[i] * !isValNull[i];
        seed[i] = isSeedNull[i] ? 0 : seed[i];
        output[i] = static_cast<int32_t>(HashLong(uVal.lVal, static_cast<uint32_t>(seed[i])));
    }
}

extern "C" DLLEXPORT void BatchMm3Decimal64(int64_t *val, int32_t precision, int32_t scale, bool *isValNull,
    int32_t *seed, bool *isSeedNull, bool *resIsNull, int32_t *output, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; ++i) {
        seed[i] = isSeedNull[i] ? 0 : seed[i];
        output[i] = static_cast<int32_t>(HashLong(val[i] * !isValNull[i], static_cast<uint32_t>(seed[i])));
    }
}

extern "C" DLLEXPORT void BatchMm3Decimal128(omniruntime::type::Decimal128 *x, int32_t precision, int32_t scale,
    bool *isValNull, int32_t *seed, bool *isSeedNull, bool *resIsNull, int32_t *output, int32_t rowCnt)
{
    int32_t byteLen = 0;
    for (int i = 0; i < rowCnt; ++i) {
        auto bytes = omniruntime::type::Decimal128Utils::Decimal128ToBytes(x[i].HighBits(), x[i].LowBits(), byteLen);
        output[i] = static_cast<int32_t>(HashUnsafeBytes(reinterpret_cast<char *>(bytes), byteLen, seed[i]));
        delete[] bytes;
    }
}

extern "C" DLLEXPORT void BatchMm3Boolean(bool *val, bool *isValNull, int32_t *seed, bool *isSeedNull, bool *resIsNull,
    int32_t *output, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; ++i) {
        seed[i] = isSeedNull[i] ? 0 : seed[i];
        output[i] = static_cast<int32_t>(
            HashInt(static_cast<uint32_t>((val[i] ? 1 : 0) * !isValNull[i]), static_cast<uint32_t>(seed[i])));
    }
}

extern "C" DLLEXPORT void BatchCombineHash(int64_t *prevHashVal, bool *isPrevHashValNull, int64_t *val, bool *isValNull,
    bool *resIsNull, int64_t *output, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; ++i) {
        prevHashVal[i] = isPrevHashValNull ? 0 : prevHashVal[i];
        val[i] = isValNull[i] ? 0 : val[i];
        output[i] = COMBINE_HASH_VALUE * prevHashVal[i] + val[i];
    }
}
}