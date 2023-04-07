/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Murmur3 Hash function
 */
#ifndef __MURMUR3HASH_H__
#define __MURMUR3HASH_H__

#include <cstdint>

namespace omniruntime::codegen::function {
// All extern functions go here temporarily
#ifdef _WIN32
#define DLLEXPORT __declspec(dllexport)
#else
#define DLLEXPORT
#endif

extern "C" DLLEXPORT int64_t CombineHash(int64_t prevHashVal, bool isPrevHashValNull, int64_t val, bool isValNull);

extern "C" DLLEXPORT int32_t Mm3Int32(int32_t val, bool isValNull, int32_t seed, bool isSeedNull);

extern "C" DLLEXPORT int32_t Mm3Int64(int64_t val, bool isValNull, int32_t seed, bool isSeedNull);

extern "C" DLLEXPORT int32_t Mm3String(char *val, int32_t valLen, bool isValNull, int32_t seed, bool isSeedNull);

extern "C" DLLEXPORT int32_t Mm3Double(double val, bool isValNull, int32_t seed, bool isSeedNull);

extern "C" DLLEXPORT int32_t Mm3Decimal64(int64_t val, int32_t precision, int32_t scale, bool isValNull, int32_t seed,
    bool isSeedNull);

extern "C" DLLEXPORT int32_t Mm3Decimal128(int64_t xHigh, uint64_t xLow, int32_t precision, int32_t scale,
    bool isValNull, int32_t seed, bool isSeedNull);

extern "C" DLLEXPORT int32_t Mm3Boolean(bool val, bool isValNull, int32_t seed, bool isSeedNull);

uint32_t HashUnsafeBytes(char *base, uint32_t lengthInBytes, uint32_t seed);
uint32_t HashLong(uint64_t input, uint32_t seed);
uint32_t HashInt(uint32_t input, uint32_t seed);
}
// OMNI_RUNTIME_MURMUR3_HASH_H
#endif