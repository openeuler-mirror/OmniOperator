/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Murmur3 Hash function
 */
#ifndef __MURMUR3HASH_H__
#define __MURMUR3HASH_H__

#include <iostream>
#include <huawei_secure_c/include/securec.h>

namespace omniruntime {
namespace codegen {
// All extern functions go here temporarily
#ifdef _WIN32
#define DLLEXPORT __declspec(dllexport)
#else
#define DLLEXPORT
#endif

extern "C" DLLEXPORT int64_t CombineHash(int64_t prevHashVal, bool isPrevHashValNull, int64_t val, bool isValNull);

extern "C" DLLEXPORT int32_t Mm3Int32(int32_t val, bool isValNull, int32_t seed, bool isSeedNull);

extern "C" DLLEXPORT int32_t Mm3Int64(int64_t val, bool isValNull, int32_t seed, bool isSeedNull);

extern "C" DLLEXPORT int32_t Mm3String(const char *val, int32_t valLen, bool isValNull, int32_t seed, bool isSeedNull);

extern "C" DLLEXPORT int32_t Mm3Double(double val, bool isValNull, int32_t seed, bool isSeedNull);

extern "C" DLLEXPORT int32_t Mm3Decimal64(int64_t val, int32_t precision, int32_t scale, bool isValNull, int32_t seed,
    bool isSeedNull);

extern "C" DLLEXPORT int32_t Mm3Decimal128(int64_t xHigh, uint64_t xLow, int32_t precision, int32_t scale,
    bool isValNull, int32_t seed, bool isSeedNull);

uint32_t HashUnsafeBytes(const std::string &base, uint32_t lengthInBytes, uint32_t seed);
uint32_t HashLong(uint64_t input, uint32_t seed);
uint32_t HashInt(uint32_t input, uint32_t seed);
uint32_t HashBytesByInt(const std::string &base, uint32_t lengthInBytes, uint32_t seed);
uint32_t ReverseBytes(uint32_t x);
bool IsBigEndian();
uint32_t Fmix(uint32_t h1, uint32_t length);
uint32_t MixH1(uint32_t h1, uint32_t k1);
uint32_t MixK1(uint32_t k1);
uint32_t RotateLeft(uint32_t i, uint32_t distance);
}
}
// OMNI_RUNTIME_MURMUR3_HASH_H
#endif