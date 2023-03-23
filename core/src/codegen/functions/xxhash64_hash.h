/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 * Description: registry  function  implementation
 */
#ifndef OMNI_RUNTIME_XXHASH64_HASH_H
#define OMNI_RUNTIME_XXHASH64_HASH_H
#include <iostream>

namespace omniruntime::codegen::function {
// All extern functions go here temporarily
#ifdef _WIN32
#define DLLEXPORT __declspec(dllexport)
#else
#define DLLEXPORT
#endif
extern "C" DLLEXPORT int64_t XxH64Int16(int16_t val, bool isValNull, int64_t seed, bool isSeedNull);

extern "C" DLLEXPORT int64_t XxH64Int32(int32_t val, bool isValNull, int64_t seed, bool isSeedNull);

extern "C" DLLEXPORT int64_t XxH64Int64(int64_t val, bool isValNull, int64_t seed, bool isSeedNull);

extern "C" DLLEXPORT int64_t XxH64String(const char *val, int32_t valLen, bool isValNull, int64_t seed,
    bool isSeedNull);

extern "C" DLLEXPORT int64_t XxH64Double(double val, bool isValNull, int64_t seed, bool isSeedNull);

extern "C" DLLEXPORT int64_t XxH64Decimal64(int64_t val, int32_t precision, int32_t scale, bool isValNull, int64_t seed,
    bool isSeedNull);

extern "C" DLLEXPORT int64_t XxH64Decimal128(int64_t xHigh, uint64_t xLow, int32_t precision, int32_t scale,
    bool isValNull, int64_t seed, bool isSeedNull);

extern "C" DLLEXPORT int64_t XxH64Boolean(bool val, bool isValNull, int64_t seed, bool isSeedNull);
}
#endif // OMNI_RUNTIME_XXHASH64_HASH_H