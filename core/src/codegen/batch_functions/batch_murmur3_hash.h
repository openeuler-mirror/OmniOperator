/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * Description: batch mmh3 functions implementation
 */

#ifndef OMNI_RUNTIME_BATCH_MURMUR3_HASH_H
#define OMNI_RUNTIME_BATCH_MURMUR3_HASH_H

#include <iostream>
#include <huawei_secure_c/include/securec.h>
#include "type/decimal128.h"
using namespace omniruntime::type;

namespace omniruntime {
namespace codegen {
#ifdef _WIN32
#define DLLEXPORT __declspec(dllexport)
#else
#define DLLEXPORT
#endif

extern "C" DLLEXPORT void BatchCombineHash(int64_t *prevHashVal, bool *isPrevHashValNull, int64_t *val, bool *isValNull,
    bool *resIsNull, int64_t *output, int32_t rowCnt);

extern "C" DLLEXPORT void BatchMm3Int32(int32_t *val, bool *isValNull, int32_t *seed, bool *isSeedNull, bool *resIsNull,
    int32_t *output, int32_t rowCnt);

extern "C" DLLEXPORT void BatchMm3Int64(int64_t *val, bool *isValNull, int32_t *seed, bool *isSeedNull, bool *resIsNull,
    int32_t *output, int32_t rowCnt);

extern "C" DLLEXPORT void BatchMm3String(uint8_t **val, int32_t *valLen, bool *isValNull, int32_t *seed,
    bool *isSeedNull, bool *resIsNull, int32_t *output, int32_t rowCnt);

extern "C" DLLEXPORT void BatchMm3Double(double *val, bool *isValNull, int32_t *seed, bool *isSeedNull, bool *resIsNull,
    int32_t *output, int32_t rowCnt);

extern "C" DLLEXPORT void BatchMm3Decimal64(int64_t *val, int32_t precision, int32_t scale, bool *isValNull,
    int32_t *seed, bool *isSeedNull, bool *resIsNull, int32_t *output, int32_t rowCnt);

extern "C" DLLEXPORT void BatchMm3Decimal128(Decimal128 *x, int32_t precision, int32_t scale, bool *isValNull,
    int32_t *seed, bool *isSeedNull, bool *resIsNull, int32_t *output, int32_t rowCnt);
}
}

#endif // OMNI_RUNTIME_BATCH_MURMUR3_HASH_H
