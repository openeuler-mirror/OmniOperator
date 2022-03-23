/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Murmur3 Hash function
 */
#ifndef __MURMUR3HASH_H__
#define __MURMUR3HASH_H__

#include <iostream>
#include <huawei_secure_c/include/securec.h>

// All extern functions go here temporarily
#ifdef _WIN32
#define DLLEXPORT __declspec(dllexport)
#else
#define DLLEXPORT
#endif

static const int32_t MM3_C1 = 0xcc9e2d51;
static const int32_t MM3_C2 = 0x1b873593;

static const int8_t MM3_BITS_INT = 32;

static const int8_t MIXK1_ROTATE_LEFT_NUM = 15;

static const int8_t MIXH1_ROTATE_LEFT_NUM = 13;
static const int32_t MIXH1_MULTIPLY_M = 5;
static const int32_t MIXH1_ADD_N = 0xe6546b64;

static const int8_t FMIX_RIGHT_SHIFT_M = 16;
static const int8_t FMIX_RIGHT_SHIFT_N = 13;
static const int32_t FMIX_MULTIPLY_M = 0x85ebca6b;
static const int32_t FMIX_MULTIPLY_N = 0xc2b2ae35;

static const int8_t HASH_LONG_RIGHT_SHIFT = 32;

static const int32_t MM3_SIZE_INT = 4;
static const int32_t MM3_SIZE_LONG = 8;

static const int8_t HASH_BYTES_MEMCPY_CNT = 1;

static const int32_t MM3_INT_ONE = 1;

static const int32_t REVERSE_SHIFT_M = 24;
static const int32_t REVERSE_SHIFT_N = 8;
static const int32_t REVERSE_AND_A = 0xff;
static const int32_t REVERSE_AND_B = 0xff0000;
static const int32_t REVERSE_AND_C = 0xff00;
static const int32_t REVERSE_AND_D = 0xff000000;

static const int32_t MM3_HALFWORD_INIT = 0;


extern "C" DLLEXPORT int32_t Mm3Int32(int32_t val, bool isNull, int32_t seed);

extern "C" DLLEXPORT int32_t Mm3Int64(int64_t val, bool isNull, int32_t seed);

extern "C" DLLEXPORT int32_t Mm3String(const char *val, int32_t valLen, bool isNull, int32_t seed);

extern "C" DLLEXPORT int32_t Mm3Double(double val, bool isNull, int32_t seed);

// OMNI_RUNTIME_MURMUR3_HASH_H
#endif