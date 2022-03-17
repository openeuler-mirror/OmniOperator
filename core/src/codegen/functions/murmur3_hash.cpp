/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Murmur3 Hash function
 */
#include <iostream>
#include <string>

#include "murmur3_hash.h"

using namespace std;

int32_t RotateLeft(int32_t i, int8_t distance)
{
    return (i << distance) | (static_cast<uint32_t>(i) >> (MM3_BITS_INT - distance));
}

int32_t MixK1(int32_t k1)
{
    k1 *= MM3_C1;
    k1 = RotateLeft(k1, MIXK1_ROTATE_LEFT_NUM);
    k1 *= MM3_C2;
    return k1;
}

int32_t MixH1(int32_t h1, int32_t k1)
{
    h1 ^= k1;
    h1 = RotateLeft(h1, MIXH1_ROTATE_LEFT_NUM);
    h1 = h1 * MIXH1_MULTIPLY_M + MIXH1_ADD_N;
    return h1;
}

int32_t Fmix(int32_t h1, int32_t length)
{
    h1 ^= length;
    h1 ^= static_cast<uint32_t>(h1) >> FMIX_RIGHT_SHIFT_M;
    h1 *= FMIX_MULTIPLY_M;
    h1 ^= static_cast<uint32_t>(h1) >> FMIX_RIGHT_SHIFT_N;
    h1 *= FMIX_MULTIPLY_N;
    h1 ^= static_cast<uint32_t>(h1) >> FMIX_RIGHT_SHIFT_M;
    return h1;
}

bool IsBigEndian()
{
    union {
        int32_t m;
        char n;
    } uval = {0};
    uval.m = MM3_INT_ONE;
    if (uval.n == MM3_INT_ONE) {
        return false;
    } else {
        return true;
    }
}

int32_t ReverseBytes(int32_t x)
{
    return ((x >> REVERSE_SHIFT_M) & REVERSE_AND_A) |
           ((x << REVERSE_SHIFT_N) & REVERSE_AND_B) |
           ((x >> REVERSE_SHIFT_N) & REVERSE_AND_C) |
           ((x << REVERSE_SHIFT_M) & REVERSE_AND_D);
}

int32_t HashBytesByInt(const string base, int32_t lengthInBytes, int32_t seed)
{
    int32_t h1 = seed;
    for (int i = 0; i < lengthInBytes; i += MM3_SIZE_INT) {
        int32_t halfWord;
        errno_t ret = memcpy_s(&halfWord, sizeof(halfWord), base.c_str() + i, MM3_SIZE_INT);
        if (ret != EOK) {
            cerr << "Error memcpy in HashBytesByInt!" << endl;
        }
        if (IsBigEndian()) {
            halfWord = ReverseBytes(halfWord);
        }
        h1 = MixH1(h1, MixK1(halfWord));
    }
    return h1;
}


int32_t HashInt(int32_t input, uint32_t seed)
{
    int32_t k1 = MixK1(input);
    int32_t h1 = MixH1(seed, k1);

    return Fmix(h1, MM3_SIZE_INT);
}

int32_t HashLong(int64_t input, int32_t seed)
{
    auto low = static_cast<int32_t>(input);
    auto high = static_cast<int32_t>(static_cast<uint64_t>(input) >> HASH_LONG_RIGHT_SHIFT);

    int32_t k1 = MixK1(low);
    int32_t h1 = MixH1(seed, k1);

    k1 = MixK1(high);
    h1 = MixH1(h1, k1);

    return Fmix(h1, MM3_SIZE_LONG);
}

int32_t HashUnsafeBytes(const string base, int32_t lengthInBytes, int32_t seed)
{
    int32_t lengthAligned = lengthInBytes - lengthInBytes % MM3_SIZE_INT;
    int32_t h1 = HashBytesByInt(base, lengthAligned, seed);
    for (int i = lengthAligned; i < lengthInBytes; i++) {
        int32_t halfWord = MM3_HALFWORD_INIT;
        errno_t ret = memcpy_s(&halfWord, sizeof(halfWord), base.c_str() + i, HASH_BYTES_MEMCPY_CNT);
        if (ret != EOK) {
            cout << "Error memcpy in HashUnsafeBytes!" << endl;
        }
        int32_t k1 = MixK1(halfWord);
        h1 = MixH1(h1, k1);
    }
    return Fmix(h1, lengthInBytes);
}

extern "C" DLLEXPORT int32_t Mm3Int32(int32_t val, bool isNull, int32_t seed)
{
    return HashInt(val * !isNull, seed);
}

extern "C" DLLEXPORT int32_t Mm3Int64(int64_t val, bool isNull, int32_t seed)
{
    return HashLong(val * !isNull, seed);
}

extern "C" DLLEXPORT int32_t Mm3String(const char *val, int32_t valLen, bool isNull, int32_t seed)
{
    string as = string(val, valLen * !isNull);
    return HashUnsafeBytes(as, valLen, seed);
}

extern "C" DLLEXPORT int32_t Mm3Double(double val, bool isNull, int32_t seed)
{
    union {
        int64_t lVal;
        double dVal;
    } uVal = {0};
    uVal.dVal = val * !isNull;
    return HashLong(uVal.lVal, seed);
}