/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2021. All rights reserved.
 */
#include "hash_util.h"
#include <stdint.h>
#include <math.h>

const int64_t PRIME64_1 = 0x9E3779B185EBCA87L;
const int64_t PRIME64_2 = 0xC2B2AE3D27D4EB4FL;
const int64_t PRIME64_3 = 0x165667B19E3779F9L;
const int64_t PRIME64_4 = 0x85EBCA77C2b2AE63L;
const int64_t PRIME64_5 = 0x27D4EB2F165667C5L;
const int32_t MAX_ARRAY_SIZE = 1073741824;

int64_t NextPowerOfTwo(int64_t x);
int32_t HashUtil::HashArraySize(int32_t expected, float f)
{
    double result = static_cast<double>(expected) / static_cast<double>(f);
    int64_t s = static_cast<int64_t>(std::ceil(result));
    s = NextPowerOfTwo(s);

    if (s > MAX_ARRAY_SIZE) {
        // TODO:
        return expected;
    } else {
        return static_cast<int32_t>(s);
    }
}

int64_t NextPowerOfTwo(int64_t x)
{
    if (x == 0) {
        return 1;
    } else {
        --x;
        x |= x >> ROTATE_DISTANCE_1;
        x |= x >> ROTATE_DISTANCE_2;
        x |= x >> ROTATE_DISTANCE_4;
        x |= x >> ROTATE_DISTANCE_8;
        x |= x >> ROTATE_DISTANCE_16;
        return (x | x >> ROTATE_DISTANCE_32) + 1;
    }
}

#pragma clang optimize off
int64_t RotateLeft(int64_t i, int32_t distance)
{
    return (i << distance) | (static_cast<uint64_t>(i) >> -distance);
}
#pragma clang optimize on

// for hashing a real data value
// from AbstractLongType#hash()
// for type double, how to convert double value to long value?
int64_t HashUtil::HashValue(uint64_t value)
{
    return RotateLeft(value * PRIME64_2, ROTATE_DISTANCE_31) * PRIME64_1;
}

int64_t Reverse(int64_t rawHash);
int64_t UpdateTail(int64_t hash, int64_t value);
int64_t FinalShuffle(int64_t hash);

int32_t HashUtil::GetRawHashPartition(uint64_t rawHash, uint32_t mask)
{
    int64_t value = Reverse(rawHash);
    int64_t hash = DEFAULT_SEED + PRIME64_5 + SIZE_OF_LONG;
    hash = UpdateTail(hash, value);
    hash = FinalShuffle(hash);

    return static_cast<int32_t>(hash) & mask;
}

int64_t Reverse(int64_t rawHash)
{
    uint64_t hash = static_cast<uint64_t>(rawHash);
    hash = hash >> ROTATE_DISTANCE_1;
    rawHash =
        ((rawHash & 0x5555555555555555L) << ROTATE_DISTANCE_1) | (static_cast<int64_t>(hash) & 0x5555555555555555L);

    hash = static_cast<uint64_t>(rawHash);
    hash = hash >> ROTATE_DISTANCE_2;
    rawHash =
        ((rawHash & 0x3333333333333333L) << ROTATE_DISTANCE_2) | (static_cast<int64_t>(hash) & 0x3333333333333333L);

    hash = static_cast<uint64_t>(rawHash);
    hash = hash >> ROTATE_DISTANCE_4;
    rawHash =
        ((rawHash & 0x0f0f0f0f0f0f0f0fL) << ROTATE_DISTANCE_4) | (static_cast<int64_t>(hash) & 0x0f0f0f0f0f0f0f0fL);

    hash = static_cast<uint64_t>(rawHash);
    hash = hash >> ROTATE_DISTANCE_8;
    rawHash =
        ((rawHash & 0x00ff00ff00ff00ffL) << ROTATE_DISTANCE_8) | (static_cast<int64_t>(hash) & 0x00ff00ff00ff00ffL);

    hash = static_cast<uint64_t>(rawHash);
    int64_t temp1 = static_cast<int64_t>(hash >> ROTATE_DISTANCE_16);
    int64_t temp2 = static_cast<int64_t>(hash >> ROTATE_DISTANCE_48);
    rawHash = (rawHash << ROTATE_DISTANCE_48) | ((rawHash & 0xffff0000L) << ROTATE_DISTANCE_16) |
        (temp1 & 0xffff0000L) | temp2;
    return rawHash;
}

int64_t Mix(int64_t current, int64_t value)
{
    return RotateLeft(current + value * PRIME64_2, ROTATE_DISTANCE_31) * PRIME64_1;
}

int64_t UpdateTail(int64_t hash, int64_t value)
{
    int64_t temp = hash ^ Mix(0, value);
    return RotateLeft(temp, ROTATE_DISTANCE_27) * PRIME64_1 + PRIME64_4;
}

int64_t FinalShuffle(int64_t hash)
{
    uint64_t hashValue = static_cast<uint64_t>(hash) >> ROTATE_DISTANCE_33;
    hash ^= static_cast<int64_t>(hashValue);
    hash *= PRIME64_2;

    hashValue = static_cast<uint64_t>(hash) >> ROTATE_DISTANCE_29;
    hash ^= static_cast<int64_t>(hashValue);
    hash *= PRIME64_3;

    hashValue = static_cast<uint64_t>(hash) >> ROTATE_DISTANCE_32;
    hash ^= static_cast<int64_t>(hashValue);

    return hash;
}
