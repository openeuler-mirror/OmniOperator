/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: hash util implementations
 */
#include "hash_util.h"
#include <stdint.h>
#include <math.h>

namespace {
const int64_t PRIME64_1 = 0x9E3779B185EBCA87L;
const int64_t PRIME64_2 = 0xC2B2AE3D27D4EB4FL;
const int64_t PRIME64_3 = 0x165667B19E3779F9L;
const int64_t PRIME64_4 = 0x85EBCA77C2b2AE63L;
const int64_t PRIME64_5 = 0x27D4EB2F165667C5L;
const int32_t MAX_ARRAY_SIZE = 1073741824;
const int32_t UPDATE_BODY_LENGTH = 32;
}

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

ALWAYS_INLINE int64_t RotateLeft(uint64_t i, int32_t distance)
{
    return (i << distance) | (i >> (MAX_ROTATE_DISTANCE - distance));
}

ALWAYS_INLINE int64_t Reverse(uint64_t i)
{
    i = (i & 0x5555555555555555L) << ROTATE_DISTANCE_1 | (i >> ROTATE_DISTANCE_1) & 0x5555555555555555L;
    i = (i & 0x3333333333333333L) << ROTATE_DISTANCE_2 | (i >> ROTATE_DISTANCE_2) & 0x3333333333333333L;
    i = (i & 0x0f0f0f0f0f0f0f0fL) << ROTATE_DISTANCE_4 | (i >> ROTATE_DISTANCE_4) & 0x0f0f0f0f0f0f0f0fL;
    i = (i & 0x00ff00ff00ff00ffL) << ROTATE_DISTANCE_8 | (i >> ROTATE_DISTANCE_8) & 0x00ff00ff00ff00ffL;
    i = (i << ROTATE_DISTANCE_48) | ((i & 0xffff0000L) << ROTATE_DISTANCE_16) |
        ((i >> ROTATE_DISTANCE_16) & 0xffff0000L) | (i >> ROTATE_DISTANCE_48);
    return i;
}

ALWAYS_INLINE int64_t XxHash64FinalShuffle(uint64_t hash)
{
    hash ^= hash >> ROTATE_DISTANCE_33;
    hash *= PRIME64_2;
    hash ^= hash >> ROTATE_DISTANCE_29;
    hash *= PRIME64_3;
    hash ^= hash >> ROTATE_DISTANCE_32;
    return hash;
}

ALWAYS_INLINE int64_t XxHash64Mix(int64_t current, int64_t value)
{
    return RotateLeft(current + value * PRIME64_2, ROTATE_DISTANCE_31) * PRIME64_1;
}

ALWAYS_INLINE int64_t XxHash64UpdateTail(int64_t hash, int64_t value)
{
    int64_t temp = hash ^ XxHash64Mix(0, value);
    return RotateLeft(temp, ROTATE_DISTANCE_27) * PRIME64_1 + PRIME64_4;
}

ALWAYS_INLINE int64_t XxHash64UpdateTail(int64_t hash, int32_t value)
{
    int64_t unsignedValue = value & 0xFFFFFFFFL;
    int64_t temp = hash ^ (unsignedValue * PRIME64_1);
    return RotateLeft(temp, ROTATE_DISTANCE_23) * PRIME64_2 + PRIME64_3;
}

ALWAYS_INLINE int64_t XxHash64UpdateTail(int64_t hash, int8_t value)
{
    int32_t unsignedValue = value & 0xFF;
    int64_t temp = hash ^ (unsignedValue * PRIME64_5);
    return RotateLeft(temp, ROTATE_DISTANCE_11) * PRIME64_1;
}

ALWAYS_INLINE int64_t XxHash64UpdateTail(int64_t hash, int8_t *address, int index, int length)
{
    while (index < length) {
        hash = XxHash64UpdateTail(hash, address[index]);
        index++;
    }
    hash = XxHash64FinalShuffle(hash);
    return hash;
}

ALWAYS_INLINE int64_t XxHash64Update(int64_t hash, int64_t value)
{
    int64_t temp = hash ^ XxHash64Mix(0, value);
    return temp * PRIME64_1 + PRIME64_4;
}

int64_t XxHash64UpdateBody(int64_t seed, int8_t *address, int32_t length)
{
    int64_t v1 = seed + PRIME64_1 + PRIME64_2;
    int64_t v2 = seed + PRIME64_2;
    int64_t v3 = seed;
    int64_t v4 = seed - PRIME64_1;

    int64_t hash = RotateLeft(v1, ROTATE_DISTANCE_1) + RotateLeft(v2, ROTATE_DISTANCE_7) +
        RotateLeft(v3, ROTATE_DISTANCE_12) + RotateLeft(v4, ROTATE_DISTANCE_18);

    hash = XxHash64Update(hash, v1);
    hash = XxHash64Update(hash, v2);
    hash = XxHash64Update(hash, v3);
    hash = XxHash64Update(hash, v4);

    return hash;
}

ALWAYS_INLINE int64_t XxHash64Hash(int64_t seed, int8_t *data, int32_t offset, int32_t length)
{
    int8_t *address = data + offset;
    int64_t hash = 0;
    if (length >= UPDATE_BODY_LENGTH) {
        hash = XxHash64UpdateBody(seed, address, length);
    } else {
        hash = seed + PRIME64_5;
    }
    hash += length;

    int index = length & 0xFFFFFFE0;
    return XxHash64UpdateTail(hash, address, index, length);
}

ALWAYS_INLINE int64_t HashUtil::XxHash64HashValue(int64_t value)
{
    int64_t hash = DEFAULT_SEED + PRIME64_5 + SIZE_OF_LONG;
    hash = XxHash64UpdateTail(hash, value);
    hash = XxHash64FinalShuffle(hash);
    return hash;
}

ALWAYS_INLINE int64_t HashUtil::HashValue(int32_t value)
{
    return RotateLeft(value * PRIME64_2, ROTATE_DISTANCE_31) * PRIME64_1;
}

// for hashing a real data value
// from AbstractLongType#hash()
// for type double, how to convert double value to long value?
ALWAYS_INLINE int64_t HashUtil::HashValue(int64_t value)
{
    return RotateLeft(value * PRIME64_2, ROTATE_DISTANCE_31) * PRIME64_1;
}

ALWAYS_INLINE int64_t DoubleToLongBits(double value)
{
    return static_cast<int64_t>(value);
}

ALWAYS_INLINE int64_t HashUtil::HashValue(double value)
{
    return HashValue(DoubleToLongBits(value));
}

ALWAYS_INLINE int64_t HashUtil::HashValue(int8_t *value, int32_t length)
{
    return XxHash64Hash(DEFAULT_SEED, value, 0, length);
}

ALWAYS_INLINE long UnpackUnsignedLong(int64_t value)
{
    return value & ~SIGN_LONG_MASK;
}

ALWAYS_INLINE int64_t HashUtil::HashValue(int64_t low, int64_t high)
{
    return XxHash64HashValue(low) ^ XxHash64HashValue(UnpackUnsignedLong(high));
}

ALWAYS_INLINE int32_t HashUtil::GetRawHashPartition(int64_t rawHash, int32_t mask)
{
    int64_t value = Reverse(rawHash);
    int64_t hash = DEFAULT_SEED + PRIME64_5 + SIZE_OF_LONG;
    hash = XxHash64UpdateTail(hash, value);
    hash = XxHash64FinalShuffle(hash);

    return static_cast<int32_t>(hash) & mask;
}
