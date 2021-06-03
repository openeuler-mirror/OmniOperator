#include "hash_util.h"
#include "../vector/table.h"
#include <stdint.h>
#include <math.h>

const int64_t PRIME64_1 = 0x9E3779B185EBCA87L;
const int64_t PRIME64_2 = 0xC2B2AE3D27D4EB4FL;
const int64_t PRIME64_3 = 0x165667B19E3779F9L;
const int64_t PRIME64_4 = 0x85EBCA77C2b2AE63L;
const int64_t PRIME64_5 = 0x27D4EB2F165667C5L;
const int32_t ROTATE_DISTANCE_31 = 31;
const int32_t ROTATE_DISTANCE_27 = 27;
const int64_t DEFAULT_SEED = 0;
const int32_t SIZE_OF_LONG = 8;

int64_t nextPowerOfTwo(int64_t x);
int32_t HashUtil::hashArraySize(int32_t expected, float f)
{
    double result = (double)expected / (double)f;
    int64_t s = (int64_t)std::ceil(result);
    s = nextPowerOfTwo(s);

    if (s > 1073741824) {
        //TODO:
        return expected;
    }
    else {
        return (int32_t)s;
    }
}

int64_t nextPowerOfTwo(int64_t x)
{
    if (x == 0) {
        return 1;
    }
    else {
        --x;
        x |= x >> 1;
        x |= x >> 2;
        x |= x >> 4;
        x |= x >> 8;
        x |= x >> 16;
        return (x | x >> 32) + 1;
    }
}

#pragma clang optimize off
int64_t rotateLeft(int64_t i, int32_t distance)
{
    uint64_t right = ((uint64_t)i) >> -distance;
    int64_t result = (i << distance) | right;
    return result;
}
#pragma clang optimize on

// for hashing a real data value
// from AbstractLongType#hash()
// for type double, how to convert double value to long value?
int64_t HashUtil::hashValue(int64_t value)
{
    return rotateLeft(value * PRIME64_2, ROTATE_DISTANCE_31) * PRIME64_1;
}

int64_t HashUtil::getHash(int64_t previousHashValue, int64_t value)
{
    return (31 * previousHashValue + value);
}

int32_t HashUtil::getRawHashPosition(int64_t rawHash, int64_t mask)
{
    uint64_t hashValue = ((uint64_t)rawHash) >> 33;
    rawHash ^= (int64_t)hashValue;
    rawHash *= 0xff51afd7ed558ccdL;

    hashValue = ((uint64_t)rawHash) >> 33;
    rawHash ^= (int64_t)hashValue;
    rawHash *= 0xc4ceb9fe1a85ec53L;

    hashValue = ((uint64_t)rawHash) >> 33;
    rawHash ^= (int64_t)hashValue;

    return (int32_t)(rawHash & mask);
}

int64_t reverse(int64_t rawHash);
int64_t updateTail(int64_t hash, int64_t value);
int64_t finalShuffle(int64_t hash);

int32_t HashUtil::getRawHashPartition(int64_t rawHash, int32_t mask)
{
    int64_t value = reverse(rawHash);
    int64_t hash = DEFAULT_SEED + PRIME64_5 + SIZE_OF_LONG;
    hash = updateTail(hash, value);
    hash = finalShuffle(hash);

    return (int32_t)hash & mask;
}

int64_t reverse(int64_t rawHash)
{
    uint64_t hash = (uint64_t)rawHash;
    hash = hash >> 1;
    rawHash = (rawHash & 0x5555555555555555L) << 1 | ((int64_t)hash) & 0x5555555555555555L;

    hash = (uint64_t)rawHash;
    hash = hash >> 2;
    rawHash = (rawHash & 0x3333333333333333L) << 2 | ((int64_t)hash) & 0x3333333333333333L;

    hash = (uint64_t)rawHash;
    hash = hash >> 4;
    rawHash = (rawHash & 0x0f0f0f0f0f0f0f0fL) << 4 | ((int64_t)hash) & 0x0f0f0f0f0f0f0f0fL;

    hash = (uint64_t)rawHash;
    hash = hash >> 8;
    rawHash = (rawHash & 0x00ff00ff00ff00ffL) << 8 | ((int64_t)hash) & 0x00ff00ff00ff00ffL;

    hash = (uint64_t)rawHash;
    int64_t temp1 = (int64_t)(hash >> 16);
    int64_t temp2 = (int64_t)(hash >> 48);
    rawHash = (rawHash << 48) | ((rawHash & 0xffff0000L) << 16) |
        (temp1 & 0xffff0000L) | temp2;
    return rawHash;
}

int64_t mix(int64_t current, int64_t value)
{
    return rotateLeft(current + value * PRIME64_2, ROTATE_DISTANCE_31) * PRIME64_1;
}

int64_t updateTail(int64_t hash, int64_t value)
{
    int64_t temp = hash ^ mix(0, value);
    return rotateLeft(temp, ROTATE_DISTANCE_27) * PRIME64_1 + PRIME64_4;
}

int64_t finalShuffle(int64_t hash)
{
    uint64_t hashValue = ((uint64_t)hash) >> 33;
    hash ^= (int64_t)hashValue;
    hash *= PRIME64_2;

    hashValue = ((uint64_t)hash) >> 29;
    hash ^= (int64_t)hashValue;
    hash *= PRIME64_3;

    hashValue = ((uint64_t)hash) >> 32;
    hash ^= (int64_t)hashValue;

    return hash;
}
