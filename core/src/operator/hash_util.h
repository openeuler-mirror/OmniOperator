/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2021. All rights reserved.
 */
#ifndef __HASH_UTIL_H__
#define __HASH_UTIL_H__

#include <stdint.h>

const int32_t ROTATE_DISTANCE_1 = 1;
const int32_t ROTATE_DISTANCE_2 = 2;
const int32_t ROTATE_DISTANCE_4 = 4;
const int32_t ROTATE_DISTANCE_8 = 8;
const int32_t ROTATE_DISTANCE_16 = 16;
const int32_t ROTATE_DISTANCE_27 = 27;
const int32_t ROTATE_DISTANCE_29 = 29;
const int32_t ROTATE_DISTANCE_31 = 31;
const int32_t ROTATE_DISTANCE_32 = 32;
const int32_t ROTATE_DISTANCE_33 = 33;
const int32_t ROTATE_DISTANCE_48 = 48;
const int64_t DEFAULT_SEED = 0;
const int32_t SIZE_OF_LONG = 8;

class HashUtil {
public:
    static int32_t HashArraySize(int32_t expected, float f);

    static int64_t HashValue(uint64_t value);

    uint64_t operator()(uint64_t combinedHash) const
    {
        return combinedHash;
    }

    static int64_t GetHash(uint64_t previousHashValue, uint64_t value)
    {
        return (ROTATE_DISTANCE_31 * previousHashValue + value);
    }

    /*
     * it is used to get position for rawHash when reading or writing join hash vecBatch
     */
    static int32_t GetRawHashPosition(uint64_t rawHash, uint64_t mask)
    {
        uint64_t hashValue = (static_cast<uint64_t>(rawHash)) >> ROTATE_DISTANCE_33;
        rawHash ^= static_cast<int64_t>(hashValue);
        rawHash *= 0xff51afd7ed558ccdL;

        hashValue = (static_cast<uint64_t>(rawHash)) >> ROTATE_DISTANCE_33;
        rawHash ^= static_cast<int64_t>(hashValue);
        rawHash *= 0xc4ceb9fe1a85ec53L;

        hashValue = (static_cast<uint64_t>(rawHash)) >> ROTATE_DISTANCE_33;
        rawHash ^= static_cast<int64_t>(hashValue);

        return static_cast<int32_t>(rawHash & mask);
    }

    /*
     * it is used to get partition for rawHash when getting partition for probe of join and local exchange
     */
    static int32_t GetRawHashPartition(uint64_t rawHash, uint32_t mask);
};

#endif
