/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: hash util implementations
 */
#ifndef __HASH_UTIL_H__
#define __HASH_UTIL_H__

#include <stdint.h>
#include "../util/compiler_util.h"

const int32_t ROTATE_DISTANCE_1 = 1;
const int32_t ROTATE_DISTANCE_2 = 2;
const int32_t ROTATE_DISTANCE_4 = 4;
const int32_t ROTATE_DISTANCE_7 = 7;
const int32_t ROTATE_DISTANCE_8 = 8;
const int32_t ROTATE_DISTANCE_11 = 11;
const int32_t ROTATE_DISTANCE_12 = 12;
const int32_t ROTATE_DISTANCE_16 = 16;
const int32_t ROTATE_DISTANCE_18 = 18;
const int32_t ROTATE_DISTANCE_23 = 23;
const int32_t ROTATE_DISTANCE_27 = 27;
const int32_t ROTATE_DISTANCE_29 = 29;
const int32_t ROTATE_DISTANCE_31 = 31;
const int32_t ROTATE_DISTANCE_32 = 32;
const int32_t ROTATE_DISTANCE_33 = 33;
const int32_t ROTATE_DISTANCE_48 = 48;
const int32_t MAX_ROTATE_DISTANCE = 64;
const int64_t DEFAULT_SEED = 0;
const int32_t SIZE_OF_LONG = 8;
const int64_t SIGN_LONG_MASK = 1L << 63;
const int64_t HASH_OF_TRUE = 1231;
const int64_t HASH_OF_FALSE = 1237;

class HashUtil {
public:
    static int32_t HashArraySize(int32_t expected, float f);

    static ALWAYS_INLINE int64_t HashValue(int32_t value);

    static ALWAYS_INLINE int64_t HashValue(int64_t value);

    static ALWAYS_INLINE int64_t HashValue(double value);

    static ALWAYS_INLINE int64_t HashValue(bool value)
    {
        return value ? HASH_OF_TRUE : HASH_OF_FALSE;
    }

    static ALWAYS_INLINE int64_t HashDecimal64Value(int64_t value)
    {
        return value;
    }

    static ALWAYS_INLINE int64_t HashValue(int64_t low, int64_t high);

    static ALWAYS_INLINE int64_t HashValue(int8_t *value, int32_t length);

    static ALWAYS_INLINE int64_t XxHash64HashValue(int64_t value);

    ALWAYS_INLINE uint64_t operator()(uint64_t combinedHash) const
    {
        return combinedHash;
    }

    static ALWAYS_INLINE int64_t CombineHash(int64_t previousHashValue, int64_t value)
    {
        return (ROTATE_DISTANCE_31 * previousHashValue + value);
    }

    /*
     * it is used to get position for rawHash when reading or writing join hash vecBatch
     */
    static ALWAYS_INLINE int32_t GetRawHashPosition(int64_t rawHash, int64_t mask)
    {
        uint64_t hash = static_cast<uint64_t>(rawHash);
        hash ^= hash >> ROTATE_DISTANCE_33;
        hash *= 0xff51afd7ed558ccdL;
        hash ^= hash >> ROTATE_DISTANCE_33;
        hash *= 0xc4ceb9fe1a85ec53L;
        hash ^= hash >> ROTATE_DISTANCE_33;

        return static_cast<int32_t>(hash & mask);
    }

    /*
     * it is used to get partition for rawHash when getting partition for probe of join and local exchange
     */
    static ALWAYS_INLINE int32_t GetRawHashPartition(int64_t rawHash, int32_t mask);
};

#endif
