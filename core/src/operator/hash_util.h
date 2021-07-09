#ifndef __HASH_UTIL_H__
#define __HASH_UTIL_H__

#include <stdint.h>

class HashUtil
{
public:
    static int32_t hashArraySize(int32_t expected, float f);

    static int64_t hashValue(int64_t value);

    static int64_t getHash(int64_t previousHashValue, int64_t value)
    {
        return (31 * previousHashValue + value);
    }

    /*
     * it is used to get position for rawHash when reading or writing join hash vecBatch
     */
    static int32_t getRawHashPosition(int64_t rawHash, int64_t mask)
    {
        uint64_t hashValue = (static_cast<uint64_t>(rawHash)) >> 33;
        rawHash ^= static_cast<int64_t>(hashValue);
        rawHash *= 0xff51afd7ed558ccdL;

        hashValue = (static_cast<uint64_t>(rawHash)) >> 33;
        rawHash ^= static_cast<int64_t>(hashValue);
        rawHash *= 0xc4ceb9fe1a85ec53L;

        hashValue = (static_cast<uint64_t>(rawHash)) >> 33;
        rawHash ^= static_cast<int64_t>(hashValue);

        return static_cast<int32_t>(rawHash & mask);
    }

    /*
     * it is used to get partition for rawHash when getting partition for probe of join and local exchange
     */
    static int32_t getRawHashPartition(int64_t rawHash, int32_t mask);
};

#endif
