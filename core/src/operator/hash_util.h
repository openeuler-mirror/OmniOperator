#ifndef __HASH_UTIL_H__
#define __HASH_UTIL_H__

#include <stdint.h>

class HashUtil
{
public:
    static int32_t hashArraySize(int32_t expected, float f);

    static int64_t hashValue(int64_t value);

    static int64_t getHash(int64_t previousHashValue, int64_t value);

    /*
     * it is used to get position for rawHash when reading or writing join hash vecBatch
     */
    static int32_t getRawHashPosition(int64_t rawHash, int64_t mask);

    /*
     * it is used to get partition for rawHash when getting partition for probe of join and local exchange
     */
    static int32_t getRawHashPartition(int64_t rawHash, int32_t mask);
};

#endif
