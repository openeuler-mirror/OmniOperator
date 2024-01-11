/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 * @Description: hash functions implementation.
 */

#ifndef __HASHER_H__
#define __HASHER_H__

#include <arm_neon.h>

#include "type/decimal128.h"
#include "type/string_ref.h"
#include "crc32c.h"

namespace omniruntime {
namespace simdutil {
using namespace omniruntime::type;

inline uint64_t HashUint64(uint64_t x)
{
    uint64_t crc = -1ULL;
    __asm__ __volatile__("crc32cx %w[c], %w[c], %x[x]\n\t" : [ c ] "+r"(crc) : [ x ] "r"(x));
    return crc;
}

template <typename T> inline size_t CRC32Hasher(T key)
{
    union {
        T in;
        uint64_t out;
    } u;
    u.out = 0;
    u.in = key;
    return HashUint64(u.out);
}

template <typename T> struct HashCRC32 {
    size_t operator () (T key) const
    {
        return CRC32Hasher<T>(key);
    }
};

template <> struct HashCRC32<Decimal128> {
    size_t operator () (Decimal128 key) const
    {
        uint64_t crc = -1ULL;
        auto x = key.LowBits();
        auto y = key.HighBits();

        __asm__ __volatile__("crc32cx %w[c], %w[c], %x[x]\n\t" : [ c ] "+r"(crc) : [ x ] "r"(x));
        __asm__ __volatile__("crc32cx %w[c], %w[c], %x[y]\n\t" : [ c ] "+r"(crc) : [ y ] "r"(y));
        return crc;
    }
};

template <> struct HashCRC32<StringRef> {
    size_t operator () (StringRef key) const
    {
        return Extend(0, key.data, key.size);
    }
};
}
}
#endif
