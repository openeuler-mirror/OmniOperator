/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 * @Description: hash functions implementation.
 */

#ifndef __HASHER_H__
#define __HASHER_H__

#include <arm_neon.h>
#include <cstdint>

#include "type/decimal128.h"
#include "type/string_ref.h"
#include "crc32c.h"

namespace omniruntime {
namespace simdutil {
constexpr uint32_t BITS_OF_LONG = 64;
using namespace omniruntime::type;

// murmur3 64-bit finalizer. crc32cx only produces a 32-bit result (the top 32 bits of
// the register are zeroed), which leaves the SwissTable's H1 = hash >> 7 with merely 25
// bits of entropy and confines home buckets to the low 2^25 region once a single hash
// table grows past 2^25 slots. Spreading the value across all 64 bits feeds both the tag
// (H2, low 7 bits) and the bucket index (H1) with enough entropy on large tables.
inline uint64_t Mix64(uint64_t h)
{
    h ^= h >> 33;
    h *= 0xff51afd7ed558ccdULL;
    h ^= h >> 33;
    h *= 0xc4ceb9fe1a85ec53ULL;
    h ^= h >> 33;
    return h;
}

inline uint64_t HashUint64(uint64_t x)
{
    uint64_t crc = -1ULL;
    __asm__ __volatile__("crc32cx %w[c], %w[c], %x[x]\n\t" : [ c ] "+r"(crc) : [ x ] "r"(x));
    return Mix64(crc);
}

template <typename T> inline size_t CRC32Hasher(T key)
{
    // For integers less than or equal to 64 bits, calculating their hash value as a uint64 is unnecessary, as these
    // integers can be directly converted into a uint64 type instead of performing a CRC32-based hash calculation.
//    if constexpr (std::is_integral_v<T>) {
//        return static_cast<uint64_t>(key);
//    }
    union {
        T in;
        uint64_t out;
    } u;
    u.out = 0;
    u.in = key;
    return HashUint64(u.out);
}

template <typename T> inline size_t CRC32HasherForInt(T key)
{
    // Calculating integers's hash value
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

template <> struct HashCRC32<int128_t> {
    size_t operator () (int128_t key) const
    {
        uint64_t crc = -1ULL;
        uint64_t low = static_cast<uint64_t>(key);
        uint64_t high = static_cast<uint64_t>((key >> BITS_OF_LONG));
        __asm__ __volatile__("crc32cx %w[c], %w[c], %x[x]\n\t" : [ c ] "+r"(crc) : [ x ] "r"(low));
        __asm__ __volatile__("crc32cx %w[c], %w[c], %x[x]\n\t" : [ c ] "+r"(crc) : [ x ] "r"(high));
        return Mix64(crc);
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
        return Mix64(crc);
    }
};

template <> struct HashCRC32<StringRef> {
    size_t operator () (StringRef key) const
    {
        return Mix64(Extend(0, key.data, key.size));
    }
};
}
}
#endif
