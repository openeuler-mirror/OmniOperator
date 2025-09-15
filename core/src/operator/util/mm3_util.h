/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * @Description: hash util implementations
 */

#ifndef OMNI_RUNTIME_MM3_UTIL_H
#define OMNI_RUNTIME_MM3_UTIL_H

#include "type/decimal128_utils.h"
#include "vector/unsafe_vector.h"
#include <cstdint>
#include <arm_neon.h>


namespace omniruntime::op {
static const uint32_t MM3_C1 = 0xcc9e2d51;
static const uint32_t MM3_C2 = 0x1b873593;

static const uint32_t MM3_BITS_INT = 32;

static const uint32_t MIXK1_ROTATE_LEFT_NUM = 15;

static const uint32_t MIXH1_ROTATE_LEFT_NUM = 13;
static const uint32_t MIXH1_MULTIPLY_M = 5;
static const uint32_t MIXH1_ADD_N = 0xe6546b64;

static const uint32_t FMIX_RIGHT_SHIFT_M = 16;
static const uint32_t FMIX_RIGHT_SHIFT_N = 13;
static const uint32_t FMIX_MULTIPLY_M = 0x85ebca6b;
static const uint32_t FMIX_MULTIPLY_N = 0xc2b2ae35;

static const uint32_t HASH_LONG_RIGHT_SHIFT = 32;

static const uint32_t MM3_SIZE_INT = 4;
static const uint32_t MM3_SIZE_LONG = 8;

static const uint32_t REVERSE_SHIFT_M = 24;
static const uint32_t REVERSE_SHIFT_N = 8;
static const uint32_t REVERSE_AND_A = 0xff;
static const uint32_t REVERSE_AND_B = 0xff0000;
static const uint32_t REVERSE_AND_C = 0xff00;
static const uint32_t REVERSE_AND_D = 0xff000000;

static const uint32_t STEP_FOUR = 4;

uint32_t inline RotateLeft(uint32_t i, uint32_t distance)
{
    return (i << distance) | (i >> (MM3_BITS_INT - distance));
}

uint32_t inline MixK1(uint32_t k1)
{
    k1 *= MM3_C1;
    k1 = RotateLeft(k1, MIXK1_ROTATE_LEFT_NUM);
    k1 *= MM3_C2;
    return k1;
}

uint32_t inline MixH1(uint32_t h1, uint32_t k1)
{
    h1 ^= k1;
    h1 = RotateLeft(h1, MIXH1_ROTATE_LEFT_NUM);
    h1 = h1 * MIXH1_MULTIPLY_M + MIXH1_ADD_N;
    return h1;
}

uint32_t inline Fmix(uint32_t h1, uint32_t length)
{
    h1 ^= length;
    h1 ^= h1 >> FMIX_RIGHT_SHIFT_M;
    h1 *= FMIX_MULTIPLY_M;
    h1 ^= h1 >> FMIX_RIGHT_SHIFT_N;
    h1 *= FMIX_MULTIPLY_N;
    h1 ^= h1 >> FMIX_RIGHT_SHIFT_M;
    return h1;
}

uint32_t inline ReverseBytes(uint32_t x)
{
    return ((x >> REVERSE_SHIFT_M) & REVERSE_AND_A) | ((x << REVERSE_SHIFT_N) & REVERSE_AND_B) |
           ((x >> REVERSE_SHIFT_N) & REVERSE_AND_C) | ((x << REVERSE_SHIFT_M) & REVERSE_AND_D);
}

uint32_t inline HashBytesByInt(char *base, uint32_t lengthInBytes, uint32_t seed)
{
    uint32_t h1 = seed;
    for (uint32_t i = 0; i < lengthInBytes; i += MM3_SIZE_INT) {
        uint32_t halfWord = *reinterpret_cast<uint32_t *>(base + i);
        if constexpr (IS_BIG_ENDIAN) {
            halfWord = ReverseBytes(halfWord);
        }
        h1 = MixH1(h1, MixK1(halfWord));
    }
    return h1;
}

static uint32_t HashInt(uint32_t input, uint32_t seed)
{
    uint32_t k1 = MixK1(input);
    uint32_t h1 = MixH1(seed, k1);

    return Fmix(h1, MM3_SIZE_INT);
}

static uint32_t HashLong(uint64_t input, uint32_t seed)
{
    auto low = static_cast<uint32_t>(input);
    auto high = static_cast<uint32_t>(input >> HASH_LONG_RIGHT_SHIFT);

    uint32_t k1 = MixK1(low);
    uint32_t h1 = MixH1(seed, k1);

    k1 = MixK1(high);
    h1 = MixH1(h1, k1);

    return Fmix(h1, MM3_SIZE_LONG);
}

static uint32_t HashUnsafeBytes(char *base, uint32_t lengthInBytes, uint32_t seed)
{
    uint32_t lengthAligned = lengthInBytes - lengthInBytes % MM3_SIZE_INT;
    uint32_t h1 = HashBytesByInt(base, lengthAligned, seed);
    for (uint32_t i = lengthAligned; i < lengthInBytes; i++) {
        auto charVal = *(base + i);
        auto halfWord = static_cast<int32_t>(charVal);
        halfWord &= 0x000000FF; // get the lower eight bits
        uint32_t k1 = MixK1(halfWord);
        h1 = MixH1(h1, k1);
    }
    return Fmix(h1, lengthInBytes);
}

uint32x4_t inline RotateLeft_Neon(uint32x4_t i, uint32_t distance)
{
    uint32x4_t re = vshlq_n_u32(i, distance);
    re = vorrq_u32(re, vshrq_n_u32(i, MM3_BITS_INT - distance));
    return re;
}

uint32x4_t inline MixK1_Neon(uint32x4_t k1)
{
    k1 = vmulq_u32(k1, vdupq_n_u32(MM3_C1));
    k1 = RotateLeft_Neon(k1, MIXK1_ROTATE_LEFT_NUM);
    k1 = vmulq_u32(k1, vdupq_n_u32(MM3_C2));
    return k1;
}

uint32x4_t inline MixH1_Neon(uint32x4_t h1, uint32x4_t k1)
{
    h1 = veorq_u32(h1, k1);
    h1 = RotateLeft_Neon(h1, MIXH1_ROTATE_LEFT_NUM);
    h1 = vmulq_u32(h1, vdupq_n_u32(MIXH1_MULTIPLY_M));
    h1 = vaddq_u32(h1, vdupq_n_u32(MIXH1_ADD_N));
    return h1;
}

uint32x4_t inline Fmix_Neon(uint32x4_t h1, uint32x4_t length)
{
    h1 = veorq_u32(h1, length);
    h1 = veorq_u32(h1, vshrq_n_u32(h1, FMIX_RIGHT_SHIFT_M));
    h1 = vmulq_u32(h1, vdupq_n_u32(FMIX_MULTIPLY_M));
    h1 = veorq_u32(h1, vshrq_n_u32(h1, FMIX_RIGHT_SHIFT_N));
    h1 = vmulq_u32(h1, vdupq_n_u32(FMIX_MULTIPLY_N));
    h1 = veorq_u32(h1, vshrq_n_u32(h1, FMIX_RIGHT_SHIFT_M));
    return h1;
}

static void NeonInt(omniruntime::vec::Vector<int32_t>* currentCol, std::vector<uint32_t> &partitionIds)
{
    auto value_ptr = omniruntime::vec::unsafe::UnsafeVector::GetRawValues(currentCol);
    uint32_t rowCount = partitionIds.size();
    int32_t *partition_ptr = reinterpret_cast<int32_t*>(partitionIds.data());
    uint32x4_t vMM3_SIZE_INT = vdupq_n_u32(MM3_SIZE_INT);
    uint32_t row = 0;
    for (; row + STEP_FOUR < rowCount; row += STEP_FOUR) {
        uint32x4_t value = vld1q_u32(reinterpret_cast<uint32_t*>(value_ptr + row));
        value = MixK1_Neon(value);
        uint32x4_t vseed = vld1q_u32(reinterpret_cast<uint32_t*>(partition_ptr + row));
        vseed = MixH1_Neon(vseed, value);
        vseed = Fmix_Neon(vseed, vMM3_SIZE_INT);
        vst1q_s32(partition_ptr + row, vreinterpretq_s32_u32(vseed));
    }
    for (; row<rowCount; row++) {
        partitionIds[row] = static_cast<int32_t>(
            HashInt(static_cast<uint32_t>(currentCol->GetValue(row)), partitionIds[row])
        );
    }
}

static void NeonLong(omniruntime::vec::Vector<int64_t>* currentCol, std::vector<uint32_t> &partitionIds)
{
    auto value_ptr = omniruntime::vec::unsafe::UnsafeVector::GetRawValues(currentCol);
    uint32_t rowCount = partitionIds.size();
    uint32_t *partition_ptr = reinterpret_cast<uint32_t*>(partitionIds.data());
    uint32x4_t vMM3_SIZE_LONG = vdupq_n_u32(MM3_SIZE_LONG);
    uint32_t row = 0;
    for (; row + STEP_FOUR < rowCount; row += STEP_FOUR) {
        uint32x4x2_t value = vld2q_u32(reinterpret_cast<uint32_t*>(value_ptr + row));
        uint32x4_t vlow = value.val[0];
        uint32x4_t vhigh = value.val[1];
        uint32x4_t vseed = vld1q_u32(reinterpret_cast<uint32_t*>(partition_ptr + row));
        uint32x4_t k1 = MixK1_Neon(vlow);
        uint32x4_t h1 = MixH1_Neon(vseed, k1);
        k1 = MixK1_Neon(vhigh);
        h1 = MixH1_Neon(h1, k1);
        vseed = Fmix_Neon(h1, vMM3_SIZE_LONG);
        vst1q_u32(partition_ptr + row, vseed);
    }
    for (; row<rowCount; row++) {
        partitionIds[row] = static_cast<int32_t>(
            HashLong(static_cast<uint64_t>(currentCol->GetValue(row)), partitionIds[row])
        );
    }
}

static void Mm3Int(omniruntime::vec::BaseVector* vec, int32_t &rowCount, std::vector<uint32_t> &partitionIds)
{
    if (vec->GetEncoding() == vec::OMNI_DICTIONARY) {
        auto currentCol = reinterpret_cast<vec::Vector<vec::DictionaryContainer<int32_t>> *>(vec);
        if (UNLIKELY(currentCol->HasNull())) {
            for (auto row = 0; row < rowCount; row++) {
                if (!currentCol->IsNull(row)) {
                    partitionIds[row] = HashInt(
                        static_cast<uint32_t>(currentCol->GetValue(row)), partitionIds[row]);
                }
            }
        } else {
            for (auto row = 0; row < rowCount; row++) {
                partitionIds[row] = HashInt(
                    static_cast<uint32_t>(currentCol->GetValue(row)), partitionIds[row]);
            }
        }
    } else {
        auto currentCol = reinterpret_cast<omniruntime::vec::Vector<int32_t> *>(vec);
        if (UNLIKELY(currentCol->HasNull())) {
            for (auto row = 0; row < rowCount; row++) {
                if (!currentCol->IsNull(row)) {
                    partitionIds[row] = HashInt(
                        static_cast<uint32_t>(currentCol->GetValue(row)), partitionIds[row]);
                }
            }
        } else {
            NeonInt(currentCol, partitionIds);
        }
    }
}

static void Mm3Long(omniruntime::vec::BaseVector* vec, int32_t &rowCount, std::vector<uint32_t> &partitionIds)
{
    if (vec->GetEncoding() == vec::OMNI_DICTIONARY) {
        auto currentCol = reinterpret_cast<vec::Vector<vec::DictionaryContainer<int64_t>> *>(vec);
        if (UNLIKELY(currentCol->HasNull())) {
            for (auto row = 0; row < rowCount; row++) {
                if (!currentCol->IsNull(row)) {
                    partitionIds[row] = HashLong(
                        static_cast<uint64_t>(currentCol->GetValue(row)), partitionIds[row]);
                }
            }
        } else {
            for (auto row = 0; row < rowCount; row++) {
                partitionIds[row] = HashLong(
                    static_cast<uint64_t>(currentCol->GetValue(row)), partitionIds[row]);
            }
        }
    } else {
        auto currentCol = reinterpret_cast<vec::Vector<int64_t> *>(vec);
        if (UNLIKELY(currentCol->HasNull())) {
            for (auto row = 0; row < rowCount; row++) {
                if (!currentCol->IsNull(row)) {
                    partitionIds[row] = HashLong(
                        static_cast<uint64_t>(currentCol->GetValue(row)), partitionIds[row]);
                }
            }
        } else {
            NeonLong(currentCol, partitionIds);
        }
    }
}

static void Mm3String(omniruntime::vec::BaseVector* vec, int32_t &rowCount, std::vector<uint32_t> &partitionIds)
{
    if (vec->GetEncoding() == vec::OMNI_DICTIONARY) {
        auto currentCol = reinterpret_cast<vec::Vector<vec::DictionaryContainer<std::string_view>> *>(vec);
        if (UNLIKELY(currentCol->HasNull())) {
            for (auto row = 0; row < rowCount; row++) {
                if (!currentCol->IsNull(row)) {
                    std::string_view value = currentCol->GetValue(row);
                    partitionIds[row] = HashUnsafeBytes(
                        const_cast<char *>(value.data()), value.size(), partitionIds[row]);
                }
            }
        } else {
            for (auto row = 0; row < rowCount; row++) {
                std::string_view value = currentCol->GetValue(row);
                partitionIds[row] = HashUnsafeBytes(
                    const_cast<char *>(value.data()), value.size(), partitionIds[row]);
            }
        }
    } else {
        auto currentCol = reinterpret_cast<vec::Vector<vec::LargeStringContainer<std::string_view>> *>(vec);
        if (UNLIKELY(currentCol->HasNull())) {
            for (auto row = 0; row < rowCount; row++) {
                if (!currentCol->IsNull(row)) {
                    std::string_view value = currentCol->GetValue(row);
                    partitionIds[row] = HashUnsafeBytes(
                        const_cast<char *>(value.data()), value.size(), partitionIds[row]);
                }
            }
        } else {
            for (auto row = 0; row < rowCount; row++) {
                std::string_view value = currentCol->GetValue(row);
                partitionIds[row] = HashUnsafeBytes(
                    const_cast<char *>(value.data()), value.size(), partitionIds[row]);
            }
        }
    }
}

static void Mm3Decimal128(omniruntime::vec::BaseVector* vec, int32_t &rowCount, std::vector<uint32_t> &partitionIds)
{
    if (vec->GetEncoding() == vec::OMNI_DICTIONARY) {
        auto currentCol = reinterpret_cast<vec::Vector<vec::DictionaryContainer<type::Decimal128>> *>(vec);
        if (UNLIKELY(currentCol->HasNull())) {
            for (auto row = 0; row < rowCount; row++) {
                if (!currentCol->IsNull(row)) {
                    int32_t byteLen = 0;
                    auto val = currentCol->GetValue(row);
                    auto bytes = omniruntime::type::Decimal128Utils::Decimal128ToBytes(
                        val.HighBits(), val.LowBits(), byteLen);
                    partitionIds[row] = HashUnsafeBytes(
                        reinterpret_cast<char *>(bytes), byteLen, partitionIds[row]);
                    delete[] bytes;
                }
            }
        } else {
            for (auto row = 0; row < rowCount; row++) {
                int32_t byteLen = 0;
                auto val = currentCol->GetValue(row);
                auto bytes = omniruntime::type::Decimal128Utils::Decimal128ToBytes(
                    val.HighBits(), val.LowBits(), byteLen);
                partitionIds[row] = HashUnsafeBytes(
                    reinterpret_cast<char *>(bytes), byteLen, partitionIds[row]);
                delete[] bytes;
            }
        }
    } else {
        auto currentCol = reinterpret_cast<vec::Vector<type::Decimal128> *>(vec);
        if (UNLIKELY(currentCol->HasNull())) {
            for (auto row = 0; row < rowCount; row++) {
                if (!currentCol->IsNull(row)) {
                    int32_t byteLen = 0;
                    auto val = currentCol->GetValue(row);
                    auto bytes = omniruntime::type::Decimal128Utils::Decimal128ToBytes(
                        val.HighBits(), val.LowBits(), byteLen);
                    partitionIds[row] = HashUnsafeBytes(
                        reinterpret_cast<char *>(bytes), byteLen, partitionIds[row]);
                    delete[] bytes;
                }
            }
        } else {
            for (auto row = 0; row < rowCount; row++) {
                int32_t byteLen = 0;
                auto val = currentCol->GetValue(row);
                auto bytes = omniruntime::type::Decimal128Utils::Decimal128ToBytes(
                    val.HighBits(), val.LowBits(), byteLen);
                partitionIds[row] = HashUnsafeBytes(
                    reinterpret_cast<char *>(bytes), byteLen, partitionIds[row]);
                delete[] bytes;
            }
        }
    }
}

static void Mm3Boolean(omniruntime::vec::BaseVector* vec, int32_t &rowCount, std::vector<uint32_t> &partitionIds)
{
    if (vec->GetEncoding() == vec::OMNI_DICTIONARY) {
        auto currentCol = reinterpret_cast<vec::Vector<vec::DictionaryContainer<bool>> *>(vec);
        if (UNLIKELY(currentCol->HasNull())) {
            for (auto row = 0; row < rowCount; row++) {
                if (!currentCol->IsNull(row)) {
                    partitionIds[row] = HashInt(
                        currentCol->GetValue(row) ? 1 : 0,
                        partitionIds[row]);
                }
            }
        } else {
            for (auto row = 0; row < rowCount; row++) {
                partitionIds[row] = HashInt(
                    currentCol->GetValue(row) ? 1 : 0,
                    partitionIds[row]);
            }
        }
    } else {
        auto currentCol = reinterpret_cast<vec::Vector<bool> *>(vec);
        if (UNLIKELY(currentCol->HasNull())) {
            for (auto row = 0; row < rowCount; row++) {
                if (!currentCol->IsNull(row)) {
                    partitionIds[row] = HashInt(
                        currentCol->GetValue(row) ? 1 : 0,
                        partitionIds[row]);
                }
            }
        } else {
            for (auto row = 0; row < rowCount; row++) {
                partitionIds[row] = HashInt(
                    currentCol->GetValue(row) ? 1 : 0,
                    partitionIds[row]);
            }
        }
    }
}

static int32_t Pmod(int32_t x, int32_t y)
{
    int32_t r = x % y;
    if (r < 0) {
        return r + y;
    }
    return r;
}

}
#endif // OMNI_RUNTIME_MM3_UTIL_H
