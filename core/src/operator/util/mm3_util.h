/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * @Description: hash util implementations
 */

#ifndef OMNI_RUNTIME_MM3_UTIL_H
#define OMNI_RUNTIME_MM3_UTIL_H

#include "type/decimal128_utils.h"
#include "vector/unsafe_vector.h"
#include "vector/array_vector.h"
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

static void NeonInt8(omniruntime::vec::Vector<int8_t>* currentCol, std::vector<uint32_t> &partitionIds)
{
    auto value_ptr = omniruntime::vec::unsafe::UnsafeVector::GetRawValues(currentCol);
    uint32_t rowCount = partitionIds.size();
    int32_t *partition_ptr = reinterpret_cast<int32_t*>(partitionIds.data());
    uint32x4_t vMM3_SIZE_INT = vdupq_n_u32(MM3_SIZE_INT);

    uint32_t row = 0;
    for (; row + STEP_FOUR < rowCount; row += STEP_FOUR) {
        // load 16 int8_ts，but only process the first 4
        int8x16_t v8 = vld1q_s8(value_ptr + row);

        // zero extends to 32 bits
        int16x8_t v16_low = vmovl_s8(vget_low_s8(v8));
        int16x8_t v16_high = vmovl_s8(vget_high_s8(v8));
        int32x4_t v32_0 = vmovl_s16(vget_low_s16(v16_low));

        uint32x4_t value = vreinterpretq_u32_s32(v32_0);
        value = MixK1_Neon(value);

        uint32x4_t vseed = vld1q_u32(reinterpret_cast<uint32_t*>(partition_ptr + row));
        vseed = MixH1_Neon(vseed, value);
        vseed = Fmix_Neon(vseed, vMM3_SIZE_INT);
        vst1q_s32(partition_ptr + row, vreinterpretq_s32_u32(vseed));
    }

    for (; row<rowCount; row++) {
        uint32_t val = static_cast<uint32_t>(static_cast<int32_t>(value_ptr[row]));
        partitionIds[row] = static_cast<int32_t>(HashInt(val, partitionIds[row]));
    }
}

static void NeonInt16(omniruntime::vec::Vector<int16_t>* currentCol, std::vector<uint32_t> &partitionIds)
{
    auto value_ptr = omniruntime::vec::unsafe::UnsafeVector::GetRawValues(currentCol);
    uint32_t rowCount = partitionIds.size();
    int32_t *partition_ptr = reinterpret_cast<int32_t*>(partitionIds.data());
    uint32x4_t vMM3_SIZE_INT = vdupq_n_u32(MM3_SIZE_INT);

    uint32_t row = 0;

    for (; row + STEP_FOUR < rowCount; row += STEP_FOUR) {
        // load 8 int16_ts，but only process the first 4
        int16x8_t v16 = vld1q_s16(value_ptr + row);

        // extend the four int16_t zeros to int32_t
        int32x4_t v32 = vmovl_s16(vget_low_s16(v16));

        uint32x4_t value = vreinterpretq_u32_s32(v32);
        value = MixK1_Neon(value);

        uint32x4_t vseed = vld1q_u32(reinterpret_cast<uint32_t*>(partition_ptr + row));
        vseed = MixH1_Neon(vseed, value);
        vseed = Fmix_Neon(vseed, vMM3_SIZE_INT);

        vst1q_s32(partition_ptr + row, vreinterpretq_s32_u32(vseed));
    }

    for (; row < rowCount; row++) {
        int16_t val = currentCol->GetValue(row);
        partitionIds[row] = static_cast<int32_t>(
                HashInt(static_cast<uint32_t>(static_cast<int32_t>(val)),
                        static_cast<uint32_t>(partitionIds[row]))
        );
    }
}

static void NeonFloat(omniruntime::vec::Vector<float>* currentCol, std::vector<uint32_t> &partitionIds)
{
    auto value_ptr = omniruntime::vec::unsafe::UnsafeVector::GetRawValues(currentCol);
    uint32_t rowCount = partitionIds.size();
    int32_t *partition_ptr = reinterpret_cast<int32_t*>(partitionIds.data());
    uint32x4_t vMM3_SIZE_INT = vdupq_n_u32(MM3_SIZE_INT);

    uint32_t row = 0;

    for (; row + STEP_FOUR < rowCount; row += STEP_FOUR) {
        // load 4 floats
        float32x4_t vfloat = vld1q_f32(reinterpret_cast<float*>(value_ptr + row));

        // reinterpret the bit pattern of float as uint32_t
        uint32x4_t value = vreinterpretq_u32_f32(vfloat);

        value = MixK1_Neon(value);
        uint32x4_t vseed = vld1q_u32(reinterpret_cast<uint32_t*>(partition_ptr + row));
        vseed = MixH1_Neon(vseed, value);
        vseed = Fmix_Neon(vseed, vMM3_SIZE_INT);

        vst1q_s32(partition_ptr + row, vreinterpretq_s32_u32(vseed));
    }

    for (; row < rowCount; row++) {
        float fval = currentCol->GetValue(row);

        uint32_t intVal;
        static_assert(sizeof(float) == sizeof(uint32_t), "float and uint32_t size mismatch");
        memcpy(&intVal, &fval, sizeof(uint32_t));

        partitionIds[row] = static_cast<int32_t>(
                HashInt(intVal, partitionIds[row])
        );
    }
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

static void NeonDouble(omniruntime::vec::Vector<double>* currentCol, std::vector<uint32_t> &partitionIds)
{
    auto value_ptr = omniruntime::vec::unsafe::UnsafeVector::GetRawValues(currentCol);
    uint32_t rowCount = partitionIds.size();
    uint32_t *partition_ptr = reinterpret_cast<uint32_t*>(partitionIds.data());
    uint32x4_t vMM3_SIZE_LONG = vdupq_n_u32(MM3_SIZE_LONG);

    uint32_t row = 0;

    for (; row + STEP_FOUR < rowCount; row += STEP_FOUR) {
        // load 4 doubles to 2 128-bit registers
        float64x2_t dval0 = vld1q_f64(reinterpret_cast<double*>(value_ptr + row));
        float64x2_t dval1 = vld1q_f64(reinterpret_cast<double*>(value_ptr + row + 2));

        // reinterpret the bit pattern of double as uint64_t
        uint64x2_t u64_0 = vreinterpretq_u64_f64(dval0);
        uint64x2_t u64_1 = vreinterpretq_u64_f64(dval1);

        // for register u64_0, extract the high 32 bits and low 32 bits
        uint32x4_t low_high_0;
        low_high_0 = vsetq_lane_u32(vgetq_lane_u64(u64_0, 0) & 0xFFFFFFFF, low_high_0, 0);
        low_high_0 = vsetq_lane_u32(vgetq_lane_u64(u64_0, 0) >> 32, low_high_0, 1);
        low_high_0 = vsetq_lane_u32(vgetq_lane_u64(u64_0, 1) & 0xFFFFFFFF, low_high_0, 2);
        low_high_0 = vsetq_lane_u32(vgetq_lane_u64(u64_0, 1) >> 32, low_high_0, 3);

        uint32x4_t low_high_1;
        low_high_1 = vsetq_lane_u32(vgetq_lane_u64(u64_1, 0) & 0xFFFFFFFF, low_high_1, 0);
        low_high_1 = vsetq_lane_u32(vgetq_lane_u64(u64_1, 0) >> 32, low_high_1, 1);
        low_high_1 = vsetq_lane_u32(vgetq_lane_u64(u64_1, 1) & 0xFFFFFFFF, low_high_1, 2);
        low_high_1 = vsetq_lane_u32(vgetq_lane_u64(u64_1, 1) >> 32, low_high_1, 3);

        uint32x4_t vlow = vcombine_u32(vget_low_u32(low_high_0), vget_low_u32(low_high_1));
        uint32x4_t vhigh = vcombine_u32(vget_high_u32(low_high_0), vget_high_u32(low_high_1));

        uint32x4_t vseed = vld1q_u32(partition_ptr + row);

        uint32x4_t k1 = MixK1_Neon(vlow);
        uint32x4_t h1 = MixH1_Neon(vseed, k1);

        k1 = MixK1_Neon(vhigh);
        h1 = MixH1_Neon(h1, k1);

        vseed = Fmix_Neon(h1, vMM3_SIZE_LONG);

        vst1q_u32(partition_ptr + row, vseed);
    }

    for (; row < rowCount; row++) {
        double dval = currentCol->GetValue(row);

        uint64_t intVal;
        static_assert(sizeof(double) == sizeof(uint64_t), "double and uint64_t size mismatch");
        memcpy(&intVal, &dval, sizeof(uint64_t));

        partitionIds[row] = static_cast<int32_t>(
                HashLong(intVal, partitionIds[row])
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

static void Mm3Byte(omniruntime::vec::BaseVector* vec, int32_t &rowCount, std::vector<uint32_t> &partitionIds)
{
    if (vec->GetEncoding() == vec::OMNI_DICTIONARY) {
        auto currentCol = reinterpret_cast<vec::Vector<vec::DictionaryContainer<int8_t>> *>(vec);
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
        auto currentCol = reinterpret_cast<omniruntime::vec::Vector<int8_t> *>(vec);
        if (UNLIKELY(currentCol->HasNull())) {
            for (auto row = 0; row < rowCount; row++) {
                if (!currentCol->IsNull(row)) {
                    partitionIds[row] = HashInt(
                            static_cast<uint32_t>(currentCol->GetValue(row)), partitionIds[row]);
                }
            }
        } else {
            NeonInt8(currentCol, partitionIds);
        }
    }
}

static void Mm3Short(omniruntime::vec::BaseVector* vec, int32_t &rowCount, std::vector<uint32_t> &partitionIds)
{
    if (vec->GetEncoding() == vec::OMNI_DICTIONARY) {
        auto currentCol = reinterpret_cast<vec::Vector<vec::DictionaryContainer<int16_t>> *>(vec);
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
        auto currentCol = reinterpret_cast<omniruntime::vec::Vector<int16_t> *>(vec);
        if (UNLIKELY(currentCol->HasNull())) {
            for (auto row = 0; row < rowCount; row++) {
                if (!currentCol->IsNull(row)) {
                    partitionIds[row] = HashInt(
                            static_cast<uint32_t>(currentCol->GetValue(row)), partitionIds[row]);
                }
            }
        } else {
            NeonInt16(currentCol, partitionIds);
        }
    }
}

static void Mm3Float(omniruntime::vec::BaseVector* vec, int32_t &rowCount, std::vector<uint32_t> &partitionIds)
{
    if (vec->GetEncoding() == vec::OMNI_DICTIONARY) {
        auto currentCol = reinterpret_cast<vec::Vector<vec::DictionaryContainer<float>> *>(vec);
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
        auto currentCol = reinterpret_cast<omniruntime::vec::Vector<float> *>(vec);
        if (UNLIKELY(currentCol->HasNull())) {
            for (auto row = 0; row < rowCount; row++) {
                if (!currentCol->IsNull(row)) {
                    partitionIds[row] = HashInt(
                            static_cast<uint32_t>(currentCol->GetValue(row)), partitionIds[row]);
                }
            }
        } else {
            NeonFloat(currentCol, partitionIds);
        }
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

static void Mm3Double(omniruntime::vec::BaseVector* vec, int32_t &rowCount, std::vector<uint32_t> &partitionIds)
{
    if (vec->GetEncoding() == vec::OMNI_DICTIONARY) {
        auto currentCol = reinterpret_cast<vec::Vector<vec::DictionaryContainer<double>> *>(vec);
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
        auto currentCol = reinterpret_cast<vec::Vector<double> *>(vec);
        if (UNLIKELY(currentCol->HasNull())) {
            for (auto row = 0; row < rowCount; row++) {
                if (!currentCol->IsNull(row)) {
                    partitionIds[row] = HashLong(
                            static_cast<uint64_t>(currentCol->GetValue(row)), partitionIds[row]);
                }
            }
        } else {
            NeonDouble(currentCol, partitionIds);
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

static uint32_t HashArrayElements(vec::BaseVector* elementVector, type::DataTypeId elementType,
                                  int64_t start, int64_t length, uint32_t seed)
{
    uint32_t hash = seed;

    switch (elementType) {
        case type::OMNI_BYTE: {
            auto byteVector = reinterpret_cast<vec::Vector<int8_t> *>(elementVector);
            for (int i = start; i < start + length; i++) {
                if (UNLIKELY(byteVector->HasNull() && byteVector->IsNull(i))) {
                    continue;
                }
                hash = HashInt(static_cast<uint32_t>(byteVector->GetValue(i)), hash);
            }
            break;
        }
        case type::OMNI_SHORT: {
            auto shortVector = reinterpret_cast<vec::Vector<int16_t> *>(elementVector);
            for (int i = start; i < start + length; i++) {
                if (UNLIKELY(shortVector->HasNull() && shortVector->IsNull(i))) {
                    continue;
                }
                hash = HashInt(static_cast<uint32_t>(shortVector->GetValue(i)), hash);
            }
            break;
        }
        case type::OMNI_FLOAT: {
            auto floatVector = reinterpret_cast<vec::Vector<float> *>(elementVector);
            for (int i = start; i < start + length; i++) {
                if (UNLIKELY(floatVector->HasNull() && floatVector->IsNull(i))) {
                    continue;
                }
                hash = HashInt(static_cast<uint32_t>(floatVector->GetValue(i)), hash);
            }
            break;
        }
        case type::OMNI_INT:
        case type::OMNI_DATE32: {
            auto intVector = reinterpret_cast<vec::Vector<int32_t> *>(elementVector);
            for (int i = start; i < start + length; i++) {
                if (UNLIKELY(intVector->HasNull() && intVector->IsNull(i))) {
                    continue;
                }
                hash = HashInt(intVector->GetValue(i), hash);
            }
            break;
        }
        case type::OMNI_DOUBLE: {
            auto doubleVector = reinterpret_cast<vec::Vector<double> *>(elementVector);
            for (int i = start; i < start + length; i++) {
                if (UNLIKELY(doubleVector->HasNull() && doubleVector->IsNull(i))) {
                    continue;
                }
                hash = HashLong(static_cast<uint64_t>(doubleVector->GetValue(i)), hash);
            }
            break;
        }
        case type::OMNI_LONG:
        case type::OMNI_TIMESTAMP:
        case type::OMNI_DECIMAL64: {
            auto longVector = reinterpret_cast<vec::Vector<int64_t> *>(elementVector);
            for (int i = start; i < start + length; i++) {
                if (UNLIKELY(longVector->HasNull() && longVector->IsNull(i))) {
                    continue;
                }
                hash = HashLong(longVector->GetValue(i), hash);
            }
            break;
        }
        case type::OMNI_CHAR:
        case type::OMNI_VARCHAR: {
            auto charVector = reinterpret_cast<vec::Vector<vec::LargeStringContainer<std::string_view>> *>(elementVector);
            for (int i = start; i < start + length; i++) {
                if (UNLIKELY(charVector->HasNull() && charVector->IsNull(i))) {
                    continue;
                }
                std::string_view value = charVector->GetValue(i);
                hash = HashUnsafeBytes(
                        const_cast<char *>(value.data()), value.size(), hash);
            }
            break;
        }
        case type::OMNI_DECIMAL128: {
            auto decimalVector = reinterpret_cast<vec::Vector<type::Decimal128> *>(elementVector);
            for (int i = start; i < start + length; i++) {
                if (UNLIKELY(decimalVector->HasNull() && decimalVector->IsNull(i))) {
                    continue;
                }
                int32_t byteLen = 0;
                auto val = decimalVector->GetValue(i);
                auto bytes = omniruntime::type::Decimal128Utils::Decimal128ToBytes(
                        val.HighBits(), val.LowBits(), byteLen);
                hash = HashUnsafeBytes(
                        reinterpret_cast<char *>(bytes), byteLen, hash);
                delete[] bytes;
            }
            break;
        }
        case type::OMNI_BOOLEAN: {
            auto boolVector = reinterpret_cast<vec::Vector<bool> *>(elementVector);
            for (int i = start; i < start + length; i++) {
                if (UNLIKELY(boolVector->HasNull() && boolVector->IsNull(i))) {
                    continue;
                }
                hash = HashInt(boolVector->GetValue(i) ? 1 : 0, hash);
            }
            break;
        }
        default:
            std::string omniExceptionInfo =
                    "Error in array element hash, not support type: " +
                    std::to_string(elementVector->GetTypeId());
            throw omniruntime::exception::OmniException("UNSUPPORTED_ERROR", omniExceptionInfo);
    }

    return hash;
}

static void Mm3Array(omniruntime::vec::BaseVector* vec, int32_t &rowCount, std::vector<uint32_t> &partitionIds)
{
    auto* arrayVec = reinterpret_cast<vec::ArrayVector *>(vec);

    auto offsets = arrayVec->GetOffsets();
    auto elementVector = arrayVec->GetElementVector();
    auto elementType = elementVector->GetTypeId();

    if (UNLIKELY(arrayVec->HasNull())) {
        for (int32_t row = 0; row < rowCount; row++) {
            if (!arrayVec->IsNull(row)) {
                int64_t start = offsets[row];
                int64_t size = arrayVec->GetSize(row);
                partitionIds[row] = HashArrayElements(elementVector.get(), elementType,
                                                      start, size, partitionIds[row]);
            }
        }
    } else {
        for (int32_t row = 0; row < rowCount; row++) {
            int64_t start = offsets[row];
            int64_t size = arrayVec->GetSize(row);
            partitionIds[row] = HashArrayElements(elementVector.get(), elementType,
                                                  start, size, partitionIds[row]);
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
