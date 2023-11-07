/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 * @Description: quick sort implementations using simd
 */

#include "quick_sort_simd.h"
#include "util/compiler_util.h"
#include "huawei_secure_c/include/securec.h"
#include "neon_sort64.h"

constexpr int32_t SMALL_CASE_LENGTH = 16;
constexpr int32_t CHUNK_SIZE = 8;

enum class PivotResult {
    Done,  // skip all partition
    Normal,// handle left and right partition
    First, // skip left partition
    Last,  // skip right partition
};

template <int32_t sortAscending>
void QuickSortSmallCase(int64_t *values, int32_t from, int32_t to)
{
    for (int32_t i = from + 1; i < to; ++i) {
        int64_t iValue = values[i];
        int32_t j = i - 1;
        while (j >= from) {
            int64_t jValue = values[j];
            if constexpr (sortAscending != 0) {
                if (jValue <= iValue) {
                    break;
                }
            } else {
                if (jValue >= iValue) {
                    break;
                }
            }
            values[j + 1] = jValue;
            --j;
        }
        values[j + 1] = iValue;
    }
}

template <int32_t sortAscending>
void QuickSortSmallCase(int64_t *values, uint64_t *addresses, int32_t from, int32_t to)
{
    for (int32_t i = from + 1; i < to; ++i) {
        int64_t iValue = values[i];
        uint64_t iAddr = addresses[i];
        int32_t j = i - 1;
        while (j >= from) {
            int64_t jValue = values[j];
            if constexpr (sortAscending != 0) {
                if (jValue <= iValue) {
                    break;
                }
            } else {
                if (jValue >= iValue) {
                    break;
                }
            }
            uint64_t jAddr = addresses[j];
            values[j + 1] = jValue;
            addresses[j + 1] = jAddr;
            --j;
        }
        values[j + 1] = iValue;
        addresses[j + 1] = iAddr;
    }
}

static int32_t ALWAYS_INLINE CountTrue(uint64x2_t mask)
{
    auto sMask = vreinterpretq_s64_u64(mask);
    auto ones = vnegq_s64(sMask);
    return static_cast<int32_t>(vaddvq_s64(ones));
}

static uint64x2_t ALWAYS_INLINE Not(uint64x2_t value)
{
    uint8x16_t result = vreinterpretq_u8_u64(value);
    uint8x16_t negResult = vmvnq_u8(result);
    return vreinterpretq_u64_u8(negResult);
}

template <int32_t sortAscending>
static int64x2_t ALWAYS_INLINE GetSmallestVec()
{
    if constexpr (sortAscending == 0) {
        return vdupq_n_s64(-9223372036854775808);
    } else {
        return vdupq_n_s64(9223372036854775807);
    }
}

template <int32_t sortAscending>
static int64x2_t ALWAYS_INLINE GetLargestVec()
{
    if constexpr (sortAscending == 0) {
        return vdupq_n_s64(9223372036854775807);
    } else {
        return vdupq_n_s64(-9223372036854775808);
    }
}

template <int32_t sortAscending>
static void ALWAYS_INLINE UpdateMinMax(int64x2_t value, int64x2_t &smallest, int64x2_t &largest)
{
    if constexpr (sortAscending == 0) {
        uint64x2_t compare1 = vcgtq_s64(value, smallest);
        smallest = vbslq_s64(compare1, value, smallest);
        uint64x2_t compare2 = vcltq_s64(value, largest);
        largest = vbslq_s64(compare2, value, largest);
    } else {
        uint64x2_t compare1 = vcltq_s64(value, smallest);
        smallest = vbslq_s64(compare1, value, smallest);
        uint64x2_t compare2 = vcgtq_s64(value, largest);
        largest = vbslq_s64(compare2, value, largest);
    }
}

template <int32_t sortAscending>
static int64_t ALWAYS_INLINE GetSmallest(int64x2_t smallestVec)
{
    int64_t val0 = vgetq_lane_s64(smallestVec, 0);
    int64_t val1 = vgetq_lane_s64(smallestVec, 1);
    bool compare = val0 < val1;
    if constexpr (sortAscending == 0) {
        return compare ? val1 : val0;
    } else {
        return compare ? val0 : val1;
    }
}

template <int32_t sortAscending>
static int64_t ALWAYS_INLINE GetLargest(int64x2_t largestVec)
{
    int64_t val0 = vgetq_lane_s64(largestVec, 0);
    int64_t val1 = vgetq_lane_s64(largestVec, 1);
    bool compare = val0 > val1;
    if constexpr (sortAscending == 0) {
        return compare ? val1 : val0;
    } else {
        return compare ? val0 : val1;
    }
}

template <int32_t sortAscending>
static uint64x2_t ALWAYS_INLINE Compare(int64x2_t vec0, int64x2_t vec1)
{
    if constexpr (sortAscending == 0) {
        return vcltq_s64(vec1, vec0);
    } else {
        return vcltq_s64(vec0, vec1);
    }
}

static uint64_t ALWAYS_INLINE BitsFromMask(uint64x2_t maskVec)
{
    alignas(16) static constexpr uint64_t kSliceLanes[2] = {1, 2};
    auto sliceLaneVec = vld1q_u64(kSliceLanes);
    uint64x2_t values = vandq_u64(maskVec, sliceLaneVec);
    auto result = vaddvq_u64(values);
    return result;
}

static uint64x2_t ALWAYS_INLINE FirstN(int32_t num)
{
    int64_t kIota[2] = {0, 1};
    int64x2_t iotaVec = vld1q_s64(kIota);
    int64x2_t setVec = vdupq_n_s64(static_cast<int64_t>(num));
    uint64x2_t compareVec = vcltq_s64(iotaVec, setVec);
    return compareVec;
}

static bool ALWAYS_INLINE AllTrue(uint64x2_t mask)
{
    uint8x8_t shrResult = vshrn_n_u16(vreinterpretq_u16_u64(mask), 4);
    uint64_t value = vget_lane_u64(vreinterpret_u64_u8(shrResult), 0);
    return value == ~0ull;
}

static bool ALWAYS_INLINE AllFalse(uint64x2_t mask)
{
    uint8x8_t shrResult = vshrn_n_u16(vreinterpretq_u16_u64(mask), 4);
    uint64_t value = vget_lane_u64(vreinterpret_u64_u8(shrResult), 0);
    return value == 0;
}

template <int32_t sortAscending>
static void ALWAYS_INLINE GetMedianValue(int64x2_t val0, int64x2_t val1, int64x2_t val2, int64x2_t &medialVal)
{
    int64x2_t val0Copy = val0;
    uint64x2_t comp = vcltq_s64(val2, val0);
    if constexpr (sortAscending == 0) {
        val0 = vbslq_s64(comp, val0, val2);
        val2 = vbslq_s64(comp, val2, val0Copy);

        comp = vcltq_s64(val1, val0);
        val1 = vbslq_s64(comp, val1, val0);
        comp = vcltq_s64(val2, val1);
        val1 = vbslq_s64(comp, val1, val2);
    } else {
        val0 = vbslq_s64(comp, val2, val0);
        val2 = vbslq_s64(comp, val0Copy, val2);

        comp = vcltq_s64(val1, val0);
        val1 = vbslq_s64(comp, val0, val1);
        comp = vcltq_s64(val2, val1);
        val1 = vbslq_s64(comp, val2, val1);
    }
    medialVal = val1;
}

template <int32_t sortAscending>
static int32_t ALWAYS_INLINE DrawSamples(int64_t *values, int32_t from, int32_t to, int64_t *valueBuf)
{
    constexpr int32_t N = 2;
    int32_t num = to - from;
    int64_t *valueStart = values + from;
    int64_t *valueMid = values + from + num / 2;
    int64_t *valueLast = values + to - N;

    int32_t oneStep = num / CHUNK_SIZE;
    int32_t twoStep = oneStep + oneStep;

    int64x2_t valueVec1 = vld1q_s64(valueStart);
    int64x2_t valueVec2 = vld1q_s64(valueStart + oneStep);
    int64x2_t valueVec3 = vld1q_s64(valueStart + twoStep);
    int64x2_t valueVec4 = vld1q_s64(valueMid - oneStep);
    int64x2_t valueVec5 = vld1q_s64(valueMid);
    int64x2_t valueVec6 = vld1q_s64(valueMid + oneStep);
    int64x2_t valueVec7 = vld1q_s64(valueLast);
    int64x2_t valueVec8 = vld1q_s64(valueLast - oneStep);
    int64x2_t valueVec9 = vld1q_s64(valueLast - twoStep);

    int64x2_t medianVal1, medianVal2, medianVal3;
    GetMedianValue<sortAscending>(valueVec1, valueVec2, valueVec3, medianVal1);
    GetMedianValue<sortAscending>(valueVec4, valueVec5, valueVec6, medianVal2);
    GetMedianValue<sortAscending>(valueVec7, valueVec8, valueVec9, medianVal3);
    vst1q_s64(valueBuf, medianVal1);
    vst1q_s64(valueBuf + N, medianVal2);
    vst1q_s64(valueBuf + 2 * N, medianVal3);

    return N * 3;
}

static int64x2_t ALWAYS_INLINE OrXor(int64x2_t v1, int64x2_t x1, int64x2_t x2) {
    int64x2_t eorRes = veorq_s64(x1, x2);
    return vorrq_s64(v1, eorRes);
}

static bool ALWAYS_INLINE NoValueDifference(int64x2_t diff)
{
    auto zero = vdupq_n_s64(0);
    auto equalResult = vceqq_s64(diff, zero);
    return AllTrue(equalResult);
}

static bool ALWAYS_INLINE UnsortedSampleEqual(int64_t *valueBuf, int32_t from ,int32_t to)
{
    constexpr int32_t N = 2;
    int64x2_t first = vdupq_n_s64(valueBuf[from]);
    int64x2_t diff = vdupq_n_s64(0);
    for (int32_t i = from; i < to; i += N) {
        int64x2_t vec = vld1q_s64(valueBuf + i);
        diff = OrXor(diff, first, vec);
    }

    return NoValueDifference(diff);
}

static uint64x2_t ALWAYS_INLINE NotEqualValues(int64x2_t valueVec, int64x2_t pivotVec)
{
    uint64x2_t equalResult = vceqq_s64(valueVec, pivotVec);
    return Not(equalResult);
}

static int32_t ALWAYS_INLINE FindKnownFirstTrue(uint64x2_t mask)
{
    uint8x8_t result = vshrn_n_u16(vreinterpretq_u16_u64(mask), 4);
    uint64_t value = vget_lane_u64(vreinterpret_u64_u8(result), 0);
    constexpr int32_t kDiv = 4 * sizeof(int64_t);
    return __builtin_ctzll(value) / kDiv;
}

static bool ALWAYS_INLINE AllEqual(int64x2_t pivotVec, int64_t *values, int32_t from, int32_t to, int32_t *secondIdx)
{
    constexpr int32_t N = 2;
    int32_t num = to - from;
    int64_t *valueStart = values + from;

    int64x2_t zero = vdupq_n_s64(0);
    const size_t misalign =
            (reinterpret_cast<uintptr_t>(valueStart) / sizeof(int64_t)) & (N - 1);
    int32_t consume = N - misalign;

    int64x2_t firstVec = vld1q_s64(valueStart);
    uint64x2_t firstMask = FirstN(consume);
    uint64x2_t notEqualResult = NotEqualValues(firstVec, pivotVec);
    uint64x2_t firstDiff = vandq_u64(firstMask, notEqualResult);
    if (!AllFalse(firstDiff)) {
        auto lane = FindKnownFirstTrue(firstDiff);
        *secondIdx = from + lane;
        return false;
    }

    int32_t i = consume;
    int64x2_t diff0 = zero;
    int64x2_t diff1 = zero;
    constexpr int32_t kLoops = 8;
    int32_t lanesPerGroup = kLoops * 2 * N;
    for (; i + lanesPerGroup <= num; i += lanesPerGroup) {
        for (size_t loop = 0; loop < kLoops; ++loop) {
            int64x2_t v0 = vld1q_s64(valueStart + i + loop * 2 * N);
            int64x2_t v1 = vld1q_s64(valueStart + i + loop * 2 * N + N);
            diff0 = OrXor(diff0, v0, pivotVec);
            diff1 = OrXor(diff1, v1, pivotVec);
        }

        int64x2_t orResult = vorrq_s64(diff0, diff1);
        if (!NoValueDifference(orResult)) {
            for (;; i += N) {
                int64x2_t curVec = vld1q_s64(valueStart + i);
                uint64x2_t diff = NotEqualValues(curVec, pivotVec);
                if (!AllFalse(diff)) {
                    auto lane = FindKnownFirstTrue(diff);
                    *secondIdx = from + i + lane;
                    return false;
                }
            }
        }
    }

    for (; i + N <= num; i += N) {
        int64x2_t curVec = vld1q_s64(valueStart + i);
        uint64x2_t diff = NotEqualValues(curVec, pivotVec);
        if (!AllFalse(diff)) {
            auto lane = FindKnownFirstTrue(diff);
            *secondIdx = from + i + lane;
            return false;
        }
    }

    i = num - N;
    int64x2_t lastVec = vld1q_s64(valueStart + i);
    uint64x2_t diff = NotEqualValues(lastVec, pivotVec);
    if (!AllFalse(diff)) {
        auto lane = FindKnownFirstTrue(diff);
        *secondIdx = from + i + lane;
        return false;
    }
    return true;
}

static void ALWAYS_INLINE SafeCopyN(int32_t num, int64_t *from, int64_t *to)
{
    for (int32_t i = 0; i < num; ++i) {
        to[i] = from[i];
    }
}

static void ALWAYS_INLINE BlendedStore(int64x2_t vec, uint64x2_t mask, int64_t *valuePtr)
{
    int64x2_t tmpValueVec = vld1q_s64(valuePtr);
    int64x2_t blendedAddrVec = vbslq_s64(mask, vec, tmpValueVec);
    vst1q_s64(valuePtr, blendedAddrVec);
}

static bool ALWAYS_INLINE MaybePartitionTwoValueR(int64_t *values, uint64_t *addresses, int32_t from, int32_t to, int64x2_t valueL, int64x2_t valueR, int32_t addrLeftIdx, int64_t *valueBuf, int64x2_t &third)
{
    constexpr int32_t N = 2;
    uint64x2_t addrL = vdupq_n_u64(addresses[addrLeftIdx]);
    std::vector<uint64_t> leftAddrs;
    int64_t *valueStart = values + from;
    uint64_t *addrStart = addresses + from;
    int32_t num = to - from;
    int32_t pos = num - N;
    int32_t countR = 0;
    for (; pos >= 0; pos -= N) {
        int64x2_t curValVec = vld1q_s64(valueStart + pos);
        uint64x2_t curAddrVec = vld1q_u64(addrStart + pos);
        // It is not clear how to apply OrXor here - that can check if *both*
        // comparisons are true, but here we want *either*. Comparing the unsigned
        // min of differences to zero works, but is expensive for u64 prior to AVX3.
        uint64x2_t eqL = vceqq_s64(curValVec, valueL);
        uint64x2_t eqR = vceqq_s64(curValVec, valueR);
        uint64x2_t orResult = vorrq_u64(eqL, eqR);
        if (!AllTrue(orResult)) {
            uint64x2_t andNotResult = vbicq_u64(Not(eqR), eqL);
            auto lane = FindKnownFirstTrue(andNotResult);
            third = vdupq_n_s64(valueStart[pos + lane]);
            pos += N;
            int32_t endL = num - countR;
            for (; pos + N <= endL; pos += N) {
                vst1q_s64(valueStart + pos, valueL);
            }
            int32_t remaining = endL - pos;
            if (remaining > 0) {
                uint64x2_t firstNMask = FirstN(remaining);
                BlendedStore(valueL, firstNMask, valueStart + pos);
                SafeCopyN(remaining, (int64_t *)addrStart + pos, (int64_t *)addrStart);
                BlendedStore(vreinterpretq_s64_u64(addrL), firstNMask, (int64_t *)addrStart + pos);
            }
            return false;
        }
        vst1q_s64(valueStart + pos, valueR);
        countR += CountTrue(eqR);
    }

    // Final partial (or empty) vector, masked comparison.
    int32_t remaining = pos + N;
    int64x2_t vec = vld1q_s64(valueStart);  // Safe because num >= N.
    uint64x2_t firstNMask = FirstN(remaining);
    uint64x2_t eqL = vceqq_s64(vec, valueL);
    uint64x2_t eqR = vandq_u64(vceqq_s64(vec, valueR), firstNMask);
    uint64x2_t eq = vorrq_u64(vorrq_u64(eqL, eqR), Not(firstNMask));
    if (!AllTrue(eq)) {
        auto lane = FindKnownFirstTrue(Not(eq));
        third = vdupq_n_s64(valueStart[lane]);
        pos += N;  // rewind: we haven't yet committed changes in this iteration.
        int32_t endL = num - countR;
        for (; pos + N <= endL; pos += N) {
            vst1q_s64(valueStart + pos, valueL);
        }
        remaining = endL - pos;
        if (remaining > 0) {
            firstNMask = FirstN( remaining);
            BlendedStore(valueL, firstNMask, valueStart + pos);
            SafeCopyN(remaining, (int64_t *)addrStart + pos, (int64_t *)addrStart);
            BlendedStore(vreinterpretq_s64_u64(addrL), firstNMask, (int64_t *)addrStart + pos);
        }
        return false;
    }
    auto lastR = CountTrue(eqR);
    countR += lastR;

    // First finish writing valueR - [0, N) lanes were not yet written.
    vst1q_s64(valueStart, valueR);

    // Fill left side (ascending order for clarity)
    int32_t endL = num - countR;
    int32_t i = 0;
    for (; i + N <= endL; i += N) {
        vst1q_s64(valueStart + i, valueL);
    }
    vst1q_s64(valueBuf, valueL);
    SafeCopyN(endL - i, valueBuf, valueStart + i);
    auto tmpAddr = addresses[addrLeftIdx];
    addresses[addrLeftIdx] = addrStart[i];
    addrStart[i] = tmpAddr;
    return true;
}

static bool ALWAYS_INLINE MaybePartitionTwoValue(int64_t *values, uint64_t *addresses, int32_t from, int32_t to, int64x2_t valueL, int64x2_t valueR, int32_t addrRightIdx, int64_t *valueBuf, uint64_t *addrBuf, int64x2_t &third)
{
    constexpr int32_t N = 2;
    uint64x2_t addrL = vdupq_n_u64(addrBuf[0]);
    uint64x2_t addrR = vdupq_n_u64(addresses[addrRightIdx]);
    int64_t *valueStart = values + from;
    uint64_t *addrStart = addresses + from;
    int32_t num = to - from;
    int32_t i = 0;
    int32_t writeL = 0;

    for (; i + N <= num; i += N) {
        int64x2_t curVec = vld1q_s64(valueStart + i);
        uint64x2_t eqL = vceqq_s64(curVec, valueL);
        uint64x2_t eqR = vceqq_s64(curVec, valueR);
        uint64x2_t orResult = vorrq_u64(eqL, eqR);
        if (!AllTrue(orResult)) {
            uint64x2_t andNotResult = vbicq_u64(Not(eqR), eqL);
            auto lane = FindKnownFirstTrue(andNotResult);
            third = vdupq_n_s64(valueStart[i + lane]);

            // 'Undo' what we did by filling the remainder of what we read with R.
            for (; writeL + N <= i; writeL += N) {
                vst1q_s64(valueStart + writeL, valueR);
            }
            int32_t remaining = i - writeL;
            uint64x2_t firstNMask = FirstN(remaining);
            BlendedStore(valueR, firstNMask, valueStart + writeL);
            SafeCopyN(remaining, (int64_t *)addrStart + writeL, (int64_t *)addrStart);
            BlendedStore(vreinterpretq_s64_u64(addrR), firstNMask, (int64_t *)addrStart + writeL);
            return false;
        }
        vst1q_s64(valueStart + writeL, valueL);
        writeL += CountTrue(eqL);
    }

    // Final vector, masked comparison (no effect if i == num)
    int32_t remaining = num - i;
    SafeCopyN(remaining, valueStart + i, valueBuf);
    SafeCopyN(remaining, (int64_t *)addrStart + i, (int64_t *)addrBuf);
    int64x2_t vec = vld1q_s64(valueBuf);
    uint64x2_t firstNMask = FirstN(remaining);
    uint64x2_t eqL = vandq_u64(vceqq_s64(vec, valueL), firstNMask);
    uint64x2_t eqR = vceqq_s64(vec, valueR);
    // Invalid lanes are considered equal.
    uint64x2_t eq = vorrq_u64(vorrq_u64(eqL, eqR), Not(firstNMask));
    // At least one other value present; will require a regular partition.
    if (!AllTrue(eq)) {
        auto lane = FindKnownFirstTrue(Not(eq));
        third = vdupq_n_s64(valueStart[i + lane]);
        // 'Undo' what we did by filling the remainder of what we read with R.
        for (; writeL + N <= i; writeL += N) {
            vst1q_s64(valueStart + writeL, valueR);
        }
        remaining = i - writeL;
        firstNMask = FirstN(remaining);
        BlendedStore(valueR, firstNMask, valueStart + writeL);
        SafeCopyN(remaining, (int64_t *)addrStart + writeL, (int64_t *)addrStart);
        BlendedStore(vreinterpretq_s64_u64(addrR), firstNMask, (int64_t *)addrStart + writeL);
        return false;
    }
    BlendedStore(valueL, firstNMask, valueStart + writeL);
    SafeCopyN(remaining, (int64_t *)addrStart + writeL, (int64_t *)addrStart);
    BlendedStore(vreinterpretq_s64_u64(addrL), firstNMask, (int64_t *)addrStart + writeL);
    writeL += CountTrue(eqL);

    // Fill right side
    i = writeL;
    for (; i + N <= num; i += N) {
        vst1q_s64(valueStart + i, valueR);
    }
    remaining = num - i;
    firstNMask = FirstN(remaining);
    BlendedStore(valueR, firstNMask, valueStart + i);
    SafeCopyN(remaining, (int64_t *)addrStart + i, (int64_t *)addrStart);
    BlendedStore(vreinterpretq_s64_u64(addrR), firstNMask, (int64_t *)addrStart + i);

    return true;
}

template <int32_t sortAscending>
static bool ALWAYS_INLINE PartitionIfTwoKeys(int64x2_t pivotVal, int64_t *values, uint64_t *addresses, int32_t from, int32_t to, int32_t secondIdx, int64x2_t secondVal, int64x2_t &thirdVal, int64_t *valueBuf, uint64_t *addrBuf)
{
    uint64x2_t pivotAddr = vdupq_n_u64(addrBuf[0]);
    uint64x2_t secondAddr = vdupq_n_u64(addresses[secondIdx]);

    uint64x2_t compareResult = Compare<sortAscending>(pivotVal, secondVal);
    bool isPivotR = AllFalse(compareResult);
    if (isPivotR) {
        return MaybePartitionTwoValueR(values, addresses, from, to, secondVal, pivotVal, secondIdx, valueBuf, thirdVal);
    } else {
        return MaybePartitionTwoValue(values, addresses, secondIdx, to, pivotVal, secondVal, secondIdx, valueBuf, addrBuf, thirdVal);
    }
}

template <int32_t sortAscending>
static bool ExistsAnyAfter(int64_t *values, int32_t from, int32_t to, int64x2_t pivot)
{
    constexpr int32_t N = 2;
    constexpr int32_t kLoops = 16;
    constexpr int32_t lanesPerGroup = kLoops * N;
    int32_t num = to - from;
    int64_t *valueStart = values + from;

    int32_t i = 0;
    int64x2_t last = pivot;
    for (; i + lanesPerGroup <= num; i += lanesPerGroup) {
        for (int32_t loop = 0; loop < kLoops; ++loop) {
            int64x2_t curVec = vld1q_s64(valueStart + i + loop * N);
            uint64x2_t comp = vcltq_s64(curVec, last);
            if constexpr (sortAscending == 0) {
                last = vbslq_s64(comp, curVec, last);
            } else {
                last = vbslq_s64(comp, last, curVec);
            }
        }
        uint64x2_t comp = Compare<sortAscending>(pivot, last);
        bool allFalse = !AllFalse(comp);
        if (allFalse) {
            return true;
        }
    }

    for (; i + N <= num; i += N) {
        int64x2_t curVec = vld1q_s64(valueStart + i);
        uint64x2_t comp = Compare<sortAscending>(pivot, curVec);
        bool allFalse = !AllFalse(comp);
        if (allFalse) {
            return true;
        }
    }

    if (i != num) {
        int64x2_t curVec = vld1q_s64(valueStart + num - N);
        uint64x2_t comp = Compare<sortAscending>(pivot, curVec);
        bool allFalse = !AllFalse(comp);
        if (allFalse) {
            return true;
        }
    }
    return false;
}

template <int32_t sortAscending>
static bool ExistsAnyBefore(int64_t *values, int32_t from, int32_t to, int64x2_t pivot)
{
    constexpr int32_t N = 2;
    constexpr int32_t kLoops = 16;
    constexpr int32_t lanesPerGroup = kLoops * N;
    int32_t num = to - from;
    int64_t *valueStart = values + from;

    int32_t i = 0;
    int64x2_t first = pivot;
    for (; i + lanesPerGroup <= num; i += lanesPerGroup) {
        for (int32_t loop = 0; loop < kLoops; ++loop) {
            int64x2_t curVec = vld1q_s64(valueStart + i + loop * N);
            uint64x2_t comp = vcltq_s64(curVec, first);
            if constexpr (sortAscending == 0) {
                first = vbslq_s64(comp, first, curVec);
            } else {
                first = vbslq_s64(comp, curVec, first);
            }
        }

        uint64x2_t comp = Compare<sortAscending>(first, pivot);
        bool allFalse = !AllFalse(comp);
        if (allFalse) {
            return true;
        }
    }

    for (; i + N <= num; i += N) {
        int64x2_t curVec = vld1q_s64(valueStart + i);
        uint64x2_t comp = Compare<sortAscending>(curVec, pivot);
        bool allFalse = !AllFalse(comp);
        if (allFalse) {
            return true;
        }
    }

    if (i != num) {
        int64x2_t curVec = vld1q_s64(valueStart + num - N);
        uint64x2_t comp = Compare<sortAscending>(curVec, pivot);
        bool allFalse = !AllFalse(comp);
        if (allFalse) {
            return true;
        }
    }
    return false;
}

template <int32_t sortAscending>
static int64x2_t ChoosePivotForEqualSamples(int64_t *values, int32_t from, int32_t to, int64_t *valueBuf, int64x2_t secondVec, int64x2_t thirdVec, PivotResult &result)
{
    int64x2_t pivotVec = vdupq_n_s64(valueBuf[0]);
    int64x2_t firstVec = GetLargestVec<sortAscending>();
    uint64x2_t eq1 = vceqq_s64(pivotVec, firstVec);
    if (AllTrue(eq1)) {
        result = PivotResult::First;
        return pivotVec;
    }

    int64x2_t lastVec = GetSmallestVec<sortAscending>();
    uint64x2_t eq2 = vceqq_s64(pivotVec, lastVec);
    if (AllTrue(eq2)) {
        result = PivotResult::Last;
        int64x2_t resultVec;
        if constexpr (sortAscending == 0) {
            resultVec = vaddq_s64(pivotVec, vdupq_n_s64(1));
        } else {
            resultVec = vsubq_s64(pivotVec, vdupq_n_s64(1));
        }
        return resultVec;
    }

    int64x2_t secondCopy = secondVec;
    uint64x2_t comp = vcltq_s64(thirdVec, secondVec);
    if constexpr (sortAscending == 0) {
        secondVec = vbslq_s64(comp, secondVec, thirdVec);
        thirdVec = vbslq_s64(comp, thirdVec, secondCopy);
    } else {
        secondVec = vbslq_s64(comp, thirdVec, secondVec);
        thirdVec = vbslq_s64(comp, secondCopy, thirdVec);
    }
    uint64x2_t comp1 = Compare<sortAscending>(secondVec, pivotVec);
    uint64x2_t comp2 = Compare<sortAscending>(pivotVec, thirdVec);
    bool before = !AllFalse(comp1);
    bool after = !AllFalse(comp2);
    if (before) {
        if (after || ExistsAnyAfter<sortAscending>(values, from, to, pivotVec)) {
            result = PivotResult::Normal;
            return pivotVec;
        }
        result = PivotResult::Last;
        int64x2_t resultVec;
        if constexpr (sortAscending == 0) {
            resultVec = vaddq_s64(pivotVec, vdupq_n_s64(1));
        }  else {
            resultVec = vsubq_s64(pivotVec, vdupq_n_s64(1));
        }
        return resultVec;
    }

    if (ExistsAnyBefore<sortAscending>(values, from, to, pivotVec)) {
        result = PivotResult::Normal;
        return pivotVec;
    }

    result = PivotResult::First;
    return pivotVec;
}

static int64x2_t ALWAYS_INLINE ChoosePivot(int64_t *buf, int32_t from, int32_t to)
{
    constexpr int32_t N1 = 1;
    int32_t mid = from + (to - from) / 2;
    int64_t midValue = buf[mid];
    int64x2_t pivotVec;

    int32_t prev = mid - N1;
    for (; buf[prev] == midValue; prev -= N1) {
        if (prev == from) {
            return vdupq_n_s64(buf[from]);
        }
    }

    int32_t next = prev + N1;
    int32_t last = to - 1;
    for (; buf[next] == midValue; next += N1) {
        if (next == last) {
            return vdupq_n_s64(buf[prev]);
        }
    }

    int32_t excessIfMedian = next - mid;
    int32_t excessIfPrev = mid - prev;
    int64_t pivot =  excessIfMedian < excessIfPrev ? midValue : buf[prev];
    pivotVec = vdupq_n_s64(pivot);
    return pivotVec;
}

static uint64x2_t ALWAYS_INLINE GetIdxFromBitsForLong(uint64_t maskBits)
{
    // There are only 2 lanes, so we can afford to load the index vector directly.
    alignas(16) static constexpr uint8_t u8_indices[64] = {
            // PrintCompress64x2Tables
            0, 1, 2,  3,  4,  5,  6,  7,  8, 9, 10, 11, 12, 13, 14, 15,
            0, 1, 2,  3,  4,  5,  6,  7,  8, 9, 10, 11, 12, 13, 14, 15,
            8, 9, 10, 11, 12, 13, 14, 15, 0, 1, 2,  3,  4,  5,  6,  7,
            0, 1, 2,  3,  4,  5,  6,  7,  8, 9, 10, 11, 12, 13, 14, 15};
    auto idxBytes = vld1q_u8(u8_indices + 16 * maskBits);
    return vreinterpretq_u64_u8(idxBytes);
}

static uint64x2_t ALWAYS_INLINE SwapMask(uint64x2_t mask)
{
    uint64x2_t tmpMaskL = vzip1q_u64(mask, mask);
    uint64x2_t tmpMaskH = vzip2q_u64(mask, mask);
    uint64x2_t tmpSwap = vbicq_u64(tmpMaskL, tmpMaskH);
    return tmpSwap;
}

static int64x2_t ALWAYS_INLINE CompressNot(int64x2_t value, uint64x2_t swapMask)
{
    // shuffle
    uint8x16_t valueBytes = vreinterpretq_u8_s64(value);
    uint8x16_t shuffleBytes = vextq_u8(valueBytes, valueBytes, 8);
    int64x2_t compressVec = vbslq_s64(swapMask, vreinterpretq_s64_u8(shuffleBytes), value);
    return compressVec;
}

static int64x2_t ALWAYS_INLINE Compress(int64x2_t value, uint64_t maskBits)
{
    uint64x2_t idx = GetIdxFromBitsForLong(maskBits);
    uint8x16_t valueBytes = vreinterpretq_u8_s64(value);
    uint8x16_t idxBytes = vreinterpretq_u8_u64(idx);
    uint8x16_t result = vqtbl1q_u8(valueBytes, idxBytes);
    return vreinterpretq_s64_u8(result);
}

static int32_t ALWAYS_INLINE CompressStore(int64x2_t valueVec, uint64x2_t addrVec, uint64x2_t mask, int64_t *valuePtr, uint64_t *addrPtr)
{
    uint64_t maskBits = BitsFromMask(mask);
    int64x2_t compressValueVec = Compress(valueVec, maskBits);
    vst1q_s64(valuePtr, compressValueVec);
    int64x2_t compressAddrVec = Compress(vreinterpretq_s64_u64(addrVec), maskBits);
    vst1q_u64(addrPtr, vreinterpretq_u64_s64(compressAddrVec));
    int32_t result = __builtin_popcountll(maskBits);
    return result;
}

static int32_t ALWAYS_INLINE CompressBlendedStore(int64x2_t valueVec, uint64x2_t addrVec, uint64x2_t mask, int64_t *valuePtr, uint64_t *addrPtr)
{
    uint64_t maskBits = BitsFromMask(mask);
    int32_t count = __builtin_popcountll(maskBits);
    uint64x2_t storeMask = FirstN(count);

    int64x2_t compressValueVec = Compress(valueVec, maskBits);
    int64x2_t tmpValueVec = vld1q_s64(valuePtr);
    uint64x2_t blendedValueVec = vbslq_u64(storeMask, vreinterpretq_u64_s64(compressValueVec), vreinterpretq_u64_s64(tmpValueVec)); // blendedVec = {26, 19, 97, 19}
    vst1q_s64(valuePtr, vreinterpretq_s64_u64(blendedValueVec));

    int64x2_t compressAddrVec = Compress(vreinterpretq_s64_u64(addrVec), maskBits);
    uint64x2_t tmpAddrVec = vld1q_u64(addrPtr);
    uint64x2_t blendedAddrVec = vbslq_u64(storeMask, vreinterpretq_u64_s64(compressAddrVec), tmpAddrVec);
    vst1q_u64(addrPtr, blendedAddrVec);
    return count;
}

template <int32_t sortAscending>
int32_t PartitionToMultipleOfUnroll(int64_t *values, uint64_t *addresses, int32_t &num, int64x2_t pivotVec, int64_t *valueBuf, uint64_t *addrBuf, int64x2_t &smallestVec, int64x2_t &largestVec)
{
    constexpr int32_t kUnroll = 4;
    constexpr int32_t N = 2;
    const int32_t num_rem = (num < 2 * kUnroll * N) ? num : (num & (kUnroll * N - 1));
    if (num_rem <= 0) {
        return 0;
    }

    int32_t readL = 0;
    int64_t *valuePosL = values;
    uint64_t *addrPosL = addresses;
    int32_t bufR = 0;
    uint64x2_t blendedMask;
    uint64x2_t compareResult;
    int32_t i = 0;
    for (; i + N <= num_rem; i += N) {
        int64x2_t valueVec = vld1q_s64(values + readL);
        UpdateMinMax<sortAscending>(valueVec, smallestVec, largestVec);
        uint64x2_t addrVec = vld1q_u64(addresses + readL);
        readL += N;

        compareResult = Compare<sortAscending>(pivotVec, valueVec);
        blendedMask = Not(compareResult);
        auto sizeL = CompressBlendedStore(valueVec, addrVec, blendedMask, valuePosL, addrPosL);
        valuePosL += sizeL;
        addrPosL += sizeL;

        bufR += CompressStore(valueVec, addrVec, compareResult, valueBuf + bufR, addrBuf + bufR);
    }

    if (i != num_rem) {
        uint64x2_t firstMask = FirstN(num_rem - i);
        int64x2_t lastValueVec = vld1q_s64(values + readL);
        UpdateMinMax<sortAscending>(lastValueVec, smallestVec, largestVec);
        uint64x2_t lastAddrVec = vld1q_u64(addresses + readL);

        compareResult = Compare<sortAscending>(pivotVec, lastValueVec);
        blendedMask = vbicq_u64(firstMask, compareResult);
        auto sizeL = CompressBlendedStore(lastValueVec, lastAddrVec, blendedMask, valuePosL, addrPosL);
        valuePosL += sizeL;
        addrPosL += sizeL;
        uint64x2_t mask = vandq_u64(compareResult, firstMask);
        bufR += CompressStore(lastValueVec, lastAddrVec, mask, valueBuf + bufR, addrBuf + bufR);
    }
    num -= static_cast<int>(bufR);
    auto copyByteSize = bufR * sizeof(int64_t);
    memcpy(valuePosL, values + num, copyByteSize);
    memcpy(addrPosL, addresses + num, copyByteSize);
    memcpy(values + num, valueBuf, copyByteSize);
    memcpy(addresses + num, addrBuf, copyByteSize);
    return static_cast<int32_t>(valuePosL - values);
}

template <int32_t sortAscending>
static void ALWAYS_INLINE StoreLeftRight4(int64_t *values, int64x2_t pivotVec, int64x2_t valueVec0, int64x2_t valueVec1, int64x2_t valueVec2, int64x2_t valueVec3, uint64_t *addresses, uint64x2_t addrVec0, uint64x2_t addrVec1, uint64x2_t addrVec2, uint64x2_t addrVec3, int32_t &writeL, int32_t &remaining, int64x2_t &smallestVec, int64x2_t &largestVec)
{
    uint64x2_t compareResult0 = Compare<sortAscending>(pivotVec, valueVec0);
    uint64x2_t compareResult1 = Compare<sortAscending>(pivotVec, valueVec1);
    uint64x2_t compareResult2 = Compare<sortAscending>(pivotVec, valueVec2);
    uint64x2_t compareResult3 = Compare<sortAscending>(pivotVec, valueVec3);
    uint64x2_t swapMask0 = SwapMask(compareResult0);
    uint64x2_t swapMask1 = SwapMask(compareResult1);
    uint64x2_t swapMask2 = SwapMask(compareResult2);
    uint64x2_t swapMask3 = SwapMask(compareResult3);
    int32_t numRight0 = CountTrue(compareResult0);
    int32_t numRight1 = CountTrue(compareResult1);
    int32_t numRight2 = CountTrue(compareResult2);
    int32_t numRight3 = CountTrue(compareResult3);
    UpdateMinMax<sortAscending>(valueVec0, smallestVec, largestVec);

    int64x2_t compressValueVec0 = CompressNot(valueVec0, swapMask0);
    int64x2_t compressValueVec1 = CompressNot(valueVec1, swapMask1);
    int64x2_t compressValueVec2 = CompressNot(valueVec2, swapMask2);
    int64x2_t compressValueVec3 = CompressNot(valueVec3, swapMask3);
    uint64x2_t compressAddrVec0 = vreinterpretq_u64_s64(CompressNot(vreinterpretq_s64_u64(addrVec0), swapMask0));
    uint64x2_t compressAddrVec1 = vreinterpretq_u64_s64(CompressNot(vreinterpretq_s64_u64(addrVec1), swapMask1));
    uint64x2_t compressAddrVec2 = vreinterpretq_u64_s64(CompressNot(vreinterpretq_s64_u64(addrVec2), swapMask2));
    uint64x2_t compressAddrVec3 = vreinterpretq_u64_s64(CompressNot(vreinterpretq_s64_u64(addrVec3), swapMask3));
    UpdateMinMax<sortAscending>(valueVec1, smallestVec, largestVec);

    int32_t tmpPos1 = writeL - numRight0;
    int32_t tmpPos2 = tmpPos1 - numRight1;
    int32_t tmpPos3 = tmpPos2 - numRight2;

    constexpr int32_t N = 2;
    int32_t tmpRemaining = remaining - N;
    int32_t writeL0 = writeL;
    int32_t writeR0 = tmpRemaining + writeL;
    int32_t writeL1 = N + tmpPos1;
    int32_t writeR1 = tmpRemaining + tmpPos1;
    int32_t writeL2 = 2 * N + tmpPos2;
    int32_t writeR2 = tmpRemaining + tmpPos2;
    int32_t writeL3 = 3 * N + tmpPos3;
    int32_t writeR3 = tmpRemaining + tmpPos3;

    vst1q_s64(values + writeL0, compressValueVec0);
    vst1q_s64(values + writeL1, compressValueVec1);
    vst1q_s64(values + writeL2, compressValueVec2);
    vst1q_s64(values + writeL3, compressValueVec3);
    vst1q_s64(values + writeR0, compressValueVec0);
    vst1q_s64(values + writeR1, compressValueVec1);
    vst1q_s64(values + writeR2, compressValueVec2);
    vst1q_s64(values + writeR3, compressValueVec3);
    UpdateMinMax<sortAscending>(valueVec2, smallestVec, largestVec);

    vst1q_u64(addresses + writeL0, compressAddrVec0);
    vst1q_u64(addresses + writeL1, compressAddrVec1);
    vst1q_u64(addresses + writeL2, compressAddrVec2);
    vst1q_u64(addresses + writeL3, compressAddrVec3);
    vst1q_u64(addresses + writeR0, compressAddrVec0);
    vst1q_u64(addresses + writeR1, compressAddrVec1);
    vst1q_u64(addresses + writeR2, compressAddrVec2);
    vst1q_u64(addresses + writeR3, compressAddrVec3);
    UpdateMinMax<sortAscending>(valueVec3, smallestVec, largestVec);

    remaining -= 4 * N;
    writeL += 4 * N - numRight0 - numRight1 - numRight2 - numRight3;
}

template <int32_t sortAscending>
int32_t PartitionWithSIMD(int64_t *values, uint64_t *addresses, int32_t from, int32_t to, int64x2_t pivotVec, int64_t *valueBuf, uint64_t *addrBuf, int64x2_t &smallestVec, int64x2_t &largestVec)
{
    constexpr int32_t N = 2;
    constexpr int32_t kUnroll = 4;
    int32_t num = to - from;
    int64_t *valueStart = values + from;
    uint64_t *addressStart = addresses + from;

    num -= N;
    int32_t last = num;

    int64x2_t valueLast = vld1q_s64(valueStart + last);
    UpdateMinMax<sortAscending>(valueLast, smallestVec, largestVec);
    uint64x2_t addrLast = vld1q_u64(addressStart + last);

    int32_t consumedL = PartitionToMultipleOfUnroll<sortAscending>(valueStart, addressStart, num, pivotVec, valueBuf, addrBuf, smallestVec, largestVec);
    valueStart += consumedL;
    addressStart += consumedL;
    last -= consumedL;
    num -= consumedL;

    int64_t *valueReadL = valueStart;
    int64_t *valueReadR = valueStart + num;
    uint64_t *addrReadL = addressStart;
    uint64_t *addrReadR = addressStart + num;
    int32_t writeL = 0;
    int32_t remaining = num;
    if (num != 0) {
        constexpr int32_t pos0 = 0 * N;
        constexpr int32_t pos1 = 1 * N;
        constexpr int32_t pos2 = 2 * N;
        constexpr int32_t pos3 = 3 * N;
        constexpr int32_t advanceStep = kUnroll * N;

        int64x2_t leftValueVec0 = vld1q_s64(valueReadL + pos0);
        int64x2_t leftValueVec1 = vld1q_s64(valueReadL + pos1);
        int64x2_t leftValueVec2 = vld1q_s64(valueReadL + pos2);
        int64x2_t leftValueVec3 = vld1q_s64(valueReadL + pos3);
        valueReadL += advanceStep;
        valueReadR -= advanceStep;
        int64x2_t rightValueVec0 = vld1q_s64(valueReadR + pos0);
        int64x2_t rightValueVec1 = vld1q_s64(valueReadR + pos1);
        int64x2_t rightValueVec2 = vld1q_s64(valueReadR + pos2);
        int64x2_t rightValueVec3 = vld1q_s64(valueReadR + pos3);

        uint64x2_t leftAddrVec0 = vld1q_u64(addrReadL + pos0);
        uint64x2_t leftAddrVec1 = vld1q_u64(addrReadL + pos1);
        uint64x2_t leftAddrVec2 = vld1q_u64(addrReadL + pos2);
        uint64x2_t leftAddrVec3 = vld1q_u64(addrReadL + pos3);
        addrReadL += advanceStep;
        addrReadR -= advanceStep;
        uint64x2_t rightAddrVec0 = vld1q_u64(addrReadR + pos0);
        uint64x2_t rightAddrVec1 = vld1q_u64(addrReadR + pos1);
        uint64x2_t rightAddrVec2 = vld1q_u64(addrReadR + pos2);
        uint64x2_t rightAddrVec3 = vld1q_u64(addrReadR + pos3);

        while (valueReadL != valueReadR) {
            int64x2_t curValueVec0;
            int64x2_t curValueVec1;
            int64x2_t curValueVec2;
            int64x2_t curValueVec3;
            uint64x2_t curAddrVec0;
            uint64x2_t curAddrVec1;
            uint64x2_t curAddrVec2;
            uint64x2_t curAddrVec3;
            constexpr int32_t prefetchStep = 3 * kUnroll * N;

            int32_t capacityL = static_cast<int32_t>(valueReadL - valueStart) - writeL;
            if (capacityL > advanceStep) {
                valueReadR -= advanceStep;
                curValueVec0 = vld1q_s64(valueReadR + pos0);
                curValueVec1 = vld1q_s64(valueReadR + pos1);
                curValueVec2 = vld1q_s64(valueReadR + pos2);
                curValueVec3 = vld1q_s64(valueReadR + pos3);
                __builtin_prefetch(valueReadR - prefetchStep, 0, 3);

                addrReadR -= advanceStep;
                curAddrVec0 = vld1q_u64(addrReadR + pos0);
                curAddrVec1 = vld1q_u64(addrReadR + pos1);
                curAddrVec2 = vld1q_u64(addrReadR + pos2);
                curAddrVec3 = vld1q_u64(addrReadR + pos3);
                __builtin_prefetch(addrReadR - prefetchStep, 0, 3);
            } else {
                curValueVec0 = vld1q_s64(valueReadL + pos0);
                curValueVec1 = vld1q_s64(valueReadL + pos1);
                curValueVec2 = vld1q_s64(valueReadL + pos2);
                curValueVec3 = vld1q_s64(valueReadL + pos3);
                valueReadL += advanceStep;
                __builtin_prefetch(valueReadL + prefetchStep, 0, 3);

                curAddrVec0 = vld1q_u64(addrReadL + pos0);
                curAddrVec1 = vld1q_u64(addrReadL + pos1);
                curAddrVec2 = vld1q_u64(addrReadL + pos2);
                curAddrVec3 = vld1q_u64(addrReadL + pos3);
                addrReadL += advanceStep;
                __builtin_prefetch(addrReadL + prefetchStep, 0, 3);
            }

            StoreLeftRight4<sortAscending>(valueStart, pivotVec, curValueVec0, curValueVec1, curValueVec2, curValueVec3, addressStart, curAddrVec0, curAddrVec1, curAddrVec2, curAddrVec3, writeL, remaining, smallestVec, largestVec);
        }

        StoreLeftRight4<sortAscending>(valueStart, pivotVec, leftValueVec0, leftValueVec1, leftValueVec2, leftValueVec3, addressStart, leftAddrVec0, leftAddrVec1, leftAddrVec2, leftAddrVec3, writeL, remaining, smallestVec, largestVec);
        StoreLeftRight4<sortAscending>(valueStart, pivotVec, rightValueVec0, rightValueVec1, rightValueVec2, rightValueVec3, addressStart, rightAddrVec0, rightAddrVec1, rightAddrVec2, rightAddrVec3, writeL, remaining, smallestVec, largestVec);
    }

    int32_t totalR = last - writeL;
    int32_t startR = totalR < N ? writeL + totalR - 4 : writeL;
    int64x2_t valueRightVec = vld1q_s64(valueStart + startR);
    UpdateMinMax<sortAscending>(valueRightVec, smallestVec, largestVec);
    vst1q_s64(valueStart + last, valueRightVec);
    uint64x2_t addrRightVec = vld1q_u64(addressStart + startR);
    vst1q_u64(addressStart + last, addrRightVec);

    uint64x2_t compareResult = Compare<sortAscending>(pivotVec, valueLast);
    uint64x2_t blendedMask = Not(compareResult);
    writeL += CompressBlendedStore(valueLast, addrLast, blendedMask, valueStart + writeL, addressStart + writeL);
    CompressBlendedStore(valueLast, addrLast, compareResult, valueStart + writeL, addressStart + writeL);
    return consumedL + writeL + from;
}

template <int32_t sortAscending>
void QuickSortInternalSIMD(int64_t *values, uint64_t *addresses, int32_t from, int32_t to, int64_t *valueBuf, uint64_t *addrBuf, bool chooseAvg = false, int64_t avg = 0)
{
    int32_t num = to - from;
    if (num <= SMALL_CASE_LENGTH) {
        SmallCaseSort<sortAscending>(values, addresses, from, to);
        return;
    }

    int64x2_t pivotVal;
    if (chooseAvg) {
        pivotVal = vdupq_n_s64(avg);
    } else {
        auto count = DrawSamples<sortAscending>(values, from, to, valueBuf);
        SmallCaseSortWithoutAddress<sortAscending>(valueBuf, 0, count);
        pivotVal = ChoosePivot(valueBuf, 0, count);
    }

    /*if (depth <= 0) {
        LogError("THE DEPTH IS LESS THAN OR EQUAL TO ZERO!!!");
    }

    --depth;*/
    int64x2_t smallestVec = GetSmallestVec<sortAscending>();
    int64x2_t largestVec = GetLargestVec<sortAscending>();
    int32_t partitionIdx = PartitionWithSIMD<sortAscending>(values, addresses, from, to, pivotVal, valueBuf, addrBuf, smallestVec, largestVec);

    double ratio = std::min(to - partitionIdx, partitionIdx - from) / double(to - from);
    if (ratio < 0.2) {
        chooseAvg = !chooseAvg;
    }

    int64_t pivot = vgetq_lane_s64(pivotVal, 0);
    int64_t smallest = GetSmallest<sortAscending>(smallestVec);
    int64_t largest = GetLargest<sortAscending>(largestVec);
    if (pivot != smallest) {
        int64_t newAvg = (smallest & pivot) + ((smallest ^ pivot) >> 1);
        QuickSortInternalSIMD<sortAscending>(values, addresses, from, partitionIdx, valueBuf, addrBuf, chooseAvg, newAvg);
    }
    if (pivot != largest) {
        int64_t newAvg = (largest & pivot) + ((largest ^ pivot) >> 1);
        QuickSortInternalSIMD<sortAscending>(values, addresses, partitionIdx, to, valueBuf, addrBuf, chooseAvg, newAvg);
    }
}

void QuickSortAscSIMD(int64_t *values, uint64_t *addresses, int32_t from, int32_t to)
{
    int64_t valueBuf[50];
    uint64_t addrBuf[50];
    QuickSortInternalSIMD<1>(values, addresses, from, to, valueBuf, addrBuf);
}

void QuickSortDescSIMD(int64_t *values, uint64_t *addresses, int32_t from, int32_t to)
{
    int64_t valueBuf[50];
    uint64_t addrBuf[50];
    QuickSortInternalSIMD<0>(values, addresses, from, to, valueBuf, addrBuf);
}