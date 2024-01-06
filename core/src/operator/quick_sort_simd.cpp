/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 * @Description: quick sort implementations using simd
 */

#include "quick_sort_simd.h"
#include <algorithm>
#include <cfloat>
#include <cmath>
#include "util/compiler_util.h"
#include "huawei_secure_c/include/securec.h"
#include "small_case_sort_simd.h"
#include "arm_neon.h"

constexpr int32_t SMALL_CASE_LENGTH = 16;
constexpr int32_t CHUNK_SIZE = 8;
constexpr int32_t LANE_SIZE = 2;
constexpr int32_t UNROLL_SIZE = 4;
constexpr int32_t LOOP_SIZE = UNROLL_SIZE * LANE_SIZE;
constexpr int32_t BUFFER_SIZE = 50;

static int64x2_t SMALLEST_VEC = { INT64_MIN, INT64_MIN };
static int64x2_t LARGEST_VEC = { INT64_MAX, INT64_MAX };
static float64x2_t SMALLEST_DOUBLE_VEC = { -DBL_MAX, -DBL_MAX };
static float64x2_t LARGEST_DOUBLE_VEC = { DBL_MAX, DBL_MAX };

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

template <class RawType, int32_t sortAscending> static int64x2_t ALWAYS_INLINE GetSmallestVec()
{
    if constexpr (std::is_same_v<RawType, double>) {
        if constexpr (sortAscending == 0) {
            return vreinterpretq_s64_f64(SMALLEST_DOUBLE_VEC);
        } else {
            return vreinterpretq_s64_f64(LARGEST_DOUBLE_VEC);
        }
    } else {
        if constexpr (sortAscending == 0) {
            return SMALLEST_VEC;
        } else {
            return LARGEST_VEC;
        }
    }
}

template <class RawType, int32_t sortAscending> static int64x2_t ALWAYS_INLINE GetLargestVec()
{
    if constexpr (std::is_same_v<RawType, double>) {
        if constexpr (sortAscending == 0) {
            return vreinterpretq_s64_f64(LARGEST_DOUBLE_VEC);
        } else {
            return vreinterpretq_s64_f64(SMALLEST_DOUBLE_VEC);
        }
    } else {
        if constexpr (sortAscending == 0) {
            return LARGEST_VEC;
        } else {
            return SMALLEST_VEC;
        }
    }
}

template <typename RawType, int32_t sortAscending>
static void ALWAYS_INLINE UpdateMinMax(int64x2_t value, int64x2_t &smallest, int64x2_t &largest)
{
    if constexpr (std::is_same_v<RawType, double>) {
        float64x2_t fValue = vreinterpretq_f64_s64(value);
        float64x2_t fSmallest = vreinterpretq_f64_s64(smallest);
        float64x2_t fLargest = vreinterpretq_f64_s64(largest);
        if constexpr (sortAscending == 0) {
            uint64x2_t compare1 = vcgtq_f64(fValue, fSmallest);
            uint64x2_t compare2 = vcltq_f64(fValue, fLargest);
            fSmallest = vbslq_f64(compare1, fValue, fSmallest);
            fLargest = vbslq_f64(compare2, fValue, fLargest);
        } else {
            uint64x2_t compare1 = vcltq_f64(fValue, fSmallest);
            uint64x2_t compare2 = vcgtq_f64(fValue, fLargest);
            fSmallest = vbslq_f64(compare1, fValue, fSmallest);
            fLargest = vbslq_f64(compare2, fValue, fLargest);
        }
        smallest = vreinterpretq_s64_f64(fSmallest);
        largest = vreinterpretq_s64_f64(fLargest);
    } else {
        if constexpr (sortAscending == 0) {
            uint64x2_t compare1 = vcgtq_s64(value, smallest);
            uint64x2_t compare2 = vcltq_s64(value, largest);
            smallest = vbslq_s64(compare1, value, smallest);
            largest = vbslq_s64(compare2, value, largest);
        } else {
            uint64x2_t compare1 = vcltq_s64(value, smallest);
            uint64x2_t compare2 = vcgtq_s64(value, largest);
            smallest = vbslq_s64(compare1, value, smallest);
            largest = vbslq_s64(compare2, value, largest);
        }
    }
}

template <typename RawType, int32_t sortAscending> static RawType ALWAYS_INLINE GetSmallest(int64x2_t smallestVec)
{
    if constexpr (std::is_same_v<RawType, double>) {
        float64x2_t fSmallest = vreinterpretq_f64_s64(smallestVec);
        float64_t fVal0 = vgetq_lane_f64(fSmallest, 0);
        float64_t fVal1 = vgetq_lane_f64(fSmallest, 1);
        bool compare = fVal0 < fVal1;
        if constexpr (sortAscending == 0) {
            return compare ? fVal1 : fVal0;
        } else {
            return compare ? fVal0 : fVal1;
        }
    } else {
        int64_t val0 = vgetq_lane_s64(smallestVec, 0);
        int64_t val1 = vgetq_lane_s64(smallestVec, 1);
        bool compare = val0 < val1;
        if constexpr (sortAscending == 0) {
            return compare ? val1 : val0;
        } else {
            return compare ? val0 : val1;
        }
    }
}

template <typename RawType, int32_t sortAscending> static RawType ALWAYS_INLINE GetLargest(int64x2_t largestVec)
{
    if constexpr (std::is_same_v<RawType, double>) {
        float64x2_t fLargest = vreinterpretq_f64_s64(largestVec);
        float64_t fVal0 = vgetq_lane_f64(fLargest, 0);
        float64_t fVal1 = vgetq_lane_f64(fLargest, 1);
        bool compare = fVal0 > fVal1;
        if constexpr (sortAscending == 0) {
            return compare ? fVal1 : fVal0;
        } else {
            return compare ? fVal0 : fVal1;
        }
    } else {
        int64_t val0 = vgetq_lane_s64(largestVec, 0);
        int64_t val1 = vgetq_lane_s64(largestVec, 1);
        bool compare = val0 > val1;
        if constexpr (sortAscending == 0) {
            return compare ? val1 : val0;
        } else {
            return compare ? val0 : val1;
        }
    }
}

template <typename RawType, int32_t sortAscending>
static uint64x2_t ALWAYS_INLINE Compare(int64x2_t vec0, int64x2_t vec1)
{
    if constexpr (std::is_same_v<RawType, double>) {
        if constexpr (sortAscending == 0) {
            return vcltq_f64(vreinterpretq_f64_s64(vec1), vreinterpretq_f64_s64(vec0));
        } else {
            return vcltq_f64(vreinterpretq_f64_s64(vec0), vreinterpretq_f64_s64(vec1));
        }
    } else {
        if constexpr (sortAscending == 0) {
            return vcltq_s64(vec1, vec0);
        } else {
            return vcltq_s64(vec0, vec1);
        }
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

template <typename RawType> static uint64x2_t ALWAYS_INLINE FirstN(int32_t num)
{
    int64_t kIota[2] = {0, 1};
    int64x2_t iotaVec = vld1q_s64(kIota);
    int64x2_t setVec = vdupq_n_s64(static_cast<int64_t>(num));
    uint64x2_t compareVec;
    if constexpr (std::is_same_v<RawType, double>) {
        compareVec = vcltq_f64(vreinterpretq_f64_s64(iotaVec), vreinterpretq_f64_s64(setVec));
    } else {
        compareVec = vcltq_s64(iotaVec, setVec);
    }
    return compareVec;
}

template <typename RawType, int32_t sortAscending>
static void ALWAYS_INLINE GetMedianValue(int64x2_t val0, int64x2_t val1, int64x2_t val2, int64x2_t &medialVal)
{
    if constexpr (std::is_same_v<RawType, double>) {
        float64x2_t fVal0 = vreinterpretq_f64_s64(val0);
        float64x2_t fVal1 = vreinterpretq_f64_s64(val1);
        float64x2_t fVal2 = vreinterpretq_f64_s64(val2);
        float64x2_t fVal0Copy = fVal0;
        uint64x2_t comp = vcltq_f64(fVal2, fVal0);
        if constexpr (sortAscending == 0) {
            fVal0 = vbslq_f64(comp, fVal0, fVal2);
            fVal2 = vbslq_f64(comp, fVal2, fVal0Copy);

            comp = vcltq_f64(fVal1, fVal0);
            fVal1 = vbslq_f64(comp, fVal1, fVal0);
            comp = vcltq_f64(fVal2, fVal1);
            fVal1 = vbslq_f64(comp, fVal1, fVal2);
        } else {
            fVal0 = vbslq_f64(comp, fVal2, fVal0);
            fVal2 = vbslq_f64(comp, fVal0Copy, fVal2);

            comp = vcltq_f64(fVal1, fVal0);
            fVal1 = vbslq_f64(comp, fVal0, fVal1);
            comp = vcltq_f64(fVal2, fVal1);
            fVal1 = vbslq_f64(comp, fVal2, fVal1);
        }
        medialVal = vreinterpretq_s64_f64(fVal1);
    } else {
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
}

template <typename RawType, int32_t sortAscending>
static int32_t ALWAYS_INLINE DrawSamples(int64_t *values, int32_t from, int32_t to, int64_t *valueBuf)
{
    int32_t num = to - from;
    int64_t *valueStart = values + from;
    int64_t *valueMid = values + from + num / 2;
    int64_t *valueLast = values + to - LANE_SIZE;

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
    GetMedianValue<RawType, sortAscending>(valueVec1, valueVec2, valueVec3, medianVal1);
    GetMedianValue<RawType, sortAscending>(valueVec4, valueVec5, valueVec6, medianVal2);
    GetMedianValue<RawType, sortAscending>(valueVec7, valueVec8, valueVec9, medianVal3);
    vst1q_s64(valueBuf, medianVal1);
    vst1q_s64(valueBuf + LANE_SIZE, medianVal2);
    vst1q_s64(valueBuf + 2 * LANE_SIZE, medianVal3);

    return LANE_SIZE * 3;
}

template <typename RawType> static int64x2_t ALWAYS_INLINE ChoosePivot(int64_t *buf, int32_t from, int32_t to)
{
    constexpr int32_t N1 = 1;
    int32_t mid = from + (to - from) / 2;
    int32_t prev = mid - N1;

    if constexpr (std::is_same_v<RawType, double>) {
        auto doubleBuf = reinterpret_cast<double *>(buf);
        double midValue = doubleBuf[mid];
        for (; std::fabs(doubleBuf[prev] - midValue) < DBL_EPSILON; prev -= N1) {
            if (prev == from) {
                return vreinterpretq_s64_f64(vdupq_n_f64(doubleBuf[from]));
            }
        }

        int32_t next = prev + N1;
        int32_t last = to - 1;
        for (; std::fabs(doubleBuf[next] - midValue) < DBL_EPSILON; next += N1) {
            if (next == last) {
                return vreinterpretq_s64_f64(vdupq_n_f64(doubleBuf[prev]));
            }
        }
        int32_t excessIfMedian = next - mid;
        int32_t excessIfPrev = mid - prev;
        double pivot = excessIfMedian < excessIfPrev ? midValue : doubleBuf[prev];
        return vreinterpretq_s64_f64(vdupq_n_f64(pivot));
    } else {
        int64_t midValue = buf[mid];
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
        int64_t pivot = excessIfMedian < excessIfPrev ? midValue : buf[prev];
        return vdupq_n_s64(pivot);
    }
}

static uint64x2_t ALWAYS_INLINE GetIdxFromBitsForLong(uint64_t maskBits)
{
    // There are only 2 lanes, so we can afford to load the index vector directly.
    alignas(16) static constexpr uint8_t u8_indices[64] = { // PrintCompress64x2Tables
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

static int32_t ALWAYS_INLINE CompressStore(int64x2_t valueVec, uint64x2_t addrVec, uint64x2_t mask, int64_t *valuePtr,
    uint64_t *addrPtr)
{
    uint64_t maskBits = BitsFromMask(mask);
    int64x2_t compressValueVec = Compress(valueVec, maskBits);
    vst1q_s64(valuePtr, compressValueVec);
    int64x2_t compressAddrVec = Compress(vreinterpretq_s64_u64(addrVec), maskBits);
    vst1q_u64(addrPtr, vreinterpretq_u64_s64(compressAddrVec));
    int32_t result = __builtin_popcountll(maskBits);
    return result;
}

template <typename RawType>
static int32_t ALWAYS_INLINE CompressBlendedStore(int64x2_t valueVec, uint64x2_t addrVec, uint64x2_t mask,
    int64_t *valuePtr, uint64_t *addrPtr)
{
    uint64_t maskBits = BitsFromMask(mask);
    int32_t count = __builtin_popcountll(maskBits);
    uint64x2_t storeMask = FirstN<RawType>(count);

    int64x2_t compressValueVec = Compress(valueVec, maskBits);
    int64x2_t tmpValueVec = vld1q_s64(valuePtr);
    uint64x2_t blendedValueVec =
        vbslq_u64(storeMask, vreinterpretq_u64_s64(compressValueVec), vreinterpretq_u64_s64(tmpValueVec));
    vst1q_s64(valuePtr, vreinterpretq_s64_u64(blendedValueVec));

    int64x2_t compressAddrVec = Compress(vreinterpretq_s64_u64(addrVec), maskBits);
    uint64x2_t tmpAddrVec = vld1q_u64(addrPtr);
    uint64x2_t blendedAddrVec = vbslq_u64(storeMask, vreinterpretq_u64_s64(compressAddrVec), tmpAddrVec);
    vst1q_u64(addrPtr, blendedAddrVec);
    return count;
}

template <typename RawType, int32_t sortAscending>
int32_t PartitionToMultipleOfUnroll(int64_t *values, uint64_t *addresses, int32_t &num, int64x2_t pivotVec,
    int64_t *valueBuf, uint64_t *addrBuf, int64x2_t &smallestVec, int64x2_t &largestVec)
{
    const int32_t num_rem = (num < 2 * LOOP_SIZE) ? num : (num & (LOOP_SIZE - 1));
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
    for (; i + LANE_SIZE <= num_rem; i += LANE_SIZE) {
        int64x2_t valueVec = vld1q_s64(values + readL);
        UpdateMinMax<RawType, sortAscending>(valueVec, smallestVec, largestVec);
        uint64x2_t addrVec = vld1q_u64(addresses + readL);
        readL += LANE_SIZE;

        compareResult = Compare<RawType, sortAscending>(pivotVec, valueVec);
        blendedMask = Not(compareResult);
        auto sizeL = CompressBlendedStore<RawType>(valueVec, addrVec, blendedMask, valuePosL, addrPosL);
        valuePosL += sizeL;
        addrPosL += sizeL;

        bufR += CompressStore(valueVec, addrVec, compareResult, valueBuf + bufR, addrBuf + bufR);
    }

    if (i != num_rem) {
        uint64x2_t firstMask = FirstN<RawType>(num_rem - i);
        int64x2_t lastValueVec = vld1q_s64(values + readL);
        UpdateMinMax<RawType, sortAscending>(lastValueVec, smallestVec, largestVec);
        uint64x2_t lastAddrVec = vld1q_u64(addresses + readL);

        compareResult = Compare<RawType, sortAscending>(pivotVec, lastValueVec);
        blendedMask = vbicq_u64(firstMask, compareResult);
        auto sizeL = CompressBlendedStore<RawType>(lastValueVec, lastAddrVec, blendedMask, valuePosL, addrPosL);
        valuePosL += sizeL;
        addrPosL += sizeL;
        uint64x2_t mask = vandq_u64(compareResult, firstMask);
        bufR += CompressStore(lastValueVec, lastAddrVec, mask, valueBuf + bufR, addrBuf + bufR);
    }
    num -= static_cast<int>(bufR);
    auto copyByteSize = bufR * sizeof(int64_t);
    memcpy_s(valuePosL, copyByteSize, values + num, copyByteSize);
    memcpy_s(addrPosL, copyByteSize, addresses + num, copyByteSize);
    memcpy_s(values + num, copyByteSize, valueBuf, copyByteSize);
    memcpy_s(addresses + num, copyByteSize, addrBuf, copyByteSize);
    return static_cast<int32_t>(valuePosL - values);
}

template <typename RawType, int32_t sortAscending>
static void ALWAYS_INLINE StoreLeftRight4(int64_t *values, int64x2_t pivotVec, int64x2_t valueVec0, int64x2_t valueVec1,
    int64x2_t valueVec2, int64x2_t valueVec3, uint64_t *addresses, uint64x2_t addrVec0, uint64x2_t addrVec1,
    uint64x2_t addrVec2, uint64x2_t addrVec3, int32_t &writeL, int32_t &remaining, int64x2_t &smallestVec,
    int64x2_t &largestVec)
{
    UpdateMinMax<RawType, sortAscending>(valueVec0, smallestVec, largestVec);
    uint64x2_t compareResult0 = Compare<RawType, sortAscending>(pivotVec, valueVec0);
    uint64x2_t compareResult1 = Compare<RawType, sortAscending>(pivotVec, valueVec1);
    uint64x2_t compareResult2 = Compare<RawType, sortAscending>(pivotVec, valueVec2);
    uint64x2_t compareResult3 = Compare<RawType, sortAscending>(pivotVec, valueVec3);
    uint64x2_t swapMask0 = SwapMask(compareResult0);
    uint64x2_t swapMask1 = SwapMask(compareResult1);
    uint64x2_t swapMask2 = SwapMask(compareResult2);
    uint64x2_t swapMask3 = SwapMask(compareResult3);

    int64x2_t compressValueVec0 = CompressNot(valueVec0, swapMask0);
    int64x2_t compressValueVec1 = CompressNot(valueVec1, swapMask1);
    int64x2_t compressValueVec2 = CompressNot(valueVec2, swapMask2);
    int64x2_t compressValueVec3 = CompressNot(valueVec3, swapMask3);
    uint64x2_t compressAddrVec0 = vreinterpretq_u64_s64(CompressNot(vreinterpretq_s64_u64(addrVec0), swapMask0));
    uint64x2_t compressAddrVec1 = vreinterpretq_u64_s64(CompressNot(vreinterpretq_s64_u64(addrVec1), swapMask1));
    uint64x2_t compressAddrVec2 = vreinterpretq_u64_s64(CompressNot(vreinterpretq_s64_u64(addrVec2), swapMask2));
    uint64x2_t compressAddrVec3 = vreinterpretq_u64_s64(CompressNot(vreinterpretq_s64_u64(addrVec3), swapMask3));
    UpdateMinMax<RawType, sortAscending>(valueVec1, smallestVec, largestVec);

    int32_t numRight0 = CountTrue(compareResult0);
    int32_t numRight1 = CountTrue(compareResult1);
    int32_t numRight2 = CountTrue(compareResult2);
    int32_t numRight3 = CountTrue(compareResult3);

    int32_t writeL0 = writeL;
    int32_t writeR0 = remaining - LANE_SIZE + writeL;
    int32_t tmpPos1 = writeL - numRight0;
    int32_t writeL1 = LANE_SIZE + tmpPos1;
    int32_t writeR1 = remaining - LANE_SIZE + tmpPos1;
    int32_t tmpPos2 = tmpPos1 - numRight1;
    int32_t writeL2 = 2 * LANE_SIZE + tmpPos2;
    int32_t writeR2 = remaining - LANE_SIZE + tmpPos2;
    int32_t tmpPos3 = tmpPos2 - numRight2;
    int32_t writeL3 = 3 * LANE_SIZE + tmpPos3;
    int32_t writeR3 = remaining - LANE_SIZE + tmpPos3;

    remaining -= LOOP_SIZE;
    int32_t tmpPos = tmpPos3 - numRight3;
    writeL = LOOP_SIZE + tmpPos;

    vst1q_s64(values + writeL0, compressValueVec0);
    vst1q_s64(values + writeL1, compressValueVec1);
    vst1q_s64(values + writeL2, compressValueVec2);
    vst1q_s64(values + writeL3, compressValueVec3);
    vst1q_s64(values + writeR0, compressValueVec0);
    vst1q_s64(values + writeR1, compressValueVec1);
    vst1q_s64(values + writeR2, compressValueVec2);
    vst1q_s64(values + writeR3, compressValueVec3);
    UpdateMinMax<RawType, sortAscending>(valueVec2, smallestVec, largestVec);

    vst1q_u64(addresses + writeL0, compressAddrVec0);
    vst1q_u64(addresses + writeL1, compressAddrVec1);
    vst1q_u64(addresses + writeL2, compressAddrVec2);
    vst1q_u64(addresses + writeL3, compressAddrVec3);
    vst1q_u64(addresses + writeR0, compressAddrVec0);
    vst1q_u64(addresses + writeR1, compressAddrVec1);
    vst1q_u64(addresses + writeR2, compressAddrVec2);
    vst1q_u64(addresses + writeR3, compressAddrVec3);
    UpdateMinMax<RawType, sortAscending>(valueVec3, smallestVec, largestVec);
}

template <typename RawType, int32_t sortAscending>
int32_t PartitionWithSIMD(int64_t *values, uint64_t *addresses, int32_t from, int32_t to, int64x2_t pivotVec,
    int64_t *valueBuf, uint64_t *addrBuf, int64x2_t &smallestVec, int64x2_t &largestVec)
{
    int32_t num = to - from;
    int64_t *valueStart = values + from;
    uint64_t *addressStart = addresses + from;

    num -= LANE_SIZE;
    int32_t last = num;

    int64x2_t valueLast = vld1q_s64(valueStart + last);
    UpdateMinMax<RawType, sortAscending>(valueLast, smallestVec, largestVec);
    uint64x2_t addrLast = vld1q_u64(addressStart + last);

    int32_t consumedL = PartitionToMultipleOfUnroll<RawType, sortAscending>(valueStart, addressStart, num, pivotVec,
        valueBuf, addrBuf, smallestVec, largestVec);
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
        constexpr int32_t pos0 = 0;
        constexpr int32_t pos1 = LANE_SIZE;
        constexpr int32_t pos2 = 2 * LANE_SIZE;
        constexpr int32_t pos3 = 3 * LANE_SIZE;

        int64x2_t leftValueVec0 = vld1q_s64(valueReadL + pos0);
        int64x2_t leftValueVec1 = vld1q_s64(valueReadL + pos1);
        int64x2_t leftValueVec2 = vld1q_s64(valueReadL + pos2);
        int64x2_t leftValueVec3 = vld1q_s64(valueReadL + pos3);
        valueReadL += LOOP_SIZE;
        valueReadR -= LOOP_SIZE;
        int64x2_t rightValueVec0 = vld1q_s64(valueReadR + pos0);
        int64x2_t rightValueVec1 = vld1q_s64(valueReadR + pos1);
        int64x2_t rightValueVec2 = vld1q_s64(valueReadR + pos2);
        int64x2_t rightValueVec3 = vld1q_s64(valueReadR + pos3);

        uint64x2_t leftAddrVec0 = vld1q_u64(addrReadL + pos0);
        uint64x2_t leftAddrVec1 = vld1q_u64(addrReadL + pos1);
        uint64x2_t leftAddrVec2 = vld1q_u64(addrReadL + pos2);
        uint64x2_t leftAddrVec3 = vld1q_u64(addrReadL + pos3);
        addrReadL += LOOP_SIZE;
        addrReadR -= LOOP_SIZE;
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
            constexpr int32_t prefetchStep = 3 * LOOP_SIZE;

            int32_t capacityL = static_cast<int32_t>(valueReadL - valueStart) - writeL;
            if (capacityL > LOOP_SIZE) {
                valueReadR -= LOOP_SIZE;
                curValueVec0 = vld1q_s64(valueReadR + pos0);
                curValueVec1 = vld1q_s64(valueReadR + pos1);
                curValueVec2 = vld1q_s64(valueReadR + pos2);
                curValueVec3 = vld1q_s64(valueReadR + pos3);
                __builtin_prefetch(valueReadR - prefetchStep, 0, 3);

                addrReadR -= LOOP_SIZE;
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
                valueReadL += LOOP_SIZE;
                __builtin_prefetch(valueReadL + prefetchStep, 0, 3);

                curAddrVec0 = vld1q_u64(addrReadL + pos0);
                curAddrVec1 = vld1q_u64(addrReadL + pos1);
                curAddrVec2 = vld1q_u64(addrReadL + pos2);
                curAddrVec3 = vld1q_u64(addrReadL + pos3);
                addrReadL += LOOP_SIZE;
                __builtin_prefetch(addrReadL + prefetchStep, 0, 3);
            }

            StoreLeftRight4<RawType, sortAscending>(valueStart, pivotVec, curValueVec0, curValueVec1, curValueVec2,
                curValueVec3, addressStart, curAddrVec0, curAddrVec1, curAddrVec2, curAddrVec3, writeL, remaining,
                smallestVec, largestVec);
        }

        StoreLeftRight4<RawType, sortAscending>(valueStart, pivotVec, leftValueVec0, leftValueVec1, leftValueVec2,
            leftValueVec3, addressStart, leftAddrVec0, leftAddrVec1, leftAddrVec2, leftAddrVec3, writeL, remaining,
            smallestVec, largestVec);
        StoreLeftRight4<RawType, sortAscending>(valueStart, pivotVec, rightValueVec0, rightValueVec1, rightValueVec2,
            rightValueVec3, addressStart, rightAddrVec0, rightAddrVec1, rightAddrVec2, rightAddrVec3, writeL, remaining,
            smallestVec, largestVec);
    }

    int32_t totalR = last - writeL;
    uint64x2_t compareResult = Compare<RawType, sortAscending>(pivotVec, valueLast);
    uint64x2_t blendedMask = Not(compareResult);
    if (totalR == 0) {
        writeL += CompressStore(valueLast, addrLast, blendedMask, valueStart + writeL, addressStart + writeL);
    } else {
        int32_t startR = totalR < LANE_SIZE ? writeL + totalR - LANE_SIZE : writeL;
        int64x2_t valueRightVec = vld1q_s64(valueStart + startR);
        UpdateMinMax<RawType, sortAscending>(valueRightVec, smallestVec, largestVec);
        vst1q_s64(valueStart + last, valueRightVec);
        uint64x2_t addrRightVec = vld1q_u64(addressStart + startR);
        vst1q_u64(addressStart + last, addrRightVec);

        writeL +=
            CompressBlendedStore<RawType>(valueLast, addrLast, blendedMask, valueStart + writeL, addressStart + writeL);
        CompressBlendedStore<RawType>(valueLast, addrLast, compareResult, valueStart + writeL, addressStart + writeL);
    }
    return consumedL + writeL + from;
}

template <typename RawType, int32_t sortAscending>
void QuickSortInternalSIMD(int64_t *values, uint64_t *addresses, int32_t from, int32_t to, int64_t *valueBuf,
    uint64_t *addrBuf, bool chooseAvg, RawType avg)
{
    int32_t num = to - from;
    if (num <= SMALL_CASE_LENGTH) {
        if constexpr (sortAscending == 0) {
            SmallCaseSortDesc<RawType>(values, addresses, from, to);
        } else {
            SmallCaseSortAsec<RawType>(values, addresses, from, to);
        }
        return;
    }

    int64x2_t pivotVal;
    if (chooseAvg) {
        pivotVal = vdupq_n_s64(avg);
    } else {
        auto count = DrawSamples<RawType, sortAscending>(values, from, to, valueBuf);
        if constexpr (sortAscending == 0) {
            SmallCaseSortWithoutAddressDesc<RawType>(valueBuf, 0, count);
        } else {
            SmallCaseSortWithoutAddressAsec<RawType>(valueBuf, 0, count);
        }
        pivotVal = ChoosePivot<RawType>(valueBuf, 0, count);
    }

    int64x2_t smallestVec = GetSmallestVec<RawType, sortAscending>();
    int64x2_t largestVec = GetLargestVec<RawType, sortAscending>();
    int32_t partitionIdx = PartitionWithSIMD<RawType, sortAscending>(values, addresses, from, to, pivotVal, valueBuf,
        addrBuf, smallestVec, largestVec);

    double ratio = std::min(to - partitionIdx, partitionIdx - from) / double(to - from);
    if (ratio < 0.2) {
        chooseAvg = !chooseAvg;
    }

    auto smallest = GetSmallest<RawType, sortAscending>(smallestVec);
    auto largest = GetLargest<RawType, sortAscending>(largestVec);
    RawType pivot;
    if constexpr (std::is_same_v<RawType, double>) {
        pivot = vgetq_lane_f64(vreinterpretq_f64_s64(pivotVal), 0);
        if ((partitionIdx - from > 1) && (std::fabs(pivot - smallest) >= DBL_EPSILON)) {
            RawType newAvg = (smallest + pivot) / 2;
            QuickSortInternalSIMD<RawType, sortAscending>(values, addresses, from, partitionIdx, valueBuf, addrBuf,
                chooseAvg, newAvg);
        }
        if ((to - partitionIdx > 1) && (std::fabs(pivot - largest) >= DBL_EPSILON)) {
            RawType newAvg = (pivot + largest) / 2;
            QuickSortInternalSIMD<RawType, sortAscending>(values, addresses, partitionIdx, to, valueBuf, addrBuf,
                chooseAvg, newAvg);
        }
    } else {
        pivot = vgetq_lane_s64(pivotVal, 0);
        if ((partitionIdx - from > 1) && (pivot != smallest)) {
            RawType newAvg = (smallest & pivot) + ((smallest ^ pivot) >> 1);
            QuickSortInternalSIMD<RawType, sortAscending>(values, addresses, from, partitionIdx, valueBuf, addrBuf,
                chooseAvg, newAvg);
        }
        if ((to - partitionIdx > 1) && (pivot != largest)) {
            RawType newAvg = (largest & pivot) + ((largest ^ pivot) >> 1);
            QuickSortInternalSIMD<RawType, sortAscending>(values, addresses, partitionIdx, to, valueBuf, addrBuf,
                chooseAvg, newAvg);
        }
    }
}

void QuickSortAscSIMD(int64_t *values, uint64_t *addresses, int32_t from, int32_t to)
{
    int64_t valueBuf[BUFFER_SIZE];
    uint64_t addrBuf[BUFFER_SIZE];
    QuickSortInternalSIMD<int64_t, 1>(values, addresses, from, to, valueBuf, addrBuf);
}

void QuickSortDescSIMD(int64_t *values, uint64_t *addresses, int32_t from, int32_t to)
{
    int64_t valueBuf[BUFFER_SIZE];
    uint64_t addrBuf[BUFFER_SIZE];
    QuickSortInternalSIMD<int64_t, 0>(values, addresses, from, to, valueBuf, addrBuf);
}

void QuickSortDoubleAscSIMD(int64_t *values, uint64_t *addresses, int32_t from, int32_t to)
{
    int64_t valueBuf[BUFFER_SIZE];
    uint64_t addrBuf[BUFFER_SIZE];
    QuickSortInternalSIMD<double, 1>(values, addresses, from, to, valueBuf, addrBuf);
}

void QuickSortDoubleDescSIMD(int64_t *values, uint64_t *addresses, int32_t from, int32_t to)
{
    int64_t valueBuf[BUFFER_SIZE];
    uint64_t addrBuf[BUFFER_SIZE];
    QuickSortInternalSIMD<double, 0>(values, addresses, from, to, valueBuf, addrBuf);
}