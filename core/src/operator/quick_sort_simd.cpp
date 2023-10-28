/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 * @Description: quick sort implementations using simd
 */

#include "quick_sort_simd.h"
#include "util/compiler_util.h"
#include "huawei_secure_c/include/securec.h"
#include "neon_sort64.h"

const int32_t SMALL_CASE_LENGTH = 16;

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

template <int32_t sortAscending>
static int64x2_t ALWAYS_INLINE GetMedianValue(int64x2_t v0, int64x2_t v1, int64x2_t v2)
{
    int64x2_t v0Copy = v0;
    uint64x2_t comp = vcltq_s64(v2, v0);
    if constexpr (sortAscending == 0) {
        v0 = vbslq_s64(comp, v0, v2);
        v2 = vbslq_s64(comp, v2, v0Copy);

        comp = vcltq_s64(v1, v0);
        v1 = vbslq_s64(comp, v1, v0);
        comp = vcltq_s64(v2, v1);
        v1 = vbslq_s64(comp, v1, v2);
    } else {
        v0 = vbslq_s64(comp, v2, v0);
        v2 = vbslq_s64(comp, v0Copy, v2);

        comp = vcltq_s64(v1, v0);
        v1 = vbslq_s64(comp, v0, v1);
        comp = vcltq_s64(v2, v1);
        v1 = vbslq_s64(comp, v2, v1);
    }
    return v1;
}

template <int32_t sortAscending>
static int32_t ALWAYS_INLINE DrawSamples(int64_t *values, int32_t from, int32_t to, int64_t *buf)
{
    constexpr int32_t N = 2;
    int32_t num = to - from;
    int64_t *valueStart = values + from;
    int64_t *valueMid = values + from + num / 2;
    int64_t *valueLast = values + to - N;

    int32_t oneStep = num / 8;
    int32_t twoStep = oneStep + oneStep;

    int64x2_t v1 = vld1q_s64(valueStart);
    int64x2_t v2 = vld1q_s64(valueStart + oneStep);
    int64x2_t v3 = vld1q_s64(valueStart + twoStep);

    int64x2_t v4 = vld1q_s64(valueMid - oneStep);
    int64x2_t v5 = vld1q_s64(valueMid);
    int64x2_t v6 = vld1q_s64(valueMid + oneStep);

    int64x2_t v7 = vld1q_s64(valueLast);
    int64x2_t v8 = vld1q_s64(valueLast - oneStep);
    int64x2_t v9 = vld1q_s64(valueLast - twoStep);

    int64x2_t medianVec1 = GetMedianValue<sortAscending>(v1, v2, v3);
    int64x2_t medianVec2 = GetMedianValue<sortAscending>(v4, v5, v6);
    int64x2_t medianVec3 = GetMedianValue<sortAscending>(v7, v8, v9);
    int64_t *bufPtr = buf;
    vst1q_s64(bufPtr, medianVec1);
    bufPtr += N;
    vst1q_s64(bufPtr, medianVec2);
    bufPtr += N;
    vst1q_s64(bufPtr, medianVec3);
    return N * 3;
}

static int64x2_t ALWAYS_INLINE ChoosePivot(int64_t *buf, int32_t from, int32_t to)
{
    int32_t N1 = 1;
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

static int64x2_t ALWAYS_INLINE CompressNot(int64x2_t value, uint64x2_t mask)
{
    uint64x2_t tmpMaskL = vzip1q_u64(mask, mask);
    uint64x2_t tmpMaskH = vzip2q_u64(mask, mask);
    uint64x2_t tmpSwap = vbicq_u64(tmpMaskL, tmpMaskH);

    // shuffle
    uint8x16_t valueBytes = vreinterpretq_u8_s64(value);
    uint8x16_t shuffleBytes = vextq_u8(valueBytes, valueBytes, 8);
    int64x2_t compressVec = vbslq_s64(tmpSwap, vreinterpretq_s64_u8(shuffleBytes), value);
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
int32_t PartitionToMultipleOfUnroll(int64_t *values, uint64_t *addresses, int32_t &num, int64x2_t pivotVec, int64_t *valueBuf, uint64_t *addrBuf)
{
    int32_t kUnroll = 4;
    int32_t N = 2;
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
        uint64x2_t addrVec = vld1q_u64(addresses + readL);
        readL += N;

        if constexpr (sortAscending == 0) {
            compareResult = vcltq_s64(valueVec, pivotVec);
        } else {
            compareResult = vcltq_s64(pivotVec, valueVec);
        }

        blendedMask = Not(compareResult);
        auto sizeL = CompressBlendedStore(valueVec, addrVec, blendedMask, valuePosL, addrPosL);
        valuePosL += sizeL;
        addrPosL += sizeL;

        bufR += CompressStore(valueVec, addrVec, compareResult, valueBuf + bufR, addrBuf + bufR);
    }

    if (i != num_rem) {
        uint64x2_t firstMask = FirstN(num_rem - i);
        int64x2_t lastValueVec = vld1q_s64(values + readL);
        uint64x2_t lastAddrVec = vld1q_u64(addresses + readL);

        if constexpr (sortAscending == 0) {
            compareResult = vcltq_s64(lastValueVec, pivotVec);
        } else {
            compareResult = vcltq_s64(pivotVec, lastValueVec);
        }

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
static void ALWAYS_INLINE StoreLeftRight(int64_t *values, int64x2_t pivotVec, int64x2_t valueVec, uint64_t *addresses, uint64x2_t addrVec, int32_t &writeL, int32_t &remaining)
{
    int32_t N = 2;
    remaining -= N;
    uint64x2_t compareResult;
    if constexpr (sortAscending == 0) {
        compareResult = vcltq_s64(valueVec, pivotVec);
    } else {
        compareResult = vcltq_s64(pivotVec, valueVec);
    }
    int64x2_t compressValueVec = CompressNot(valueVec, compareResult);
    vst1q_s64(values + writeL, compressValueVec);
    vst1q_s64(values + remaining + writeL, compressValueVec);

    uint64x2_t compressAddrVec = vreinterpretq_u64_s64(CompressNot(vreinterpretq_s64_u64(addrVec), compareResult));
    vst1q_u64(addresses + writeL, compressAddrVec);
    vst1q_u64(addresses + remaining + writeL, compressAddrVec);

    int32_t numLeft = N - CountTrue(compareResult);
    writeL += numLeft;
}

template <int32_t sortAscending>
static void ALWAYS_INLINE StoreLeftRight4(int64_t *values, int64x2_t pivotVec, int64x2_t valueVec0, int64x2_t valueVec1, int64x2_t valueVec2, int64x2_t valueVec3, uint64_t *addresses, uint64x2_t addrVec0, uint64x2_t addrVec1, uint64x2_t addrVec2, uint64x2_t addrVec3, int32_t &writeL, int32_t &remaining)
{
    StoreLeftRight<sortAscending>(values, pivotVec, valueVec0, addresses, addrVec0, writeL, remaining);
    StoreLeftRight<sortAscending>(values, pivotVec, valueVec1, addresses, addrVec1, writeL, remaining);
    StoreLeftRight<sortAscending>(values, pivotVec, valueVec2, addresses, addrVec2, writeL, remaining);
    StoreLeftRight<sortAscending>(values, pivotVec, valueVec3, addresses, addrVec3, writeL, remaining);
}

template <int32_t sortAscending>
int32_t PartitionWithSIMD(int64_t *values, uint64_t *addresses, int32_t from, int32_t to, int64x2_t pivotVec, int64_t *valueBuf, uint64_t *addrBuf)
{
    constexpr int32_t N = 2;
    int32_t num = to - from;
    int64_t *valueStart = values + from;
    uint64_t *addressStart = addresses + from;

    num -= N;
    int32_t last = num;

    int64x2_t valueLast = vld1q_s64(valueStart + last);
    uint64x2_t addrLast = vld1q_u64(addressStart + last);

    int32_t consumedL = PartitionToMultipleOfUnroll<sortAscending>(valueStart, addressStart, num, pivotVec, valueBuf, addrBuf);
    valueStart += consumedL;
    addressStart += consumedL;
    last -= consumedL;
    num -= consumedL;

    constexpr int32_t kUnroll = 4;
    int64_t *valueReadL = valueStart;
    int64_t *valueReadR = valueStart + num;
    uint64_t *addrReadL = addressStart;
    uint64_t *addrReadR = addressStart + num;
    int32_t writeL = 0;
    int32_t remaining = num;
    if (num != 0) {
        int32_t pos0 = 0 * N;
        int32_t pos1 = 1 * N;
        int32_t pos2 = 2 * N;
        int32_t pos3 = 3 * N;
        int32_t advanceStep = kUnroll * N;

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
            int32_t prefetchStep = 3 * kUnroll * N;

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
            StoreLeftRight4<sortAscending>(valueStart, pivotVec, curValueVec0, curValueVec1, curValueVec2, curValueVec3, addressStart, curAddrVec0, curAddrVec1, curAddrVec2, curAddrVec3, writeL, remaining);
        }

        StoreLeftRight4<sortAscending>(valueStart, pivotVec, leftValueVec0, leftValueVec1, leftValueVec2, leftValueVec3, addressStart, leftAddrVec0, leftAddrVec1, leftAddrVec2, leftAddrVec3, writeL, remaining);
        StoreLeftRight4<sortAscending>(valueStart, pivotVec, rightValueVec0, rightValueVec1, rightValueVec2, rightValueVec3, addressStart, rightAddrVec0, rightAddrVec1, rightAddrVec2, rightAddrVec3, writeL, remaining);
    }

    int32_t totalR = last - writeL;
    int32_t startR = totalR < N ? writeL + totalR - 4 : writeL;
    int64x2_t valueRightVec = vld1q_s64(valueStart + startR);
    vst1q_s64(valueStart + last, valueRightVec);
    uint64x2_t addrRightVec = vld1q_u64(addressStart + startR);
    vst1q_u64(addressStart + last, addrRightVec);

    uint64x2_t compareResult;
    if constexpr (sortAscending == 0) {
        compareResult = vcltq_s64(valueLast, pivotVec);
    } else {
        compareResult = vcltq_s64(pivotVec, valueLast);
    }
    uint64x2_t blendedMask = Not(compareResult);
    writeL += CompressBlendedStore(valueLast, addrLast, blendedMask, valueStart + writeL, addressStart + writeL);
    CompressBlendedStore(valueLast, addrLast, compareResult, valueStart + writeL, addressStart + writeL);
    return consumedL + writeL + from;
}

template <int32_t sortAscending>
void QuickSortInternalSIMD(int64_t *values, uint64_t *addresses, int32_t from, int32_t to, int64_t *valueBuf, uint64_t *addrBuf, int32_t depth)
{
    int32_t num = to - from;
    if (num <= SMALL_CASE_LENGTH) {
        QuickSortSmallCase<sortAscending>(values, addresses, from, to);
        return;
    }

    auto count = DrawSamples<sortAscending>(values, from, to, valueBuf);
    QuickSortSmallCase<sortAscending>(valueBuf, 0, count);
    int64x2_t pivotVec = ChoosePivot(valueBuf, 0, count);

    if (depth == 0) {
        LogError("THE DEPTH IS ZERO AND THE DATA COULD BE UNORDER!!!");
        return;
    }

    --depth;

    int32_t pivotIndex = PartitionWithSIMD<sortAscending>(values, addresses, from, to, pivotVec, valueBuf, addrBuf);
    QuickSortInternalSIMD<sortAscending>(values, addresses, from, pivotIndex, valueBuf, addrBuf, depth);
    QuickSortInternalSIMD<sortAscending>(values, addresses, pivotIndex, to, valueBuf, addrBuf, depth);
}

void QuickSortAscSIMD(int64_t *values, uint64_t *addresses, int32_t from, int32_t to)
{
    int64_t valueBuf[50];
    uint64_t addrBuf[50];
    QuickSortInternalSIMD<1>(values, addresses, from, to, valueBuf, addrBuf, 50);
}

void QuickSortDescSIMD(int64_t *values, uint64_t *addresses, int32_t from, int32_t to)
{
    int64_t valueBuf[50];
    uint64_t addrBuf[50];
    QuickSortInternalSIMD<0>(values, addresses, from, to, valueBuf, addrBuf, 50);
}