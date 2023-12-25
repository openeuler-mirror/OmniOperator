/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 * Description: aggregator will call these functions
 */

#ifndef OMNI_RUNTIME_SIMD_AGGREGATION_EXTERNAL_H
#define OMNI_RUNTIME_SIMD_AGGREGATION_EXTERNAL_H

#include "neon_aggregation_func.h"
#include "simd_template_dispatcher.h"

namespace omniruntime {
namespace simd {
template <typename T> using CurUnderlyingSimd = SimdTemplateDispatcher<NeonSimd, T>;
template <typename IN, typename OUT, BasicOp op>
void SIMDAdd(OUT *res_, int64_t &flag_, const IN *__restrict ptr, const size_t rowCount)
{
    auto basicValue = BinaryOperation<op, IN, OUT>::InitValue();
    auto simdValue = CurUnderlyingSimd<OUT>::InitFunc(basicValue);
    auto remain = rowCount;
    while (CurUnderlyingSimd<OUT>::HandleNumOnce <= remain) {
        auto loadValue = CurUnderlyingSimd<IN>::template LoadFunc<OUT>(ptr);
        ptr += CurUnderlyingSimd<OUT>::HandleNumOnce;
        remain -= CurUnderlyingSimd<OUT>::HandleNumOnce;
        simdValue = BinarySimdFunc<op, OUT>::CalcSimd(simdValue, loadValue);
    }
    // simd to primitive data
    auto simdRet = CurUnderlyingSimd<OUT>::template BasicConvert<op>(simdValue);
    // handle overleft value
    auto remainRet = BinaryOperation<op, IN, OUT>::ArrayHandleFunc(ptr, remain);
    // sum all value
    simdRet = BinaryOperation<op, OUT, OUT>::BasicHandleFunc(simdRet, remainRet);
    *res_ = BinaryOperation<op, OUT, OUT>::BasicHandleFunc(simdRet, *res_);
    flag_ += rowCount;
}


template <uint32_t> struct BytesType;

template <> struct BytesType<1> {
    using type = uint8_t;
};
template <> struct BytesType<2> {
    using type = uint16_t;
};
template <> struct BytesType<4> {
    using type = uint32_t;
};
template <> struct BytesType<8> {
    using type = uint64_t;
};
template <> struct BytesType<16> {
    using type = __uint128_t;
};

template <BasicOp op, typename IN, typename OUT>
inline typename CurUnderlyingSimd<OUT>::SimdType GatherFlatWithNullValues(const uint8_t *__restrict *nulls,
    const IN *__restrict *ptr, int64_t &flag_, uint32_t &off, const size_t rowCount)
{
    OUT data[CurUnderlyingSimd<OUT>::HandleNumOnce];
    uint32_t assignIndex = 0;
    // vector有null，但一次性处理的n个数据中无null
    if (*(typename BytesType<CurUnderlyingSimd<OUT>::HandleNumOnce>::type *)(*nulls) == 0) {
        auto retPtr = *ptr;
        (*nulls) += CurUnderlyingSimd<OUT>::HandleNumOnce;
        (*ptr) += CurUnderlyingSimd<OUT>::HandleNumOnce;
        off += CurUnderlyingSimd<OUT>::HandleNumOnce;
        flag_ += CurUnderlyingSimd<OUT>::HandleNumOnce;
        // output out type data from in type value
        return CurUnderlyingSimd<IN>::template LoadFunc<OUT>(retPtr);
    }
    // vector有null，一次性处理的n个数据中有null
    while (assignIndex < CurUnderlyingSimd<OUT>::HandleNumOnce) {
        if (not *(*nulls)) {
            data[assignIndex++] = *(*ptr);
            ++flag_;
        }
        (++off);
        if (off == rowCount) {
            break;
        }
        ++(*nulls);
        ++(*ptr);
    }
    for (; assignIndex < CurUnderlyingSimd<OUT>::HandleNumOnce; ++assignIndex) {
        data[assignIndex] = BinaryOperation<op, IN, OUT>::InitValue();
    }

    return CurUnderlyingSimd<OUT>::template LoadFunc<OUT>(data);
}

template <typename IN, typename OUT>
inline typename CurUnderlyingSimd<OUT>::SimdType GatherDictValues(const IN *__restrict ptr,
    const int32_t *__restrict indexMap)
{
    OUT data[CurUnderlyingSimd<OUT>::HandleNumOnce];
    for (uint32_t i = 0; i < CurUnderlyingSimd<OUT>::HandleNumOnce; ++i) {
        data[i] = ptr[indexMap[i]];
    }
    return CurUnderlyingSimd<OUT>::template LoadFunc<OUT>(data);
}

template <BasicOp op, typename IN, typename OUT>
inline typename CurUnderlyingSimd<OUT>::SimdType GatherDictWithNullValues(const uint8_t *__restrict *nulls,
    const IN *__restrict ptr, const int32_t *__restrict *indexMap, int64_t &flag_, uint32_t &off, uint32_t rowCount)
{
    OUT data[CurUnderlyingSimd<OUT>::HandleNumOnce];

    uint32_t assignIndex = 0;

    // vector有null，但一次性处理的n个数据中无null
    if (*(typename BytesType<CurUnderlyingSimd<OUT>::HandleNumOnce>::type *)(*nulls) == 0) {
        (*nulls) += CurUnderlyingSimd<OUT>::HandleNumOnce;
        off += CurUnderlyingSimd<OUT>::HandleNumOnce;
        flag_ += CurUnderlyingSimd<OUT>::HandleNumOnce;
        for (; assignIndex < CurUnderlyingSimd<OUT>::HandleNumOnce; ++assignIndex, ++(*indexMap)) {
            data[assignIndex] = (ptr)[*(*indexMap)];
        }
        // output out type data from data
        return CurUnderlyingSimd<OUT>::template LoadFunc<OUT>(data);
    }

    while (assignIndex < CurUnderlyingSimd<OUT>::HandleNumOnce) {
        if (not *(*nulls)) {
            data[assignIndex++] = (ptr)[*(*indexMap)];
            ++flag_;
        }
        ++off;
        if (off == rowCount) {
            break;
        }
        ++(*nulls);
        ++(*indexMap);
    }
    for (; assignIndex < CurUnderlyingSimd<OUT>::HandleNumOnce; ++assignIndex) {
        data[assignIndex] = BinaryOperation<op, IN, OUT>::InitValue();
    }
    // data type is OUT
    return CurUnderlyingSimd<OUT>::template LoadFunc<OUT>(data);
}

template <typename IN, typename OUT, BasicOp op>
inline void SIMDAddConditional(OUT *res_, int64_t &flag_, const IN *__restrict ptr, const size_t rowCount,
    const uint8_t *__restrict condition)
{
    uint32_t i = 0;
    auto basicValue = BinaryOperation<op, IN, OUT>::InitValue();
    auto simdValue = CurUnderlyingSimd<OUT>::InitFunc(basicValue);
    auto gatherValue = CurUnderlyingSimd<OUT>::InitFunc(basicValue);
    while (i + CurUnderlyingSimd<OUT>::HandleNumOnce <= rowCount) {
        gatherValue = GatherFlatWithNullValues<op, IN, OUT>(&condition, &ptr, flag_, i, rowCount);
        simdValue = BinarySimdFunc<op, OUT>::CalcSimd(simdValue, gatherValue);
    }
    auto simdRet = CurUnderlyingSimd<OUT>::template BasicConvert<op>(simdValue);
    for (; i < rowCount; ++i) {
        if (not *condition) {
            simdRet = BinaryOperation<op, IN, OUT>::BasicHandleFunc(*ptr, simdRet);
            ++flag_;
        }
        ++ptr;
        ++condition;
    }
    *res_ = BinaryOperation<op, IN, OUT>::BasicHandleFunc(*res_, simdRet);
}

template <typename IN, typename OUT, BasicOp op>
void SIMDAddDict(OUT *res_, int64_t &flag_, const IN *__restrict ptr, const size_t rowCount,
    const int32_t *__restrict indexMap)
{
    uint32_t i = 0;
    auto basicValue = BinaryOperation<op, IN, OUT>::InitValue();
    auto simdValue = CurUnderlyingSimd<OUT>::InitFunc(basicValue);
    while (i + CurUnderlyingSimd<OUT>::HandleNumOnce <= rowCount) {
        auto loadValue = GatherDictValues<IN, OUT>(ptr, indexMap);
        indexMap += CurUnderlyingSimd<OUT>::HandleNumOnce;
        i += CurUnderlyingSimd<OUT>::HandleNumOnce;
        simdValue = BinarySimdFunc<op, OUT>::CalcSimd(simdValue, loadValue);
    }
    auto simdRet = CurUnderlyingSimd<OUT>::template BasicConvert<op>(simdValue);
    for (; i < rowCount; ++i) {
        simdRet = BinaryOperation<op, IN, OUT>::BasicHandleFunc(ptr[*(indexMap++)], simdRet);
    }

    *res_ = BinaryOperation<op, IN, OUT>::BasicHandleFunc(*res_, simdRet);
    flag_ += rowCount;
}

template <typename IN, typename OUT, BasicOp op>
void SIMDAddDictConditional(OUT *res_, int64_t &flag_, const IN *__restrict ptr, const size_t rowCount,
    const uint8_t *__restrict condition, const int32_t *__restrict indexMap)
{
    uint32_t i = 0;
    auto basicValue = BinaryOperation<op, IN, OUT>::InitValue();
    auto simdValue = CurUnderlyingSimd<OUT>::InitFunc(basicValue);
    while (i + CurUnderlyingSimd<OUT>::HandleNumOnce <= rowCount) {
        auto gatherValue = GatherDictWithNullValues<op, IN, OUT>(&condition, ptr, &indexMap, flag_, i, rowCount);
        simdValue = BinarySimdFunc<op, OUT>::CalcSimd(simdValue, gatherValue);
    }
    auto simdRet = CurUnderlyingSimd<OUT>::template BasicConvert<op>(simdValue);
    for (; i < rowCount; ++i) {
        if (not *condition) {
            simdRet = BinaryOperation<op, IN, OUT>::BasicHandleFunc(ptr[*(indexMap)], simdRet);
        }
        ++indexMap;
        ++condition;
        ++flag_;
    }

    *res_ = BinaryOperation<op, IN, OUT>::BasicHandleFunc(*res_, simdRet);
}

template <typename IN, typename OUT, BasicOp op>
void AddWithCount(OUT *res_, int64_t &flag_, const IN *__restrict ptr, const int64_t *__restrict cntPtr,
    const size_t rowCount)
{
    auto basicValue = BinaryOperation<op, IN, OUT>::InitValue();
    auto simdValue = CurUnderlyingSimd<IN>::InitFunc(basicValue);
    auto remain = rowCount;
    while (CurUnderlyingSimd<IN>::HandleNumOnce <= remain) {
        auto loadValue = CurUnderlyingSimd<IN>::LoadFunc(ptr);
        ptr += CurUnderlyingSimd<IN>::HandleNumOnce;
        remain -= CurUnderlyingSimd<IN>::HandleNumOnce;
        simdValue = BinarySimdFunc<op, IN>::CalcSimd(simdValue, loadValue);
    }
    auto simdRet = CurUnderlyingSimd<IN>::template BasicConvert<op>(simdValue);
    auto remainRet = BinaryOperation<op, IN, OUT>::ArrayHandleFunc(ptr, remain);
    simdRet = BinaryOperation<op, IN, OUT>::BasicHandleFunc(simdRet, remainRet);
    *res_ = BinaryOperation<op, IN, OUT>::BasicHandleFunc(simdRet, *res_);
    flag_ += rowCount;
}
}
}

#endif // OMNI_RUNTIME_SIMD_AGGREGATION_EXTERNAL_H
