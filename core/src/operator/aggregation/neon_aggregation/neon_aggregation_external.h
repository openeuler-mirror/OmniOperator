/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 * Description: aggregator will call these functions
 */

#ifndef OMNI_RUNTIME_NEON_AGGREGATION_EXTERNAL_H
#define OMNI_RUNTIME_NEON_AGGREGATION_EXTERNAL_H

#include "neon_aggregation_func.h"
namespace omniruntime {
namespace simd {
template <typename IN, typename OUT, BasicOp op>
void SIMDAdd(OUT *res_, int64_t &flag_, const IN *__restrict ptr, const size_t rowCount)
{
    auto basicValue = BinaryOperation<op, IN, OUT>::InitValue();
    auto neonValue = NeonSimd<OUT>::InitFunc(basicValue);
    auto remain = rowCount;
    while (NeonSimd<OUT>::HandleNumOnce <= remain) {
        auto loadValue = NeonSimd<IN>::template LoadFunc<OUT>(ptr);
        ptr += NeonSimd<OUT>::HandleNumOnce;
        remain -= NeonSimd<OUT>::HandleNumOnce;
        neonValue = BinarySimdFunc<op, OUT>::CalcSimd(neonValue, loadValue);
    }
    // neon to primitive data
    auto neonRet = NeonSimd<OUT>::template BasicConvert<op>(neonValue);
    // handle overleft value
    auto remainRet = BinaryOperation<op, IN, OUT>::ArrayHandleFunc(ptr, remain);
    // sum all value
    neonRet = BinaryOperation<op, OUT, OUT>::BasicHandleFunc(neonRet, remainRet);
    *res_ = BinaryOperation<op, OUT, OUT>::BasicHandleFunc(neonRet, *res_);
    flag_ += rowCount;
}


template <uint32_t bytes> struct BytesType;

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
inline typename NeonSimd<OUT>::NeonType GatherFlatWithNullValues(const uint8_t *__restrict *nulls,
    const IN *__restrict *ptr, int64_t &flag_, uint32_t &off, const size_t rowCount)
{
    OUT data[NeonSimd<OUT>::HandleNumOnce];
    uint32_t assignIndex = 0;
    // vector有null，但一次性处理的n个数据中无null
    if (*(typename BytesType<NeonSimd<OUT>::HandleNumOnce>::type *)(*nulls) == 0) {
        auto retPtr = *ptr;
        (*nulls) += NeonSimd<OUT>::HandleNumOnce;
        (*ptr) += NeonSimd<OUT>::HandleNumOnce;
        off += NeonSimd<OUT>::HandleNumOnce;
        flag_ += NeonSimd<OUT>::HandleNumOnce;
        // output out type data from in type value
        return NeonSimd<IN>::template LoadFunc<OUT>(retPtr);
    }
    // vector有null，一次性处理的n个数据中有null
    while (assignIndex < NeonSimd<OUT>::HandleNumOnce) {
        (*(*nulls)) ? (++off) : (data[assignIndex++] = *(*ptr), ++off, ++flag_);
        if (off == rowCount) {
            break;
        }
        ++(*nulls);
        ++(*ptr);
    }
    for (; assignIndex < NeonSimd<OUT>::HandleNumOnce; ++assignIndex) {
        data[assignIndex] = BinaryOperation<op, IN, OUT>::InitValue();
    }

    return NeonSimd<OUT>::template LoadFunc<OUT>(data);
}

template <typename IN, typename OUT>
inline typename NeonSimd<OUT>::NeonType GatherDictValues(const IN *__restrict ptr, const int32_t *__restrict indexMap)
{
    OUT data[NeonSimd<OUT>::HandleNumOnce];
    for (uint32_t i = 0; i < NeonSimd<OUT>::HandleNumOnce; ++i) {
        data[i] = ptr[indexMap[i]];
    }
    return NeonSimd<OUT>::template LoadFunc<OUT>(data);
}

template <BasicOp op, typename IN, typename OUT>
inline typename NeonSimd<OUT>::NeonType GatherDictWithNullValues(const uint8_t *__restrict *nulls,
    const IN *__restrict ptr, const int32_t *__restrict *indexMap, int64_t &flag_, uint32_t &off, uint32_t rowCount)
{
    OUT data[NeonSimd<OUT>::HandleNumOnce];

    uint32_t assignIndex = 0;

    while (assignIndex < NeonSimd<OUT>::HandleNumOnce) {
        *(*nulls) ? (++off) : (data[assignIndex++] = (ptr)[*(*indexMap)], ++off, ++flag_);
        if (off == rowCount) {
            break;
        }
        ++(*nulls);
        ++(*indexMap);
    }
    for (; assignIndex < NeonSimd<OUT>::HandleNumOnce; ++assignIndex) {
        data[assignIndex] = BinaryOperation<op, IN, OUT>::InitValue();
    }
    // data type is OUT
    return NeonSimd<OUT>::template LoadFunc<OUT>(data);
}

template <typename IN, typename OUT, BasicOp op>
inline void SIMDAddConditional(OUT *res_, int64_t &flag_, const IN *__restrict ptr, const size_t rowCount,
    const uint8_t *__restrict condition)
{
    uint32_t i = 0;
    auto basicValue = BinaryOperation<op, IN, OUT>::InitValue();
    auto neonValue = NeonSimd<OUT>::InitFunc(basicValue);
    auto gatherValue = NeonSimd<OUT>::InitFunc(basicValue);
    while (i + NeonSimd<OUT>::HandleNumOnce <= rowCount) {
        gatherValue = GatherFlatWithNullValues<op, IN, OUT>(&condition, &ptr, flag_, i, rowCount);
        neonValue = BinarySimdFunc<op, OUT>::CalcSimd(neonValue, gatherValue);
    }
    auto neonRet = NeonSimd<OUT>::template BasicConvert<op>(neonValue);
    for (; i < rowCount; ++i) {
        *condition ? ++ptr : (neonRet = BinaryOperation<op, IN, OUT>::BasicHandleFunc(*ptr, neonRet), ++flag_, ++ptr);
        ++condition;
    }
    *res_ = BinaryOperation<op, IN, OUT>::BasicHandleFunc(*res_, neonRet);
}

template <typename IN, typename OUT, BasicOp op>
void SIMDAddDict(OUT *res_, int64_t &flag_, const IN *__restrict ptr, const size_t rowCount,
    const int32_t *__restrict indexMap)
{
    uint32_t i = 0;
    auto basicValue = BinaryOperation<op, IN, OUT>::InitValue();
    auto neonValue = NeonSimd<OUT>::InitFunc(basicValue);
    while (i + NeonSimd<OUT>::HandleNumOnce <= rowCount) {
        auto loadValue = GatherDictValues<IN, OUT>(ptr, indexMap);
        indexMap += NeonSimd<OUT>::HandleNumOnce;
        i += NeonSimd<OUT>::HandleNumOnce;
        neonValue = BinarySimdFunc<op, OUT>::CalcSimd(neonValue, loadValue);
    }
    auto neonRet = NeonSimd<OUT>::template BasicConvert<op>(neonValue);
    for (; i < rowCount; ++i) {
        neonRet = BinaryOperation<op, IN, OUT>::BasicHandleFunc(ptr[*(indexMap++)], neonRet);
    }

    *res_ = BinaryOperation<op, IN, OUT>::BasicHandleFunc(*res_, neonRet);
    flag_ += rowCount;
}

template <typename IN, typename OUT, BasicOp op>
void SIMDAddDictConditional(OUT *res_, int64_t &flag_, const IN *__restrict ptr, const size_t rowCount,
    const uint8_t *__restrict condition, const int32_t *__restrict indexMap)
{
    uint32_t i = 0;
    auto basicValue = BinaryOperation<op, IN, OUT>::InitValue();
    auto neonValue = NeonSimd<OUT>::InitFunc(basicValue);
    while (i + NeonSimd<OUT>::HandleNumOnce <= rowCount) {
        auto gatherValue = GatherDictWithNullValues<op, IN, OUT>(&condition, ptr, &indexMap, flag_, i, rowCount);
        neonValue = BinarySimdFunc<op, OUT>::CalcSimd(neonValue, gatherValue);
    }
    auto neonRet = NeonSimd<OUT>::template BasicConvert<op>(neonValue);
    for (; i < rowCount; ++i) {
        (*condition) > 0 ?
            (++indexMap) :
            (neonRet = BinaryOperation<op, IN, OUT>::BasicHandleFunc(ptr[*(indexMap)], neonRet), ++indexMap);
        ++condition;
        ++flag_;
    }

    *res_ = BinaryOperation<op, IN, OUT>::BasicHandleFunc(*res_, neonRet);
}

template <typename IN, typename OUT, BasicOp op>
void AddWithCount(OUT *res_, int64_t &flag_, const IN *__restrict ptr, const int64_t *__restrict cntPtr,
    const size_t rowCount)
{
    uint32_t i = 0;
    auto basicValue = BinaryOperation<op, IN, OUT>::InitValue();
    auto neonValue = NeonSimd<IN>::InitFunc(basicValue);
    auto remain = rowCount;
    while (NeonSimd<IN>::HandleNumOnce <= remain) {
        auto loadValue = NeonSimd<IN>::LoadFunc(ptr);
        ptr += NeonSimd<IN>::HandleNumOnce;
        remain -= NeonSimd<IN>::HandleNumOnce;
        neonValue = BinarySimdFunc<op, IN>::CalcSimd(neonValue, loadValue);
    }
    auto neonRet = NeonSimd<IN>::template BasicConvert<op>(neonValue);
    auto remainRet = BinaryOperation<op, IN, OUT>::ArrayHandleFunc(ptr, remain);
    neonRet = BinaryOperation<op, IN, OUT>::BasicHandleFunc(neonRet, remainRet);
    *res_ = BinaryOperation<op, IN, OUT>::BasicHandleFunc(neonRet, *res_);
    flag_ += rowCount;
}
}
}

#endif // OMNI_RUNTIME_NEON_AGGREGATION_EXTERNAL_H
