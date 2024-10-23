/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */
#ifndef OMNI_SIMD_REDUCE_FUNC_H
#define OMNI_SIMD_REDUCE_FUNC_H

#include <limits>
#include <cstring>
#include "simd/simd.h"
#include "huawei_secure_c/include/securec.h"

namespace simd {
enum class ReduceFunc {
    Sum,
    Max,
    Min,
};

template <typename D, typename IN, typename OUT>
OMNI_INLINE VFromD<D> LoadFromDiffType(D d, const IN *OMNI_RESTRICT inPtr)
{
    auto rowCount = Lanes(d);
    if constexpr (std::is_same_v<IN, OUT>) {
        return LoadU(d, inPtr);
    } else {
        OUT outPtr[rowCount];
        for (int i = 0; i < rowCount; i++) {
            outPtr[i] = static_cast<OUT>(inPtr[i]);
        }
        return LoadU(d, outPtr);
    }
}

template <typename D, typename IN, typename OUT>
OMNI_INLINE VFromD<D> LoadFromDic(D d, const IN *OMNI_RESTRICT inPtr, const int32_t *OMNI_RESTRICT indexMap)
{
    auto rowCount = Lanes(d);
    OUT outPtr[rowCount];
    for (int i = 0; i < rowCount; i++) {
        outPtr[i] = static_cast<OUT>(inPtr[indexMap[i]]);
    }
    return LoadU(d, outPtr);
}

#define INIT(Rf)                                              \
    auto Init = [&]() {                                             \
        if constexpr (Rf == ReduceFunc::Sum) {                      \
            limitValue = 0;                                         \
            reduceVec = Set(d, limitValue);                         \
        } else if constexpr (Rf == ReduceFunc::Max) {               \
            if constexpr (std::is_same_v<double, OUT>) {            \
                limitValue = std::numeric_limits<double>::lowest(); \
            } else {                                                \
                limitValue = std::numeric_limits<OUT>::min();       \
            }                                                       \
            reduceVec = Set(d, limitValue);                         \
            func = Max;                                             \
            reduce = ReduceMax;                                     \
        } else if constexpr (Rf == ReduceFunc::Min) {               \
            limitValue = std::numeric_limits<OUT>::max();           \
            reduceVec = Set(d, limitValue);                         \
            func = Min;                                             \
            reduce = ReduceMin;                                     \
        } else {                                                    \
            throw std::runtime_error("Do not support reduce func"); \
        }                                                           \
    }


template <typename IN, typename OUT, ReduceFunc reduceFunc> OUT Reduce(const IN *OMNI_RESTRICT array, size_t size)
{
    using D = ScalableTag<OUT>;
    D d;
    auto reduceVec = Zero(d);
    OUT limitValue;
    using Func = VFromD<D> (*)(VFromD<D>, VFromD<D>);
    using Reduce = TFromD<D> (*)(D, VFromD<D>);
    Func func = Add;
    Reduce reduce = ReduceSum;
    INIT(reduceFunc);
    Init();

    size_t i = 0;
    const auto lanes = Lanes(d);
    auto k = size - (size % lanes);
    for (; i < k; i += lanes) {
        auto vec = LoadFromDiffType<decltype(d), IN, OUT>(d, array + i);
        reduceVec = func(reduceVec, vec);
    }

    OUT buf[lanes];
    std::fill(buf, buf + lanes, limitValue);
    if constexpr (std::is_same_v<IN, OUT>) {
        memcpy_s(buf, (size - k) * sizeof(OUT), array + i, (size - k) * sizeof(OUT));
    } else {
        for (; i < size; i++) {
            buf[size - i] = static_cast<OUT>(array[i]);
        }
    }
    auto vec = LoadU(d, buf);
    reduceVec = func(reduceVec, vec);
    return reduce(d, reduceVec);
}

template <typename IN, typename OUT, ReduceFunc reduceFunc>
OUT ReduceWithNulls(const IN *OMNI_RESTRICT array, const uint8_t *OMNI_RESTRICT nulls, size_t size)
{
    using D = ScalableTag<OUT>;
    D d;
    auto reduceVec = Zero(d);
    CappedTag<uint8_t, D::kPrivateLanes> du;
    OUT limitValue;
    using Func = VFromD<D> (*)(VFromD<D>, VFromD<D>);
    using Reduce = TFromD<D> (*)(D, VFromD<D>);
    Func func = Add;
    Reduce reduce = ReduceSum;
    INIT(reduceFunc);
    Init();

    size_t i = 0;
    const auto lanes = Lanes(d);
    auto k = size - (size % lanes);
    for (; i < k; i += lanes) {
        auto nullVec = Zero(d);
        if constexpr (std::is_same_v<OUT, double>) {
            ScalableTag<uint64_t> dt;
            nullVec = BitCast(d, PromoteTo(dt, LoadU(du, nulls + i)));
        } else {
            nullVec = PromoteTo(d, LoadU(du, nulls + i));
        }
        auto mask = Eq(nullVec, Zero(d));
        auto vec = IfThenElse(mask, LoadFromDiffType<decltype(d), IN, OUT>(d, array + i), Set(d, limitValue));
        reduceVec = func(reduceVec, vec);
    }

    OUT buf[lanes];
    uint8_t nullBuf[lanes];
    std::fill(buf, buf + lanes, limitValue);
    std::fill(nullBuf, nullBuf + lanes, 0);
    if constexpr (std::is_same_v<IN, OUT>) {
        memcpy_s(buf, (size - k) * sizeof(OUT), array + i, (size - k) * sizeof(OUT));
        memcpy_s(nullBuf, (size - k) * sizeof(uint8_t), nulls + i, (size - k) * sizeof(uint8_t));
    } else {
        for (size_t j = i; j < size; j++) {
            buf[size - j] = static_cast<OUT>(array[j]);
            nullBuf[size - j] = nulls[j];
        }
    }

    auto nullVec = Zero(d);
    if constexpr (std::is_same_v<OUT, double>) {
        ScalableTag<uint64_t> dt;
        nullVec = BitCast(d, PromoteTo(dt, LoadU(du, nullBuf)));
    } else {
        nullVec = PromoteTo(d, LoadU(du, nullBuf));
    }

    auto mask = Eq(nullVec, Zero(d));
    auto vec = IfThenElse(mask, LoadU(d, buf), Set(d, limitValue));
    reduceVec = func(reduceVec, vec);
    return reduce(d, reduceVec);
}

template <typename IN, typename OUT, ReduceFunc reduceFunc>
OUT ReduceWithDic(const IN *OMNI_RESTRICT array, const int32_t *OMNI_RESTRICT indexMap, size_t size)
{
    using D = ScalableTag<OUT>;
    D d;
    auto reduceVec = Zero(d);
    OUT limitValue;
    using Func = VFromD<D> (*)(VFromD<D>, VFromD<D>);
    using Reduce = TFromD<D> (*)(D, VFromD<D>);
    Func func = Add;
    Reduce reduce = ReduceSum;
    INIT(reduceFunc);
    Init();

    size_t i = 0;
    const auto lanes = Lanes(d);
    auto k = size - (size % lanes);
    for (; i < k; i += lanes) {
        auto vec = LoadFromDic<decltype(d), IN, OUT>(d, array, indexMap + i);
        reduceVec = func(reduceVec, vec);
    }

    OUT buf[lanes];
    std::fill(buf, buf + lanes, limitValue);
    for (; i < size; i++) {
        buf[size - i] = static_cast<OUT>(array[indexMap[i]]);
    }
    auto vec = LoadU(d, buf);
    reduceVec = func(reduceVec, vec);
    return reduce(d, reduceVec);
}

template <typename IN, typename OUT, ReduceFunc reduceFunc>
OUT ReduceWithDicAndNulls(const IN *OMNI_RESTRICT array, const int32_t *OMNI_RESTRICT indexMap,
    const uint8_t *OMNI_RESTRICT nulls, size_t size)
{
    using D = ScalableTag<OUT>;
    D d;
    auto reduceVec = Zero(d);
    CappedTag<uint8_t, D::kPrivateLanes> du;
    OUT limitValue;
    using Func = VFromD<D> (*)(VFromD<D>, VFromD<D>);
    using Reduce = TFromD<D> (*)(D, VFromD<D>);
    Func func = Add;
    Reduce reduce = ReduceSum;
    INIT(reduceFunc);
    Init();

    size_t i = 0;
    const auto lanes = Lanes(d);
    auto k = size - (size % lanes);
    for (; i < k; i += lanes) {
        auto nullVec = Zero(d);
        if constexpr (std::is_same_v<OUT, double>) {
            ScalableTag<uint64_t> dt;
            nullVec = BitCast(d, PromoteTo(dt, LoadU(du, nulls + i)));
        } else {
            nullVec = PromoteTo(d, LoadU(du, nulls + i));
        }
        auto mask = Eq(nullVec, Zero(d));
        auto vec = IfThenElse(mask, LoadFromDic<decltype(d), IN, OUT>(d, array, indexMap + i), Set(d, limitValue));
        reduceVec = func(reduceVec, vec);
    }

    OUT buf[lanes];
    std::fill(buf, buf + lanes, limitValue);
    for (; i < size; i++) {
        if (nulls[i] != 1) {
            buf[size - i] = static_cast<OUT>(array[indexMap[i]]);
        }
    }
    auto vec = LoadU(d, buf);
    reduceVec = func(reduceVec, vec);
    return reduce(d, reduceVec);
}

template <typename IN, typename OUT, typename FLAG, typename FLAG_HANDLER, ReduceFunc op>
void ReduceExternal(OUT *res_, FLAG &flag_, const IN *__restrict ptr, const size_t rowCount)
{
    if constexpr (op == ReduceFunc::Sum) {
        *res_ += Reduce<IN, OUT, op>(ptr, rowCount);
    } else if constexpr (op == ReduceFunc::Max) {
        *res_ = std::max(Reduce<IN, OUT, op>(ptr, rowCount), *res_);
    } else {
        *res_ = std::min(Reduce<IN, OUT, op>(ptr, rowCount), *res_);
    }
    FLAG_HANDLER::Update(flag_, rowCount);
}

template <typename IN, typename OUT, typename FLAG, typename FLAG_HANDLER, ReduceFunc op>
inline void ReduceWithNullsExternal(OUT *res_, FLAG &flag_, const IN *__restrict ptr, const size_t rowCount,
    const uint8_t *__restrict condition)
{
    if constexpr (op == ReduceFunc::Sum) {
        *res_ += ReduceWithNulls<IN, OUT, ReduceFunc::Sum>(ptr, condition, rowCount);
    } else if constexpr (op == ReduceFunc::Max) {
        *res_ = std::max(ReduceWithNulls<IN, OUT, ReduceFunc::Max>(ptr, condition, rowCount), *res_);
    } else {
        *res_ = std::min(ReduceWithNulls<IN, OUT, ReduceFunc::Min>(ptr, condition, rowCount), *res_);
    }
    for (int64_t i = 0; i < rowCount; ++i) {
        if (not*condition)
            FLAG_HANDLER::Update(flag_, 1ULL);
        ++condition;
    }
}

template <typename IN, typename OUT, typename FLAG, typename FLAG_HANDLER, ReduceFunc op>
void ReduceWithDicExternal(OUT *res_, FLAG &flag_, const IN *__restrict ptr, const size_t rowCount,
    const int32_t *__restrict indexMap)
{
    if constexpr (op == ReduceFunc::Sum) {
        *res_ += ReduceWithDic<IN, OUT, ReduceFunc::Sum>(ptr, indexMap, rowCount);
    } else if constexpr (op == ReduceFunc::Max) {
        *res_ = std::max(ReduceWithDic<IN, OUT, ReduceFunc::Max>(ptr, indexMap, rowCount), *res_);
    } else {
        *res_ = std::min(ReduceWithDic<IN, OUT, ReduceFunc::Min>(ptr, indexMap, rowCount), *res_);
    }
    FLAG_HANDLER::Update(flag_, rowCount);
}

template <typename IN, typename OUT, typename FLAG, typename FLAG_HANDLER, ReduceFunc op>
void ReduceWithDicAndNullsExternal(OUT *res_, FLAG &flag_, const IN *__restrict ptr, const size_t rowCount,
    const uint8_t *__restrict condition, const int32_t *__restrict indexMap)
{
    if constexpr (op == ReduceFunc::Sum) {
        *res_ += ReduceWithDicAndNulls<IN, OUT, ReduceFunc::Sum>(ptr, indexMap, condition, rowCount);
    } else if constexpr (op == ReduceFunc::Max) {
        *res_ = std::max(ReduceWithDicAndNulls<IN, OUT, ReduceFunc::Max>(ptr, indexMap, condition, rowCount), *res_);
    } else {
        *res_ = std::min(ReduceWithDicAndNulls<IN, OUT, ReduceFunc::Min>(ptr, indexMap, condition, rowCount), *res_);
    }
    for (int64_t i = 0; i < rowCount; ++i) {
        if (not*condition)
            FLAG_HANDLER::Update(flag_, 1);
        ++condition;
    }
}
}

#endif // OMNI_SIMD_REDUCE_FUNC_H
