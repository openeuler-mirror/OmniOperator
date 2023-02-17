#pragma once

/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Inner supported aggregators header
 */

#include "operator/aggregation/definitions.h"

namespace omniruntime {
namespace op {
template <typename IN, typename OUT, void (*OP)(OUT *, int64_t &, const IN &, const int64_t &)>
VECTORIZE_LOOP FAST_MATH NO_INLINE void add(OUT *res_, int64_t &flag_, const IN *__restrict ptr, const size_t rowCount)
{
    if (rowCount > 0) {
#ifdef DEBUG
        if (reinterpret_cast<unsigned long>(ptr) % ARRAY_ALIGNMENT != 0) {
            LogWarn("[add]: Data pointer NOT aligned");
        }
#endif
        ptr = (const IN *)__builtin_assume_aligned(ptr, ARRAY_ALIGNMENT);
        OUT res = *res_;
        int64_t flag = flag_;
        for (size_t i = 0; i < rowCount; ++i) {
            OP(&res, flag, ptr[i], 1LL);
        }
        *res_ = res;
        flag_ = flag;
    }
}

template <typename IN, typename OUT, void (*OP)(OUT *, int64_t &, const IN &, const int64_t &)>
VECTORIZE_LOOP FAST_MATH NO_INLINE void addDict(OUT *res_, int64_t &flag_, const IN *__restrict ptr,
    const size_t rowCount, const int32_t *__restrict indexMap)
{
    if (rowCount > 0) {
#ifdef DEBUG
        if (reinterpret_cast<unsigned long>(ptr) % ARRAY_ALIGNMENT != 0) {
            LogWarn("[addDict]: Data pointer NOT aligned");
        }
        if (reinterpret_cast<unsigned long>(indexMap) % ARRAY_ALIGNMENT != 0) {
            LogWarn("[addDict]: Dictionary Index Map pointer NOT aligned");
        }
#endif
        ptr = (const IN *)__builtin_assume_aligned(ptr, ARRAY_ALIGNMENT);
        indexMap = (const int32_t *)__builtin_assume_aligned(indexMap, ARRAY_ALIGNMENT);
        OUT res = *res_;
        int64_t flag = flag_;
        for (size_t i = 0; i < rowCount; ++i) {
            OP(&res, flag, ptr[indexMap[i]], 1LL);
        }
        *res_ = res;
        flag_ = flag;
    }
}

template <typename IN, typename OUT, void (*OP)(OUT *, int64_t &, const IN &, const int64_t &, const uint8_t &)>
VECTORIZE_LOOP FAST_MATH NO_INLINE void addConditional(OUT *res_, int64_t &flag_, const IN *__restrict ptr,
    const size_t rowCount, const uint8_t *__restrict condition)
{
    if (rowCount > 0) {
#ifdef DEBUG
        if (reinterpret_cast<unsigned long>(ptr) % ARRAY_ALIGNMENT != 0) {
            LogWarn("[addConditional]: Data pointer NOT aligned");
        }
        if (reinterpret_cast<unsigned long>(condition) % ARRAY_ALIGNMENT != 0) {
            LogWarn("[addConditional]: ConditionMap pointer NOT aligned");
        }
#endif

        ptr = (const IN *)__builtin_assume_aligned(ptr, ARRAY_ALIGNMENT);
        condition = (const uint8_t *)__builtin_assume_aligned(condition, ARRAY_ALIGNMENT);
        OUT res = *res_;
        int64_t flag = flag_;
        for (size_t i = 0; i < rowCount; ++i) {
            OP(&res, flag, ptr[i], 1LL, condition[i]);
        }
        *res_ = res;
        flag_ = flag;
    }
}

template <typename IN, typename OUT, void (*OP)(OUT *, int64_t &, const IN &, const int64_t &, const uint8_t &)>
VECTORIZE_LOOP FAST_MATH NO_INLINE void addDictConditional(OUT *res_, int64_t &flag_, const IN *__restrict ptr,
    const size_t rowCount, const uint8_t *__restrict condition, const int32_t *__restrict indexMap)
{
    if (rowCount > 0) {
#ifdef DEBUG
        if (reinterpret_cast<unsigned long>(ptr) % ARRAY_ALIGNMENT != 0) {
            LogWarn("[addDictConditional]: Data pointer NOT aligned");
        }
        if (reinterpret_cast<unsigned long>(condition) % ARRAY_ALIGNMENT != 0) {
            LogWarn("[addDictConditional]: ConditionMap pointer NOT aligned");
        }
        if (reinterpret_cast<unsigned long>(indexMap) % ARRAY_ALIGNMENT != 0) {
            LogWarn("[addDictConditional]: Dictionary Index Map pointer NOT aligned");
        }
#endif

        ptr = (const IN *)__builtin_assume_aligned(ptr, ARRAY_ALIGNMENT);
        condition = (const uint8_t *)__builtin_assume_aligned(condition, ARRAY_ALIGNMENT);
        indexMap = (const int32_t *)__builtin_assume_aligned(indexMap, ARRAY_ALIGNMENT);
        OUT res = *res_;
        int64_t flag = flag_;
        for (size_t i = 0; i < rowCount; ++i) {
            OP(&res, flag, ptr[indexMap[i]], 1LL, condition[i]);
        }
        *res_ = res;
        flag_ = flag;
    }
}


template <typename IN, typename OUT, void (*OP)(OUT *, int64_t &, const IN &, const int64_t &)>
VECTORIZE_LOOP FAST_MATH NO_INLINE void addAvg(OUT *res_, int64_t &flag_, const IN *__restrict ptr,
    const int64_t *__restrict cntPtr, const size_t rowCount)
{
    if (rowCount > 0) {
#ifdef DEBUG
        if (reinterpret_cast<unsigned long>(ptr) % ARRAY_ALIGNMENT != 0) {
            LogWarn("[addAvg]: Data pointer NOT aligned");
        }
        if (reinterpret_cast<unsigned long>(cntPtr) % ARRAY_ALIGNMENT != 0) {
            LogWarn("[addAvg]: Counter pointer NOT aligned");
        }
#endif
        ptr = (const IN *)__builtin_assume_aligned(ptr, ARRAY_ALIGNMENT);
        cntPtr = (const int64_t *)__builtin_assume_aligned(cntPtr, ARRAY_ALIGNMENT);
        OUT res = *res_;
        int64_t flag = flag_;
        for (size_t i = 0; i < rowCount; ++i) {
            OP(&res, flag, ptr[i], cntPtr[i]);
        }
        *res_ = res;
        flag_ = flag;
    }
}

template <typename IN, typename OUT, void (*OP)(OUT *, int64_t &, const IN &, const int64_t &)>
VECTORIZE_LOOP FAST_MATH NO_INLINE void addDictAvg(OUT *res_, int64_t &flag_, const IN *__restrict ptr,
    const int64_t *__restrict cntPtr, const size_t rowCount, const int32_t *__restrict indexMap)
{
    if (rowCount > 0) {
#ifdef DEBUG
        if (reinterpret_cast<unsigned long>(ptr) % ARRAY_ALIGNMENT != 0) {
            LogWarn("[addDictAvg]: Data pointer NOT aligned");
        }
        if (reinterpret_cast<unsigned long>(cntPtr) % ARRAY_ALIGNMENT != 0) {
            LogWarn("[addDictAvg]: Counter pointer NOT aligned");
        }
        if (reinterpret_cast<unsigned long>(indexMap) % ARRAY_ALIGNMENT != 0) {
            LogWarn("[addDictAvg]: Dictionary Index Map pointer NOT aligned");
        }
#endif
        ptr = (const IN *)__builtin_assume_aligned(ptr, ARRAY_ALIGNMENT);
        cntPtr = (const int64_t *)__builtin_assume_aligned(cntPtr, ARRAY_ALIGNMENT);
        indexMap = (const int32_t *)__builtin_assume_aligned(indexMap, ARRAY_ALIGNMENT);
        OUT res = *res_;
        int64_t flag = flag_;
        for (size_t i = 0; i < rowCount; ++i) {
            const auto idx = indexMap[i];
            OP(&res, flag, ptr[idx], cntPtr[idx]);
        }
        *res_ = res;
        flag_ = flag;
    }
}

template <typename IN, typename OUT, void (*OP)(OUT *, int64_t &, const IN &, const int64_t &, const uint8_t &)>
VECTORIZE_LOOP FAST_MATH NO_INLINE void addConditionalAvg(OUT *res_, int64_t &flag_, const IN *__restrict ptr,
    const int64_t *__restrict cntPtr, const size_t rowCount, const uint8_t *__restrict condition)
{
    if (rowCount > 0) {
#ifdef DEBUG
        if (reinterpret_cast<unsigned long>(ptr) % ARRAY_ALIGNMENT != 0) {
            LogWarn("[addConditionalAvg]: Data pointer NOT aligned");
        }
        if (reinterpret_cast<unsigned long>(cntPtr) % ARRAY_ALIGNMENT != 0) {
            LogWarn("[addConditionalAvg]: Counter pointer NOT aligned");
        }
        if (reinterpret_cast<unsigned long>(condition) % ARRAY_ALIGNMENT != 0) {
            LogWarn("[addConditionalAvg]: ConditionMap pointer NOT aligned");
        }
#endif

        ptr = (const IN *)__builtin_assume_aligned(ptr, ARRAY_ALIGNMENT);
        cntPtr = (const int64_t *)__builtin_assume_aligned(cntPtr, ARRAY_ALIGNMENT);
        condition = (const uint8_t *)__builtin_assume_aligned(condition, ARRAY_ALIGNMENT);
        OUT res = *res_;
        int64_t flag = flag_;
        for (size_t i = 0; i < rowCount; ++i) {
            OP(&res, flag, ptr[i], cntPtr[i], condition[i]);
        }
        *res_ = res;
        flag_ = flag;
    }
}

template <typename IN, typename OUT, void (*OP)(OUT *, int64_t &, const IN &, const int64_t &, const uint8_t &)>
VECTORIZE_LOOP FAST_MATH NO_INLINE void addDictConditionalAvg(OUT *res_, int64_t &flag_, const IN *__restrict ptr,
    const int64_t *__restrict cntPtr, const size_t rowCount, const uint8_t *__restrict condition,
    const int32_t *__restrict indexMap)
{
    if (rowCount > 0) {
#ifdef DEBUG
        if (reinterpret_cast<unsigned long>(ptr) % ARRAY_ALIGNMENT != 0) {
            LogWarn("[addDictConditionalAvg]: Data pointer NOT aligned");
        }
        if (reinterpret_cast<unsigned long>(cntPtr) % ARRAY_ALIGNMENT != 0) {
            LogWarn("[addDictConditionalAvg]: Counter pointer NOT aligned");
        }
        if (reinterpret_cast<unsigned long>(condition) % ARRAY_ALIGNMENT != 0) {
            LogWarn("[addDictConditionalAvg]: ConditionMap pointer NOT aligned");
        }
        if (reinterpret_cast<unsigned long>(indexMap) % ARRAY_ALIGNMENT != 0) {
            LogWarn("[addDictConditionalAvg]: Dictionary Index Map pointer NOT aligned");
        }
#endif

        ptr = (const IN *)__builtin_assume_aligned(ptr, ARRAY_ALIGNMENT);
        cntPtr = (const int64_t *)__builtin_assume_aligned(cntPtr, ARRAY_ALIGNMENT);
        condition = (const uint8_t *)__builtin_assume_aligned(condition, ARRAY_ALIGNMENT);
        indexMap = (const int32_t *)__builtin_assume_aligned(indexMap, ARRAY_ALIGNMENT);
        OUT res = *res_;
        int64_t flag = flag_;
        for (size_t i = 0; i < rowCount; ++i) {
            const auto idx = indexMap[i];
            OP(&res, flag, ptr[idx], cntPtr[idx], condition[i]);
        }
        *res_ = res;
        flag_ = flag;
    }
}
} // end of namespace op
} // end of namespace omniruntime
