/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2023. All rights reserved.
 * Description: Inner supported aggregators header
 */
#pragma once
#include "operator/aggregation/definitions.h"

namespace omniruntime {
namespace op {
#define STATE_STEP 8

// Note: based on investigation, for rowIndex aggregation (a.k.a. indirect access to data array elements),
//       NoSIMD loop is faster than SIMD loop.
//       For this reason we add '__attribute__((optimize("no-tree-vectorize")))' attribute to this function so that
//       compiler does not vectorize it
template <typename IN, void (*UPDATER)(AggregateState *, const IN &)>
VECTORIZE_LOOP NO_INLINE void AddUseRowIndex(std::vector<AggregateState *> &rowStates, const size_t aggStateOffset,
    const IN *__restrict ptr)
{
    const size_t rowCount = rowStates.size();
    if (rowCount > 0) {
#ifdef DEBUG
        if (reinterpret_cast<unsigned long>(ptr) % ARRAY_ALIGNMENT != 0) {
            LogWarn("[addUseRowIndex]: Data pointer NOT aligned");
        }
#endif
        ptr = (const IN *)__builtin_assume_aligned(ptr, ARRAY_ALIGNMENT);
        for (size_t i = 0; i < rowCount; ++i) {
            UPDATER(rowStates[i] + aggStateOffset, ptr[i]);
        }
    }
}

template <typename IN, void (*UPDATER)(AggregateState *, const IN &)>
VECTORIZE_LOOP NO_INLINE void AddDictUseRowIndex(std::vector<AggregateState *> &rowStates, const size_t aggStateOffset,
    const IN *__restrict ptr, const int32_t *__restrict indexMap)
{
    const size_t rowCount = rowStates.size();
    if (rowCount > 0) {
#ifdef DEBUG
        if (reinterpret_cast<unsigned long>(ptr) % ARRAY_ALIGNMENT != 0) {
            LogWarn("[addDictUseRowIndex]: Data pointer NOT aligned");
        }
        if (reinterpret_cast<unsigned long>(indexMap) % ARRAY_ALIGNMENT != 0) {
            LogWarn("[addDictUseRowIndex]: Dictionary Index Map pointer NOT aligned");
        }
#endif
        ptr = (const IN *)__builtin_assume_aligned(ptr, ARRAY_ALIGNMENT);
        indexMap = (const int32_t *)__builtin_assume_aligned(indexMap, ARRAY_ALIGNMENT);

        for (size_t i = 0; i < rowCount; ++i) {
            AggregateState *state = rowStates[i] + aggStateOffset;
            UPDATER(state, ptr[indexMap[i]]);
        }
    }
}

template <typename IN, void (*Updater)(AggregateState *, const IN &, const uint8_t &)>
VECTORIZE_LOOP NO_INLINE void AddConditionalUseRowIndex(std::vector<AggregateState *> &rowStates,
    const size_t aggStateOffset, const IN *__restrict ptr, const NullsHelper &condition)
{
    const size_t rowCount = rowStates.size();
    if (rowCount > 0) {
#ifdef DEBUG
        if (reinterpret_cast<unsigned long>(ptr) % ARRAY_ALIGNMENT != 0) {
            LogWarn("[addConditionalUseRowIndex]: Data pointer NOT aligned");
        }
#endif
        ptr = (const IN *)__builtin_assume_aligned(ptr, ARRAY_ALIGNMENT);

        for (size_t i = 0; i < rowCount; ++i) {
            AggregateState *state = rowStates[i] + aggStateOffset;
            Updater(state, ptr[i], condition[i]);
        }
    }
}

template <typename IN, typename OUT, void (*Updater)(AggregateState *, const IN &, const uint8_t &)>
VECTORIZE_LOOP NO_INLINE void AddDictConditionalUseRowIndex(std::vector<AggregateState *> &rowStates,
    const size_t aggStateOffset, const IN *__restrict ptr, const NullsHelper &condition,
    const int32_t *__restrict indexMap)
{
    const size_t rowCount = rowStates.size();
    if (rowCount > 0) {
#ifdef DEBUG
        if (reinterpret_cast<unsigned long>(ptr) % ARRAY_ALIGNMENT != 0) {
            LogWarn("[addDictConditionalUseRowIndex]: Data pointer NOT aligned");
        }
        if (reinterpret_cast<unsigned long>(indexMap) % ARRAY_ALIGNMENT != 0) {
            LogWarn("[addDictConditionalUseRowIndex]: Dictionary Index Map pointer NOT aligned");
        }
#endif
        ptr = (const IN *)__builtin_assume_aligned(ptr, ARRAY_ALIGNMENT);
        indexMap = (const int32_t *)__builtin_assume_aligned(indexMap, ARRAY_ALIGNMENT);

        for (size_t i = 0; i < rowCount; ++i) {
            AggregateState *state = rowStates[i] + aggStateOffset;
            Updater(state, ptr[indexMap[i]], condition[i]);
        }
    }
}

template <typename IN, typename OUT, typename STATE, void (*OP)(OUT *, int64_t &, const IN &, const int64_t)>
VECTORIZE_LOOP NO_INLINE void AddUseRowIndexAvg(std::vector<AggregateState *> &rowStates, const size_t aggStateOffset,
    const IN *__restrict ptr, const int64_t *__restrict cntPtr)
{
    const size_t rowCount = rowStates.size();
    if (rowCount > 0) {
#ifdef DEBUG
        if (reinterpret_cast<unsigned long>(ptr) % ARRAY_ALIGNMENT != 0) {
            LogWarn("[addUseRowIndexAvg]: Data pointer NOT aligned");
        }
        if (reinterpret_cast<unsigned long>(cntPtr) % ARRAY_ALIGNMENT != 0) {
            LogWarn("[addUseRowIndexAvg]: Counter pointer NOT aligned");
        }
#endif
        ptr = (const IN *)__builtin_assume_aligned(ptr, ARRAY_ALIGNMENT);
        cntPtr = (const int64_t *)__builtin_assume_aligned(cntPtr, ARRAY_ALIGNMENT);

        for (size_t i = 0; i < rowCount; ++i) {
            STATE *state = STATE::CastState(rowStates[i] + aggStateOffset);
            if (cntPtr[i] >= 0) {
                OP(reinterpret_cast<OUT *>(&state->value), state->count, ptr[i], cntPtr[i]);
            } else {
                state->count = -1;
            }
        }
    }
}

template <typename IN, typename OUT, typename STATE, void (*OP)(OUT *, int64_t &, const IN &, const int64_t)>
VECTORIZE_LOOP NO_INLINE void AddDictUseRowIndexAvg(std::vector<AggregateState *> &rowStates,
    const size_t aggStateOffset, const IN *__restrict ptr, const int64_t *__restrict cntPtr,
    const int32_t *__restrict indexMap)
{
    const size_t rowCount = rowStates.size();
    if (rowCount > 0) {
#ifdef DEBUG
        if (reinterpret_cast<unsigned long>(ptr) % ARRAY_ALIGNMENT != 0) {
            LogWarn("[addDictUseRowIndexAvg]: Data pointer NOT aligned");
        }
        if (reinterpret_cast<unsigned long>(cntPtr) % ARRAY_ALIGNMENT != 0) {
            LogWarn("[addDictUseRowIndexAvg]: Counter pointer NOT aligned");
        }
        if (reinterpret_cast<unsigned long>(indexMap) % ARRAY_ALIGNMENT != 0) {
            LogWarn("[addDictUseRowIndexAvg]: Dictionary Index Map pointer NOT aligned");
        }
#endif
        ptr = (const IN *)__builtin_assume_aligned(ptr, ARRAY_ALIGNMENT);
        cntPtr = (const int64_t *)__builtin_assume_aligned(cntPtr, ARRAY_ALIGNMENT);
        indexMap = (const int32_t *)__builtin_assume_aligned(indexMap, ARRAY_ALIGNMENT);

        for (size_t i = 0; i < rowCount; ++i) {
            STATE *state = STATE::CastState(rowStates[i] + aggStateOffset);
            const auto idx = indexMap[i];
            OP(reinterpret_cast<OUT *>(&state->value), state->count, ptr[idx], cntPtr[idx]);
        }
    }
}

template <typename IN, typename OUT, typename STATE,
    void (*OP)(OUT *, int64_t &, const IN &, const int64_t, const uint8_t &)>
VECTORIZE_LOOP NO_INLINE void AddConditionalUseRowIndexAvg(std::vector<AggregateState *> &rowStates,
    const size_t aggStateOffset, const IN *__restrict ptr, const int64_t *__restrict cntPtr,
    const NullsHelper &condition)
{
    const size_t rowCount = rowStates.size();
    if (rowCount > 0) {
#ifdef DEBUG
        if (reinterpret_cast<unsigned long>(ptr) % ARRAY_ALIGNMENT != 0) {
            LogWarn("[addConditionalUseRowIndexAvg]: Data pointer NOT aligned");
        }
        if (reinterpret_cast<unsigned long>(cntPtr) % ARRAY_ALIGNMENT != 0) {
            LogWarn("[addConditionalUseRowIndexAvg]: Counter pointer NOT aligned");
        }
#endif
        ptr = (const IN *)__builtin_assume_aligned(ptr, ARRAY_ALIGNMENT);
        cntPtr = (const int64_t *)__builtin_assume_aligned(cntPtr, ARRAY_ALIGNMENT);

        for (size_t i = 0; i < rowCount; ++i) {
            STATE *state = STATE::CastState(rowStates[i] + aggStateOffset);
            if (cntPtr[i] > 0 && !static_cast<bool>(condition[i])) {
                OP(reinterpret_cast<OUT *>(&state->value), state->count, ptr[i], cntPtr[i], condition[i]);
            } else {
                state->count = -1;
            }
        }
    }
}

template <typename IN, typename OUT, typename STATE,
    void (*OP)(OUT *, int64_t &, const IN &, const int64_t, const uint8_t &)>
VECTORIZE_LOOP NO_INLINE void AddDictConditionalUseRowIndexAvg(std::vector<AggregateState *> &rowStates,
    const size_t aggStateOffset, const IN *__restrict ptr, const int64_t *__restrict cntPtr,
    const NullsHelper &condition, const int32_t *__restrict indexMap)
{
    const size_t rowCount = rowStates.size();
    if (rowCount > 0) {
#ifdef DEBUG
        if (reinterpret_cast<unsigned long>(ptr) % ARRAY_ALIGNMENT != 0) {
            LogWarn("[addDictConditionalUseRowIndexAvg]: Data pointer NOT aligned");
        }
        if (reinterpret_cast<unsigned long>(cntPtr) % ARRAY_ALIGNMENT != 0) {
            LogWarn("[addDictConditionalUseRowIndexAvg]: Counter pointer NOT aligned");
        }
        if (reinterpret_cast<unsigned long>(indexMap) % ARRAY_ALIGNMENT != 0) {
            LogWarn("[addDictConditionalUseRowIndexAvg]: Dictionary Index Map pointer NOT aligned");
        }
#endif
        ptr = (const IN *)__builtin_assume_aligned(ptr, ARRAY_ALIGNMENT);
        cntPtr = (const int64_t *)__builtin_assume_aligned(cntPtr, ARRAY_ALIGNMENT);
        indexMap = (const int32_t *)__builtin_assume_aligned(indexMap, ARRAY_ALIGNMENT);

        for (size_t i = 0; i < rowCount; ++i) {
            STATE *state = STATE::CastState(rowStates[i] + aggStateOffset);
            const auto idx = indexMap[i];
            OP(reinterpret_cast<OUT *>(&state->value), state->count, ptr[idx], cntPtr[idx], condition[i]);
        }
    }
}

template<typename STATE, void (*OP)(double &, double &, int64_t &, const double &, const int64_t)>
VECTORIZE_LOOP NO_INLINE void AddUseRowIndexMomentStats(std::vector<AggregateState *> &rowStates,
    const size_t aggStateOffset,
    const double *__restrict ptr, const int64_t *__restrict cntPtr)
{
    const size_t rowCount = rowStates.size();
    if (rowCount > 0) {
#ifdef DEBUG
        if (reinterpret_cast<unsigned long>(ptr) % ARRAY_ALIGNMENT != 0) {
            LogWarn("[addUseRowIndexAvg]: Data pointer NOT aligned");
        }
        if (reinterpret_cast<unsigned long>(cntPtr) % ARRAY_ALIGNMENT != 0) {
            LogWarn("[addUseRowIndexAvg]: Counter pointer NOT aligned");
        }
#endif
        ptr = (const double *)__builtin_assume_aligned(ptr, ARRAY_ALIGNMENT);
        cntPtr = (const int64_t *)__builtin_assume_aligned(cntPtr, ARRAY_ALIGNMENT);

        for (size_t i = 0; i < rowCount; ++i) {
            STATE *state = STATE::CastState(rowStates[i] + aggStateOffset);
            if (cntPtr[i] >= 0) {
                OP(state->mean, state->m2, state->count, ptr[i], cntPtr[i]);
            } else {
                state->count = -1;
            }
        }
    }
}

template<typename STATE, void (*OP)(double &, double &, int64_t &, const double &, const int64_t)>
VECTORIZE_LOOP NO_INLINE void AddDictUseRowIndexMomentStats(std::vector<AggregateState *> &rowStates,
    const size_t aggStateOffset, const double *__restrict ptr, const int64_t *__restrict cntPtr,
    const int32_t *__restrict indexMap)
{
    const size_t rowCount = rowStates.size();
    if (rowCount > 0) {
#ifdef DEBUG
        if (reinterpret_cast<unsigned long>(ptr) % ARRAY_ALIGNMENT != 0) {
            LogWarn("[addDictUseRowIndexAvg]: Data pointer NOT aligned");
        }
        if (reinterpret_cast<unsigned long>(cntPtr) % ARRAY_ALIGNMENT != 0) {
            LogWarn("[addDictUseRowIndexAvg]: Counter pointer NOT aligned");
        }
        if (reinterpret_cast<unsigned long>(indexMap) % ARRAY_ALIGNMENT != 0) {
            LogWarn("[addDictUseRowIndexAvg]: Dictionary Index Map pointer NOT aligned");
        }
#endif
        ptr = (const double *)__builtin_assume_aligned(ptr, ARRAY_ALIGNMENT);
        cntPtr = (const int64_t *)__builtin_assume_aligned(cntPtr, ARRAY_ALIGNMENT);
        indexMap = (const int32_t *)__builtin_assume_aligned(indexMap, ARRAY_ALIGNMENT);

        for (size_t i = 0; i < rowCount; ++i) {
            STATE *state = STATE::CastState(rowStates[i] + aggStateOffset);
            const auto idx = indexMap[i];
            OP(state->mean, state->m2, state->count, ptr[idx], cntPtr[idx]);
        }
    }
}

template<typename STATE, void (*OP)(double &, double &, int64_t &, const double &, const int64_t, const uint8_t &)>
VECTORIZE_LOOP NO_INLINE void AddConditionalUseRowIndexMomentStats(std::vector<AggregateState *> &rowStates,
    const size_t aggStateOffset, const double *__restrict ptr, const int64_t *__restrict cntPtr,
    const NullsHelper &condition)
{
    const size_t rowCount = rowStates.size();
    if (rowCount > 0) {
#ifdef DEBUG
        if (reinterpret_cast<unsigned long>(ptr) % ARRAY_ALIGNMENT != 0) {
            LogWarn("[addConditionalUseRowIndexAvg]: Data pointer NOT aligned");
        }
        if (reinterpret_cast<unsigned long>(cntPtr) % ARRAY_ALIGNMENT != 0) {
            LogWarn("[addConditionalUseRowIndexAvg]: Counter pointer NOT aligned");
        }
#endif
        ptr = (const double *)__builtin_assume_aligned(ptr, ARRAY_ALIGNMENT);
        cntPtr = (const int64_t *)__builtin_assume_aligned(cntPtr, ARRAY_ALIGNMENT);

        for (size_t i = 0; i < rowCount; ++i) {
            STATE *state = STATE::CastState(rowStates[i] + aggStateOffset);
            if (cntPtr[i] > 0 && !static_cast<bool>(condition[i])) {
                OP(state->mean, state->m2, state->count, ptr[i], cntPtr[i], condition[i]);
            } else {
                state->count = -1;
            }
        }
    }
}

template<typename STATE, void (*OP)(double &, double &, int64_t &, const double &, const int64_t, const uint8_t &)>
VECTORIZE_LOOP NO_INLINE void AddDictConditionalUseRowIndexMomentStats(std::vector<AggregateState *> &rowStates,
    const size_t aggStateOffset, const double *__restrict ptr, const int64_t *__restrict cntPtr,
    const NullsHelper &condition, const int32_t *__restrict indexMap)
{
    const size_t rowCount = rowStates.size();
    if (rowCount > 0) {
#ifdef DEBUG
        if (reinterpret_cast<unsigned long>(ptr) % ARRAY_ALIGNMENT != 0) {
            LogWarn("[addDictConditionalUseRowIndexAvg]: Data pointer NOT aligned");
        }
        if (reinterpret_cast<unsigned long>(cntPtr) % ARRAY_ALIGNMENT != 0) {
            LogWarn("[addDictConditionalUseRowIndexAvg]: Counter pointer NOT aligned");
        }
        if (reinterpret_cast<unsigned long>(indexMap) % ARRAY_ALIGNMENT != 0) {
            LogWarn("[addDictConditionalUseRowIndexAvg]: Dictionary Index Map pointer NOT aligned");
        }
#endif
        ptr = (const double *)__builtin_assume_aligned(ptr, ARRAY_ALIGNMENT);
        cntPtr = (const int64_t *)__builtin_assume_aligned(cntPtr, ARRAY_ALIGNMENT);
        indexMap = (const int32_t *)__builtin_assume_aligned(indexMap, ARRAY_ALIGNMENT);

        for (size_t i = 0; i < rowCount; ++i) {
            STATE *state = STATE::CastState(rowStates[i] + aggStateOffset);
            const auto idx = indexMap[i];
            OP(state->mean, state->m2, state->count, ptr[idx], cntPtr[idx], condition[i]);
        }
    }
}

template<typename STATE, void (*OP)(double &, double &, double &, double, double, double)>
VECTORIZE_LOOP NO_INLINE void AddUseRowIndexMomentStatsFinal(std::vector<AggregateState *> &rowStates,
    int32_t aggStateOffset, double *__restrict cntPtr, double *__restrict meanPt,
    double *__restrict m2Ptr)
{
    const size_t rowCount = rowStates.size();
    if (rowCount > 0) {
#ifdef DEBUG
        if (reinterpret_cast<unsigned long>(meanPt) % ARRAY_ALIGNMENT != 0) {
            LogWarn("[addUseRowIndexAvg]: Data pointer NOT aligned");
        }
        if (reinterpret_cast<unsigned long>(cntPtr) % ARRAY_ALIGNMENT != 0) {
            LogWarn("[addUseRowIndexAvg]: Counter pointer NOT aligned");
        }
#endif
        meanPt = (double *)__builtin_assume_aligned(meanPt, ARRAY_ALIGNMENT);
        m2Ptr = (double *)__builtin_assume_aligned(m2Ptr, ARRAY_ALIGNMENT);
        cntPtr = (double *)__builtin_assume_aligned(cntPtr, ARRAY_ALIGNMENT);

        for (size_t i = 0; i < rowCount; ++i) {
            STATE *state = STATE::CastState(rowStates[i] + aggStateOffset);
            if (cntPtr[i] >= 0) {
                OP(state->mean, state->m2, state->count, cntPtr[i], meanPt[i], m2Ptr[i]);
            } else {
                state->count = -1;
            }
        }
    }
}

template<typename STATE, void (*OP)(double &, double &, double &, double, double, double)>
VECTORIZE_LOOP NO_INLINE void AddConditionalUseRowIndexMomentStatsFinal(std::vector<AggregateState *> &rowStates,
    const size_t aggStateOffset, double *__restrict cntPtr, double *__restrict meanPt,
    double *__restrict m2Ptr, const NullsHelper &condition)
{
    const size_t rowCount = rowStates.size();
    if (rowCount > 0) {
#ifdef DEBUG
        if (reinterpret_cast<unsigned long>(cntPtr) % ARRAY_ALIGNMENT != 0) {
            LogWarn("[addConditionalUseRowIndexAvg]: Data pointer NOT aligned");
        }
        if (reinterpret_cast<unsigned long>(meanPt) % ARRAY_ALIGNMENT != 0) {
            LogWarn("[addConditionalUseRowIndexAvg]: Counter pointer NOT aligned");
        }
#endif
        meanPt = (double *)__builtin_assume_aligned(meanPt, ARRAY_ALIGNMENT);
        m2Ptr = (double *)__builtin_assume_aligned(m2Ptr, ARRAY_ALIGNMENT);
        cntPtr = (double *)__builtin_assume_aligned(cntPtr, ARRAY_ALIGNMENT);

        for (size_t i = 0; i < rowCount; ++i) {
            STATE *state = STATE::CastState(rowStates[i] + aggStateOffset);
            if (cntPtr[i] > 0 && !static_cast<bool>(condition[i])) {
                OP(state->mean, state->m2, state->count, cntPtr[i], meanPt[i], m2Ptr[i]);
            } else {
                state->count = -1;
            }
        }
    }
}

    template <typename VALUE, void (*UPDATER)(AggregateState *, const VALUE &)>
    VECTORIZE_LOOP NO_INLINE void AddCentralMomentUseRowIndex(std::vector<AggregateState *> &rowStates, const size_t aggStateOffset,
                                                 const VALUE *__restrict ptr)
{
    const size_t rowCount = rowStates.size();
    if (rowCount > 0) {
        ptr = (const VALUE *)__builtin_assume_aligned(ptr, ARRAY_ALIGNMENT);
        for (size_t i = 0; i < rowCount; ++i) {
            UPDATER(rowStates[i] + aggStateOffset, ptr[i]);
        }
    }
}

    template <typename VALUE, void (*UPDATER)(AggregateState *, const VALUE &, const uint8_t &)>
    VECTORIZE_LOOP NO_INLINE void AddCentralMomentConditionalUseRowIndex(std::vector<AggregateState *> &rowStates,
                                                                         const size_t aggStateOffset, const VALUE *__restrict ptr,
                                                                         const NullsHelper &condition)
{
    const size_t rowCount = rowStates.size();
    if (rowCount > 0) {
        ptr = (const VALUE *)__builtin_assume_aligned(ptr, ARRAY_ALIGNMENT);

        for (size_t i = 0; i < rowCount; ++i) {
            AggregateState *state = rowStates[i] + aggStateOffset;
            UPDATER(state, ptr[i], condition[i]);
        }
    }
}

template <typename VALUE, typename STATE, typename FLAG, void (*OP)(uint64_t *, VALUE *, VALUE *, VALUE *, VALUE *, long *, FLAG &, const VALUE & ,
                                     const VALUE &, const VALUE &, const VALUE &, const VALUE &)>
    VECTORIZE_LOOP FAST_MATH NO_INLINE void MergeCentralMomentUseRowIndex(std::vector<AggregateState *> &rowStates, const size_t aggStateOffset,
                                                           const VALUE *__restrict countPtr,
                                                           const VALUE *__restrict m1Ptr, const VALUE *__restrict m2Ptr,
                                                           const VALUE *__restrict m3Ptr, const VALUE *__restrict m4Ptr) {
    const size_t rowCount = rowStates.size();
    if (rowCount > 0) {
        countPtr = (const VALUE *)__builtin_assume_aligned(countPtr, ARRAY_ALIGNMENT);
        m1Ptr = (const VALUE *)__builtin_assume_aligned(m1Ptr, ARRAY_ALIGNMENT);
        m2Ptr = (const VALUE *)__builtin_assume_aligned(m2Ptr, ARRAY_ALIGNMENT);
        m3Ptr = (const VALUE *)__builtin_assume_aligned(m3Ptr, ARRAY_ALIGNMENT);
        m4Ptr = (const VALUE *)__builtin_assume_aligned(m4Ptr, ARRAY_ALIGNMENT);

        for (size_t i = 0; i < rowCount; ++i) {
            STATE *state = STATE::CastState(rowStates[i] + aggStateOffset);
            auto &centralMomentState = state->centralMomentState;
            auto count = reinterpret_cast<uint64_t *>(&centralMomentState.count);
            auto m1 = reinterpret_cast<VALUE *>(&centralMomentState.centralMoment1);
            auto m2 = reinterpret_cast<VALUE *>(&centralMomentState.centralMoment2);
            auto m3 = reinterpret_cast<VALUE *>(&centralMomentState.centralMoment3);
            auto m4 = reinterpret_cast<VALUE *>(&centralMomentState.centralMoment4);
            auto momentOrder = reinterpret_cast<long *>(&centralMomentState.momentOrder);
            if (countPtr[i] > 0) {
                OP(count, m1, m2, m3, m4, momentOrder, state->valueState, countPtr[i],
                   m1Ptr[i], m2Ptr[i], m3Ptr[i], m4Ptr[i]);
            } else if (countPtr[i] < 0) {
                state->valueState = AggValueState::OVERFLOWED;
                break;
            }
        }
    }
}

template <typename VALUE, typename STATE, typename FLAG, void (*OP)(uint64_t *, VALUE *, VALUE *, VALUE *, VALUE *, long *, FLAG &, const VALUE & ,
                                     const VALUE &, const VALUE &, const VALUE &, const VALUE &)>
    VECTORIZE_LOOP FAST_MATH NO_INLINE void MergeCentralMomentWithCountLongUseRowIndex(std::vector<AggregateState *> &rowStates, const size_t aggStateOffset,
                                                           const long *__restrict countPtr,
                                                           const VALUE *__restrict m1Ptr, const VALUE *__restrict m2Ptr,
                                                           const VALUE *__restrict m3Ptr, const VALUE *__restrict m4Ptr) {
    const size_t rowCount = rowStates.size();
    if (rowCount > 0) {
        countPtr = (const long *)__builtin_assume_aligned(countPtr, ARRAY_ALIGNMENT);
        m1Ptr = (const VALUE *)__builtin_assume_aligned(m1Ptr, ARRAY_ALIGNMENT);
        m2Ptr = (const VALUE *)__builtin_assume_aligned(m2Ptr, ARRAY_ALIGNMENT);
        m3Ptr = (const VALUE *)__builtin_assume_aligned(m3Ptr, ARRAY_ALIGNMENT);
        m4Ptr = (const VALUE *)__builtin_assume_aligned(m4Ptr, ARRAY_ALIGNMENT);

        for (size_t i = 0; i < rowCount; ++i) {
            STATE *state = STATE::CastState(rowStates[i] + aggStateOffset);
            auto &centralMomentState = state->centralMomentState;
            auto count = reinterpret_cast<uint64_t *>(&centralMomentState.count);
            auto m1 = reinterpret_cast<VALUE *>(&centralMomentState.centralMoment1);
            auto m2 = reinterpret_cast<VALUE *>(&centralMomentState.centralMoment2);
            auto m3 = reinterpret_cast<VALUE *>(&centralMomentState.centralMoment3);
            auto m4 = reinterpret_cast<VALUE *>(&centralMomentState.centralMoment4);
            auto momentOrder = reinterpret_cast<long *>(&centralMomentState.momentOrder);
            double countValue = countPtr[i];
            if (countPtr[i] > 0) {
                OP(count, m1, m2, m3, m4, momentOrder, state->valueState, countValue,
                   m1Ptr[i], m2Ptr[i], m3Ptr[i], m4Ptr[i]);
            } else if (countPtr[i] < 0) {
                state->valueState = AggValueState::OVERFLOWED;
                break;
            }
        }
    }
}

template <typename VALUE, typename STATE, typename FLAG, void (*OP)(uint64_t *, VALUE *, VALUE *, VALUE *, VALUE *, long *, FLAG &, const VALUE & ,
                                         const VALUE &, const VALUE &, const VALUE &, const VALUE &, const uint8_t &)>
VECTORIZE_LOOP FAST_MATH NO_INLINE void MergeCentralMomentConditionalWithCountLongUseRowIndex(std::vector<AggregateState *> &rowStates, const size_t aggStateOffset,
                                                           const long *__restrict countPtr,
                                                           const VALUE *__restrict m1Ptr, const VALUE *__restrict m2Ptr,
                                                           const VALUE *__restrict m3Ptr, const VALUE *__restrict m4Ptr,
                                                           const NullsHelper &condition) {
    const size_t rowCount = rowStates.size();
    if (rowCount > 0) {
        countPtr = (const long *)__builtin_assume_aligned(countPtr, ARRAY_ALIGNMENT);
        m1Ptr = (const VALUE *)__builtin_assume_aligned(m1Ptr, ARRAY_ALIGNMENT);
        m2Ptr = (const VALUE *)__builtin_assume_aligned(m2Ptr, ARRAY_ALIGNMENT);
        m3Ptr = (const VALUE *)__builtin_assume_aligned(m3Ptr, ARRAY_ALIGNMENT);
        m4Ptr = (const VALUE *)__builtin_assume_aligned(m4Ptr, ARRAY_ALIGNMENT);

        for (size_t i = 0; i < rowCount; ++i) {
            STATE *state = STATE::CastState(rowStates[i] + aggStateOffset);
            auto &centralMomentState = state->centralMomentState;
            auto count = reinterpret_cast<uint64_t *>(&centralMomentState.count);
            auto m1 = reinterpret_cast<VALUE *>(&centralMomentState.centralMoment1);
            auto m2 = reinterpret_cast<VALUE *>(&centralMomentState.centralMoment2);
            auto m3 = reinterpret_cast<VALUE *>(&centralMomentState.centralMoment3);
            auto m4 = reinterpret_cast<VALUE *>(&centralMomentState.centralMoment4);
            auto momentOrder = reinterpret_cast<long *>(&centralMomentState.momentOrder);
            if (countPtr[i] > 0) {
                OP(count, m1, m2, m3, m4, momentOrder, state->valueState, countPtr[i],
                   m1Ptr[i], m2Ptr[i], m3Ptr[i], m4Ptr[i], condition[i]);
            } else if (countPtr[i] < 0) {
                state->valueState = AggValueState::OVERFLOWED;
                break;
            }
        }
    }
}

template <typename VALUE, typename STATE, typename FLAG, void (*OP)(uint64_t *, VALUE *, VALUE *, VALUE *, VALUE *, long *, FLAG &, const VALUE & ,
                                         const VALUE &, const VALUE &, const VALUE &, const VALUE &, const uint8_t &)>
VECTORIZE_LOOP FAST_MATH NO_INLINE void MergeCentralMomentConditionalUseRowIndex(std::vector<AggregateState *> &rowStates, const size_t aggStateOffset,
                                                           const VALUE *__restrict countPtr,
                                                           const VALUE *__restrict m1Ptr, const VALUE *__restrict m2Ptr,
                                                           const VALUE *__restrict m3Ptr, const VALUE *__restrict m4Ptr,
                                                           const NullsHelper &condition) {
    const size_t rowCount = rowStates.size();
    if (rowCount > 0) {
        countPtr = (const VALUE *)__builtin_assume_aligned(countPtr, ARRAY_ALIGNMENT);
        m1Ptr = (const VALUE *)__builtin_assume_aligned(m1Ptr, ARRAY_ALIGNMENT);
        m2Ptr = (const VALUE *)__builtin_assume_aligned(m2Ptr, ARRAY_ALIGNMENT);
        m3Ptr = (const VALUE *)__builtin_assume_aligned(m3Ptr, ARRAY_ALIGNMENT);
        m4Ptr = (const VALUE *)__builtin_assume_aligned(m4Ptr, ARRAY_ALIGNMENT);

        for (size_t i = 0; i < rowCount; ++i) {
            STATE *state = STATE::CastState(rowStates[i] + aggStateOffset);
            auto &centralMomentState = state->centralMomentState;
            auto count = reinterpret_cast<uint64_t *>(&centralMomentState.count);
            auto m1 = reinterpret_cast<VALUE *>(&centralMomentState.centralMoment1);
            auto m2 = reinterpret_cast<VALUE *>(&centralMomentState.centralMoment2);
            auto m3 = reinterpret_cast<VALUE *>(&centralMomentState.centralMoment3);
            auto m4 = reinterpret_cast<VALUE *>(&centralMomentState.centralMoment4);
            auto momentOrder = reinterpret_cast<long *>(&centralMomentState.momentOrder);
            double countValue = countPtr[i];
            if (countPtr[i] > 0) {
                OP(count, m1, m2, m3, m4, momentOrder, state->valueState, countValue,
                   m1Ptr[i], m2Ptr[i], m3Ptr[i], m4Ptr[i], condition[i]);
            } else if (countPtr[i] < 0) {
                state->valueState = AggValueState::OVERFLOWED;
                break;
            }
        }
    }
}
} // end of namespace op
} // end of namespace omniruntime
