/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Inner supported aggregators source file
 */
#include "aggregator.h"

#include <memory>
#include <string.h>

#include "../../vector/vector_common.h"
#include "../../vector/vector_helper.h"
#include "../../../thirdparty/huawei_secure_c/include/securec.h"
namespace omniruntime {
namespace op {
using namespace omniruntime::vec;

static constexpr AggFunctionByType AGG_SUM_FUNCTIONS[VEC_TYPE_MAX_COUNT] = {
    {   OMNI_VEC_TYPE_NONE, nullptr, nullptr, nullptr, nullptr, nullptr},
    {
        OMNI_VEC_TYPE_INT, SumInsertImpl<IntVector, int64_t>, SumProcessGroupImpl<IntVector, int64_t>,
        SumInitiateImpl<IntVector, int64_t>, SumProcessNonGroupImpl<IntVector, int64_t>, nullptr
    },
    {
        OMNI_VEC_TYPE_LONG, SumInsertImpl<LongVector, int64_t>, SumProcessGroupImpl<LongVector, int64_t>,
        SumInitiateImpl<LongVector, int64_t>, SumProcessNonGroupImpl<LongVector, int64_t>, nullptr
    },
    {
        OMNI_VEC_TYPE_DOUBLE, SumInsertImpl<DoubleVector, double>, SumProcessGroupImpl<DoubleVector, double>,
        SumInitiateImpl<DoubleVector, double>, SumProcessNonGroupImpl<DoubleVector, double>, nullptr
    },
    {OMNI_VEC_TYPE_BOOLEAN, nullptr, nullptr, nullptr, nullptr, nullptr},
    {OMNI_VEC_TYPE_SHORT, nullptr, nullptr, nullptr, nullptr, nullptr},
    {
        OMNI_VEC_TYPE_DECIMAL64, SumInsertImpl<LongVector, Decimal128>, SumProcessGroupImpl<LongVector, Decimal128>,
        SumInitiateImpl<LongVector, Decimal128>, SumProcessNonGroupImpl<LongVector, Decimal128>, nullptr
    },
    {
        OMNI_VEC_TYPE_DECIMAL128, SumInsertImpl<Decimal128Vector, Decimal128>,
        SumProcessGroupImpl<Decimal128Vector, Decimal128>, SumInitiateImpl<Decimal128Vector, Decimal128>,
        SumProcessNonGroupImpl<Decimal128Vector, Decimal128>, nullptr
    },
    {
        OMNI_VEC_TYPE_DATE32, SumInsertImpl<IntVector, int32_t>, SumProcessGroupImpl<IntVector, int32_t>,
        SumInitiateImpl<IntVector, int32_t>, SumProcessNonGroupImpl<IntVector, int32_t>, nullptr
    },
    {OMNI_VEC_TYPE_DATE64, nullptr, nullptr, nullptr, nullptr, nullptr},
    {OMNI_VEC_TYPE_TIME32, nullptr, nullptr, nullptr, nullptr, nullptr},
    {OMNI_VEC_TYPE_TIME64, nullptr, nullptr, nullptr, nullptr, nullptr},
    {OMNI_VEC_TYPE_TIMESTAMP, nullptr, nullptr, nullptr, nullptr, nullptr},
    {OMNI_VEC_TYPE_INTERVAL_MONTHS, nullptr, nullptr, nullptr, nullptr, nullptr},
    {OMNI_VEC_TYPE_INTERVAL_DAY_TIME, nullptr, nullptr, nullptr, nullptr, nullptr},
    {OMNI_VEC_TYPE_VARCHAR, nullptr, nullptr, nullptr, nullptr, nullptr},
    {OMNI_VEC_TYPE_CHAR, nullptr, nullptr, nullptr, nullptr},
    {OMNI_VEC_TYPE_DICTIONARY, SumInsertDictionaryImpl, SumProcessGroupDictionaryImpl, SumInitiateDictionaryImpl, SumProcessNonGroupDictionaryImpl, nullptr},
    {OMNI_VEC_TYPE_CONTAINER, nullptr, nullptr, nullptr, nullptr, nullptr},
};

static constexpr AggFunctionByType AGG_AVG_FUNCTIONS[VEC_TYPE_MAX_COUNT] = {
    {OMNI_VEC_TYPE_NONE, nullptr, nullptr, nullptr, nullptr, nullptr},
    {
        OMNI_VEC_TYPE_INT, AvgInsertImpl<IntVector, double>, AvgProcessGroupImpl<IntVector, double>,
        AvgInitiateImpl<IntVector, double>, AvgProcessNonGroupImpl<IntVector, double>, AvgEvaluateImpl<double>
    },
    {
        OMNI_VEC_TYPE_LONG, AvgInsertImpl<LongVector, double>, AvgProcessGroupImpl<LongVector, double>,
        AvgInitiateImpl<LongVector, double>, AvgProcessNonGroupImpl<LongVector, double>, AvgEvaluateImpl<double>
    },
    {
        OMNI_VEC_TYPE_DOUBLE, AvgInsertImpl<DoubleVector, double>, AvgProcessGroupImpl<DoubleVector, double>,
        AvgInitiateImpl<DoubleVector, double>, AvgProcessNonGroupImpl<DoubleVector, double>, AvgEvaluateImpl<double>
    },
    {OMNI_VEC_TYPE_BOOLEAN, nullptr, nullptr, nullptr, nullptr},
    {OMNI_VEC_TYPE_SHORT, nullptr, nullptr, nullptr, nullptr},
    {
        OMNI_VEC_TYPE_DECIMAL64, AvgInsertImpl<LongVector, double>, AvgProcessGroupImpl<LongVector, double>,
        AvgInitiateImpl<LongVector, double>, AvgProcessNonGroupImpl<LongVector, double>, AvgEvaluateImpl<double>
    },
    {
        OMNI_VEC_TYPE_DECIMAL128, AvgInsertImpl<Decimal128Vector, Decimal128>, AvgProcessGroupImpl<Decimal128Vector, Decimal128>,
        AvgInitiateImpl<Decimal128Vector, Decimal128>, AvgProcessNonGroupImpl<Decimal128Vector, Decimal128>, AvgEvaluateImpl<Decimal128>
    },
    {
        OMNI_VEC_TYPE_DATE32, AvgInsertImpl<IntVector, double>, AvgProcessGroupImpl<IntVector, double>,
        AvgInitiateImpl<IntVector, double>, AvgProcessNonGroupImpl<IntVector, double>, AvgEvaluateImpl<double>
    },
    {OMNI_VEC_TYPE_DATE64, nullptr, nullptr, nullptr, nullptr, nullptr},
    {OMNI_VEC_TYPE_TIME32, nullptr, nullptr, nullptr, nullptr, nullptr},
    {OMNI_VEC_TYPE_TIME64, nullptr, nullptr, nullptr, nullptr, nullptr},
    {OMNI_VEC_TYPE_TIMESTAMP, nullptr, nullptr, nullptr, nullptr, nullptr},
    {OMNI_VEC_TYPE_INTERVAL_MONTHS, nullptr, nullptr, nullptr, nullptr, nullptr},
    {OMNI_VEC_TYPE_INTERVAL_DAY_TIME, nullptr, nullptr, nullptr, nullptr, nullptr},
    {OMNI_VEC_TYPE_VARCHAR, nullptr, nullptr, nullptr, nullptr, nullptr},
    {OMNI_VEC_TYPE_CHAR, nullptr, nullptr, nullptr, nullptr},
    {OMNI_VEC_TYPE_DICTIONARY, AvgInsertDictionaryImpl, AvgProcessGroupDictionaryImpl, AvgInitiateDictionaryImpl, AvgProcessNonGroupDictionaryImpl, nullptr},
    {OMNI_VEC_TYPE_CONTAINER, AvgInsertContainerImpl, AvgProcessGroupContainerImpl, nullptr, nullptr, nullptr},
};

static constexpr AggFunctionByType AGG_MIN_FUNCTIONS[VEC_TYPE_MAX_COUNT] = {
    {OMNI_VEC_TYPE_NONE, nullptr, nullptr, nullptr, nullptr, nullptr},
    {
        OMNI_VEC_TYPE_INT, MinInsertImpl<IntVector, int32_t>, MinProcessGroupImpl<IntVector, int32_t>,
        MinInitiateImpl<IntVector, int32_t>, MinProcessNonGroupImpl<IntVector, int32_t>, nullptr
    },
    {
        OMNI_VEC_TYPE_LONG, MinInsertImpl<LongVector, int64_t>, MinProcessGroupImpl<LongVector, int64_t>,
        MinInitiateImpl<LongVector, int64_t>, MinProcessNonGroupImpl<LongVector, int64_t>, nullptr
    },
    {
        OMNI_VEC_TYPE_DOUBLE, MinInsertImpl<DoubleVector, double>, MinProcessGroupImpl<DoubleVector, double>,
        MinInitiateImpl<DoubleVector, double>, MinProcessNonGroupImpl<DoubleVector, double>, nullptr
    },
    {OMNI_VEC_TYPE_BOOLEAN, nullptr, nullptr, nullptr, nullptr, nullptr},
    {OMNI_VEC_TYPE_SHORT, nullptr, nullptr, nullptr, nullptr, nullptr},
    {
        OMNI_VEC_TYPE_DECIMAL64, MinInsertImpl<LongVector, int64_t>, MinProcessGroupImpl<LongVector, int64_t>,
        MinInitiateImpl<LongVector, int64_t>, MinProcessNonGroupImpl<LongVector, int64_t>, nullptr
    },
    {
        OMNI_VEC_TYPE_DECIMAL128, MinInsertImpl<Decimal128Vector, Decimal128>,
        MinProcessGroupImpl<Decimal128Vector, Decimal128>, MinInitiateImpl<Decimal128Vector, Decimal128>,
        MinProcessNonGroupImpl<Decimal128Vector, Decimal128>, nullptr
    },
    {
        OMNI_VEC_TYPE_DATE32, MinInsertImpl<IntVector, int32_t>, MinProcessGroupImpl<IntVector, int32_t>,
        MinInitiateImpl<IntVector, int32_t>, MinProcessNonGroupImpl<IntVector, int32_t>, nullptr
    },
    {OMNI_VEC_TYPE_DATE64, nullptr, nullptr, nullptr, nullptr, nullptr},
    {OMNI_VEC_TYPE_TIME32, nullptr, nullptr, nullptr, nullptr, nullptr},
    {OMNI_VEC_TYPE_TIME64, nullptr, nullptr, nullptr, nullptr, nullptr},
    {OMNI_VEC_TYPE_TIMESTAMP, nullptr, nullptr, nullptr, nullptr, nullptr},
    {OMNI_VEC_TYPE_INTERVAL_MONTHS, nullptr, nullptr, nullptr, nullptr, nullptr},
    {OMNI_VEC_TYPE_INTERVAL_DAY_TIME, nullptr, nullptr, nullptr, nullptr, nullptr},
    {
        OMNI_VEC_TYPE_VARCHAR, MinInsertVarcharImpl, MinProcessGroupVarcharImpl, MinInitiateVarcharImpl,
        MinProcessNonGroupVarcharImpl, MinEvaluateVarcharImpl
    },
    {
        OMNI_VEC_TYPE_CHAR, MinInsertVarcharImpl, MinProcessGroupVarcharImpl, MinInitiateVarcharImpl,
        MinProcessNonGroupVarcharImpl
    },
    {OMNI_VEC_TYPE_DICTIONARY, MinInsertDictionaryImpl, MinProcessGroupDictionaryImpl, MinInitiateDictionaryImpl, MinProcessNonGroupDictionaryImpl, nullptr},
    {OMNI_VEC_TYPE_CONTAINER, nullptr, nullptr, nullptr, nullptr, nullptr},
};

static constexpr AggFunctionByType AGG_MAX_FUNCTIONS[VEC_TYPE_MAX_COUNT] = {
    {OMNI_VEC_TYPE_NONE, nullptr, nullptr, nullptr, nullptr, nullptr},
    {
        OMNI_VEC_TYPE_INT, MaxInsertImpl<IntVector, int32_t>, MaxProcessGroupImpl<IntVector, int32_t>,
        MaxInitiateImpl<IntVector, int32_t>, MaxProcessNonGroupImpl<IntVector, int32_t>, nullptr
    },
    {
        OMNI_VEC_TYPE_LONG, MaxInsertImpl<LongVector, int64_t>, MaxProcessGroupImpl<LongVector, int64_t>,
        MaxInitiateImpl<LongVector, int64_t>, MaxProcessNonGroupImpl<LongVector, int64_t>, nullptr
    },
    {
        OMNI_VEC_TYPE_DOUBLE, MaxInsertImpl<DoubleVector, double>, MaxProcessGroupImpl<DoubleVector, double>,
        MaxInitiateImpl<DoubleVector, double>, MaxProcessNonGroupImpl<DoubleVector, double>, nullptr
    },
    {OMNI_VEC_TYPE_BOOLEAN, nullptr, nullptr, nullptr, nullptr, nullptr},
    {OMNI_VEC_TYPE_SHORT, nullptr, nullptr, nullptr, nullptr, nullptr},
    {
        OMNI_VEC_TYPE_DECIMAL64, MaxInsertImpl<LongVector, int64_t>, MaxProcessGroupImpl<LongVector, int64_t>,
        MaxInitiateImpl<LongVector, int64_t>, MaxProcessNonGroupImpl<LongVector, int64_t>, nullptr
    },
    {
        OMNI_VEC_TYPE_DECIMAL128, MaxInsertImpl<Decimal128Vector, Decimal128>,
        MaxProcessGroupImpl<Decimal128Vector, Decimal128>, MaxInitiateImpl<Decimal128Vector, Decimal128>,
        MaxProcessNonGroupImpl<Decimal128Vector, Decimal128>, nullptr
    },
    {
        OMNI_VEC_TYPE_DATE32, MaxInsertImpl<IntVector, int32_t>, MaxProcessGroupImpl<IntVector, int32_t>,
        MaxInitiateImpl<IntVector, int32_t>, MaxProcessNonGroupImpl<IntVector, int32_t>, nullptr
    },
    {OMNI_VEC_TYPE_DATE64, nullptr, nullptr, nullptr, nullptr, nullptr},
    {OMNI_VEC_TYPE_TIME32, nullptr, nullptr, nullptr, nullptr, nullptr},
    {OMNI_VEC_TYPE_TIME64, nullptr, nullptr, nullptr, nullptr, nullptr},
    {OMNI_VEC_TYPE_TIMESTAMP, nullptr, nullptr, nullptr, nullptr, nullptr},
    {OMNI_VEC_TYPE_INTERVAL_MONTHS, nullptr, nullptr, nullptr, nullptr, nullptr},
    {OMNI_VEC_TYPE_INTERVAL_DAY_TIME, nullptr, nullptr, nullptr, nullptr, nullptr},
    {
        OMNI_VEC_TYPE_VARCHAR, MaxInsertVarcharImpl, MaxProcessGroupVarcharImpl, MaxInitiateVarcharImpl,
        MaxProcessNonGroupVarcharImpl, MaxEvaluateVarcharImpl
    },
    {
        OMNI_VEC_TYPE_CHAR, MaxInsertVarcharImpl, MaxProcessGroupVarcharImpl, MaxInitiateVarcharImpl,
        MaxProcessNonGroupVarcharImpl
    },
    {
        OMNI_VEC_TYPE_DICTIONARY, MaxInsertDictionaryImpl, MaxProcessGroupDictionaryImpl, MaxInitiateDictionaryImpl,
        MaxProcessNonGroupDictionaryImpl, nullptr
     },
    {OMNI_VEC_TYPE_CONTAINER, nullptr, nullptr, nullptr, nullptr, nullptr},
};

template <typename V, typename D>
void SumInsertImpl(GroupBySlot &groupSlot, Vector *colPtr, uint32_t offset, std::unique_ptr<ExecutionContext> &context)
{
    if (UNLIKELY(colPtr->IsValueNull(offset))) {
        return;
    }
    auto curVal = (static_cast<V *>(colPtr))->GetValue(offset);
    int32_t len = sizeof(D);
    auto ptr = context->getArena()->Allocate(len);
    *reinterpret_cast<D *>(ptr) = curVal;
    groupSlot.val = ptr;
}

void SumInsertDictionaryImpl(GroupBySlot &groupBySlot, Vector *colPtr, uint32_t offset,
    std::unique_ptr<ExecutionContext> &context)
{
    auto dictType = static_cast<DictionaryVector *>(colPtr)->ExtractDictionaryTypeId();
    int32_t originalOffset;
    Vector *originalVector = VectorHelper::ExpandVectorAndIndex(colPtr, offset, originalOffset);
    AGG_SUM_FUNCTIONS[dictType].insertFunc(groupBySlot, originalVector, originalOffset, context);
}

template <typename V, typename D>
void SumProcessGroupImpl(GroupBySlot &groupSlot, Vector *colPtr, uint32_t offset,
    std::unique_ptr<ExecutionContext> &context)
{
    if (UNLIKELY(colPtr->IsValueNull(offset))) {
        return;
    }
    if (groupSlot.val == nullptr) {
        SumInsertImpl<V, D>(groupSlot, colPtr, offset, context);
        return;
    }
    *(static_cast<D *>(groupSlot.val)) += (static_cast<V *>(colPtr))->GetValue(offset);
}

void SumProcessGroupDictionaryImpl(GroupBySlot &groupBySlot, Vector *colPtr, uint32_t offset,
    std::unique_ptr<ExecutionContext> &context)
{
    auto dictType = static_cast<DictionaryVector *>(colPtr)->ExtractDictionaryTypeId();
    int32_t originalOffset;
    Vector *originalVector = VectorHelper::ExpandVectorAndIndex(colPtr, offset, originalOffset);
    AGG_SUM_FUNCTIONS[dictType].processGroupFunc(groupBySlot, originalVector, originalOffset, context);
}

template <typename V, typename D>
void SumInitiateImpl(GroupBySlot &groupSlot, Vector *colPtr, uint32_t offset,
                     std::unique_ptr<ExecutionContext> &context)
{
    if (UNLIKELY(colPtr->IsValueNull(offset))) {
        return;
    }

    auto curVal = (static_cast<V *>(colPtr))->GetValue(offset);
    auto ptr = context->getArena()->Allocate(sizeof(D));
    *reinterpret_cast<D *>(ptr) = curVal;
    groupSlot.val = ptr;
}

void SumInitiateDictionaryImpl(GroupBySlot &groupSlot, Vector *colPtr, uint32_t offset,
                               std::unique_ptr<ExecutionContext> &context)
{
    auto dictType = static_cast<DictionaryVector *>(colPtr)->ExtractDictionaryTypeId();
    int32_t originalOffset;
    Vector *originalVector = VectorHelper::ExpandVectorAndIndex(colPtr, offset, originalOffset);
    AGG_SUM_FUNCTIONS[dictType].initiateFunc(groupSlot, originalVector, originalOffset, context);
}

template <typename V, typename D>
void SumProcessNonGroupImpl(GroupBySlot &groupSlot, Vector *colPtr, uint32_t offset,
                            std::unique_ptr<ExecutionContext> &context)
{
    if (UNLIKELY(colPtr->IsValueNull(offset))) {
        return;
    }

    if (groupSlot.val == nullptr) {
        SumInitiateImpl<V, D>(groupSlot, colPtr, offset, context);
        return;
    }
    *(static_cast<D *>(groupSlot.val)) += (static_cast<V *>(colPtr))->GetValue(offset);
}

void SumProcessNonGroupDictionaryImpl(GroupBySlot &groupSlot, Vector *colPtr, uint32_t offset,
                                      std::unique_ptr<ExecutionContext> &context)
{
    auto dictType = static_cast<DictionaryVector *>(colPtr)->ExtractDictionaryTypeId();
    int32_t originalOffset;
    Vector *originalVector = VectorHelper::ExpandVectorAndIndex(colPtr, offset, originalOffset);
    AGG_SUM_FUNCTIONS[dictType].processNonGroupFunc(groupSlot, originalVector, originalOffset, context);
}

template <typename V, typename D>
void AvgInsertImpl(GroupBySlot &groupSlot, Vector *colPtr, uint32_t offset, std::unique_ptr<ExecutionContext> &context)
{
    if (UNLIKELY(colPtr->IsValueNull(offset))) {
        return;
    }

    auto rowVal = (static_cast<V *>(colPtr))->GetValue(offset);
    auto len = sizeof(D);
    auto ptr = context->getArena()->Allocate(len);
    *reinterpret_cast<D *>(ptr) = rowVal;
    groupSlot.avgVal = ptr;
    groupSlot.avgCnt = 1;
}

void AvgInsertContainerImpl(GroupBySlot &groupSlot, Vector *colPtr, uint32_t offset,
    std::unique_ptr<ExecutionContext> &context)
{
    auto containerVector = static_cast<ContainerVector *>(colPtr);
    DoubleVector *avgValVector = reinterpret_cast<DoubleVector *>(containerVector->GetValue(0));
    LongVector *avgCountVector = reinterpret_cast<LongVector *>(containerVector->GetValue(1));
    if (UNLIKELY(containerVector->IsValueNull(offset))) {
        return;
    }
    double avgVal = avgValVector->GetValue(offset);
    int64_t avgCnt = avgCountVector->GetValue(offset);
    if (avgCnt == 0) {
        // Fixme use error code
        LogError("Divisor should not be zero! Offset = %d", offset);
    }
    groupSlot.avgVal = std::make_unique<double>(avgVal * avgCnt / avgCnt).release();
    groupSlot.avgCnt = avgCnt;
}

void AvgInsertDictionaryImpl(GroupBySlot &groupBySlot, Vector *colPtr, uint32_t offset,
    std::unique_ptr<ExecutionContext> &context)
{
    auto dictType = static_cast<DictionaryVector *>(colPtr)->ExtractDictionaryTypeId();
    int32_t originalOffset;
    Vector *originalVector = VectorHelper::ExpandVectorAndIndex(colPtr, offset, originalOffset);
    AGG_AVG_FUNCTIONS[dictType].insertFunc(groupBySlot, originalVector, originalOffset, context);
}

template <typename V, typename D>
void AvgProcessGroupImpl(GroupBySlot &groupSlot, Vector *colPtr, uint32_t offset,
    std::unique_ptr<ExecutionContext> &context)
{
    if (UNLIKELY(colPtr->IsValueNull(offset))) {
        return;
    }
    if (groupSlot.val == nullptr) {
        AvgInsertImpl<V, D>(groupSlot, colPtr, offset, context);
        return;
    }
    auto currentVal = static_cast<D *>(groupSlot.avgVal);
    *reinterpret_cast<D *>(groupSlot.avgVal) = (static_cast<V *>(colPtr))->GetValue(offset) + *currentVal;
    ++groupSlot.avgCnt;
}

void AvgProcessGroupContainerImpl(GroupBySlot &groupSlot, Vector *colPtr, uint32_t offset,
    std::unique_ptr<ExecutionContext> &context)
{
    if (UNLIKELY(colPtr->IsValueNull(offset))) {
        return;
    }
    if (groupSlot.val == nullptr) {
        AvgInsertContainerImpl(groupSlot, colPtr, offset, context);
        return;
    }
    auto containerVector = static_cast<ContainerVector *>(colPtr);
    DoubleVector *avgValVector = reinterpret_cast<DoubleVector *>(containerVector->GetValue(0));
    LongVector *avgCountVector = reinterpret_cast<LongVector *>(containerVector->GetValue(1));
    if (UNLIKELY(avgValVector->IsValueNull(offset) || avgCountVector->IsValueNull(offset))) {
        return;
    }
    double avgVal = avgValVector->GetValue(offset);
    int64_t avgCnt = avgCountVector->GetValue(offset);
    auto currentVal = static_cast<double *>(groupSlot.avgVal);
    auto currentCnt = static_cast<int64_t>(groupSlot.avgCnt);
    if (avgCnt == 0) {
        // Fixme use error code
        LogError("Divisor should not be zero! Offset = %d", offset);
    }
    groupSlot.avgCnt += avgCnt;
    *currentVal = (avgVal * avgCnt + *currentVal * currentCnt) / groupSlot.avgCnt;
}

void AvgProcessGroupDictionaryImpl(GroupBySlot &groupBySlot, Vector *colPtr, uint32_t offset,
    std::unique_ptr<ExecutionContext> &context)
{
    auto dictType = static_cast<DictionaryVector *>(colPtr)->ExtractDictionaryTypeId();
    int32_t originalOffset;
    Vector *originalVector = VectorHelper::ExpandVectorAndIndex(colPtr, offset, originalOffset);
    AGG_AVG_FUNCTIONS[dictType].processGroupFunc(groupBySlot, originalVector, originalOffset, context);
}

template <typename V, typename D>
void AvgInitiateImpl(GroupBySlot &groupSlot, Vector *colPtr, uint32_t offset,
                     std::unique_ptr<ExecutionContext> &context)
{
    if (UNLIKELY(colPtr->IsValueNull(offset))) {
        return;
    }

    auto rowVal = (static_cast<V *>(colPtr))->GetValue(offset);
    auto ptr = context->getArena()->Allocate(sizeof(D));
    *reinterpret_cast<D *>(ptr) = rowVal;
    groupSlot.avgVal = ptr;
    groupSlot.avgCnt = 1;
}

void AvgInitiateDictionaryImpl(GroupBySlot &groupSlot, Vector *colPtr, uint32_t offset,
                               std::unique_ptr<ExecutionContext> &context)
{
    auto dictType = static_cast<DictionaryVector *>(colPtr)->ExtractDictionaryTypeId();
    int32_t originalOffset;
    Vector *originalVector = VectorHelper::ExpandVectorAndIndex(colPtr, offset, originalOffset);
    AGG_AVG_FUNCTIONS[dictType].initiateFunc(groupSlot, originalVector, originalOffset, context);
}

template <typename V, typename D>
void AvgProcessNonGroupImpl(GroupBySlot &groupSlot, Vector *colPtr, uint32_t offset,
                            std::unique_ptr<ExecutionContext> &context)
{
    if (UNLIKELY(colPtr->IsValueNull(offset))) {
        return;
    }

    if (groupSlot.val == nullptr) {
        AvgInitiateImpl<V, D>(groupSlot, colPtr, offset, context);
        return;
    }
    auto currentVal = static_cast<D *>(groupSlot.avgVal);
    *currentVal += (static_cast<V *>(colPtr))->GetValue(offset);
    ++groupSlot.avgCnt;
}

void AvgProcessNonGroupDictionaryImpl(GroupBySlot &groupSlot, Vector *colPtr, uint32_t offset,
                                      std::unique_ptr<ExecutionContext> &context)
{
    auto dictType = static_cast<DictionaryVector *>(colPtr)->ExtractDictionaryTypeId();
    int32_t originalOffset;
    Vector *originalVector = VectorHelper::ExpandVectorAndIndex(colPtr, offset, originalOffset);
    AGG_AVG_FUNCTIONS[dictType].processNonGroupFunc(groupSlot, originalVector, originalOffset, context);
}

template <typename D>
void *AvgEvaluateImpl(const GroupBySlot &groupBySlot, std::unique_ptr<ExecutionContext> &context)
{
    if (groupBySlot.val == nullptr) {
        return nullptr;
    }
    if (groupBySlot.avgCnt <= 0) {
        LogError("Divisor has to be larger than 0!");
        return nullptr;
    }
    auto currentVal = static_cast<D *>(groupBySlot.avgVal);
    auto ptr = context->getArena()->Allocate(sizeof(D));
    auto finalState = reinterpret_cast<D *>(ptr);
    *finalState = *currentVal / groupBySlot.avgCnt;
    return finalState;
}

template <typename V, typename D>
void MinInsertImpl(GroupBySlot &groupSlot, Vector *colPtr, uint32_t offset, std::unique_ptr<ExecutionContext> &context)
{
    if (UNLIKELY(colPtr->IsValueNull(offset))) {
        return;
    }
    auto rowVal = static_cast<V *>(colPtr)->GetValue(offset);
    int32_t len = sizeof(D);
    auto ptr = context->getArena()->Allocate(len);
    *reinterpret_cast<D *>(ptr) = rowVal;
    groupSlot.val = ptr;
}

void MinInsertVarcharImpl(GroupBySlot &groupSlot, Vector *colPtr, uint32_t offset,
    std::unique_ptr<ExecutionContext> &context)
{
    if (UNLIKELY(colPtr->IsValueNull(offset))) {
        return;
    }
    uint8_t *data = nullptr;
    int valLen = static_cast<VarcharVector *>(colPtr)->GetValue(offset, &data);
    uint8_t *state = context->getArena()->Allocate(valLen);
    auto err = memcpy_s(state, valLen, data, valLen);
    if (err != EOK) {
        LogError("set data failed in variable vector. %d", err);
    }
    groupSlot.strVal = state;
    groupSlot.strLen = valLen;
}

void MinInsertDictionaryImpl(GroupBySlot &groupBySlot, Vector *colPtr, uint32_t offset,
    std::unique_ptr<ExecutionContext> &context)
{
    auto dictType = static_cast<DictionaryVector *>(colPtr)->ExtractDictionaryTypeId();
    int32_t originalOffset;
    Vector *originalVector = VectorHelper::ExpandVectorAndIndex(colPtr, offset, originalOffset);
    AGG_MIN_FUNCTIONS[dictType].insertFunc(groupBySlot, originalVector, originalOffset, context);
}

template <typename V, typename D>
void MinProcessGroupImpl(GroupBySlot &groupSlot, Vector *colPtr, uint32_t offset,
    std::unique_ptr<ExecutionContext> &context)
{
    if (UNLIKELY(colPtr->IsValueNull(offset))) {
        return;
    }
    if (groupSlot.val == nullptr) {
        MinInsertImpl<V, D>(groupSlot, colPtr, offset, context);
        return;
    }
    auto rowVal = (static_cast<V *>(colPtr))->GetValue(offset);
    auto leftVal = static_cast<D *>(groupSlot.val);
    *leftVal = (Compare(*leftVal, rowVal) == -1) ? *leftVal : rowVal;
}

void MinProcessGroupVarcharImpl(GroupBySlot &groupSlot, Vector *colPtr, uint32_t offset,
    std::unique_ptr<ExecutionContext> &context)
{
    if (UNLIKELY(colPtr->IsValueNull(offset))) {
        return;
    }
    if (groupSlot.val == nullptr) {
        MinInsertVarcharImpl(groupSlot, colPtr, offset, context);
        return;
    }
    uint8_t *rowVal = nullptr;
    int valLen = (static_cast<VarcharVector *>(colPtr))->GetValue(offset, &rowVal);
    auto leftVal = reinterpret_cast<char *>(groupSlot.strVal);
    if (memcmp(leftVal, (char *)rowVal, std::min(valLen, groupSlot.strLen)) > 0) {
        auto err = memcpy_s(leftVal, valLen, rowVal, valLen);
        if (err != EOK) {
            LogError("set data failed in variable vector. %d", err);
        }
    }
    return;
}

void MinProcessGroupDictionaryImpl(GroupBySlot &groupBySlot, Vector *colPtr, uint32_t offset,
    std::unique_ptr<ExecutionContext> &context)
{
    auto dictType = static_cast<DictionaryVector *>(colPtr)->ExtractDictionaryTypeId();
    int32_t originalOffset;
    Vector *originalVector = VectorHelper::ExpandVectorAndIndex(colPtr, offset, originalOffset);
    AGG_MIN_FUNCTIONS[dictType].processGroupFunc(groupBySlot, originalVector, originalOffset, context);
}

template <typename V, typename D>
void MinInitiateImpl(GroupBySlot &groupSlot, Vector *colPtr, uint32_t offset,
                     std::unique_ptr<ExecutionContext> &context)
{
    if (UNLIKELY(colPtr->IsValueNull(offset))) {
        return;
    }

    auto curVal = (static_cast<V *>(colPtr))->GetValue(offset);
    auto ptr = context->getArena()->Allocate(sizeof(D));
    *reinterpret_cast<D *>(ptr) = curVal;
    groupSlot.val = ptr;
}

void MinInitiateVarcharImpl(GroupBySlot &groupSlot, Vector *colPtr, uint32_t offset,
                            std::unique_ptr<ExecutionContext> &context)
{
    if (UNLIKELY(colPtr->IsValueNull(offset))) {
        return;
    }

    uint8_t *data = nullptr;
    int valLen = static_cast<VarcharVector *>(colPtr)->GetValue(offset, &data);
    uint8_t *state = context->getArena()->Allocate(valLen);
    auto err = memcpy_s(state, valLen, data, valLen);
    if (err != EOK) {
        LogError("set data failed in variable vector. %d", err);
    }
    groupSlot.strVal = state;
    groupSlot.strLen = valLen;
}

void MinInitiateDictionaryImpl(GroupBySlot &groupSlot, Vector *colPtr, uint32_t offset,
                               std::unique_ptr<ExecutionContext> &context)
{
    auto dictType = static_cast<DictionaryVector *>(colPtr)->ExtractDictionaryTypeId();
    int32_t originalOffset;
    Vector *originalVector = VectorHelper::ExpandVectorAndIndex(colPtr, offset, originalOffset);
    AGG_MIN_FUNCTIONS[dictType].initiateFunc(groupSlot, originalVector, originalOffset, context);
}

template<typename V, typename D>
void MinProcessNonGroupImpl(GroupBySlot &groupSlot, Vector *colPtr, uint32_t offset,
                            std::unique_ptr<ExecutionContext> &context)
{
    if (UNLIKELY(colPtr->IsValueNull(offset))) {
        return;
    }

    if (groupSlot.val == nullptr) {
        MinInitiateImpl<V, D>(groupSlot, colPtr, offset, context);
        return;
    }
    auto rowVal = (static_cast<V *>(colPtr))->GetValue(offset);
    auto leftVal = static_cast<D *>(groupSlot.val);
    *leftVal = (Compare(*leftVal, rowVal) == -1) ? *leftVal : rowVal;
}

void MinProcessNonGroupVarcharImpl(GroupBySlot &groupSlot, Vector *colPtr, uint32_t offset,
                                   std::unique_ptr<ExecutionContext> &context)
{
    if (UNLIKELY(colPtr->IsValueNull(offset))) {
        return;
    }

    if (groupSlot.val == nullptr) {
        MinInitiateVarcharImpl(groupSlot, colPtr, offset, context);
        return;
    }
    uint8_t *rowVal = nullptr;
    int valLen = (static_cast<VarcharVector *>(colPtr))->GetValue(offset, &rowVal);
    auto leftVal = reinterpret_cast<char *>(groupSlot.strVal);
    if (memcmp(leftVal, (char *)rowVal, std::min(valLen, groupSlot.strLen)) > 0) {
        auto err = memcpy_s(leftVal, valLen, rowVal, valLen);
        if (err != EOK) {
            LogError("set data failed in variable vector. %d", err);
        }
    }
    return;
}

void MinProcessNonGroupDictionaryImpl(GroupBySlot &groupSlot, Vector *colPtr, uint32_t offset,
                                      std::unique_ptr<ExecutionContext> &context)
{
    auto dictType = static_cast<DictionaryVector *>(colPtr)->ExtractDictionaryTypeId();
    int32_t originalOffset;
    Vector *originalVector = VectorHelper::ExpandVectorAndIndex(colPtr, offset, originalOffset);
    AGG_MAX_FUNCTIONS[dictType].processNonGroupFunc(groupSlot, originalVector, originalOffset, context);
}

void *MinEvaluateVarcharImpl(const GroupBySlot &groupBySlot, std::unique_ptr<ExecutionContext> &context)
{
    return new std::string(reinterpret_cast<char *>(groupBySlot.strVal),0, groupBySlot.strLen);
}

template <typename V, typename D>
void MaxInsertImpl(GroupBySlot &groupSlot, Vector *colPtr, uint32_t offset, std::unique_ptr<ExecutionContext> &context)
{
    if (UNLIKELY(colPtr->IsValueNull(offset))) {
        return;
    }
    auto rowVal = static_cast<V *>(colPtr)->GetValue(offset);
    int32_t len = sizeof(D);
    auto ptr = context->getArena()->Allocate(len);
    *reinterpret_cast<D *>(ptr) = rowVal;
    groupSlot.val = ptr;
}

void MaxInsertVarcharImpl(GroupBySlot &groupSlot, Vector *colPtr, uint32_t offset,
    std::unique_ptr<ExecutionContext> &context)
{
    if (UNLIKELY(colPtr->IsValueNull(offset))) {
        return;
    }
    uint8_t *data = nullptr;
    int valLen = static_cast<VarcharVector *>(colPtr)->GetValue(offset, &data);
    auto state = context->getArena()->Allocate(valLen);
    auto err = memcpy_s(state, valLen, data, valLen);
    if (err != EOK) {
        LogError("set data failed in variable vector. %d", err);
    }
    groupSlot.strVal = state;
    groupSlot.strLen = valLen;
}

void MaxInsertDictionaryImpl(GroupBySlot &groupBySlot, Vector *colPtr, uint32_t offset,
    std::unique_ptr<ExecutionContext> &context)
{
    auto dictType = static_cast<DictionaryVector *>(colPtr)->ExtractDictionaryTypeId();
    int32_t originalOffset;
    Vector *originalVector = VectorHelper::ExpandVectorAndIndex(colPtr, offset, originalOffset);
    AGG_MAX_FUNCTIONS[dictType].insertFunc(groupBySlot, originalVector, originalOffset, context);
}

template <typename V, typename D>
void MaxProcessGroupImpl(GroupBySlot &groupSlot, Vector *colPtr, uint32_t offset,
    std::unique_ptr<ExecutionContext> &context)
{
    if (UNLIKELY(colPtr->IsValueNull(offset))) {
        return;
    }
    if (groupSlot.val == nullptr) {
        MaxInsertImpl<V, D>(groupSlot, colPtr, offset, context);
        return;
    }
    auto rowVal = (static_cast<V *>(colPtr))->GetValue(offset);
    auto leftVal = static_cast<D *>(groupSlot.val);
    *leftVal = (Compare(*leftVal, rowVal) == 1) ? *leftVal : rowVal;
}

void MaxProcessGroupVarcharImpl(GroupBySlot &groupSlot, Vector *colPtr, uint32_t offset,
    std::unique_ptr<ExecutionContext> &context)
{
    if (UNLIKELY(colPtr->IsValueNull(offset))) {
        return;
    }
    if (groupSlot.val == nullptr) {
        MaxInsertVarcharImpl(groupSlot, colPtr, offset, context);
        return;
    }
    uint8_t *rowVal = nullptr;
    int valLen = (static_cast<VarcharVector *>(colPtr))->GetValue(offset, &rowVal);
    auto leftVal = reinterpret_cast<char *>(groupSlot.strVal);
    if (memcmp(leftVal, (char *)rowVal, std::min(valLen, groupSlot.strLen)) < 0) {
        auto err = memcpy_s(leftVal, valLen, rowVal, valLen);
        if (err != EOK) {
            LogError("set data failed in variable vector. %d", err);
        }
    }
    return;
}

void MaxProcessGroupDictionaryImpl(GroupBySlot &groupBySlot, Vector *colPtr, uint32_t offset,
    std::unique_ptr<ExecutionContext> &context)
{
    auto dictType = static_cast<DictionaryVector *>(colPtr)->ExtractDictionaryTypeId();
    int32_t originalOffset;
    Vector *originalVector = VectorHelper::ExpandVectorAndIndex(colPtr, offset, originalOffset);
    AGG_MAX_FUNCTIONS[dictType].processGroupFunc(groupBySlot, originalVector, originalOffset, context);
}

template <typename V, typename D>
void MaxInitiateImpl(GroupBySlot &groupSlot, Vector *colPtr, uint32_t offset,
                     std::unique_ptr<ExecutionContext> &context)
{
    if (UNLIKELY(colPtr->IsValueNull(offset))) {
        return;
    }

    auto curVal = (static_cast<V *>(colPtr))->GetValue(offset);
    auto ptr = context->getArena()->Allocate(sizeof(D));
    *reinterpret_cast<D *>(ptr) = curVal;
    groupSlot.val = ptr;
}

void MaxInitiateVarcharImpl(GroupBySlot &groupSlot, Vector *colPtr, uint32_t offset,
                            std::unique_ptr<ExecutionContext> &context)
{
    if (UNLIKELY(colPtr->IsValueNull(offset))) {
        return;
    }

    uint8_t *data = nullptr;
    int valLen = static_cast<VarcharVector *>(colPtr)->GetValue(offset, &data);
    uint8_t *state = context->getArena()->Allocate(valLen);
    auto err = memcpy_s(state, valLen, data, valLen);
    if (err != EOK) {
        LogError("set data failed in variable vector. %d", err);
    }
    groupSlot.strVal = state;
    groupSlot.strLen = valLen;
}

void MaxInitiateDictionaryImpl(GroupBySlot &groupSlot, Vector *colPtr, uint32_t offset,
                               std::unique_ptr<ExecutionContext> &context)
{
    auto dictType = static_cast<DictionaryVector *>(colPtr)->ExtractDictionaryTypeId();
    int32_t originalOffset;
    Vector *originalVector = VectorHelper::ExpandVectorAndIndex(colPtr, offset, originalOffset);
    AGG_MAX_FUNCTIONS[dictType].initiateFunc(groupSlot, originalVector, originalOffset, context);
}
template <typename V, typename D>
void MaxProcessNonGroupImpl(GroupBySlot &groupSlot, Vector *colPtr, uint32_t offset,
                            std::unique_ptr<ExecutionContext> &context)
{
    if (UNLIKELY(colPtr->IsValueNull(offset))) {
        return;
    }

    if (groupSlot.val == nullptr) {
        MaxInitiateImpl<V, D>(groupSlot, colPtr, offset, context);
        return;
    }
    auto rowVal = (static_cast<V *>(colPtr))->GetValue(offset);
    auto leftVal = static_cast<D *>(groupSlot.val);
    *leftVal = (Compare(*leftVal, rowVal) == 1) ? *leftVal : rowVal;
}

void MaxProcessNonGroupVarcharImpl(GroupBySlot &groupSlot, Vector *colPtr, uint32_t offset,
                                   std::unique_ptr<ExecutionContext> &context)
{
    if (UNLIKELY(colPtr->IsValueNull(offset))) {
        return;
    }

    if (groupSlot.val == nullptr) {
        MaxInitiateVarcharImpl(groupSlot, colPtr, offset, context);
        return;
    }
    uint8_t *rowVal = nullptr;
    int valLen = (static_cast<VarcharVector *>(colPtr))->GetValue(offset, &rowVal);
    auto leftVal = reinterpret_cast<char *>(groupSlot.strVal);
    if (memcmp(leftVal, (char *)rowVal, std::min(valLen, groupSlot.strLen)) < 0) {
        auto err = memcpy_s(leftVal, valLen, rowVal, valLen);
        if (err != EOK) {
            LogError("set data failed in variable vector. %d", err);
        }
    }
}

void MaxProcessNonGroupDictionaryImpl(GroupBySlot &groupSlot, Vector *colPtr, uint32_t offset,
                                      std::unique_ptr<ExecutionContext> &context)
{
    auto dictType = static_cast<DictionaryVector *>(colPtr)->ExtractDictionaryTypeId();
    int32_t originalOffset;
    Vector *originalVector = VectorHelper::ExpandVectorAndIndex(colPtr, offset, originalOffset);
    AGG_MAX_FUNCTIONS[dictType].processNonGroupFunc(groupSlot, originalVector, originalOffset, context);
}

void *MaxEvaluateVarcharImpl(const GroupBySlot &groupBySlot, std::unique_ptr<ExecutionContext> &context)
{
    return new std::string(reinterpret_cast<char *>(groupBySlot.strVal), 0, groupBySlot.strLen);
}

bool Aggregator::IsInputRaw() const
{
    return this->inputRaw;
}

bool Aggregator::IsOutputPartial() const
{
    return this->outputPartial;
}

void SumAggregator::Insert(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    auto typeId = static_cast<VecTypeId>(type);
    AGG_SUM_FUNCTIONS[typeId].insertFunc(groupSlot, colPtr, offset, executionContext);
}

void SumAggregator::ProcessGroup(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    auto typeId = static_cast<VecTypeId>(type);
    AGG_SUM_FUNCTIONS[typeId].processGroupFunc(groupSlot, colPtr, offset, executionContext);
}

void SumAggregator::Initiate(Vector *colPtr, int32_t type, uint32_t offset)
{
    auto typeId = static_cast<VecTypeId>(type);
    AGG_SUM_FUNCTIONS[typeId].initiateFunc(nonGroupState, colPtr, offset, executionContext);
    initiated = true;
}

void SumAggregator::ProcessNonGroup(Vector *colPtr, int32_t type, uint32_t offset)
{
    if (!initiated) {
        Initiate(colPtr, type, offset);
        return;
    }

    auto typeId = static_cast<VecTypeId>(type);
    AGG_SUM_FUNCTIONS[typeId].processNonGroupFunc(nonGroupState, colPtr, offset, executionContext);
}

void *SumAggregator::Evaluate(const GroupBySlot &groupBySlot, int32_t type)
{
    return groupBySlot.val;
}

void CountAggregator::Insert(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    // It is only effective when COUNT(col). When COUNT(*) or COUNT(1) should directly accumulate vector size;
    if (UNLIKELY(colPtr->IsValueNull(offset))) {
        groupSlot.count = 0;
        return;
    }

    if (inputRaw) {
        groupSlot.count = 1;
        return;
    }

    groupSlot.count = (static_cast<LongVector *>(colPtr))->GetValue(offset);
}

void CountAggregator::ProcessGroup(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    if (UNLIKELY(colPtr->IsValueNull(offset))) {
        return;
    }
    if (inputRaw) {
        groupSlot.count++;
    } else {
        groupSlot.count += (static_cast<LongVector *>(colPtr))->GetValue(offset);
    }
}

void CountAggregator::Initiate(Vector *colPtr, int32_t type, uint32_t offset)
{
    // It is only effective when COUNT(col). When COUNT(*) or COUNT(1) should directly accumulate vector size;
    if (UNLIKELY(colPtr->IsValueNull(offset))) {
        nonGroupState.count = 0;
        return;
    }

    if (inputRaw) {
        nonGroupState.count = 1;
        initiated = true;
        return;
    }
    nonGroupState.count = (static_cast<LongVector *>(colPtr))->GetValue(offset);
    initiated = true;
}

void CountAggregator::ProcessNonGroup(Vector *colPtr, int32_t type, uint32_t offset)
{
    if (!initiated) {
        Initiate(colPtr, type, offset);
        return;
    }
    if (UNLIKELY(colPtr->IsValueNull(offset))) {
        return;
    }
    if (inputRaw) {
        nonGroupState.count++;
    } else {
        nonGroupState.count += reinterpret_cast<LongVector *>(colPtr)->GetValue(offset);
    }
}

void *CountAggregator::Evaluate(const GroupBySlot &groupBySlot, int32_t type)
{
    return &(const_cast<GroupBySlot &>(groupBySlot).count);
}

void AverageAggregator::Initiate(Vector *colPtr, int32_t type, uint32_t offset)
{
    auto typeId = static_cast<VecTypeId>(type);
    AGG_AVG_FUNCTIONS[typeId].initiateFunc(nonGroupState, colPtr, offset, executionContext);
    initiated = true;
}

void AverageAggregator::ProcessNonGroup(Vector *colPtr, int32_t type, uint32_t offset)
{
    if (!initiated) {
        Initiate(colPtr, type, offset);
        return;
    }

    auto typeId = static_cast<VecTypeId>(type);
    AGG_AVG_FUNCTIONS[typeId].processNonGroupFunc(nonGroupState, colPtr, offset, executionContext);
}

void AverageAggregator::Insert(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    auto typeId = static_cast<VecTypeId>(type);
    AGG_AVG_FUNCTIONS[typeId].insertFunc(groupSlot, colPtr, offset, executionContext);
}

void AverageAggregator::ProcessGroup(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    auto typeId = static_cast<VecTypeId>(type);
    AGG_AVG_FUNCTIONS[typeId].processGroupFunc(groupSlot, colPtr, offset, executionContext);
}

void *AverageAggregator::Evaluate(const GroupBySlot &groupBySlot, int32_t type)
{
    auto typeId = static_cast<VecTypeId>(type);
    return AGG_AVG_FUNCTIONS[typeId].evaluateFunc(groupBySlot, executionContext);
}

void MinAggregator::Insert(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    if (UNLIKELY(colPtr->IsValueNull(offset))) {
        return;
    }
    auto typeId = static_cast<VecTypeId>(type);
    AGG_MIN_FUNCTIONS[typeId].insertFunc(groupSlot, colPtr, offset, executionContext);
}

void MinAggregator::ProcessGroup(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    if (UNLIKELY(colPtr->IsValueNull(offset))) {
        return;
    }
    auto typeId = static_cast<VecTypeId>(type);
    AGG_MIN_FUNCTIONS[typeId].processGroupFunc(groupSlot, colPtr, offset, executionContext);
}

void MinAggregator::Initiate(Vector *colPtr, int32_t type, uint32_t offset)
{
    if (UNLIKELY(colPtr->IsValueNull(offset))) {
        return;
    }

    auto typeId = static_cast<VecTypeId>(type);
    AGG_MIN_FUNCTIONS[typeId].initiateFunc(nonGroupState, colPtr, offset, executionContext);
    initiated = true;
}

void MinAggregator::ProcessNonGroup(Vector *colPtr, int32_t type, uint32_t offset)
{
    if (!initiated) {
        Initiate(colPtr, type, offset);
        return;
    }

    if (UNLIKELY(colPtr->IsValueNull(offset))) {
        return;
    }

    auto typeId = static_cast<VecTypeId>(type);
    AGG_MIN_FUNCTIONS[typeId].processNonGroupFunc(nonGroupState, colPtr, offset, executionContext);
}

void *MinAggregator::Evaluate(const GroupBySlot &groupBySlot, int32_t type)
{
    if (type == OMNI_VEC_TYPE_VARCHAR || type == OMNI_VEC_TYPE_CHAR) {
        return AGG_MIN_FUNCTIONS[type].evaluateFunc(groupBySlot, executionContext);
    }
    return groupBySlot.val;
}

void MaxAggregator::Insert(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    if (UNLIKELY(colPtr->IsValueNull(offset))) {
        return;
    }
    auto typeId = static_cast<VecTypeId>(type);
    AGG_MAX_FUNCTIONS[typeId].insertFunc(groupSlot, colPtr, offset, executionContext);
}

void MaxAggregator::ProcessGroup(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    if (UNLIKELY(colPtr->IsValueNull(offset))) {
        return;
    }

    auto typeId = static_cast<VecTypeId>(type);
    AGG_MAX_FUNCTIONS[typeId].processGroupFunc(groupSlot, colPtr, offset, executionContext);
}


void MaxAggregator::Initiate(Vector *colPtr, int32_t type, uint32_t offset)
{
    if (UNLIKELY(colPtr->IsValueNull(offset))) {
        return;
    }
    auto typeId = static_cast<VecTypeId>(type);
    AGG_MAX_FUNCTIONS[typeId].initiateFunc(nonGroupState, colPtr, offset, executionContext);
    initiated = true;
}

void MaxAggregator::ProcessNonGroup(Vector *colPtr, int32_t type, uint32_t offset)
{
    if (!initiated) {
        Initiate(colPtr, type, offset);
        return;
    }

    if (UNLIKELY(colPtr->IsValueNull(offset))) {
        return;
    }

    auto typeId = static_cast<VecTypeId>(type);
    AGG_MAX_FUNCTIONS[typeId].processNonGroupFunc(nonGroupState, colPtr, offset, executionContext);
}

void *MaxAggregator::Evaluate(const GroupBySlot &groupBySlot, int32_t type)
{
    if (type == OMNI_VEC_TYPE_VARCHAR || type == OMNI_VEC_TYPE_CHAR) {
        return AGG_MAX_FUNCTIONS[type].evaluateFunc(groupBySlot, executionContext);
    }
    return groupBySlot.val;
}

std::unique_ptr<Aggregator> SumAggregatorFactory::CreateAggregator(int32_t inputType, int32_t outputType, bool inputRaw,
    bool outputPartial)
{
    if (inputType >= OMNI_VEC_TYPE_INVALID || outputType >= OMNI_VEC_TYPE_INVALID) {
        throw std::exception();
    }
    return std::make_unique<SumAggregator>(inputType, outputType, inputRaw, outputPartial);
}

std::unique_ptr<Aggregator> CountAggregatorFactory::CreateAggregator(int32_t inputType, int32_t outputType,
    bool inputRaw, bool outputPartial)
{
    if (inputType >= OMNI_VEC_TYPE_INVALID || outputType >= OMNI_VEC_TYPE_INVALID) {
        throw std::exception();
    }
    return std::make_unique<CountAggregator>(inputType, outputType, inputRaw, outputPartial);
}

std::unique_ptr<Aggregator> MinAggregatorFactory::CreateAggregator(int32_t inputType, int32_t outputType, bool inputRaw,
    bool outputPartial)
{
    if (inputType >= OMNI_VEC_TYPE_INVALID || outputType >= OMNI_VEC_TYPE_INVALID) {
        throw std::exception();
    }
    return std::make_unique<MinAggregator>(inputType, outputType, inputRaw, outputPartial);
}

std::unique_ptr<Aggregator> MaxAggregatorFactory::CreateAggregator(int32_t inputType, int32_t outputType, bool inputRaw,
    bool outputPartial)
{
    if (inputType >= OMNI_VEC_TYPE_INVALID || outputType >= OMNI_VEC_TYPE_INVALID) {
        throw std::exception();
    }
    return std::make_unique<MaxAggregator>(inputType, outputType, inputRaw, outputPartial);
}

std::unique_ptr<Aggregator> AverageAggregatorFactory::CreateAggregator(int32_t inputType, int32_t outputType,
    bool inputRaw, bool outputPartial)
{
    if (inputType >= OMNI_VEC_TYPE_INVALID || outputType >= OMNI_VEC_TYPE_INVALID) {
        throw std::exception();
    }
    return std::make_unique<AverageAggregator>(inputType, outputType, inputRaw, outputPartial);
}
} // end of namespace op
} // end of namespace omniruntime
