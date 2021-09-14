/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Inner supported aggregators source file
 */
#include "aggregator.h"

#include <memory>

#include "../../vector/vector_common.h"
#include "../../vector/vector_helper.h"

namespace omniruntime {
namespace op {
using namespace omniruntime::vec;
template<typename T>
int32_t ALWAYS_INLINE Compare(const T& leftVal, const T& rightVal)
{
    return (leftVal > rightVal ? 1 : (leftVal < rightVal ? -1 : 0));
}

template<typename V, typename D>
void SumInsertImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    auto curVal = (static_cast<V *>(colPtr))->GetValue(offset);
    auto val = std::make_unique<D>(curVal);
    groupSlot.val = val.release();
}

void SumInsertDictImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    auto dictVector = static_cast<DictionaryVector*>(colPtr);
    auto dictType = dictVector->GetDictionaryType().GetId();
    if (dictType == OMNI_VEC_TYPE_INT) {
        auto curVal = dictVector->GetInt(offset);
        auto val = std::make_unique<int32_t>(curVal);
        groupSlot.val = val.release();
    } else if (dictType == OMNI_VEC_TYPE_LONG) {
        auto curVal = dictVector->GetLong(offset);
        auto val = std::make_unique<int64_t>(curVal);
        groupSlot.val = val.release();
    }
}

template<typename V, typename D>
void SumProcessGroupImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    *(static_cast<D *>(groupSlot.val)) += (static_cast<V *>(colPtr))->GetValue(offset);
}

void SumProcessGroupDictImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    auto dictVector = static_cast<DictionaryVector*>(colPtr);
    auto dictType = dictVector->GetDictionaryType().GetId();
    if (dictType == OMNI_VEC_TYPE_INT) {
        *(static_cast<int32_t *>(groupSlot.val)) += dictVector->GetInt(offset);
    } else if (dictType == OMNI_VEC_TYPE_LONG) {
        *(static_cast<int64_t *>(groupSlot.val)) += dictVector->GetLong(offset);
    }
}

template<typename V, typename D>
void SumInitiateImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    auto curVal = (static_cast<V *>(colPtr))->GetValue(offset);
    auto val = std::make_unique<D>(curVal);
    groupSlot = { val.release() };
}

void SumInitiateDictImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    auto dictVector = static_cast<DictionaryVector*>(colPtr);
    auto dictType = dictVector->GetDictionaryType().GetId();
    if (dictType == OMNI_VEC_TYPE_INT) {
        auto curVal = dictVector->GetInt(offset);
        auto val = std::make_unique<int32_t>(curVal);
        groupSlot = { val.release() };
    } else if (dictType == OMNI_VEC_TYPE_LONG) {
        auto curVal = dictVector->GetLong(offset);
        auto val = std::make_unique<int64_t>(curVal);
        groupSlot = { val.release() };
    }
}

template<typename V, typename D>
void SumProcessNonGroupImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    *(static_cast<D *>(groupSlot.val)) += (static_cast<V *>(colPtr))->GetValue(offset);
}

void SumProcessNonGroupDictImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    auto dictVector = static_cast<DictionaryVector*>(colPtr);
    auto dictType = dictVector->GetDictionaryType().GetId();
    if (dictType == OMNI_VEC_TYPE_INT) {
        *(static_cast<int32_t *>(groupSlot.val)) += dictVector->GetInt(offset);
    } else if (dictType == OMNI_VEC_TYPE_LONG) {
        *(static_cast<int64_t *>(groupSlot.val)) += dictVector->GetLong(offset);
    }
}

template<typename V, typename D>
void AvgInsertImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    auto rowVal = (static_cast<V *>(colPtr))->GetValue(offset);
    groupSlot.avgVal = std::make_unique<D>(rowVal).release();
    groupSlot.avgCnt = 1;
}

void AvgInsertDictImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    auto dictVector = static_cast<DictionaryVector*>(colPtr);
    auto dictType = dictVector->GetDictionaryType().GetId();
    if (dictType == OMNI_VEC_TYPE_INT) {
        int32_t rowVal = dictVector->GetInt(offset);
        groupSlot.avgVal = std::make_unique<double>(rowVal / 1.0).release();
        groupSlot.avgCnt = 1;
    } else if (dictType == OMNI_VEC_TYPE_LONG) {
        int64_t rowVal = dictVector->GetLong(offset);
        groupSlot.avgVal = std::make_unique<double>(rowVal / 1.0).release();
        groupSlot.avgCnt = 1;
    }
}

void AvgInsertContainerImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    auto containerVector = static_cast<ContainerVector *>(colPtr);
    DoubleVector *avgValVector = reinterpret_cast<DoubleVector *>(containerVector->getValue(0));
    double avgVal = avgValVector->GetValue(offset);
    LongVector *avgCountVector = reinterpret_cast<LongVector *>(containerVector->getValue(1));
    int64_t avgCnt = avgCountVector->GetValue(offset);
    if (avgCnt == 0) {
        // Fixme use error code
        DebugError("Divisor should not be zero! Offset = %d", offset);
    }
    groupSlot.avgVal = std::make_unique<double>(avgVal * avgCnt / avgCnt).release();
    groupSlot.avgCnt = avgCnt;
}

template<typename V, typename D>
void AvgProcessGroupImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    auto currentVal = static_cast<D *>(groupSlot.avgVal);
    auto sum = (static_cast<V *>(colPtr))->GetValue(offset) + *currentVal * groupSlot.avgCnt;
    *currentVal = sum / ++groupSlot.avgCnt;
}

void AvgProcessGroupDictImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    auto dictVector = static_cast<DictionaryVector*>(colPtr);
    auto dictType = dictVector->GetDictionaryType().GetId();
    if (dictType == OMNI_VEC_TYPE_INT) {
        auto currentVal = static_cast<double *>(groupSlot.avgVal);
        auto sum = dictVector->GetInt(offset) + *currentVal * groupSlot.avgCnt;
        *currentVal = sum / ++groupSlot.avgCnt;
    } else if (dictType == OMNI_VEC_TYPE_LONG) {
        auto currentVal = static_cast<double *>(groupSlot.avgVal);
        auto sum = dictVector->GetLong(offset) + *currentVal * groupSlot.avgCnt;
        *currentVal = sum / ++groupSlot.avgCnt;
    }
}

void AvgProcessGroupContainerImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    auto currentVal = static_cast<double *>(groupSlot.avgVal);
    auto currentCnt = static_cast<int64_t>(groupSlot.avgCnt);
    auto containerVector = static_cast<ContainerVector *>(colPtr);
    DoubleVector *avgValVector = reinterpret_cast<DoubleVector *>(containerVector->getValue(0));
    double avgVal = avgValVector->GetValue(offset);
    LongVector *avgCountVector = reinterpret_cast<LongVector *>(containerVector->getValue(1));
    int64_t avgCnt = avgCountVector->GetValue(offset);
    if (avgCnt == 0) {
        // Fixme use error code
        DebugError("Divisor should not be zero! Offset = %d", offset);
    }
    groupSlot.avgCnt += avgCnt;
    *currentVal = (avgVal * avgCnt + *currentVal * currentCnt) / groupSlot.avgCnt;
}

template<typename V, typename D>
void AvgInitiateImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    auto val = std::make_unique<D>(0);
    auto rowVal = (static_cast<V *>(colPtr))->GetValue(offset);
    *val = rowVal;
    groupSlot = { { val.release(), 1 } };
}

void AvgInitiateDictImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    auto val = std::make_unique<double>(0.0);
    auto dictVector = static_cast<DictionaryVector*>(colPtr);
    auto dictType = dictVector->GetDictionaryType().GetId();
    if (dictType == OMNI_VEC_TYPE_INT) {
        int32_t rowVal = dictVector->GetInt(offset);
        *val = rowVal / 1.0;
        groupSlot = { { val.release(), 1 } };
    } else if (dictType == OMNI_VEC_TYPE_LONG) {
        int64_t rowVal = dictVector->GetLong(offset);
        *val = rowVal / 1.0;
        groupSlot = { { val.release(), 1 } };
    }
}

template<typename V, typename D>
void AvgProcessNonGroupImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    auto currentVal = static_cast<D *>(groupSlot.avgVal);
    auto rowVal = (static_cast<V *>(colPtr))->GetValue(offset);
    *currentVal = (rowVal + *currentVal * groupSlot.avgCnt) / (++groupSlot.avgCnt);
}

void AvgProcessNonGroupDictImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    auto dictVector = static_cast<DictionaryVector*>(colPtr);
    auto dictType = dictVector->GetDictionaryType().GetId();
    if (dictType == OMNI_VEC_TYPE_INT) {
        auto currentVal = static_cast<double *>(groupSlot.avgVal);
        int32_t rowVal = dictVector->GetInt(offset);
        *currentVal = (rowVal + *currentVal * groupSlot.avgCnt) / (++groupSlot.avgCnt);
    } else if (dictType == OMNI_VEC_TYPE_LONG) {
        auto currentVal = static_cast<double *>(groupSlot.avgVal);
        int64_t rowVal = dictVector->GetLong(offset);
        *currentVal = (rowVal + *currentVal * groupSlot.avgCnt) / (++groupSlot.avgCnt);
    }
}

template<typename V, typename D>
void MinInsertImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    auto rowVal = static_cast<V*>(colPtr)->GetValue(offset);
    auto val = std::make_unique<D>(rowVal);
    groupSlot.val = val.release();
}

void MinInsertDictImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    auto dictVector = static_cast<DictionaryVector*>(colPtr);
    auto dictType = dictVector->GetDictionaryType().GetId();
    if (dictType == OMNI_VEC_TYPE_INT) {
        auto rowVal = dictVector->GetInt(offset);
        auto val = std::make_unique<int32_t>(rowVal);
        groupSlot.val = val.release();
    } else if (dictType == OMNI_VEC_TYPE_LONG) {
        auto rowVal = dictVector->GetLong(offset);
        auto val = std::make_unique<int64_t>(rowVal);
        groupSlot.val = val.release();
    }
}

void MinInsertVarcharImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    uint8_t *data = nullptr;
    int valLen = static_cast<VarcharVector*>(colPtr)->GetValue(offset, &data);
    auto val = std::make_unique<std::string>(reinterpret_cast<char*>(data), valLen);
    groupSlot.val = val.release();
}

template<typename V, typename D>
void MinProcessGroupImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    auto rowVal = (static_cast<V *>(colPtr))->GetValue(offset);
    auto leftVal = static_cast<D *>(groupSlot.val);
    *leftVal = (Compare(*leftVal, rowVal) == -1) ? *leftVal : rowVal;
}

void MinProcessGroupDictImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    auto dictVector = static_cast<DictionaryVector*>(colPtr);
    auto dictType = dictVector->GetDictionaryType().GetId();
    if (dictType == OMNI_VEC_TYPE_INT) {
        int32_t rowVal = dictVector->GetInt(offset);
        auto leftVal = static_cast<int32_t *>(groupSlot.val);
        *leftVal = (Compare(*leftVal, rowVal) == -1) ? *leftVal : rowVal;
    } else if (dictType == OMNI_VEC_TYPE_LONG) {
        int64_t rowVal = dictVector->GetLong(offset);
        auto leftVal = static_cast<int64_t *>(groupSlot.val);
        *leftVal = (Compare(*leftVal, rowVal) == -1) ? *leftVal : rowVal;
    }
}

void MinProcessGroupVarcharImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    uint8_t *data = nullptr;
    int valLen = (static_cast<VarcharVector *>(colPtr))->GetValue(offset, &data);
    std::string rowVal(reinterpret_cast<char *>(data), valLen);
    auto leftVal = static_cast<std::string *>(groupSlot.val);
    *leftVal = (Compare(*leftVal, rowVal) == -1) ? *leftVal : rowVal;
}

template<typename V, typename D>
void MinInitiateImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    auto curVal = (static_cast<V *>(colPtr))->GetValue(offset);
    auto val = std::make_unique<D>(curVal);
    groupSlot = { val.release() };
}

void MinInitiateDictImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    auto dictVector = static_cast<DictionaryVector*>(colPtr);
    auto dictType = dictVector->GetDictionaryType().GetId();
    if (dictType == OMNI_VEC_TYPE_INT) {
        auto curVal = dictVector->GetInt(offset);
        auto val = std::make_unique<int32_t>(curVal);
        groupSlot = { val.release() };
    } else if (dictType == OMNI_VEC_TYPE_LONG) {
        auto curVal = dictVector->GetLong(offset);
        auto val = std::make_unique<int64_t>(curVal);
        groupSlot = { val.release() };
    }
}

void MinInitiateVarcharImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    uint8_t *data = nullptr;
    int valLen = (static_cast<VarcharVector *>(colPtr))->GetValue(offset, &data);
    auto val = std::make_unique<std::string>(reinterpret_cast<char *>(data), 0, valLen);
    groupSlot = {val.release()};
}

template<typename V, typename D>
void MinProcessNonGroupImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    auto rowVal = (static_cast<V *>(colPtr))->GetValue(offset);
    auto leftVal = static_cast<D *>(groupSlot.val);
    *leftVal = (Compare(*leftVal, rowVal) == -1) ? *leftVal : rowVal;
}

void MinProcessNonGroupDictImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    auto dictVector = static_cast<DictionaryVector*>(colPtr);
    auto dictType = dictVector->GetDictionaryType().GetId();
    if (dictType == OMNI_VEC_TYPE_INT) {
        int32_t rowVal = dictVector->GetInt(offset);
        auto leftVal = static_cast<int32_t *>(groupSlot.val);
        *leftVal = (Compare(*leftVal, rowVal) == -1) ? *leftVal : rowVal;
    } else if (dictType == OMNI_VEC_TYPE_LONG) {
        int64_t rowVal = dictVector->GetLong(offset);
        auto leftVal = static_cast<int64_t *>(groupSlot.val);
        *leftVal = (Compare(*leftVal, rowVal) == -1) ? *leftVal : rowVal;
    }
}

void MinProcessNonGroupVarcharImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    uint8_t *data = nullptr;
    int valLen = (static_cast<VarcharVector *>(colPtr))->GetValue(offset, &data);
    std::string rowVal(reinterpret_cast<char *>(data), 0, valLen);
    auto leftVal = static_cast<std::string *>(groupSlot.val);
    *leftVal = (Compare(*leftVal, rowVal) == -1) ? *leftVal : rowVal;
}

template<typename V, typename D>
void MaxInsertImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    auto rowVal = static_cast<V*>(colPtr)->GetValue(offset);
    auto val = std::make_unique<D>(rowVal);
    groupSlot.val = val.release();
}

void MaxInsertDictImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    auto dictVector = static_cast<DictionaryVector*>(colPtr);
    auto dictType = dictVector->GetDictionaryType().GetId();
    if (dictType == OMNI_VEC_TYPE_INT) {
        auto rowVal = dictVector->GetInt(offset);
        auto val = std::make_unique<int32_t>(rowVal);
        groupSlot.val = val.release();
    } else if (dictType == OMNI_VEC_TYPE_LONG) {
        auto rowVal = dictVector->GetLong(offset);
        auto val = std::make_unique<int64_t>(rowVal);
        groupSlot.val = val.release();
    }
}

void MaxInsertVarcharImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    uint8_t *data = nullptr;
    int valLen = static_cast<VarcharVector *>(colPtr)->GetValue(offset, &data);
    auto val = std::make_unique<std::string>(reinterpret_cast<char *>(data), 0, valLen);
    groupSlot.val = val.release();
}

template<typename V, typename D>
void MaxProcessGroupImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    auto rowVal = (static_cast<V *>(colPtr))->GetValue(offset);
    auto leftVal = static_cast<D *>(groupSlot.val);
    *leftVal = (Compare(*leftVal, rowVal) == 1) ? *leftVal : rowVal;
}

void MaxProcessGroupDictImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    auto dictVector = static_cast<DictionaryVector*>(colPtr);
    auto dictType = dictVector->GetDictionaryType().GetId();
    if (dictType == OMNI_VEC_TYPE_INT) {
        int32_t rowVal = dictVector->GetInt(offset);
        auto leftVal = static_cast<int32_t *>(groupSlot.val);
        *leftVal = (Compare(*leftVal, rowVal) == 1) ? *leftVal : rowVal;
    } else if (dictType == OMNI_VEC_TYPE_LONG) {
        int64_t rowVal = dictVector->GetLong(offset);
        auto leftVal = static_cast<int64_t *>(groupSlot.val);
        *leftVal = (Compare(*leftVal, rowVal) == 1) ? *leftVal : rowVal;
    }
}

void MaxProcessGroupVarcharImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    uint8_t *data = nullptr;
    int valLen = (static_cast<VarcharVector *>(colPtr))->GetValue(offset, &data);
    std::string rowVal(reinterpret_cast<char *>(data), 0, valLen);
    auto leftVal = static_cast<std::string *>(groupSlot.val);
    *leftVal = (Compare(*leftVal, rowVal) == 1) ? *leftVal : rowVal;
}

template<typename V, typename D>
void MaxInitiateImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    auto curVal = (static_cast<V *>(colPtr))->GetValue(offset);
    auto val = std::make_unique<D>(curVal);
    groupSlot = { val.release() };
}

void MaxInitiateDictImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    auto dictVector = static_cast<DictionaryVector*>(colPtr);
    auto dictType = dictVector->GetDictionaryType().GetId();
    if (dictType == OMNI_VEC_TYPE_INT) {
        auto curVal = dictVector->GetInt(offset);
        auto val = std::make_unique<int32_t>(curVal);
        groupSlot = { val.release() };
    } else if (dictType == OMNI_VEC_TYPE_LONG) {
        auto curVal = dictVector->GetLong(offset);
        auto val = std::make_unique<int64_t>(curVal);
        groupSlot = { val.release() };
    }
}

void MaxInitiateVarcharImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    uint8_t *data = nullptr;
    int valLen = static_cast<VarcharVector*>(colPtr)->GetValue(offset, &data);
    auto val = std::make_unique<std::string>(reinterpret_cast<char*>(data), valLen);
    groupSlot = { val.release() };
}

template<typename V, typename D>
void MaxProcessNonGroupImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    auto rowVal = (static_cast<V *>(colPtr))->GetValue(offset);
    auto leftVal = static_cast<D *>(groupSlot.val);
    *leftVal = (Compare(*leftVal, rowVal) == 1) ? *leftVal : rowVal;
}

void MaxProcessNonGroupDictImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    auto dictVector = static_cast<DictionaryVector*>(colPtr);
    auto dictType = dictVector->GetDictionaryType().GetId();
    if (dictType == OMNI_VEC_TYPE_INT) {
        int32_t rowVal = dictVector->GetInt(offset);
        auto leftVal = static_cast<int32_t *>(groupSlot.val);
        *leftVal = (Compare(*leftVal, rowVal) == 1) ? *leftVal : rowVal;
    } else if (dictType == OMNI_VEC_TYPE_LONG) {
        int64_t rowVal = dictVector->GetLong(offset);
        auto *leftVal = static_cast<int64_t *>(groupSlot.val);
        *leftVal = (Compare(*leftVal, rowVal) == 1) ? *leftVal : rowVal;
    }
}

void MaxProcessNonGroupVarcharImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    uint8_t *data = nullptr;
    int valLen = static_cast<VarcharVector*>(colPtr)->GetValue(offset, &data);
    std::string rowVal(reinterpret_cast<char*>(data), valLen);
    auto leftVal = static_cast<std::string*>(groupSlot.val);
    *leftVal = (Compare(*leftVal, rowVal) == 1) ? *leftVal : rowVal;
}

bool Aggregator::IsInputRaw() const
{
    return this->inputRaw;
}

bool Aggregator::IsOutputPartial() const
{
    return this->outputPartial;
}

void SumAggregator::Insert(GroupBySlot &groupSlot, Vector *colPtr, int32_t type,
    uint32_t offset)
{
    auto typeId = static_cast<VecTypeId>(type);
    SumAggregator::SUM_FUNCTIONS[typeId].insertFunc(groupSlot, colPtr, type, offset);
}

void SumAggregator::ProcessGroup(GroupBySlot &groupSlot, Vector *colPtr, int32_t type,
    uint32_t offset)
{
    int32_t rowIdx = offset;
    colPtr = VectorHelper::GetDictionary(colPtr, rowIdx);
    if (UNLIKELY(colPtr->IsValueNull(rowIdx))) {
        return;
    }
    auto typeId = static_cast<VecTypeId>(type);
    SumAggregator::SUM_FUNCTIONS[typeId].processGroupFunc(groupSlot, colPtr, type, rowIdx);
}

void SumAggregator::Initiate(Vector *colPtr, int32_t type, uint32_t offset)
{
    auto typeId = static_cast<VecTypeId>(type);
    SumAggregator::SUM_FUNCTIONS[typeId].initiateFunc(nonGroupState, colPtr, type, offset);
    initiated = true;
}

void SumAggregator::ProcessNonGroup(Vector *colPtr, int32_t type, uint32_t offset)
{
    if (!initiated) {
        Initiate(colPtr, type, offset);
        return;
    }

    auto typeId = static_cast<VecTypeId>(type);
    SumAggregator::SUM_FUNCTIONS[typeId].processNonGroupFunc(nonGroupState, colPtr, type, offset);
}

void CountAggregator::ProcessGroup(GroupBySlot &groupSlot, Vector *colPtr, int32_t type,
    uint32_t offset)
{
    int32_t rowIdx = offset;
    colPtr = VectorHelper::GetDictionary(colPtr, rowIdx);
    if (UNLIKELY(colPtr->IsValueNull(rowIdx))) {
        return;
    }
    if (inputRaw) {
        groupSlot.count++;
    } else {
        groupSlot.count += (static_cast<LongVector *>(colPtr))->GetValue(rowIdx);
    }
}

void CountAggregator::Initiate(Vector *colPtr, int32_t type, uint32_t offset)
{
    if (type != OMNI_VEC_TYPE_LONG) {
        DebugError("Count column type %d is not long!", type);
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
    if (inputRaw) {
        nonGroupState.count++;
    } else {
        nonGroupState.count += reinterpret_cast<LongVector *>(colPtr)->GetValue(offset);
    }
}

void CountAggregator::Insert(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    if (inputRaw) {
        groupSlot.count = 1;
        return;
    }
    groupSlot.count = (static_cast<LongVector *>(colPtr))->GetValue(offset);
}

void AverageAggregator::Initiate(Vector *colPtr, int32_t type, uint32_t offset)
{
    auto typeId = static_cast<VecTypeId>(type);
    AverageAggregator::AVG_FUNCTIONS[typeId].initiateFunc(nonGroupState, colPtr, type, offset);
    initiated = true;
}

void AverageAggregator::ProcessNonGroup(Vector *colPtr, int32_t type, uint32_t offset)
{
    if (!initiated) {
        Initiate(colPtr, type, offset);
        return;
    }
    auto typeId = static_cast<VecTypeId>(type);
    AverageAggregator::AVG_FUNCTIONS[typeId].processNonGroupFunc(nonGroupState, colPtr, type, offset);
}

void AverageAggregator::Insert(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    auto typeId = static_cast<VecTypeId>(type);
    AverageAggregator::AVG_FUNCTIONS[typeId].insertFunc(groupSlot, colPtr, type, offset);
}

void ALWAYS_INLINE ProcessIntermediateAvg(GroupBySlot &groupSlot, Vector *colPtr, uint32_t offset)
{
    auto currentVal = static_cast<double *>(groupSlot.avgVal);
    auto currentCnt = static_cast<int64_t>(groupSlot.avgCnt);
    auto containerVector = static_cast<ContainerVector *>(colPtr);
    DoubleVector *avgValVector = reinterpret_cast<DoubleVector *>(containerVector->getValue(0));
    double avgVal = avgValVector->GetValue(offset);
    LongVector *avgCountVector = reinterpret_cast<LongVector *>(containerVector->getValue(1));
    int64_t avgCnt = avgCountVector->GetValue(offset);
    if (avgCnt == 0) {
        // Fixme use error code
        DebugError("Divisor should not be zero! Offset = %d", offset);
    }
    groupSlot.avgCnt += avgCnt;
    *currentVal = (avgVal * avgCnt + *currentVal * currentCnt) / groupSlot.avgCnt;
}

void AverageAggregator::ProcessGroup(GroupBySlot &groupSlot, Vector *colPtr, int32_t type,
    uint32_t offset)
{
    int32_t rowIdx = offset;
    colPtr = VectorHelper::GetDictionary(colPtr, rowIdx);
    if (UNLIKELY(colPtr->IsValueNull(rowIdx))) {
        return;
    }

    auto typeId = static_cast<VecTypeId>(type);
    AverageAggregator::AVG_FUNCTIONS[typeId].processGroupFunc(groupSlot, colPtr, type, rowIdx);
}


void MinAggregator::Insert(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    auto typeId = static_cast<VecTypeId>(type);
    MinAggregator::MIN_FUNCTIONS[typeId].insertFunc(groupSlot, colPtr, type, offset);
}

void MinAggregator::ProcessGroup(GroupBySlot &groupSlot, Vector *colPtr, int32_t type,
    uint32_t offset)
{
    int32_t rowIdx = offset;
    colPtr = VectorHelper::GetDictionary(colPtr, rowIdx);
    if (UNLIKELY(colPtr->IsValueNull(rowIdx))) {
        return;
    }
    auto typeId = static_cast<VecTypeId>(type);
    MinAggregator::MIN_FUNCTIONS[typeId].processGroupFunc(groupSlot, colPtr, type, rowIdx);
}

void MinAggregator::Initiate(Vector *colPtr, int32_t type, uint32_t offset)
{
    auto typeId = static_cast<VecTypeId>(type);
    MinAggregator::MIN_FUNCTIONS[typeId].initiateFunc(nonGroupState, colPtr, type, offset);
    initiated = true;
}

void MinAggregator::ProcessNonGroup(Vector *colPtr, int32_t type, uint32_t offset)
{
    if (!initiated) {
        Initiate(colPtr, type, offset);
        return;
    }

    auto typeId = static_cast<VecTypeId>(type);
    MinAggregator::MIN_FUNCTIONS[typeId].processNonGroupFunc(nonGroupState, colPtr, type, offset);
}

void MaxAggregator::Insert(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    auto typeId = static_cast<VecTypeId>(type);
    MaxAggregator::MAX_FUNCTIONS[typeId].insertFunc(groupSlot, colPtr, type, offset);
}

void MaxAggregator::ProcessGroup(GroupBySlot &groupSlot, Vector *colPtr, int32_t type,
    uint32_t offset)
{
    int32_t rowIdx = offset;
    colPtr = VectorHelper::GetDictionary(colPtr, rowIdx);
    if (UNLIKELY(colPtr->IsValueNull(rowIdx))) {
        return;
    }

    auto typeId = static_cast<VecTypeId>(type);
    MaxAggregator::MAX_FUNCTIONS[typeId].processGroupFunc(groupSlot, colPtr, type, rowIdx);
}


void MaxAggregator::Initiate(Vector *colPtr, int32_t type, uint32_t offset)
{
    auto typeId = static_cast<VecTypeId>(type);
    MaxAggregator::MAX_FUNCTIONS[typeId].initiateFunc(nonGroupState, colPtr, type, offset);
    initiated = true;
}

void MaxAggregator::ProcessNonGroup(Vector *colPtr, int32_t type, uint32_t offset)
{
    if (!initiated) {
        Initiate(colPtr, type, offset);
        return;
    }

    auto typeId = static_cast<VecTypeId>(type);
    MaxAggregator::MAX_FUNCTIONS[typeId].processNonGroupFunc(nonGroupState, colPtr, type, offset);
}

std::unique_ptr<Aggregator> SumAggregatorFactory::CreateAggregator(int32_t inputType,
                                                                   int32_t outputType,
                                                                   bool inputRaw,
                                                                   bool outputPartial)
{
    if (inputType >= OMNI_VEC_TYPE_INVALID || outputType >= OMNI_VEC_TYPE_INVALID) {
        throw std::exception();
    }
    return std::make_unique<SumAggregator>(inputType, outputType, inputRaw, outputPartial);
}

std::unique_ptr<Aggregator> CountAggregatorFactory::CreateAggregator(int32_t inputType,
                                                                     int32_t outputType,
                                                                     bool inputRaw,
                                                                     bool outputPartial)
{
    if (inputType >= OMNI_VEC_TYPE_INVALID || outputType >= OMNI_VEC_TYPE_INVALID) {
        throw std::exception();
    }
    return std::make_unique<CountAggregator>(inputType, outputType, inputRaw, outputPartial);
}

std::unique_ptr<Aggregator> MinAggregatorFactory::CreateAggregator(int32_t inputType,
                                                                   int32_t outputType,
                                                                   bool inputRaw,
                                                                   bool outputPartial)
{
    if (inputType >= OMNI_VEC_TYPE_INVALID || outputType >= OMNI_VEC_TYPE_INVALID) {
        throw std::exception();
    }
    return std::make_unique<MinAggregator>(inputType, outputType, inputRaw, outputPartial);
}

std::unique_ptr<Aggregator> MaxAggregatorFactory::CreateAggregator(int32_t inputType,
                                                                   int32_t outputType,
                                                                   bool inputRaw,
                                                                   bool outputPartial)
{
    if (inputType >= OMNI_VEC_TYPE_INVALID || outputType >= OMNI_VEC_TYPE_INVALID) {
        throw std::exception();
    }
    return std::make_unique<MaxAggregator>(inputType, outputType, inputRaw, outputPartial);
}

std::unique_ptr<Aggregator> AverageAggregatorFactory::CreateAggregator(int32_t inputType,
                                                                       int32_t outputType,
                                                                       bool inputRaw,
                                                                       bool outputPartial)
{
    if (inputType >= OMNI_VEC_TYPE_INVALID || outputType >= OMNI_VEC_TYPE_INVALID) {
        throw std::exception();
    }
    return std::make_unique<AverageAggregator>(inputType, outputType, inputRaw, outputPartial);
}
} // end of namespace op
} // end of namespace omniruntime
