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
template <typename T> int32_t ALWAYS_INLINE Compare(const T &leftVal, const T &rightVal)
{
    return (leftVal > rightVal ? 1 : (leftVal < rightVal ? -1 : 0));
}

template <typename V, typename D>
void SumInsertImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    auto curVal = (static_cast<V *>(colPtr))->GetValue(offset);
    auto val = std::make_unique<D>(curVal);
    groupSlot.val = val.release();
}

template <typename V, typename D>
void SumProcessGroupImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    *(static_cast<D *>(groupSlot.val)) += (static_cast<V *>(colPtr))->GetValue(offset);
}

template <typename V, typename D>
void SumInitiateImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    auto curVal = (static_cast<V *>(colPtr))->GetValue(offset);
    auto val = std::make_unique<D>(curVal);
    groupSlot = { val.release() };
}

template <typename V, typename D>
void SumProcessNonGroupImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    *(static_cast<D *>(groupSlot.val)) += (static_cast<V *>(colPtr))->GetValue(offset);
}

template <typename V, typename D>
void AvgInsertImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    auto rowVal = (static_cast<V *>(colPtr))->GetValue(offset);
    groupSlot.avgVal = std::make_unique<D>(rowVal).release();
    groupSlot.avgCnt = 1;
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
        LogError("Divisor should not be zero! Offset = %d", offset);
    }
    groupSlot.avgVal = std::make_unique<double>(avgVal * avgCnt / avgCnt).release();
    groupSlot.avgCnt = avgCnt;
}

template <typename V, typename D>
void AvgProcessGroupImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    auto currentVal = static_cast<D *>(groupSlot.avgVal);
    auto sum = (static_cast<V *>(colPtr))->GetValue(offset) + *currentVal * groupSlot.avgCnt;
    *currentVal = sum / ++groupSlot.avgCnt;
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
        LogError("Divisor should not be zero! Offset = %d", offset);
    }
    groupSlot.avgCnt += avgCnt;
    *currentVal = (avgVal * avgCnt + *currentVal * currentCnt) / groupSlot.avgCnt;
}

template <typename V, typename D>
void AvgInitiateImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    auto val = std::make_unique<D>(0);
    auto rowVal = (static_cast<V *>(colPtr))->GetValue(offset);
    *val = rowVal;
    groupSlot = { { val.release(), 1 } };
}

template <typename V, typename D>
void AvgProcessNonGroupImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    auto currentVal = static_cast<D *>(groupSlot.avgVal);
    auto rowVal = (static_cast<V *>(colPtr))->GetValue(offset);
    *currentVal = (rowVal + *currentVal * groupSlot.avgCnt) / (++groupSlot.avgCnt);
}

template <typename V, typename D>
void MinInsertImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    auto rowVal = static_cast<V *>(colPtr)->GetValue(offset);
    auto val = std::make_unique<D>(rowVal);
    groupSlot.val = val.release();
}

void MinInsertVarcharImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    uint8_t *data = nullptr;
    int valLen = static_cast<VarcharVector *>(colPtr)->GetValue(offset, &data);
    auto val = std::make_unique<std::string>(reinterpret_cast<char *>(data), valLen);
    groupSlot.val = val.release();
}

template <typename V, typename D>
void MinProcessGroupImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    auto rowVal = (static_cast<V *>(colPtr))->GetValue(offset);
    auto leftVal = static_cast<D *>(groupSlot.val);
    *leftVal = (Compare(*leftVal, rowVal) == -1) ? *leftVal : rowVal;
}

void MinProcessGroupVarcharImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    uint8_t *data = nullptr;
    int valLen = (static_cast<VarcharVector *>(colPtr))->GetValue(offset, &data);
    std::string rowVal(reinterpret_cast<char *>(data), valLen);
    auto leftVal = static_cast<std::string *>(groupSlot.val);
    *leftVal = (Compare(*leftVal, rowVal) == -1) ? *leftVal : rowVal;
}

template <typename V, typename D>
void MinInitiateImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    auto curVal = (static_cast<V *>(colPtr))->GetValue(offset);
    auto val = std::make_unique<D>(curVal);
    groupSlot = { val.release() };
}

void MinInitiateVarcharImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    uint8_t *data = nullptr;
    int valLen = (static_cast<VarcharVector *>(colPtr))->GetValue(offset, &data);
    auto val = std::make_unique<std::string>(reinterpret_cast<char *>(data), 0, valLen);
    groupSlot = { val.release() };
}

template <typename V, typename D>
void MinProcessNonGroupImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    auto rowVal = (static_cast<V *>(colPtr))->GetValue(offset);
    auto leftVal = static_cast<D *>(groupSlot.val);
    *leftVal = (Compare(*leftVal, rowVal) == -1) ? *leftVal : rowVal;
}

void MinProcessNonGroupVarcharImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    uint8_t *data = nullptr;
    int valLen = (static_cast<VarcharVector *>(colPtr))->GetValue(offset, &data);
    std::string rowVal(reinterpret_cast<char *>(data), 0, valLen);
    auto leftVal = static_cast<std::string *>(groupSlot.val);
    *leftVal = (Compare(*leftVal, rowVal) == -1) ? *leftVal : rowVal;
}

template <typename V, typename D>
void MaxInsertImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    auto rowVal = static_cast<V *>(colPtr)->GetValue(offset);
    auto val = std::make_unique<D>(rowVal);
    groupSlot.val = val.release();
}

void MaxInsertVarcharImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    uint8_t *data = nullptr;
    int valLen = static_cast<VarcharVector *>(colPtr)->GetValue(offset, &data);
    auto val = std::make_unique<std::string>(reinterpret_cast<char *>(data), 0, valLen);
    groupSlot.val = val.release();
}

template <typename V, typename D>
void MaxProcessGroupImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    auto rowVal = (static_cast<V *>(colPtr))->GetValue(offset);
    auto leftVal = static_cast<D *>(groupSlot.val);
    *leftVal = (Compare(*leftVal, rowVal) == 1) ? *leftVal : rowVal;
}

void MaxProcessGroupVarcharImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    uint8_t *data = nullptr;
    int valLen = (static_cast<VarcharVector *>(colPtr))->GetValue(offset, &data);
    std::string rowVal(reinterpret_cast<char *>(data), 0, valLen);
    auto leftVal = static_cast<std::string *>(groupSlot.val);
    *leftVal = (Compare(*leftVal, rowVal) == 1) ? *leftVal : rowVal;
}

template <typename V, typename D>
void MaxInitiateImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    auto curVal = (static_cast<V *>(colPtr))->GetValue(offset);
    auto val = std::make_unique<D>(curVal);
    groupSlot = { val.release() };
}

void MaxInitiateVarcharImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    uint8_t *data = nullptr;
    int valLen = static_cast<VarcharVector *>(colPtr)->GetValue(offset, &data);
    auto val = std::make_unique<std::string>(reinterpret_cast<char *>(data), valLen);
    groupSlot = { val.release() };
}

template <typename V, typename D>
void MaxProcessNonGroupImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    auto rowVal = (static_cast<V *>(colPtr))->GetValue(offset);
    auto leftVal = static_cast<D *>(groupSlot.val);
    *leftVal = (Compare(*leftVal, rowVal) == 1) ? *leftVal : rowVal;
}

void MaxProcessNonGroupVarcharImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    uint8_t *data = nullptr;
    int valLen = static_cast<VarcharVector *>(colPtr)->GetValue(offset, &data);
    std::string rowVal(reinterpret_cast<char *>(data), valLen);
    auto leftVal = static_cast<std::string *>(groupSlot.val);
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

void Aggregator::AggInsert(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    int32_t originalOffset;
    Vector *originalVector = VectorHelper::ExpandVectorAndIndex(colPtr, offset, originalOffset);
    Insert(groupSlot, originalVector, type, originalOffset);
}

void Aggregator::AggProcessNonGroup(Vector *colPtr, int32_t type, uint32_t offset)
{
    int32_t originalOffset;
    Vector *originalVector = VectorHelper::ExpandVectorAndIndex(colPtr, offset, originalOffset);
    ProcessNonGroup(originalVector, type, originalOffset);
}

void Aggregator::AggProcessGroup(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    int32_t originalOffset;
    Vector *originalVector = VectorHelper::ExpandVectorAndIndex(colPtr, offset, originalOffset);
    ProcessGroup(groupSlot, originalVector, type, originalOffset);
}

void SumAggregator::Insert(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    auto typeId = static_cast<VecTypeId>(type);
    SumAggregator::SUM_FUNCTIONS[typeId].insertFunc(groupSlot, colPtr, type, offset);
}

void SumAggregator::ProcessGroup(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    if (UNLIKELY(colPtr->IsValueNull(offset))) {
        return;
    }
    auto typeId = static_cast<VecTypeId>(type);
    SumAggregator::SUM_FUNCTIONS[typeId].processGroupFunc(groupSlot, colPtr, type, offset);
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
        LogError("Divisor should not be zero! Offset = %d", offset);
    }
    groupSlot.avgCnt += avgCnt;
    *currentVal = (avgVal * avgCnt + *currentVal * currentCnt) / groupSlot.avgCnt;
}

void AverageAggregator::ProcessGroup(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    if (UNLIKELY(colPtr->IsValueNull(offset))) {
        return;
    }

    auto typeId = static_cast<VecTypeId>(type);
    AverageAggregator::AVG_FUNCTIONS[typeId].processGroupFunc(groupSlot, colPtr, type, offset);
}


void MinAggregator::Insert(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    auto typeId = static_cast<VecTypeId>(type);
    MinAggregator::MIN_FUNCTIONS[typeId].insertFunc(groupSlot, colPtr, type, offset);
}

void MinAggregator::ProcessGroup(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    if (UNLIKELY(colPtr->IsValueNull(offset))) {
        return;
    }
    auto typeId = static_cast<VecTypeId>(type);
    MinAggregator::MIN_FUNCTIONS[typeId].processGroupFunc(groupSlot, colPtr, type, offset);
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

void MaxAggregator::ProcessGroup(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    if (UNLIKELY(colPtr->IsValueNull(offset))) {
        return;
    }

    auto typeId = static_cast<VecTypeId>(type);
    MaxAggregator::MAX_FUNCTIONS[typeId].processGroupFunc(groupSlot, colPtr, type, offset);
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
