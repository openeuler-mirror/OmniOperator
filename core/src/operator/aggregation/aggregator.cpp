/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Inner supported aggregators source file
 */
#include "aggregator.h"

#include <memory>
#include <string.h>

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
void SumInsertImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset, std::unique_ptr<ExecutionContext> &context)
{
    auto curVal = (static_cast<V *>(colPtr))->GetValue(offset);
    int32_t len = sizeof(D);
    auto ptr = context->getArena()->Allocate(len);
    memcpy_s(ptr, len, &curVal, len);
    groupSlot.val = ptr;
}

void SumInsertDecimalImpl(GroupBySlot &groupBySlot, Vector *colPtr, int32_t type, uint32_t offset, std::unique_ptr<ExecutionContext> &context)
{
    auto curVal = (static_cast<LongVector *>(colPtr))->GetValue(offset);
    auto val = std::make_unique<Decimal128>(curVal);
    groupBySlot.val = val.release();
}

template <typename V, typename D>
void SumProcessGroupImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset, std::unique_ptr<ExecutionContext> &context)
{
    if (groupSlot.val == nullptr) {
        SumInsertImpl<V, D>(groupSlot, colPtr, type, offset, context);
        return;
    }
    *(static_cast<D *>(groupSlot.val)) += (static_cast<V *>(colPtr))->GetValue(offset);
}

void SumProcessGroupDecimalImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset, std::unique_ptr<ExecutionContext> &context)
{
    if (groupSlot.val == nullptr) {
        SumInsertDecimalImpl(groupSlot, colPtr, type, offset, context);
        return;
    }
    auto val = (static_cast<LongVector *>(colPtr))->GetValue(offset);
    auto v = std::make_unique<Decimal128>(val);
    *(static_cast<Decimal128 *>(groupSlot.val)) += *v;
}

template <typename V, typename D>
void SumInitiateImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    auto curVal = (static_cast<V *>(colPtr))->GetValue(offset);
    auto val = std::make_unique<D>(curVal);
    groupSlot = { val.release() };
}

void SumInitiateDecimalImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    auto curVal = (static_cast<LongVector *>(colPtr))->GetValue(offset);
    auto val = std::make_unique<Decimal128>(curVal);
    groupSlot = { val.release() };
}

template <typename V, typename D>
void SumProcessNonGroupImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    if (groupSlot.val == nullptr) {
        SumInitiateImpl<V, D>(groupSlot, colPtr, type, offset);
        return;
    }
    *(static_cast<D *>(groupSlot.val)) += (static_cast<V *>(colPtr))->GetValue(offset);
}

void SumProcessNonGroupDecimalImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    if (groupSlot.val == nullptr) {
        SumInitiateDecimalImpl(groupSlot, colPtr, type, offset);
        return;
    }
    auto val = (static_cast<LongVector *>(colPtr))->GetValue(offset);
    auto v = std::make_unique<Decimal128>(val);
    *(static_cast<Decimal128 *>(groupSlot.val)) += *v;
}

template <typename V, typename D>
void AvgInsertImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset, std::unique_ptr<ExecutionContext> &context)
{
    double rowVal = (static_cast<V *>(colPtr))->GetValue(offset) / 1.0;
    int32_t len = sizeof(double);
    auto ptr = context->getArena()->Allocate(len);
    memcpy_s(ptr, len, &rowVal, len);
    groupSlot.avgVal = reinterpret_cast<double *>(ptr);
    groupSlot.avgCnt = 1;
}

void AvgInsertContainerImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset, std::unique_ptr<ExecutionContext> &context)
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
void AvgProcessGroupImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset, std::unique_ptr<ExecutionContext> &context)
{
    if (groupSlot.val == nullptr) {
        AvgInsertImpl<V, D>(groupSlot, colPtr, type, offset, context);
        return;
    }
    auto currentVal = static_cast<D *>(groupSlot.avgVal);
    auto sum = (static_cast<V *>(colPtr))->GetValue(offset) + *currentVal * groupSlot.avgCnt;
    *currentVal = sum / ++groupSlot.avgCnt;
}

void AvgProcessGroupContainerImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset, std::unique_ptr<ExecutionContext> &context)
{
    if (groupSlot.val == nullptr) {
        AvgInsertContainerImpl(groupSlot, colPtr, type, offset, context);
        return;
    }
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
    groupSlot.avgVal = val.release();
    groupSlot.avgCnt = 1;
}

template <typename V, typename D>
void AvgProcessNonGroupImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    if (groupSlot.val == nullptr) {
        AvgInitiateImpl<V, D>(groupSlot, colPtr, type, offset);
        return;
    }
    auto currentVal = static_cast<D *>(groupSlot.avgVal);
    auto rowVal = (static_cast<V *>(colPtr))->GetValue(offset);
    *currentVal = (rowVal + *currentVal * groupSlot.avgCnt) / (++groupSlot.avgCnt);
}

template <typename V, typename D>
void MinInsertImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset, std::unique_ptr<ExecutionContext> &context)
{
    auto rowVal = static_cast<V *>(colPtr)->GetValue(offset);
    int32_t len = sizeof(D);
    auto ptr = context->getArena()->Allocate(len);
    memcpy_s(ptr, len, &rowVal, len);
    groupSlot.val = ptr;
}

void MinInsertVarcharImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset, std::unique_ptr<ExecutionContext> &context)
{
    uint8_t *data = nullptr;
    int valLen = static_cast<VarcharVector *>(colPtr)->GetValue(offset, &data);
    uint8_t *state = context->getArena()->Allocate(valLen);
    memcpy_s(state, valLen, data, valLen);
    groupSlot.strVal = state;
    groupSlot.strLen = valLen;
}

template <typename V, typename D>
void MinProcessGroupImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset, std::unique_ptr<ExecutionContext> &context)
{
    if (groupSlot.val == nullptr) {
        MinInsertImpl<V, D>(groupSlot, colPtr, type, offset, context);
        return;
    }
    auto rowVal = (static_cast<V *>(colPtr))->GetValue(offset);
    auto leftVal = static_cast<D *>(groupSlot.val);
    *leftVal = (Compare(*leftVal, rowVal) == -1) ? *leftVal : rowVal;
}

void MinProcessGroupVarcharImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset, std::unique_ptr<ExecutionContext> &context)
{
    if (groupSlot.val == nullptr) {
        MinInsertVarcharImpl(groupSlot, colPtr, type, offset, context);
        return;
    }
    uint8_t *rowVal = nullptr;
    int valLen = (static_cast<VarcharVector *>(colPtr))->GetValue(offset, &rowVal);
    auto leftVal = reinterpret_cast<char *>(groupSlot.strVal);
    if (memcmp(leftVal, (char*)rowVal, std::min(valLen, groupSlot.strLen)) > 0) {
        memcpy_s(leftVal, valLen, rowVal, valLen);
    }
    return;
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
    if (groupSlot.val == nullptr) {
        MinInitiateImpl<V, D>(groupSlot, colPtr, type, offset);
        return;
    }
    auto rowVal = (static_cast<V *>(colPtr))->GetValue(offset);
    auto leftVal = static_cast<D *>(groupSlot.val);
    *leftVal = (Compare(*leftVal, rowVal) == -1) ? *leftVal : rowVal;
}

void MinProcessNonGroupVarcharImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    if (groupSlot.val == nullptr) {
        MinInitiateVarcharImpl(groupSlot, colPtr, type, offset);
        return;
    }
    uint8_t *data = nullptr;
    int valLen = (static_cast<VarcharVector *>(colPtr))->GetValue(offset, &data);
    std::string rowVal(reinterpret_cast<char *>(data), 0, valLen);
    auto leftVal = static_cast<std::string *>(groupSlot.val);
    *leftVal = (Compare(*leftVal, rowVal) == -1) ? *leftVal : rowVal;
}

template <typename V, typename D>
void MaxInsertImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset, std::unique_ptr<ExecutionContext> &context)
{
    auto rowVal = static_cast<V *>(colPtr)->GetValue(offset);
    int32_t len = sizeof(D);
    auto ptr = context->getArena()->Allocate(len);
    memcpy_s(ptr, len, &rowVal, len);
    groupSlot.val = ptr;
}

void MaxInsertVarcharImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset, std::unique_ptr<ExecutionContext> &context)
{
    uint8_t *data = nullptr;
    int valLen = static_cast<VarcharVector *>(colPtr)->GetValue(offset, &data);
    auto state = context->getArena()->Allocate(valLen);
    memcpy_s(state, valLen, data, valLen);
    groupSlot.strVal = state;
    groupSlot.strLen = valLen;
}

template <typename V, typename D>
void MaxProcessGroupImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset, std::unique_ptr<ExecutionContext> &context)
{
    if (groupSlot.val == nullptr) {
        MaxInsertImpl<V, D>(groupSlot, colPtr, type, offset, context);
        return;
    }
    auto rowVal = (static_cast<V *>(colPtr))->GetValue(offset);
    auto leftVal = static_cast<D *>(groupSlot.val);
    *leftVal = (Compare(*leftVal, rowVal) == 1) ? *leftVal : rowVal;
}

void MaxProcessGroupVarcharImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset, std::unique_ptr<ExecutionContext> &context)
{
    if (groupSlot.val == nullptr) {
        MaxInsertVarcharImpl(groupSlot, colPtr, type, offset, context);
        return;
    }
    uint8_t *rowVal = nullptr;
    int valLen = (static_cast<VarcharVector *>(colPtr))->GetValue(offset, &rowVal);
    auto leftVal = reinterpret_cast<char *>(groupSlot.strVal);
    if (memcmp(leftVal, (char*)rowVal, std::min(valLen, groupSlot.strLen)) < 0) {
        memcpy_s(leftVal, valLen, rowVal, valLen);
    }
    return;
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
    if (groupSlot.val == nullptr) {
        MaxInitiateImpl<V, D>(groupSlot, colPtr, type, offset);
        return;
    }
    auto rowVal = (static_cast<V *>(colPtr))->GetValue(offset);
    auto leftVal = static_cast<D *>(groupSlot.val);
    *leftVal = (Compare(*leftVal, rowVal) == 1) ? *leftVal : rowVal;
}

void MaxProcessNonGroupVarcharImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    if (groupSlot.val == nullptr) {
        MaxInitiateVarcharImpl(groupSlot, colPtr, type, offset);
        return;
    }
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
    if (UNLIKELY(colPtr->IsValueNull(offset))) {
        return;
    }

    auto typeId = static_cast<VecTypeId>(type);
    SumAggregator::SUM_FUNCTIONS[typeId].insertFunc(groupSlot, colPtr, type, offset, executionContext);
}

void SumAggregator::ProcessGroup(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    if (UNLIKELY(colPtr->IsValueNull(offset))) {
        return;
    }
    auto typeId = static_cast<VecTypeId>(type);
    SumAggregator::SUM_FUNCTIONS[typeId].processGroupFunc(groupSlot, colPtr, type, offset, executionContext);
}

void SumAggregator::Initiate(Vector *colPtr, int32_t type, uint32_t offset)
{
    if (UNLIKELY(colPtr->IsValueNull(offset))) {
        return;
    }

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

    if (UNLIKELY(colPtr->IsValueNull(offset))) {
        return;
    }

    auto typeId = static_cast<VecTypeId>(type);
    SumAggregator::SUM_FUNCTIONS[typeId].processNonGroupFunc(nonGroupState, colPtr, type, offset);
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

void AverageAggregator::Initiate(Vector *colPtr, int32_t type, uint32_t offset)
{
    if (UNLIKELY(colPtr->IsValueNull(offset))) {
        return;
    }

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

    if (UNLIKELY(colPtr->IsValueNull(offset))) {
        return;
    }

    auto typeId = static_cast<VecTypeId>(type);
    AverageAggregator::AVG_FUNCTIONS[typeId].processNonGroupFunc(nonGroupState, colPtr, type, offset);
}

void AverageAggregator::Insert(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    if (UNLIKELY(colPtr->IsValueNull(offset))) {
        return;
    }
    auto typeId = static_cast<VecTypeId>(type);
    AverageAggregator::AVG_FUNCTIONS[typeId].insertFunc(groupSlot, colPtr, type, offset, executionContext);
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
    AverageAggregator::AVG_FUNCTIONS[typeId].processGroupFunc(groupSlot, colPtr, type, offset, executionContext);
}


void MinAggregator::Insert(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    if (UNLIKELY(colPtr->IsValueNull(offset))) {
        return;
    }
    auto typeId = static_cast<VecTypeId>(type);
    MinAggregator::MIN_FUNCTIONS[typeId].insertFunc(groupSlot, colPtr, type, offset, executionContext);
}

void MinAggregator::ProcessGroup(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    if (UNLIKELY(colPtr->IsValueNull(offset))) {
        return;
    }
    auto typeId = static_cast<VecTypeId>(type);
    MinAggregator::MIN_FUNCTIONS[typeId].processGroupFunc(groupSlot, colPtr, type, offset, executionContext);
}

void MinAggregator::Initiate(Vector *colPtr, int32_t type, uint32_t offset)
{
    if (UNLIKELY(colPtr->IsValueNull(offset))) {
        return;
    }

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

    if (UNLIKELY(colPtr->IsValueNull(offset))) {
        return;
    }

    auto typeId = static_cast<VecTypeId>(type);
    MinAggregator::MIN_FUNCTIONS[typeId].processNonGroupFunc(nonGroupState, colPtr, type, offset);
}

void MaxAggregator::Insert(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    if (UNLIKELY(colPtr->IsValueNull(offset))) {
        return;
    }
    auto typeId = static_cast<VecTypeId>(type);
    MaxAggregator::MAX_FUNCTIONS[typeId].insertFunc(groupSlot, colPtr, type, offset, executionContext);
}

void MaxAggregator::ProcessGroup(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset)
{
    if (UNLIKELY(colPtr->IsValueNull(offset))) {
        return;
    }

    auto typeId = static_cast<VecTypeId>(type);
    MaxAggregator::MAX_FUNCTIONS[typeId].processGroupFunc(groupSlot, colPtr, type, offset, executionContext);
}


void MaxAggregator::Initiate(Vector *colPtr, int32_t type, uint32_t offset)
{
    if (UNLIKELY(colPtr->IsValueNull(offset))) {
        return;
    }
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

    if (UNLIKELY(colPtr->IsValueNull(offset))) {
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
