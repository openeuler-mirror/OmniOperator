/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Inner supported aggregators source file
 */
#include "aggregator.h"

#include <memory>

#include "../../util/debug.h"
#include "../../vector/vector_common.h"

namespace omniruntime {
namespace op {
    using namespace omniruntime::vec;
int32_t CompareInt(int32_t leftVal, int32_t rightVal)
{
    return (leftVal > rightVal ? 1 : (leftVal < rightVal ? -1 : 0));
}

int32_t CompareInt64(int64_t leftVal, int64_t rightVal)
{
    return (leftVal > rightVal ? 1 : (leftVal < rightVal ? -1 : 0));
}

int32_t CompareDouble(double leftVal, double rightVal)
{
    return (leftVal > rightVal ? 1 : (leftVal < rightVal ? -1 : 0));
}

bool Aggregator::IsInputRaw() const
{
    return this->inputRaw;
}

bool Aggregator::IsOutputPartial() const
{
    return this->outputPartial;
}

void SumAggregator::ProcessGroup(GroupBySlot &groupSlot, void *colPtr, int32_t type, uint32_t offset)
{
    switch (type) {
        case OMNI_VEC_TYPE_INT: {
            *((int32_t *)(groupSlot.val)) += ((IntVector *)colPtr)->GetValue(offset);
            break;
        }
        case OMNI_VEC_TYPE_LONG: {
            *((int64_t *)(groupSlot.val)) += ((LongVector *)colPtr)->GetValue(offset);
            break;
        }
        case OMNI_VEC_TYPE_DOUBLE: {
            *((double *)(groupSlot.val)) += ((DoubleVector *)colPtr)->GetValue(offset);
            break;
        }
        default: {
            DebugError("No such data type %d", type);
            break;
        }
    }
}

void SumAggregator::Initiate(void *colPtr, int32_t type, uint32_t offset)
{
    switch (type) {
        case OMNI_VEC_TYPE_INT: {
            auto curVal = ((IntVector *)colPtr)->GetValue(offset);
            auto val = std::make_unique<int32_t>(curVal);
            nonGroupState = { val.release() };
            break;
        }
        case OMNI_VEC_TYPE_LONG: {
            auto curVal = ((LongVector *)colPtr)->GetValue(offset);
            auto val = std::make_unique<int64_t>(curVal);
            nonGroupState = { val.release() };
            break;
        }
        case OMNI_VEC_TYPE_DOUBLE: {
            auto curVal = ((DoubleVector *)colPtr)->GetValue(offset);
            auto val = std::make_unique<double>(curVal);
            nonGroupState = { val.release() };
            break;
        }
        default: {
            DebugError("No such data type %d", type);
            break;
        }
    }

    initiated = true;
}

void SumAggregator::ProcessNonGroup(void *colPtr, int32_t type, uint32_t offset)
{
    if (!initiated) {
        Initiate(colPtr, type, offset);
        return;
    }

    switch (type) {
        case OMNI_VEC_TYPE_INT: {
            *((int32_t *)nonGroupState.val) += ((IntVector *)colPtr)->GetValue(offset);
            break;
        }
        case OMNI_VEC_TYPE_LONG: {
            *((int64_t *)nonGroupState.val) += ((LongVector *)colPtr)->GetValue(offset);
            break;
        }
        case OMNI_VEC_TYPE_DOUBLE: {
            *((double *)nonGroupState.val) += ((DoubleVector *)colPtr)->GetValue(offset);
            break;
        }
        default: {
            DebugError("No such data type %d", type);
            break;
        }
    }
}

void SumAggregator::Insert(int64_t key, void *colPtr, int32_t type, uint32_t offset)
{
    switch (type) {
        case OMNI_VEC_TYPE_INT: {
            auto curVal = ((IntVector *)colPtr)->GetValue(offset);
            auto val = std::make_unique<int32_t>(curVal);
            GroupBySlot slot = { val.release() };
            groupState.insert({ key, slot });
            break;
        }
        case OMNI_VEC_TYPE_LONG: {
            auto curVal = ((LongVector *)colPtr)->GetValue(offset);
            auto val = std::make_unique<int64_t>(curVal);
            GroupBySlot slot = { val.release() };
            groupState.insert({ key, slot });
            break;
        }
        case OMNI_VEC_TYPE_DOUBLE: {
            auto curVal = ((DoubleVector *)colPtr)->GetValue(offset);
            auto val = std::make_unique<double>(curVal);
            GroupBySlot slot = { val.release() };
            groupState.insert({ key, slot });
            break;
        }
        default: {
            DebugError("No such data type %d", type);
            break;
        }
    }
}

void CountAggregator::ProcessGroup(GroupBySlot &groupSlot, void *colPtr, int32_t type, uint32_t offset)
{
    if (inputRaw) {
        groupSlot.count++;
    } else {
        groupSlot.count += ((LongVector *)colPtr)->GetValue(offset);
    }
}

void CountAggregator::Initiate(void *colPtr, int32_t type, uint32_t offset)
{
    if (type != OMNI_VEC_TYPE_LONG) {
        DebugError("Count column type %d is not long!", type);
    }
    if (inputRaw) {
        nonGroupState.count = 1;
        initiated = true;
        return;
    }
    nonGroupState.count = ((LongVector *)colPtr)->GetValue(offset);
    initiated = true;
}

void CountAggregator::ProcessNonGroup(void *colPtr, int32_t type, uint32_t offset)
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

void CountAggregator::Insert(int64_t key, void *colPtr, int32_t type, uint32_t offset)
{
    if (type != OMNI_VEC_TYPE_LONG) {
        DebugError("Count column type %d is not long!", type);
    }
    GroupBySlot slot;
    if (inputRaw) {
        slot.count = 1;
        groupState.insert({ key, slot });
        return;
    }
    slot.count = ((LongVector *)colPtr)->GetValue(offset);
    groupState.insert({ key, slot });
}

void AverageAggregator::Initiate(void *colPtr, int32_t type, uint32_t offset)
{
    auto val = std::make_unique<double>(0.0);
    switch (type) {
        case OMNI_VEC_TYPE_INT: {
            int32_t rowVal = ((IntVector *)colPtr)->GetValue(offset);
            *val = rowVal / 1.0;
            nonGroupState = { { val.release(), 1 } };
            break;
        }
        case OMNI_VEC_TYPE_LONG: {
            int64_t rowVal = ((LongVector *)colPtr)->GetValue(offset);
            *val = rowVal / 1.0;
            nonGroupState = { { val.release(), 1 } };
            break;
        }
        case OMNI_VEC_TYPE_DOUBLE: {
            double rowVal = ((DoubleVector *)colPtr)->GetValue(offset);
            *val = rowVal / 1.0;
            nonGroupState = { { val.release(), 1 } };
            break;
        }
        default: {
            DebugError("No such data type %d", type);
            break;
        }
    }

    initiated = true;
}

void AverageAggregator::ProcessNonGroup(void *colPtr, int32_t type, uint32_t offset)
{
    if (!initiated) {
        Initiate(colPtr, type, offset);
        return;
    }
    double *currentVal = static_cast<double *>(nonGroupState.avgVal);
    switch (type) {
        case OMNI_VEC_TYPE_INT: {
            int32_t rowVal = ((IntVector *)colPtr)->GetValue(offset);
            *currentVal = (rowVal + *currentVal * nonGroupState.avgCnt) / (++nonGroupState.avgCnt);
            break;
        }
        case OMNI_VEC_TYPE_LONG: {
            int64_t rowVal = ((LongVector *)colPtr)->GetValue(offset);
            *currentVal = (rowVal + *currentVal * nonGroupState.avgCnt) / (++nonGroupState.avgCnt);
            break;
        }
        case OMNI_VEC_TYPE_DOUBLE: {
            double rowVal = ((DoubleVector *)colPtr)->GetValue(offset);
            *currentVal = (rowVal + *currentVal * nonGroupState.avgCnt) / (++nonGroupState.avgCnt);
            break;
        }
        default: {
            DebugError("No such data type %d", type);
            break;
        }
    }
}

void AverageAggregator::Insert(int64_t key, void *colPtr, int32_t type, uint32_t offset)
{
    auto val = std::make_unique<double>(0.0);
    switch (type) {
        case OMNI_VEC_TYPE_INT: {
            int32_t rowVal = ((IntVector *)colPtr)->GetValue(offset);
            *val = rowVal / 1.0;
            GroupBySlot slot = { { val.release(), 1 } };
            groupState.insert({ key, slot });
            break;
        }
        case OMNI_VEC_TYPE_LONG: {
            int64_t rowVal = ((LongVector *)colPtr)->GetValue(offset);
            *val = rowVal / 1.0;
            GroupBySlot slot = { { val.release(), 1 } };
            groupState.insert({ key, slot });
            break;
        }
        case OMNI_VEC_TYPE_DOUBLE: {
            double rowVal = ((DoubleVector *)colPtr)->GetValue(offset);
            *val = rowVal / 1.0;
            GroupBySlot slot = { { val.release(), 1 } };
            groupState.insert({ key, slot });
            break;
        }
        case OMNI_VEC_TYPE_CONTAINER: {
            // get intermediate state from container vector
            ContainerVector* containerVector = reinterpret_cast<ContainerVector*>(colPtr);
            DoubleVector* avgValVector = reinterpret_cast<DoubleVector*>(containerVector->getValue(0));
            double avgVal = avgValVector->GetValue(offset);
            LongVector* avgCountVector = reinterpret_cast<LongVector*>(containerVector->getValue(1));
            int64_t avgCnt = avgCountVector->GetValue(offset);
            *val = avgVal * avgCnt / avgCnt;
            GroupBySlot slot = { { val.release(), avgCnt } };
            groupState.insert({ key, slot });
            break;
        }
        default: {
            DebugError("No such data type %d", type);
            break;
        }
    }
}

void AverageAggregator::ProcessGroup(GroupBySlot &groupSlot, void *colPtr, int32_t type, uint32_t offset)
{
    double *currentVal = static_cast<double *>(groupSlot.avgVal);
    int64_t currentCnt = static_cast<int64_t>(groupSlot.avgCnt);
    switch (type) {
        case OMNI_VEC_TYPE_INT: {
            int32_t rowVal = ((IntVector *)colPtr)->GetValue(offset);
            *currentVal = (rowVal + *currentVal * groupSlot.avgCnt) / ++groupSlot.avgCnt;
            break;
        }
        case OMNI_VEC_TYPE_LONG: {
            int64_t rowVal = ((LongVector *)colPtr)->GetValue(offset);
            *currentVal = (rowVal + *currentVal * groupSlot.avgCnt) / ++groupSlot.avgCnt;
            break;
        }
        case OMNI_VEC_TYPE_DOUBLE: {
            double rowVal = ((DoubleVector *)colPtr)->GetValue(offset);
            *currentVal = (rowVal + *currentVal * groupSlot.avgCnt) / ++groupSlot.avgCnt;
            break;
        }
        case OMNI_VEC_TYPE_CONTAINER: {
            ContainerVector* containerVector = reinterpret_cast<ContainerVector*>(colPtr);
            DoubleVector* avgValVector = reinterpret_cast<DoubleVector*>(containerVector->getValue(0));
            double avgVal = avgValVector->GetValue(offset);
            LongVector* avgCountVector = reinterpret_cast<LongVector*>(containerVector->getValue(1));
            int64_t avgCnt = avgCountVector->GetValue(offset);
            groupSlot.avgCnt += avgCnt;
            *currentVal = (avgVal * avgCnt + *currentVal * currentCnt) / groupSlot.avgCnt;
            break;
        }
        default: {
            DebugError("No such data type %d", type);
            break;
        }
    }
}

void MinAggregator::ProcessGroup(GroupBySlot &groupSlot, void *colPtr, int32_t type, uint32_t offset)
{
    switch (type) {
        case OMNI_VEC_TYPE_INT: {
            int32_t rowVal = ((IntVector *)colPtr)->GetValue(offset);
            int32_t *leftVal = reinterpret_cast<int32_t *>(groupSlot.val);
            *leftVal = CompareInt(*leftVal, rowVal) == -1 ? *leftVal : rowVal;
            break;
        }
        case OMNI_VEC_TYPE_LONG: {
            int64_t rowVal = ((LongVector *)colPtr)->GetValue(offset);
            int64_t *leftVal = reinterpret_cast<int64_t *>(groupSlot.val);
            *leftVal = CompareInt64(*leftVal, rowVal) == -1 ? *leftVal : rowVal;
            break;
        }
        case OMNI_VEC_TYPE_DOUBLE: {
            double rowVal = ((DoubleVector *)colPtr)->GetValue(offset);
            double *leftVal = reinterpret_cast<double *>(groupSlot.val);
            *leftVal = CompareDouble(*leftVal, rowVal) == -1 ? *leftVal : rowVal;
            break;
        }
        default: {
            DebugError("No such data type %d", type);
            break;
        }
    }
}

void MinAggregator::Initiate(void *colPtr, int32_t type, uint32_t offset)
{
    switch (type) {
        case OMNI_VEC_TYPE_INT: {
            auto curVal = ((IntVector *)colPtr)->GetValue(offset);
            auto val = std::make_unique<int32_t>(curVal);
            nonGroupState = { val.release() };
            break;
        }
        case OMNI_VEC_TYPE_LONG: {
            auto curVal = ((LongVector *)colPtr)->GetValue(offset);
            auto val = std::make_unique<int64_t>(curVal);
            nonGroupState = { val.release() };
            break;
        }
        case OMNI_VEC_TYPE_DOUBLE: {
            auto curVal = ((DoubleVector *)colPtr)->GetValue(offset);
            auto val = std::make_unique<double>(curVal);
            nonGroupState = { val.release() };
            break;
        }
        default: {
            DebugError("No such data type %d", type);
            break;
        }
    }

    initiated = true;
}

void MinAggregator::ProcessNonGroup(void *colPtr, int32_t type, uint32_t offset)
{
    if (!initiated) {
        Initiate(colPtr, type, offset);
        return;
    }

    switch (type) {
        case OMNI_VEC_TYPE_INT: {
            int32_t rowVal = ((IntVector *)colPtr)->GetValue(offset);
            int32_t *leftVal = reinterpret_cast<int32_t *>(nonGroupState.val);
            *leftVal = CompareInt(*leftVal, rowVal) == -1 ? *leftVal : rowVal;
            break;
        }
        case OMNI_VEC_TYPE_LONG: {
            int64_t rowVal = ((LongVector *)colPtr)->GetValue(offset);
            int64_t *leftVal = reinterpret_cast<int64_t *>(nonGroupState.val);
            *leftVal = CompareInt64(*leftVal, rowVal) == -1 ? *leftVal : rowVal;
            break;
        }
        case OMNI_VEC_TYPE_DOUBLE: {
            double rowVal = ((DoubleVector *)colPtr)->GetValue(offset);
            double *leftVal = reinterpret_cast<double *>(nonGroupState.val);
            *leftVal = CompareDouble(*leftVal, rowVal) == -1 ? *leftVal : rowVal;
            break;
        }
        default: {
            DebugError("No such data type %d", type);
            break;
        }
    }
}

void MinAggregator::Insert(int64_t key, void *colPtr, int32_t type, uint32_t offset)
{
    switch (type) {
        case OMNI_VEC_TYPE_INT: {
            auto val = std::make_unique<int32_t>(INT32_MAX);
            GroupBySlot slot = { val.release() };
            groupState.insert({ key, slot });
            break;
        }
        case OMNI_VEC_TYPE_LONG: {
            auto val = std::make_unique<int64_t>(INT64_MAX);
            GroupBySlot slot = { val.release() };
            groupState.insert({ key, slot });
            break;
        }
        case OMNI_VEC_TYPE_DOUBLE: {
            auto val = std::make_unique<int64_t>(__DBL_MAX__);
            GroupBySlot slot = { val.release() };
            groupState.insert({ key, slot });
            break;
        }
        default: {
            DebugError("No such data type %d", type);
            break;
        }
    }
}

void MaxAggregator::ProcessGroup(GroupBySlot &groupSlot, void *colPtr, int32_t type, uint32_t offset)
{
    switch (type) {
        case OMNI_VEC_TYPE_INT: {
            int32_t rowVal = ((IntVector *)colPtr)->GetValue(offset);
            int32_t *leftVal = reinterpret_cast<int32_t *>(groupSlot.val);
            *leftVal = CompareInt(*leftVal, rowVal) == 1 ? *leftVal : rowVal;
            break;
        }
        case OMNI_VEC_TYPE_LONG: {
            int64_t rowVal = ((LongVector *)colPtr)->GetValue(offset);
            int64_t *leftVal = reinterpret_cast<int64_t *>(groupSlot.val);
            *leftVal = CompareInt64(*leftVal, rowVal) == 1 ? *leftVal : rowVal;
            break;
        }
        case OMNI_VEC_TYPE_DOUBLE: {
            double rowVal = ((DoubleVector *)colPtr)->GetValue(offset);
            double *leftVal = reinterpret_cast<double *>(groupSlot.val);
            *leftVal = CompareDouble(*leftVal, rowVal) == 1 ? *leftVal : rowVal;
            break;
        }
        default: {
            DebugError("No such data type %d", type);
            break;
        }
    }
}

void MaxAggregator::Initiate(void *colPtr, int32_t type, uint32_t offset)
{
    switch (type) {
        case OMNI_VEC_TYPE_INT: {
            auto curVal = ((IntVector *)colPtr)->GetValue(offset);
            auto val = std::make_unique<int32_t>(curVal);
            nonGroupState = { val.release() };
            break;
        }
        case OMNI_VEC_TYPE_LONG: {
            auto curVal = ((LongVector *)colPtr)->GetValue(offset);
            auto val = std::make_unique<int64_t>(curVal);
            nonGroupState = { val.release() };
            break;
        }
        case OMNI_VEC_TYPE_DOUBLE: {
            auto curVal = ((DoubleVector *)colPtr)->GetValue(offset);
            auto val = std::make_unique<double>(curVal);
            nonGroupState = { val.release() };
            break;
        }
        default: {
            DebugError("No such data type %d", type);
            break;
        }
    }

    initiated = true;
}

void MaxAggregator::ProcessNonGroup(void *colPtr, int32_t type, uint32_t offset)
{
    if (!initiated) {
        Initiate(colPtr, type, offset);
        return;
    }

    switch (type) {
        case OMNI_VEC_TYPE_INT: {
            int32_t rowVal = ((IntVector *)colPtr)->GetValue(offset);
            int32_t *leftVal = reinterpret_cast<int32_t *>(nonGroupState.val);
            *leftVal = CompareInt(*leftVal, rowVal) == 1 ? *leftVal : rowVal;
            break;
        }
        case OMNI_VEC_TYPE_LONG: {
            int64_t rowVal = ((LongVector *)colPtr)->GetValue(offset);
            int64_t *leftVal = reinterpret_cast<int64_t *>(nonGroupState.val);
            *leftVal = CompareInt64(*leftVal, rowVal) == 1 ? *leftVal : rowVal;
            break;
        }
        case OMNI_VEC_TYPE_DOUBLE: {
            double rowVal = ((DoubleVector *)colPtr)->GetValue(offset);
            double *leftVal = reinterpret_cast<double *>(nonGroupState.val);
            *leftVal = CompareDouble(*leftVal, rowVal) == 1 ? *leftVal : rowVal;
            break;
        }
        default: {
            DebugError("No such data type %d", type);
            break;
        }
    }
}

void MaxAggregator::Insert(int64_t key, void *colPtr, int32_t type, uint32_t offset)
{
    switch (type) {
        case OMNI_VEC_TYPE_INT: {
            auto val = std::make_unique<int32_t>(INT32_MIN);
            GroupBySlot slot = { val.release() };
            groupState.insert({ key, slot });
            break;
        }
        case OMNI_VEC_TYPE_LONG: {
            auto val = std::make_unique<int64_t>(INT64_MIN);
            GroupBySlot slot = { val.release() };
            groupState.insert({ key, slot });
            break;
        }
        case OMNI_VEC_TYPE_DOUBLE: {
            auto val = std::make_unique<double>(__DBL_MIN__);
            GroupBySlot slot = { val.release() };
            groupState.insert({ key, slot });
            break;
        }
        default: {
            DebugError("No such data type %d", type);
            break;
        }
    }
}

std::unique_ptr<Aggregator> SumAggregatorFactory::CreateAggregator(int32_t dataType, bool inputRaw, bool outputPartial)
{
    if (dataType >= OMNI_VEC_TYPE_INVALID) {
        throw std::exception();
    }
    return std::make_unique<SumAggregator>(dataType, inputRaw, outputPartial);
}

std::unique_ptr<Aggregator> CountAggregatorFactory::CreateAggregator(int32_t dataType, bool inputRaw, bool outputPartial)
{
    if (dataType >= OMNI_VEC_TYPE_INVALID) {
        throw std::exception();
    }
    return std::make_unique<CountAggregator>(dataType, inputRaw, outputPartial);
}

std::unique_ptr<Aggregator> MinAggregatorFactory::CreateAggregator(int32_t dataType, bool inputRaw, bool outputPartial)
{
    if (dataType >= OMNI_VEC_TYPE_INVALID) {
        throw std::exception();
    }
    return std::make_unique<MinAggregator>(dataType, inputRaw, outputPartial);
}

std::unique_ptr<Aggregator> MaxAggregatorFactory::CreateAggregator(int32_t dataType, bool inputRaw, bool outputPartial)
{
    if (dataType >= OMNI_VEC_TYPE_INVALID) {
        throw std::exception();
    }
    return std::make_unique<MaxAggregator>(dataType, inputRaw, outputPartial);
}

std::unique_ptr<Aggregator> AverageAggregatorFactory::CreateAggregator(int32_t dataType, bool inputRaw, bool outputPartial)
{
    if (dataType >= OMNI_VEC_TYPE_INVALID) {
        throw std::exception();
    }
    return std::make_unique<AverageAggregator>(dataType, inputRaw, outputPartial);
}
} // end of namespace op
} // end of namespace omniruntime
