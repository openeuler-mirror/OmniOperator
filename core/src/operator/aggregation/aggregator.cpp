/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Inner supported aggregators source file
 */
#include "aggregator.h"
#include "../../util/compiler_util.h"

#include <memory>

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

void ALWAYS_INLINE SumAggregator::Insert(GroupBySlot &groupSlot, omniruntime::vec::Vector *colPtr, int32_t type, uint32_t offset)
{
    switch (type) {
        case OMNI_VEC_TYPE_INT: {
            auto curVal = (static_cast<IntVector *>(colPtr))->GetValue(offset);
            auto val = std::make_unique<int32_t>(curVal);
            groupSlot.val = val.release();
            break;
        }
        case OMNI_VEC_TYPE_LONG: {
            auto curVal = (static_cast<LongVector *>(colPtr))->GetValue(offset);
            auto val = std::make_unique<int64_t>(curVal);
            groupSlot.val = val.release();
            break;
        }
        case OMNI_VEC_TYPE_DOUBLE: {
            auto curVal = (static_cast<DoubleVector *>(colPtr))->GetValue(offset);
            auto val = std::make_unique<double>(curVal);
            groupSlot.val = val.release();
            break;
        }
        default: {
            DebugError("No such data type %d", type);
            break;
        }
    }
}

void ALWAYS_INLINE SumAggregator::ProcessGroup(GroupBySlot &groupSlot, omniruntime::vec::Vector *colPtr, int32_t type, uint32_t offset)
{
    switch (type) {
        case OMNI_VEC_TYPE_INT: {
            *(static_cast<int32_t *>(groupSlot.val)) += (static_cast<IntVector *>(colPtr))->GetValue(offset);
            break;
        }
        case OMNI_VEC_TYPE_LONG: {
            *(static_cast<int64_t *>(groupSlot.val)) += (static_cast<LongVector *>(colPtr))->GetValue(offset);
            break;
        }
        case OMNI_VEC_TYPE_DOUBLE: {
            *(static_cast<double *>(groupSlot.val)) += (static_cast<DoubleVector *>(colPtr))->GetValue(offset);
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
            auto curVal = (static_cast<IntVector *>(colPtr))->GetValue(offset);
            auto val = std::make_unique<int32_t>(curVal);
            nonGroupState = { val.release() };
            break;
        }
        case OMNI_VEC_TYPE_LONG: {
            auto curVal = (static_cast<LongVector *>(colPtr))->GetValue(offset);
            auto val = std::make_unique<int64_t>(curVal);
            nonGroupState = { val.release() };
            break;
        }
        case OMNI_VEC_TYPE_DOUBLE: {
            auto curVal = (static_cast<DoubleVector *>(colPtr))->GetValue(offset);
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
            *(static_cast<int32_t *>(nonGroupState.val)) += (static_cast<IntVector *>(colPtr))->GetValue(offset);
            break;
        }
        case OMNI_VEC_TYPE_LONG: {
            *(static_cast<int64_t *>(nonGroupState.val)) += (static_cast<LongVector *>(colPtr))->GetValue(offset);
            break;
        }
        case OMNI_VEC_TYPE_DOUBLE: {
            *(static_cast<double *>(nonGroupState.val)) += (static_cast<DoubleVector *>(colPtr))->GetValue(offset);
            break;
        }
        default: {
            DebugError("No such data type %d", type);
            break;
        }
    }
}

void CountAggregator::ProcessGroup(GroupBySlot &groupSlot, omniruntime::vec::Vector *colPtr, int32_t type, uint32_t offset)
{
    if (inputRaw) {
        groupSlot.count++;
    } else {
        groupSlot.count += (static_cast<LongVector *>(colPtr))->GetValue(offset);
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
    nonGroupState.count = (static_cast<LongVector *>(colPtr))->GetValue(offset);
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

void CountAggregator::Insert(GroupBySlot &groupSlot, omniruntime::vec::Vector *colPtr, int32_t type, uint32_t offset)
{
    if (type != OMNI_VEC_TYPE_LONG) {
        DebugError("Count column type %d is not long!", type);
    }
    if (inputRaw) {
        groupSlot.count = 1;
        return;
    }
    groupSlot.count = (static_cast<LongVector *>(colPtr))->GetValue(offset);
}

void AverageAggregator::Initiate(void *colPtr, int32_t type, uint32_t offset)
{
    auto val = std::make_unique<double>(0.0);
    switch (type) {
        case OMNI_VEC_TYPE_INT: {
            int32_t rowVal = (static_cast<IntVector *>(colPtr))->GetValue(offset);
            *val = rowVal / 1.0;
            nonGroupState = { { val.release(), 1 } };
            break;
        }
        case OMNI_VEC_TYPE_LONG: {
            int64_t rowVal = (static_cast<LongVector *>(colPtr))->GetValue(offset);
            *val = rowVal / 1.0;
            nonGroupState = { { val.release(), 1 } };
            break;
        }
        case OMNI_VEC_TYPE_DOUBLE: {
            double rowVal = (static_cast<DoubleVector *>(colPtr))->GetValue(offset);
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
            int32_t rowVal = (static_cast<IntVector *>(colPtr))->GetValue(offset);
            *currentVal = (rowVal + *currentVal * nonGroupState.avgCnt) / (++nonGroupState.avgCnt);
            break;
        }
        case OMNI_VEC_TYPE_LONG: {
            int64_t rowVal = (static_cast<LongVector *>(colPtr))->GetValue(offset);
            *currentVal = (rowVal + *currentVal * nonGroupState.avgCnt) / (++nonGroupState.avgCnt);
            break;
        }
        case OMNI_VEC_TYPE_DOUBLE: {
            double rowVal = (static_cast<DoubleVector *>(colPtr))->GetValue(offset);
            *currentVal = (rowVal + *currentVal * nonGroupState.avgCnt) / (++nonGroupState.avgCnt);
            break;
        }
        default: {
            DebugError("No such data type %d", type);
            break;
        }
    }
}

void AverageAggregator::Insert(GroupBySlot &groupSlot, omniruntime::vec::Vector *colPtr, int32_t type, uint32_t offset)
{
    auto val = std::make_unique<double>(0.0);
    switch (type) {
        case OMNI_VEC_TYPE_INT: {
            int32_t rowVal = (static_cast<IntVector *>(colPtr))->GetValue(offset);
            *val = rowVal / 1.0;
            groupSlot.avgVal = val.release();
            groupSlot.avgCnt = 1;
            break;
        }
        case OMNI_VEC_TYPE_LONG: {
            int64_t rowVal = (static_cast<LongVector *>(colPtr))->GetValue(offset);
            *val = rowVal / 1.0;
            groupSlot.avgVal = val.release();
            groupSlot.avgCnt = 1;
            break;
        }
        case OMNI_VEC_TYPE_DOUBLE: {
            double rowVal = (static_cast<DoubleVector *>(colPtr))->GetValue(offset);
            *val = rowVal / 1.0;
            groupSlot.avgVal = val.release();
            groupSlot.avgCnt = 1;
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
            groupSlot.avgVal = val.release();
            groupSlot.avgCnt = avgCnt;
            break;
        }
        default: {
            DebugError("No such data type %d", type);
            break;
        }
    }
}

void AverageAggregator::ProcessGroup(GroupBySlot &groupSlot, omniruntime::vec::Vector *colPtr, int32_t type, uint32_t offset)
{
    double *currentVal = static_cast<double *>(groupSlot.avgVal);
    int64_t currentCnt = static_cast<int64_t>(groupSlot.avgCnt);
    switch (type) {
        case OMNI_VEC_TYPE_INT: {
            int32_t rowVal = (static_cast<IntVector *>(colPtr))->GetValue(offset);
            *currentVal = (rowVal + *currentVal * groupSlot.avgCnt) / ++groupSlot.avgCnt;
            break;
        }
        case OMNI_VEC_TYPE_LONG: {
            int64_t rowVal = (static_cast<LongVector *>(colPtr))->GetValue(offset);
            *currentVal = (rowVal + *currentVal * groupSlot.avgCnt) / ++groupSlot.avgCnt;
            break;
        }
        case OMNI_VEC_TYPE_DOUBLE: {
            double rowVal = (static_cast<DoubleVector *>(colPtr))->GetValue(offset);
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

void MinAggregator::ProcessGroup(GroupBySlot &groupSlot, omniruntime::vec::Vector *colPtr, int32_t type, uint32_t offset)
{
    switch (type) {
        case OMNI_VEC_TYPE_INT: {
            int32_t rowVal = (static_cast<IntVector *>(colPtr))->GetValue(offset);
            int32_t *leftVal = reinterpret_cast<int32_t *>(groupSlot.val);
            *leftVal = CompareInt(*leftVal, rowVal) == -1 ? *leftVal : rowVal;
            break;
        }
        case OMNI_VEC_TYPE_LONG: {
            int64_t rowVal = (static_cast<LongVector *>(colPtr))->GetValue(offset);
            int64_t *leftVal = reinterpret_cast<int64_t *>(groupSlot.val);
            *leftVal = CompareInt64(*leftVal, rowVal) == -1 ? *leftVal : rowVal;
            break;
        }
        case OMNI_VEC_TYPE_DOUBLE: {
            double rowVal = (static_cast<DoubleVector *>(colPtr))->GetValue(offset);
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
            auto curVal = (static_cast<IntVector *>(colPtr))->GetValue(offset);
            auto val = std::make_unique<int32_t>(curVal);
            nonGroupState = { val.release() };
            break;
        }
        case OMNI_VEC_TYPE_LONG: {
            auto curVal = (static_cast<LongVector *>(colPtr))->GetValue(offset);
            auto val = std::make_unique<int64_t>(curVal);
            nonGroupState = { val.release() };
            break;
        }
        case OMNI_VEC_TYPE_DOUBLE: {
            auto curVal = (static_cast<DoubleVector *>(colPtr))->GetValue(offset);
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
            int32_t rowVal = (static_cast<IntVector *>(colPtr))->GetValue(offset);
            int32_t *leftVal = reinterpret_cast<int32_t *>(nonGroupState.val);
            *leftVal = CompareInt(*leftVal, rowVal) == -1 ? *leftVal : rowVal;
            break;
        }
        case OMNI_VEC_TYPE_LONG: {
            int64_t rowVal = (static_cast<LongVector *>(colPtr))->GetValue(offset);
            int64_t *leftVal = reinterpret_cast<int64_t *>(nonGroupState.val);
            *leftVal = CompareInt64(*leftVal, rowVal) == -1 ? *leftVal : rowVal;
            break;
        }
        case OMNI_VEC_TYPE_DOUBLE: {
            double rowVal = (static_cast<DoubleVector *>(colPtr))->GetValue(offset);
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

void MinAggregator::Insert(GroupBySlot &groupSlot, omniruntime::vec::Vector *colPtr, int32_t type, uint32_t offset)
{
    switch (type) {
        case OMNI_VEC_TYPE_INT: {
            auto val = std::make_unique<int32_t>(INT32_MAX);
            groupSlot.val = val.release();
            break;
        }
        case OMNI_VEC_TYPE_LONG: {
            auto val = std::make_unique<int64_t>(INT64_MAX);
            groupSlot.val = val.release();
            break;
        }
        case OMNI_VEC_TYPE_DOUBLE: {
            auto val = std::make_unique<int64_t>(__DBL_MAX__);
            groupSlot.val = val.release();
            break;
        }
        default: {
            DebugError("No such data type %d", type);
            break;
        }
    }
}

void MaxAggregator::ProcessGroup(GroupBySlot &groupSlot, omniruntime::vec::Vector *colPtr, int32_t type, uint32_t offset)
{
    switch (type) {
        case OMNI_VEC_TYPE_INT: {
            int32_t rowVal = (static_cast<IntVector *>(colPtr))->GetValue(offset);
            int32_t *leftVal = reinterpret_cast<int32_t *>(groupSlot.val);
            *leftVal = CompareInt(*leftVal, rowVal) == 1 ? *leftVal : rowVal;
            break;
        }
        case OMNI_VEC_TYPE_LONG: {
            int64_t rowVal = (static_cast<LongVector *>(colPtr))->GetValue(offset);
            int64_t *leftVal = reinterpret_cast<int64_t *>(groupSlot.val);
            *leftVal = CompareInt64(*leftVal, rowVal) == 1 ? *leftVal : rowVal;
            break;
        }
        case OMNI_VEC_TYPE_DOUBLE: {
            double rowVal = (static_cast<DoubleVector *>(colPtr))->GetValue(offset);
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
            auto curVal = (static_cast<IntVector *>(colPtr))->GetValue(offset);
            auto val = std::make_unique<int32_t>(curVal);
            nonGroupState = { val.release() };
            break;
        }
        case OMNI_VEC_TYPE_LONG: {
            auto curVal = (static_cast<LongVector *>(colPtr))->GetValue(offset);
            auto val = std::make_unique<int64_t>(curVal);
            nonGroupState = { val.release() };
            break;
        }
        case OMNI_VEC_TYPE_DOUBLE: {
            auto curVal = (static_cast<DoubleVector *>(colPtr))->GetValue(offset);
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
            int32_t rowVal = (static_cast<IntVector *>(colPtr))->GetValue(offset);
            int32_t *leftVal = reinterpret_cast<int32_t *>(nonGroupState.val);
            *leftVal = CompareInt(*leftVal, rowVal) == 1 ? *leftVal : rowVal;
            break;
        }
        case OMNI_VEC_TYPE_LONG: {
            int64_t rowVal = (static_cast<LongVector *>(colPtr))->GetValue(offset);
            int64_t *leftVal = reinterpret_cast<int64_t *>(nonGroupState.val);
            *leftVal = CompareInt64(*leftVal, rowVal) == 1 ? *leftVal : rowVal;
            break;
        }
        case OMNI_VEC_TYPE_DOUBLE: {
            double rowVal = (static_cast<DoubleVector *>(colPtr))->GetValue(offset);
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

void MaxAggregator::Insert(GroupBySlot &groupSlot, omniruntime::vec::Vector *colPtr, int32_t type, uint32_t offset)
{
    switch (type) {
        case OMNI_VEC_TYPE_INT: {
            auto val = std::make_unique<int32_t>(INT32_MIN);
            groupSlot.val = val.release();
            break;
        }
        case OMNI_VEC_TYPE_LONG: {
            auto val = std::make_unique<int64_t>(INT64_MIN);
            groupSlot.val = val.release();
            break;
        }
        case OMNI_VEC_TYPE_DOUBLE: {
            auto val = std::make_unique<double>(__DBL_MIN__);
            groupSlot.val = val.release();
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
