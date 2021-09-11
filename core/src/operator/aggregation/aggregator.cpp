/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Inner supported aggregators source file
 */
#include "aggregator.h"
#include "../../util/compiler_util.h"
#include "../util/operator_util.h"

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

bool Aggregator::IsInputRaw() const
{
    return this->inputRaw;
}

bool Aggregator::IsOutputPartial() const
{
    return this->outputPartial;
}

void ALWAYS_INLINE SumAggregator::Insert(GroupBySlot &groupSlot, omniruntime::vec::Vector *colPtr, int32_t type,
    uint32_t offset)
{
    switch (type) {
        case OMNI_VEC_TYPE_INT:
        case OMNI_VEC_TYPE_DATE32: {
            auto curVal = (static_cast<IntVector *>(colPtr))->GetValue(offset);
            auto val = std::make_unique<int32_t>(curVal);
            groupSlot.val = val.release();
            break;
        }
        case OMNI_VEC_TYPE_LONG:
        case OMNI_VEC_TYPE_DECIMAL64:
         {
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
        case OMNI_VEC_TYPE_DECIMAL128: {
            Decimal128 curVal = (static_cast<Decimal128Vector *>(colPtr))->GetValue(offset);
            auto val = std::make_unique<Decimal128>(curVal.HighBits(), curVal.LowBits());
            groupSlot.val = val.release();
            break;
        }
        default: {
            DebugError("No such data type %d", type);
            break;
        }
    }
}

void ALWAYS_INLINE SumAggregator::ProcessGroup(GroupBySlot &groupSlot, omniruntime::vec::Vector *colPtr, int32_t type,
    uint32_t offset)
{
    int32_t rowIdx = offset;
    colPtr = VectorHelper::GetDictionary(colPtr, rowIdx);
    if (colPtr->IsValueNull(rowIdx)) {
        return;
    }
    switch (type) {
        case OMNI_VEC_TYPE_INT:
        case OMNI_VEC_TYPE_DATE32: {
            *(static_cast<int32_t *>(groupSlot.val)) += (static_cast<IntVector *>(colPtr))->GetValue(rowIdx);
            break;
        }
        case OMNI_VEC_TYPE_LONG:
        case OMNI_VEC_TYPE_DECIMAL64:
         {
            *(static_cast<int64_t *>(groupSlot.val)) += (static_cast<LongVector *>(colPtr))->GetValue(rowIdx);
            break;
        }
        case OMNI_VEC_TYPE_DOUBLE: {
            *(static_cast<double *>(groupSlot.val)) += (static_cast<DoubleVector *>(colPtr))->GetValue(rowIdx);
            break;
        }
        case OMNI_VEC_TYPE_DECIMAL128: {
            *(static_cast<Decimal128*>(groupSlot.val)) += (static_cast<Decimal128Vector*>(colPtr)->GetValue(rowIdx));
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
        case OMNI_VEC_TYPE_INT:
        case OMNI_VEC_TYPE_DATE32: {
            auto curVal = (static_cast<IntVector *>(colPtr))->GetValue(offset);
            auto val = std::make_unique<int32_t>(curVal);
            nonGroupState = { val.release() };
            break;
        }
        case OMNI_VEC_TYPE_LONG:
        case OMNI_VEC_TYPE_DECIMAL64:{
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
        case OMNI_VEC_TYPE_DECIMAL128: {
            auto curVal = (static_cast<Decimal128Vector*>(colPtr))->GetValue(offset);
            auto val = std::make_unique<Decimal128>(curVal.HighBits(), curVal.LowBits());
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
        case OMNI_VEC_TYPE_INT:
        case OMNI_VEC_TYPE_DATE32: {
            *(static_cast<int32_t *>(nonGroupState.val)) += (static_cast<IntVector *>(colPtr))->GetValue(offset);
            break;
        }
        case OMNI_VEC_TYPE_LONG:
        case OMNI_VEC_TYPE_DECIMAL64: {
            *(static_cast<int64_t *>(nonGroupState.val)) += (static_cast<LongVector *>(colPtr))->GetValue(offset);
            break;
        }
        case OMNI_VEC_TYPE_DOUBLE: {
            *(static_cast<double *>(nonGroupState.val)) += (static_cast<DoubleVector *>(colPtr))->GetValue(offset);
            break;
        }
        case OMNI_VEC_TYPE_DECIMAL128: {
            *(static_cast<Decimal128*>(nonGroupState.val)) += (static_cast<Decimal128Vector*>(colPtr))
                ->GetValue(offset);
            break;
        }
        default: {
            DebugError("No such data type %d", type);
            break;
        }
    }
}

void CountAggregator::ProcessGroup(GroupBySlot &groupSlot, omniruntime::vec::Vector *colPtr, int32_t type,
    uint32_t offset)
{
    if (colPtr->IsValueNull(offset)) {
        return;
    }
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
        case OMNI_VEC_TYPE_INT:
        case OMNI_VEC_TYPE_DATE32: {
            int32_t rowVal = (static_cast<IntVector *>(colPtr))->GetValue(offset);
            *val = rowVal / 1.0;
            nonGroupState = { { val.release(), 1 } };
            break;
        }
        case OMNI_VEC_TYPE_LONG:
        case OMNI_VEC_TYPE_DECIMAL64: {
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
        case OMNI_VEC_TYPE_DECIMAL128: {
            Decimal128 rowVal = (static_cast<Decimal128Vector*>(colPtr))->GetValue(offset);
            auto newVal = std::make_unique<Decimal128>(rowVal.HighBits(), rowVal.LowBits());
            nonGroupState = { { newVal.release(), 1 } };
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
    switch (type) {
        case OMNI_VEC_TYPE_INT:
        case OMNI_VEC_TYPE_DATE32: {
            auto currentVal = static_cast<double *>(nonGroupState.avgVal);
            int32_t rowVal = (static_cast<IntVector *>(colPtr))->GetValue(offset);
            *currentVal = (rowVal + *currentVal * nonGroupState.avgCnt) / (++nonGroupState.avgCnt);
            break;
        }
        case OMNI_VEC_TYPE_LONG:
        case OMNI_VEC_TYPE_DECIMAL64: {
            auto currentVal = static_cast<double *>(nonGroupState.avgVal);
            int64_t rowVal = (static_cast<LongVector *>(colPtr))->GetValue(offset);
            *currentVal = (rowVal + *currentVal * nonGroupState.avgCnt) / (++nonGroupState.avgCnt);
            break;
        }
        case OMNI_VEC_TYPE_DOUBLE: {
            auto currentVal = static_cast<double *>(nonGroupState.avgVal);
            double rowVal = (static_cast<DoubleVector *>(colPtr))->GetValue(offset);
            *currentVal = (rowVal + *currentVal * nonGroupState.avgCnt) / (++nonGroupState.avgCnt);
            break;
        }
        case OMNI_VEC_TYPE_DECIMAL128: {
            Decimal128* currentVal = static_cast<Decimal128*>(nonGroupState.avgVal);
            Decimal128 rowVal = (static_cast<Decimal128Vector*>(colPtr))->GetValue(offset);
            *currentVal = (rowVal + *currentVal * nonGroupState.avgCnt) / (++nonGroupState.avgCnt);
            break;
        }
        default: {
            DebugError("No such data type %d", type);
            break;
        }
    }
}

void ALWAYS_INLINE InsertIntermediateAvg(GroupBySlot &groupSlot, omniruntime::vec::Vector *colPtr, uint32_t offset)
{
    auto containerVector = static_cast<ContainerVector *>(colPtr);
    DoubleVector *avgValVector = reinterpret_cast<DoubleVector *>(containerVector->getValue(0));
    double avgVal = avgValVector->GetValue(offset);
    LongVector *avgCountVector = reinterpret_cast<LongVector *>(containerVector->getValue(1));
    int64_t avgCnt = avgCountVector->GetValue(offset);
    if (avgCnt == 0) {
        throw "Divisor should not be zero!";
    }
    groupSlot.avgVal = std::make_unique<double>(avgVal * avgCnt / avgCnt).release();
    groupSlot.avgCnt = avgCnt;
}

void AverageAggregator::Insert(GroupBySlot &groupSlot, omniruntime::vec::Vector *colPtr, int32_t type, uint32_t offset)
{
    switch (type) {
        case OMNI_VEC_TYPE_INT:
        case OMNI_VEC_TYPE_DATE32: {
            int32_t rowVal = (static_cast<IntVector *>(colPtr))->GetValue(offset);
            groupSlot.avgVal = std::make_unique<double>(rowVal / 1.0).release();
            groupSlot.avgCnt = 1;
            break;
        }
        case OMNI_VEC_TYPE_LONG:
        case OMNI_VEC_TYPE_DECIMAL64: {
            int64_t rowVal = (static_cast<LongVector *>(colPtr))->GetValue(offset);
            groupSlot.avgVal = std::make_unique<double>(rowVal / 1.0).release();
            groupSlot.avgCnt = 1;
            break;
        }
        case OMNI_VEC_TYPE_DOUBLE: {
            double rowVal = (static_cast<DoubleVector *>(colPtr))->GetValue(offset);
            groupSlot.avgVal = std::make_unique<double>(rowVal / 1.0).release();
            groupSlot.avgCnt = 1;
            break;
        }
        case OMNI_VEC_TYPE_CONTAINER: {
            InsertIntermediateAvg(groupSlot, colPtr, offset);
            break;
        }
        case OMNI_VEC_TYPE_DECIMAL128: {
            Decimal128 rowVal = (static_cast<Decimal128Vector *>(colPtr))->GetValue(offset);
            groupSlot.avgVal = std::make_unique<Decimal128>(rowVal).release();
            groupSlot.avgCnt = 1;
            break;
        }
        default: {
            DebugError("No such data type %d", type);
            break;
        }
    }
}

void ALWAYS_INLINE ProcessIntermediateAvg(GroupBySlot &groupSlot, omniruntime::vec::Vector *colPtr, uint32_t offset)
{
    double *currentVal = static_cast<double *>(groupSlot.avgVal);
    auto currentCnt = static_cast<int64_t>(groupSlot.avgCnt);
    auto containerVector = static_cast<ContainerVector *>(colPtr);
    DoubleVector *avgValVector = reinterpret_cast<DoubleVector *>(containerVector->getValue(0));
    double avgVal = avgValVector->GetValue(offset);
    LongVector *avgCountVector = reinterpret_cast<LongVector *>(containerVector->getValue(1));
    int64_t avgCnt = avgCountVector->GetValue(offset);
    if (avgCnt == 0) {
        throw "Divisor should not be zero!";
    }
    groupSlot.avgCnt += avgCnt;
    *currentVal = (avgVal * avgCnt + *currentVal * currentCnt) / groupSlot.avgCnt;
}

void AverageAggregator::ProcessGroup(GroupBySlot &groupSlot, omniruntime::vec::Vector *colPtr, int32_t type,
    uint32_t offset)
{
    if (colPtr->IsValueNull(offset)) {
        return;
    }

    switch (type) {
        case OMNI_VEC_TYPE_INT:
        case OMNI_VEC_TYPE_DATE32: {
            auto currentVal = static_cast<double *>(groupSlot.avgVal);
            auto sum = (static_cast<IntVector *>(colPtr))->GetValue(offset) + *currentVal * groupSlot.avgCnt;
            *currentVal = sum / ++groupSlot.avgCnt;
            break;
        }
        case OMNI_VEC_TYPE_LONG:
        case OMNI_VEC_TYPE_DECIMAL64: {
            auto currentVal = static_cast<double *>(groupSlot.avgVal);
            auto sum = (static_cast<LongVector *>(colPtr))->GetValue(offset) + *currentVal * groupSlot.avgCnt;
            *currentVal = sum / ++groupSlot.avgCnt;
            break;
        }
        case OMNI_VEC_TYPE_DOUBLE: {
            auto currentVal = static_cast<double *>(groupSlot.avgVal);
            auto sum = (static_cast<DoubleVector *>(colPtr))->GetValue(offset) + *currentVal * groupSlot.avgCnt;
            *currentVal = sum / ++groupSlot.avgCnt;
            break;
        }
        case OMNI_VEC_TYPE_CONTAINER: {
            ProcessIntermediateAvg(groupSlot, colPtr, offset);
            break;
        }
        case OMNI_VEC_TYPE_DECIMAL128: {
            auto currentVal = static_cast<Decimal128 *>(groupSlot.avgVal);
            auto sum = static_cast<Decimal128Vector*>(colPtr)->GetValue(offset) + *currentVal * groupSlot.avgCnt;
            *currentVal = sum / ++groupSlot.avgCnt;
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
        case OMNI_VEC_TYPE_INT:
        case OMNI_VEC_TYPE_DATE32: {
            auto rowVal = static_cast<IntVector*>(colPtr)->GetValue(offset);
            auto val = std::make_unique<int32_t>(rowVal);
            groupSlot.val = val.release();
            break;
        }
        case OMNI_VEC_TYPE_LONG:
        case OMNI_VEC_TYPE_DECIMAL64: {
            auto rowVal = static_cast<LongVector*>(colPtr)->GetValue(offset);
            auto val = std::make_unique<int64_t>(rowVal);
            groupSlot.val = val.release();
            break;
        }
        case OMNI_VEC_TYPE_DOUBLE: {
            auto rowVal = static_cast<DoubleVector*>(colPtr)->GetValue(offset);
            auto val = std::make_unique<int64_t>(rowVal);
            groupSlot.val = val.release();
            break;
        }
        case OMNI_VEC_TYPE_DECIMAL128: {
            auto rowVal = static_cast<Decimal128Vector*>(colPtr)->GetValue(offset);
            auto val = std::make_unique<Decimal128>(rowVal);
            groupSlot.val = val.release();
            break;
        }
        case OMNI_VEC_TYPE_VARCHAR: {
            uint8_t *data = nullptr;
            int valLen = static_cast<VarcharVector*>(colPtr)->GetValue(offset, &data);
            auto val = std::make_unique<std::string>(reinterpret_cast<char*>(data), valLen);
            groupSlot.val = val.release();
            break;
        }
        default: {
            DebugError("No such data type %d", type);
            break;
        }
    }
}

void MinAggregator::ProcessGroup(GroupBySlot &groupSlot, omniruntime::vec::Vector *colPtr, int32_t type,
    uint32_t offset)
{
    if (colPtr->IsValueNull(offset)) {
        return;
    }

    switch (type) {
        case OMNI_VEC_TYPE_INT:
        case OMNI_VEC_TYPE_DATE32: {
            int32_t rowVal = (static_cast<IntVector *>(colPtr))->GetValue(offset);
            auto leftVal = static_cast<int32_t *>(groupSlot.val);
            *leftVal = (Compare(*leftVal, rowVal) == -1) ? *leftVal : rowVal;
            break;
        }
        case OMNI_VEC_TYPE_LONG:
        case OMNI_VEC_TYPE_DECIMAL64: {
            int64_t rowVal = (static_cast<LongVector *>(colPtr))->GetValue(offset);
            auto leftVal = static_cast<int64_t *>(groupSlot.val);
            *leftVal = (Compare(*leftVal, rowVal) == -1) ? *leftVal : rowVal;
            break;
        }
        case OMNI_VEC_TYPE_DOUBLE: {
            double rowVal = (static_cast<DoubleVector *>(colPtr))->GetValue(offset);
            auto leftVal = static_cast<double *>(groupSlot.val);
            *leftVal = (Compare(*leftVal, rowVal) == -1) ? *leftVal : rowVal;
            break;
        }
        case OMNI_VEC_TYPE_DECIMAL128: {
            Decimal128 rowVal = (static_cast<Decimal128Vector*>(colPtr))->GetValue(offset);
            auto leftVal = static_cast<Decimal128*>(groupSlot.val);
            *leftVal = (Compare(*leftVal, rowVal) == -1) ? *leftVal : rowVal;
            break;
        }
        case OMNI_VEC_TYPE_VARCHAR: {
            uint8_t *data = nullptr;
            int valLen = (static_cast<VarcharVector *>(colPtr))->GetValue(offset, &data);
            std::string rowVal(reinterpret_cast<char *>(data), valLen);
            auto leftVal = static_cast<std::string *>(groupSlot.val);
            *leftVal = (Compare(*leftVal, rowVal) == -1) ? *leftVal : rowVal;
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
        case OMNI_VEC_TYPE_INT:
        case OMNI_VEC_TYPE_DATE32: {
            auto curVal = (static_cast<IntVector *>(colPtr))->GetValue(offset);
            auto val = std::make_unique<int32_t>(curVal);
            nonGroupState = { val.release() };
            break;
        }
        case OMNI_VEC_TYPE_LONG:
        case OMNI_VEC_TYPE_DECIMAL64: {
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
        case OMNI_VEC_TYPE_DECIMAL128: {
            auto curVal = static_cast<Decimal128Vector*>(colPtr)->GetValue(offset);
            auto val = std::make_unique<Decimal128>(curVal);
            nonGroupState = { val.release() };
            break;
        }
        case OMNI_VEC_TYPE_VARCHAR: {
            uint8_t *data = nullptr;
            int valLen = (static_cast<VarcharVector *>(colPtr))->GetValue(offset, &data);
            auto val = std::make_unique<std::string>(reinterpret_cast<char *>(data), 0, valLen);
            nonGroupState = {val.release()};
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
        case OMNI_VEC_TYPE_INT:
        case OMNI_VEC_TYPE_DATE32: {
            int32_t rowVal = (static_cast<IntVector *>(colPtr))->GetValue(offset);
            auto leftVal = static_cast<int32_t *>(nonGroupState.val);
            *leftVal = (Compare(*leftVal, rowVal) == -1) ? *leftVal : rowVal;
            break;
        }
        case OMNI_VEC_TYPE_LONG:
        case OMNI_VEC_TYPE_DECIMAL64: {
            int64_t rowVal = (static_cast<LongVector *>(colPtr))->GetValue(offset);
            auto leftVal = static_cast<int64_t *>(nonGroupState.val);
            *leftVal = (Compare(*leftVal, rowVal) == -1) ? *leftVal : rowVal;
            break;
        }
        case OMNI_VEC_TYPE_DOUBLE: {
            double rowVal = (static_cast<DoubleVector *>(colPtr))->GetValue(offset);
            auto leftVal = static_cast<double *>(nonGroupState.val);
            *leftVal = (Compare(*leftVal, rowVal) == -1) ? *leftVal : rowVal;
            break;
        }
        case OMNI_VEC_TYPE_DECIMAL128: {
            Decimal128 rowVal = static_cast<Decimal128Vector*>(colPtr)->GetValue(offset);
            auto leftVal = static_cast<Decimal128*>(nonGroupState.val);
            *leftVal = (Compare(*leftVal, rowVal) == -1) ? *leftVal : rowVal;
            break;
        }
        case OMNI_VEC_TYPE_VARCHAR: {
            uint8_t *data = nullptr;
            int valLen = (static_cast<VarcharVector *>(colPtr))->GetValue(offset, &data);
            std::string rowVal(reinterpret_cast<char *>(data), 0, valLen);
            auto leftVal = static_cast<std::string *>(nonGroupState.val);
            *leftVal = (Compare(*leftVal, rowVal) == -1) ? *leftVal : rowVal;
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
        case OMNI_VEC_TYPE_INT:
        case OMNI_VEC_TYPE_DATE32: {
            auto rowVal = static_cast<IntVector*>(colPtr)->GetValue(offset);
            auto val = std::make_unique<int32_t>(rowVal);
            groupSlot.val = val.release();
            break;
        }
        case OMNI_VEC_TYPE_LONG:
        case OMNI_VEC_TYPE_DECIMAL64: {
            auto rowVal = static_cast<LongVector*>(colPtr)->GetValue(offset);
            auto val = std::make_unique<int64_t>(rowVal);
            groupSlot.val = val.release();
            break;
        }
        case OMNI_VEC_TYPE_DOUBLE: {
            auto rowVal = static_cast<DoubleVector*>(colPtr)->GetValue(offset);
            auto val = std::make_unique<double>(rowVal);
            groupSlot.val = val.release();
            break;
        }
        case OMNI_VEC_TYPE_DECIMAL128: {
            auto rowVal = static_cast<Decimal128Vector*>(colPtr)->GetValue(offset);
            auto val = std::make_unique<Decimal128>(rowVal);
            groupSlot.val = val.release();
            break;
        }
        case OMNI_VEC_TYPE_VARCHAR: {
            uint8_t *data = nullptr;
            int valLen = static_cast<VarcharVector *>(colPtr)->GetValue(offset, &data);
            auto val = std::make_unique<std::string>(reinterpret_cast<char *>(data), 0, valLen);
            groupSlot.val = val.release();
            break;
        }
        default: {
            DebugError("No such data type %d", type);
            break;
        }
    }
}

void MaxAggregator::ProcessGroup(GroupBySlot &groupSlot, omniruntime::vec::Vector *colPtr, int32_t type,
    uint32_t offset)
{
    if (colPtr->IsValueNull(offset)) {
        return;
    }

    switch (type) {
        case OMNI_VEC_TYPE_INT:
        case OMNI_VEC_TYPE_DATE32: {
            int32_t rowVal = (static_cast<IntVector *>(colPtr))->GetValue(offset);
            auto leftVal = static_cast<int32_t *>(groupSlot.val);
            *leftVal = (Compare(*leftVal, rowVal) == 1) ? *leftVal : rowVal;
            break;
        }
        case OMNI_VEC_TYPE_LONG:
        case OMNI_VEC_TYPE_DECIMAL64: {
            int64_t rowVal = (static_cast<LongVector *>(colPtr))->GetValue(offset);
            auto leftVal = static_cast<int64_t *>(groupSlot.val);
            *leftVal = (Compare(*leftVal, rowVal) == 1) ? *leftVal : rowVal;
            break;
        }
        case OMNI_VEC_TYPE_DOUBLE: {
            double rowVal = (static_cast<DoubleVector *>(colPtr))->GetValue(offset);
            auto leftVal = static_cast<double *>(groupSlot.val);
            *leftVal = (Compare(*leftVal, rowVal) == 1) ? *leftVal : rowVal;
            break;
        }
        case OMNI_VEC_TYPE_DECIMAL128: {
            Decimal128 rowVal = (static_cast<Decimal128Vector *>(colPtr))->GetValue(offset);
            auto leftVal = static_cast<Decimal128 *>(groupSlot.val);
            *leftVal = (Compare(*leftVal, rowVal) == 1) ? *leftVal : rowVal;
            break;
        }
        case OMNI_VEC_TYPE_VARCHAR: {
            uint8_t *data = nullptr;
            int valLen = (static_cast<VarcharVector *>(colPtr))->GetValue(offset, &data);
            std::string rowVal(reinterpret_cast<char *>(data), 0, valLen);
            auto leftVal = static_cast<std::string *>(groupSlot.val);
            *leftVal = (Compare(*leftVal, rowVal) == 1) ? *leftVal : rowVal;
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
        case OMNI_VEC_TYPE_INT:
        case OMNI_VEC_TYPE_DATE32: {
            auto curVal = (static_cast<IntVector *>(colPtr))->GetValue(offset);
            auto val = std::make_unique<int32_t>(curVal);
            nonGroupState = { val.release() };
            break;
        }
        case OMNI_VEC_TYPE_LONG:
        case OMNI_VEC_TYPE_DECIMAL64: {
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
        case OMNI_VEC_TYPE_VARCHAR: {
            uint8_t *data = nullptr;
            int valLen = ((VarcharVector*) colPtr)->GetValue(offset, &data);
            auto val = std::make_unique<std::string>(reinterpret_cast<char*>(data), valLen);
            nonGroupState = { val.release() };
            break;
        }
        case OMNI_VEC_TYPE_DECIMAL128: {
            auto curVal = (static_cast<Decimal128Vector *>(colPtr))->GetValue(offset);
            auto val = std::make_unique<Decimal128>(curVal);
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
        case OMNI_VEC_TYPE_INT:
        case OMNI_VEC_TYPE_DATE32: {
            int32_t rowVal = (static_cast<IntVector *>(colPtr))->GetValue(offset);
            auto leftVal = static_cast<int32_t *>(nonGroupState.val);
            *leftVal = (Compare(*leftVal, rowVal) == 1) ? *leftVal : rowVal;
            break;
        }
        case OMNI_VEC_TYPE_LONG:
        case OMNI_VEC_TYPE_DECIMAL64:{
            int64_t rowVal = (static_cast<LongVector *>(colPtr))->GetValue(offset);
            auto *leftVal = static_cast<int64_t *>(nonGroupState.val);
            *leftVal = (Compare(*leftVal, rowVal) == 1) ? *leftVal : rowVal;
            break;
        }
        case OMNI_VEC_TYPE_DOUBLE: {
            double rowVal = (static_cast<DoubleVector *>(colPtr))->GetValue(offset);
            auto *leftVal = static_cast<double *>(nonGroupState.val);
            *leftVal = (Compare(*leftVal, rowVal) == 1) ? *leftVal : rowVal;
            break;
        }
        case OMNI_VEC_TYPE_DECIMAL128: {
            Decimal128 rowVal = (static_cast<Decimal128Vector *>(colPtr))->GetValue(offset);
            auto leftVal = static_cast<Decimal128 *>(nonGroupState.val);
            *leftVal = (Compare(*leftVal, rowVal)) == 1 ? *leftVal : rowVal;
            break;
        }
        case OMNI_VEC_TYPE_VARCHAR: {
            uint8_t *data = nullptr;
            int valLen = ((VarcharVector *) colPtr)->GetValue(offset, &data);
            std::string rowVal(reinterpret_cast<char*>(data), valLen);
            auto leftVal = static_cast<std::string*>(nonGroupState.val);
            *leftVal = (Compare(*leftVal, rowVal) == 1) ? *leftVal : rowVal;
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

std::unique_ptr<Aggregator> CountAggregatorFactory::CreateAggregator(int32_t dataType, bool inputRaw,
    bool outputPartial)
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

std::unique_ptr<Aggregator> AverageAggregatorFactory::CreateAggregator(int32_t dataType, bool inputRaw,
    bool outputPartial)
{
    if (dataType >= OMNI_VEC_TYPE_INVALID) {
        throw std::exception();
    }
    return std::make_unique<AverageAggregator>(dataType, inputRaw, outputPartial);
}
} // end of namespace op
} // end of namespace omniruntime
