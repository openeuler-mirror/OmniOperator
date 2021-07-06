#include "aggregator.h"
#include "../../util/debug.h"
#include "../../vector/vector_common.h"

namespace omniruntime {
namespace op {

int32_t cmp_int(int32_t leftVal, int32_t rightVal)
{
    return (leftVal > rightVal ? 1 : (leftVal < rightVal ? -1 : 0));
}

int32_t cmp_int64(int64_t leftVal, int64_t rightVal)
{
    return (leftVal > rightVal ? 1 : (leftVal < rightVal ? -1 : 0));
}

int32_t cmp_double(double leftVal, double rightVal)
{
    return (leftVal > rightVal ? 1 : (leftVal < rightVal ? -1 : 0));
}

bool Aggregator::isInputRaw() {
    return this->inputRaw;
}

bool Aggregator::isOutputPartial() {
    return this->outputPartial;
}

void SumAggregator::processGroup(GroupBySlot& groupSlot, void* colPtr, int32_t type, uint32_t offset)
{
    switch (type)
    {
        case 1: {
            *((int32_t*)(groupSlot.val)) += ((IntVector *)colPtr)->getValue(offset);
            break; 
        }
        case 2: {
            *((int64_t*)(groupSlot.val)) += ((LongVector *)colPtr)->getValue(offset);
            break; 
        }
        case 3: {
            *((double*)(groupSlot.val)) += ((DoubleVector *)colPtr)->getValue(offset);
            break; 
        }
        default: {
            DebugError("No such data type %d", type);
            break;
        }
    }
}

void SumAggregator::initiate(void* colPtr, int32_t type, uint32_t offset)
{
    switch (type) 
    {
        case 1: {
            int32_t* val = new int32_t;
            *val = ((IntVector *)colPtr)->getValue(offset);
            nonGroupState = {val};
            break; 
        }
        case 2: {
            int64_t* val = new int64_t;
            *val = ((LongVector *)colPtr)->getValue(offset);
            nonGroupState = {val};
            break; 
        }
        case 3: {
            double* val = new double;
            *val = ((DoubleVector *)colPtr)->getValue(offset);
            nonGroupState = {val};
            break; 
        }
        default: {
            DebugError("No such data type %d", type);
            break;
        }
    }

    initiated = true;
}

void SumAggregator::processNonGroup(void* colPtr, int32_t type, uint32_t offset)
{
    if (!initiated) {
        initiate(colPtr, type, offset);
        return;
    }

    switch (type)
    {
        case 1: {
            *((int32_t*)nonGroupState.val) += ((IntVector *)colPtr)->getValue(offset);
            break; 
        }
        case 2: {
            *((int64_t*)nonGroupState.val) += ((LongVector *)colPtr)->getValue(offset);
            break; 
        }
        case 3: {
            *((double*)nonGroupState.val) += ((DoubleVector *)colPtr)->getValue(offset);
            break; 
        }
        default: {
            DebugError("No such data type %d", type);
            break;
        }
    }
}

void SumAggregator::insert(int64_t key, void* colPtr, int32_t type, uint32_t offset)
{
    switch (type)
    {
        case 1: {
            int32_t* val = new int32_t;
            *val = ((IntVector *)colPtr)->getValue(offset);
            GroupBySlot slot = {val};
            groupState.insert({key, slot});
            break; 
        }
        case 2: {
            int64_t* val = new int64_t;
            *val = ((LongVector *)colPtr)->getValue(offset);
            GroupBySlot slot = {val};
            groupState.insert({key, slot});
            break; 
        }
        case 3: {
            double* val = new double;
            *val = ((DoubleVector *)colPtr)->getValue(offset);
            GroupBySlot slot = {val};
            groupState.insert({key, slot});
            break; 
        }
        default: {
            DebugError("No such data type %d", type);
            break;
        }
    }
}

void CountAggregator::processGroup(GroupBySlot& groupSlot, void* colPtr, int32_t type, uint32_t offset)
{
    if (inputRaw) {
        groupSlot.count++;
    } else {
        groupSlot.count += ((LongVector*)colPtr)->getValue(offset);
    }
}

void CountAggregator::initiate(void* colPtr, int32_t type, uint32_t offset)
{
    if (type != 2) {
        DebugError("Count column type %d is not long!", type);
    }
    if (inputRaw) {
        nonGroupState.count = 1;
        initiated = true;
        return;
    }
    nonGroupState.count = ((LongVector*)colPtr)->getValue(offset);
    initiated = true;
}

void CountAggregator::processNonGroup(void* colPtr, int32_t type, uint32_t offset)
{
    if (!initiated) {
        initiate(colPtr, type, offset);
        return;
    }
    if (inputRaw) {
        nonGroupState.count++;
    } else {
        nonGroupState.count += *(reinterpret_cast<int64_t*>(colPtr) + offset);
    }
}

void CountAggregator::insert(int64_t key, void* colPtr, int32_t type, uint32_t offset)
{
    if (type != 2) {
        DebugError("Count column type %d is not long!", type);
    }
    GroupBySlot slot;
    if (inputRaw) {
        slot.count = 1;
        groupState.insert({key, slot});
        return;
    }
    slot.count = ((LongVector*)colPtr)->getValue(offset);
    groupState.insert({key, slot});
}

void AverageAggregator::initiate(void* colPtr, int32_t type, uint32_t offset)
{
    double* val = new double;
    switch (type)
    {
        case 1: {
            int32_t rowVal = ((IntVector *)colPtr)->getValue(offset);
            *val = rowVal / 1.0;
            nonGroupState = {{val, 1}};
            break; 
        }
        case 2: {
            int64_t rowVal = ((LongVector *)colPtr)->getValue(offset);
            *val = rowVal / 1.0;
            nonGroupState = {{val, 1}};
            break; 
        }
        case 3: {
            double rowVal = ((DoubleVector *)colPtr)->getValue(offset);
            *val = rowVal / 1.0;
            nonGroupState = {{val, 1}};
            break; 
        }
        default: {
            DebugError("No such data type %d", type);
            break;
        }
    }

    initiated = true;
}

void AverageAggregator::processNonGroup(void* colPtr, int32_t type, uint32_t offset)
{
    if (!initiated) {
        initiate(colPtr, type, offset);
        return;
    }
    double* currentVal = static_cast<double*>(nonGroupState.avgVal);
    switch (type)
    {
        case 1: {
            int32_t rowVal = ((IntVector *)colPtr)->getValue(offset);
            *currentVal = (rowVal + *currentVal * nonGroupState.avgCnt) / (++nonGroupState.avgCnt);
            break;
        }
        case 2: {
            int64_t rowVal = ((LongVector *)colPtr)->getValue(offset);
            *currentVal = (rowVal + *currentVal * nonGroupState.avgCnt) / (++nonGroupState.avgCnt);
            break;
        }
        case 3: {
            double rowVal = ((DoubleVector *)colPtr)->getValue(offset);
            *currentVal = (rowVal + *currentVal * nonGroupState.avgCnt) / (++nonGroupState.avgCnt);
            break;
        }
        default: {
            DebugError("No such data type %d", type);
            break;
        }
    }
}

void AverageAggregator::insert(int64_t key, void* colPtr, int32_t type, uint32_t offset)
{
    double* val = new double;
    switch (type)
    {
        case 1: {
            int32_t rowVal = ((IntVector *)colPtr)->getValue(offset);
            *val = rowVal / 1.0;
            GroupBySlot slot = {{val, 1}};
            groupState.insert({key, slot});
            break;
        }
        case 2: {
            int64_t rowVal = ((LongVector *)colPtr)->getValue(offset);
            *val = rowVal / 1.0;
            GroupBySlot slot = {{val, 1}};
            groupState.insert({key, slot});
            break;
        }
        case 3: {
            double rowVal = ((DoubleVector *)colPtr)->getValue(offset);
            *val = rowVal / 1.0;
            GroupBySlot slot = {{val, 1}};
            groupState.insert({key, slot});
            break;
        }
        default: {
            DebugError("No such data type %d", type);
            break;
        }
    }
}

void AverageAggregator::processGroup(GroupBySlot& groupSlot, void* colPtr, int32_t type, uint32_t offset)
{
    double* currentVal = static_cast<double*>(groupSlot.avgVal);
    int64_t currentCnt = static_cast<int64_t>(groupSlot.avgCnt);
    switch (type)
    {
        case 1: {
            int32_t rowVal = ((IntVector *)colPtr)->getValue(offset);
            *currentVal = (rowVal + *currentVal * groupSlot.avgCnt) / ++groupSlot.avgCnt;
            break;
        }
        case 2: {
            int64_t rowVal = ((LongVector *)colPtr)->getValue(offset);
            *currentVal = (rowVal + *currentVal * groupSlot.avgCnt) / ++groupSlot.avgCnt;
            break;
        }
        case 3: {
            double rowVal = ((DoubleVector *)colPtr)->getValue(offset);
            *currentVal = (rowVal + *currentVal * groupSlot.avgCnt) / ++groupSlot.avgCnt;
            break;
        }
        case 4: {
            // row type
            break;
        }
        default: {
            DebugError("No such data type %d", type);
            break;
        }
    }
}

void MinAggregator::processGroup(GroupBySlot& groupSlot, void* colPtr, int32_t type, uint32_t offset)
{
    switch (type)
    {
        case 1: {
            int32_t rowVal = ((IntVector *)colPtr)->getValue(offset);
            int32_t* leftVal = reinterpret_cast<int32_t*>(groupSlot.val);
            *leftVal = cmp_int(*leftVal, rowVal) == -1 ? *leftVal : rowVal;
            break;
        }
        case 2: {
            int64_t rowVal = ((LongVector *)colPtr)->getValue(offset);
            int64_t* leftVal = reinterpret_cast<int64_t*>(groupSlot.val);
            *leftVal = cmp_int64(*leftVal, rowVal) == -1 ? *leftVal : rowVal;
            break;
        }
        case 3: {
            double rowVal = ((DoubleVector *)colPtr)->getValue(offset);
            double* leftVal = reinterpret_cast<double*>(groupSlot.val);
            *leftVal = cmp_double(*leftVal, rowVal) == -1 ? *leftVal : rowVal;
            break;
        }
        default: {
            DebugError("No such data type %d", type);
            break;
        }
    }
}

void MinAggregator::initiate(void* colPtr, int32_t type, uint32_t offset)
{
    switch (type) 
    {
        case 1: {
            int32_t* val = new int32_t;
            *val = ((IntVector *)colPtr)->getValue(offset);
            nonGroupState = {val};
            break; 
        }
        case 2: {
            int64_t* val = new int64_t;
            *val = ((LongVector *)colPtr)->getValue(offset);
            nonGroupState = {val};
            break; 
        }
        case 3: {
            double* val = new double;
            *val = ((DoubleVector *)colPtr)->getValue(offset);
            nonGroupState = {val};
            break; 
        }
        default: {
            DebugError("No such data type %d", type);
            break;
        }
    }

    initiated = true;
}

void MinAggregator::processNonGroup(void* colPtr, int32_t type, uint32_t offset)
{
    if (!initiated) {
        initiate(colPtr, type, offset);
        return;
    }

    switch (type)
    {
        case 1: {
            int32_t rowVal = ((IntVector *)colPtr)->getValue(offset);
            int32_t* leftVal = reinterpret_cast<int32_t*>(nonGroupState.val);
            *leftVal = cmp_int(*leftVal, rowVal) == -1 ? *leftVal : rowVal;
            break;
        }
        case 2: {
            int64_t rowVal = ((LongVector *)colPtr)->getValue(offset);
            int64_t* leftVal = reinterpret_cast<int64_t*>(nonGroupState.val);
            *leftVal = cmp_int64(*leftVal, rowVal) == -1 ? *leftVal : rowVal;
            break;
        }
        case 3: {
            double rowVal = ((DoubleVector *)colPtr)->getValue(offset);
            double* leftVal = reinterpret_cast<double*>(nonGroupState.val);
            *leftVal = cmp_double(*leftVal, rowVal) == -1 ? *leftVal : rowVal;
            break;
        }
        default: {
            DebugError("No such data type %d", type);
            break;
        }
    }
}

void MinAggregator::insert(int64_t key, void* colPtr, int32_t type, uint32_t offset)
{
    switch (type)
    {
        case 1: {
            int32_t* val = new int32_t;
            *val = INT32_MAX;
            GroupBySlot slot = {val};
            groupState.insert({key, slot});
            break;
        }
        case 2: {
            int64_t* val = new int64_t;
            *val = INT64_MAX;
            GroupBySlot slot = {val};
            groupState.insert({key, slot});
            break;
        }
        case 3: {
            double* val = new double;
            *val = __DBL_MAX__;
            GroupBySlot slot = {val};
            groupState.insert({key, slot});
            break;
        }
        default: {
            DebugError("No such data type %d", type);
            break;
        }
    }
}

void MaxAggregator::processGroup(GroupBySlot& groupSlot, void* colPtr, int32_t type, uint32_t offset)
{
    switch (type)
    {
        case 1: {
            int32_t rowVal = ((IntVector *)colPtr)->getValue(offset);
            int32_t* leftVal = reinterpret_cast<int32_t*>(groupSlot.val);
            *leftVal = cmp_int(*leftVal, rowVal) == 1 ? *leftVal : rowVal;
            break;
        }
        case 2: {
            int64_t rowVal = ((LongVector *)colPtr)->getValue(offset);
            int64_t* leftVal = reinterpret_cast<int64_t*>(groupSlot.val);
            *leftVal = cmp_int64(*leftVal, rowVal) == 1 ? *leftVal : rowVal;
            break;
        }
        case 3: {
            double rowVal = ((DoubleVector *)colPtr)->getValue(offset);
            double* leftVal = reinterpret_cast<double*>(groupSlot.val);
            *leftVal = cmp_double(*leftVal, rowVal) == 1 ? *leftVal : rowVal;
            break;
        }
        default: {
            DebugError("No such data type %d", type);
            break;
        }
    }
}

void MaxAggregator::initiate(void* colPtr, int32_t type, uint32_t offset)
{
    switch (type)
    {
        case 1: {
            int32_t* val = new int32_t;
            *val = ((IntVector *)colPtr)->getValue(offset);
            nonGroupState = {val};
            break;
        }
        case 2: {
            int64_t* val = new int64_t;
            *val = ((LongVector *)colPtr)->getValue(offset);
            nonGroupState = {val};
            break;
        }
        case 3: {
            double* val = new double;
            *val = ((DoubleVector *)colPtr)->getValue(offset);
            nonGroupState = {val};
            break;
        }
        default: {
            DebugError("No such data type %d", type);
            break;
        }
    }

    initiated = true;
}

void MaxAggregator::processNonGroup(void* colPtr, int32_t type, uint32_t offset)
{
    if (!initiated) {
        initiate(colPtr, type, offset);
        return;
    }

    switch (type)
    {
        case 1: {
            int32_t rowVal = ((IntVector *)colPtr)->getValue(offset);
            int32_t* leftVal = reinterpret_cast<int32_t*>(nonGroupState.val);
            *leftVal = cmp_int(*leftVal, rowVal) == 1 ? *leftVal : rowVal;
            break;
        }
        case 2: {
            int64_t rowVal = ((LongVector *)colPtr)->getValue(offset);
            int64_t* leftVal = reinterpret_cast<int64_t*>(nonGroupState.val);
            *leftVal = cmp_int64(*leftVal, rowVal) == 1 ? *leftVal : rowVal;
            break;
        }
        case 3: {
            double rowVal = ((DoubleVector *)colPtr)->getValue(offset);
            double* leftVal = reinterpret_cast<double*>(nonGroupState.val);
            *leftVal = cmp_double(*leftVal, rowVal) == 1 ? *leftVal : rowVal;
            break;
        }
        default: {
            DebugError("No such data type %d", type);
            break;
        }
    }
}

void MaxAggregator::insert(int64_t key, void* colPtr, int32_t type, uint32_t offset)
{
    switch (type)
    {
        case 1: {
            int32_t* val = new int32_t;
            *val = INT32_MIN;
            GroupBySlot slot = {val};
            groupState.insert({key, slot});
            break;
        }
        case 2: {
            int64_t* val = new int64_t;
            *val = INT64_MIN;
            GroupBySlot slot = {val};
            groupState.insert({key, slot});
            break;
        }
        case 3: {
            double* val = new double;
            *val = __DBL_MIN__;
            GroupBySlot slot = {val};
            groupState.insert({key, slot});
            break;
        }
        default: {
            DebugError("No such data type %d", type);
            break;
        }
    }
}

} // end of namespace op
} // end of namespace omniruntime

