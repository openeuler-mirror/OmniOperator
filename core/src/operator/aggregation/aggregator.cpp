#include "aggregator.h"
#include "../../util/debug.h"
#include "../../memory/memory_pool.h"
 
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

void SumAggregator::processGroup(GroupBySlot& groupSlot, void* colPtr, int32_t type, uint32_t offset)
{
    switch (type)
    {
        case 1: {
            int32_t* rowVal = reinterpret_cast<int32_t*>(colPtr) + offset;
            *((int32_t*)(groupSlot.val)) += *rowVal;
            break; 
        }
        case 2: {
            int64_t* rowVal = reinterpret_cast<int64_t*>(colPtr) + offset;
            *((int64_t*)(groupSlot.val)) += *rowVal;
            break; 
        }
        case 3: {
            double* rowVal = reinterpret_cast<double*>(colPtr) + offset;
            *((double*)(groupSlot.val)) += *rowVal;
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
            int32_t* rowVal = reinterpret_cast<int32_t*>(colPtr) + offset;
            int32_t* val = new int32_t;
            *val = *rowVal;
            nonGroupState = {val};
            break; 
        }
        case 2: {
            int64_t* rowVal = reinterpret_cast<int64_t*>(colPtr) + offset;
            int64_t* val = new int64_t;
            *val = *rowVal;
            nonGroupState = {val};
            break; 
        }
        case 3: {
            double* rowVal = reinterpret_cast<double*>(colPtr) + offset;
            double* val = new double;
            *val = *rowVal;
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
            int32_t* rowVal = reinterpret_cast<int32_t*>(colPtr) + offset;
            *((int32_t*)nonGroupState.val) += *rowVal;
            break; 
        }
        case 2: {
            int64_t* rowVal = reinterpret_cast<int64_t*>(colPtr) + offset;
            *((int64_t*)nonGroupState.val) += *rowVal;
            break; 
        }
        case 3: {
            double* rowVal = reinterpret_cast<double*>(colPtr) + offset;
            *((double*)nonGroupState.val) += *rowVal;
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
            int32_t* rowVal = reinterpret_cast<int32_t*>(colPtr) + offset;
            int32_t* val = new int32_t;
            *val = *rowVal;
            GroupBySlot slot = {val};
            groupState.insert({key, slot});
            break; 
        }
        case 2: {
            int64_t* rowVal = reinterpret_cast<int64_t*>(colPtr) + offset;
            int64_t* val = new int64_t;
            *val = *rowVal;
            GroupBySlot slot = {val};
            groupState.insert({key, slot});
            break; 
        }
        case 3: {
            double* rowVal = reinterpret_cast<double*>(colPtr) + offset;
            double* val = new double;
            *val = *rowVal;
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
    switch (type)
    {
        case 1: 
        case 2: 
        case 3: {
            groupSlot.count++;
            break; 
        }
        default: {
            DebugError("No such data type %d", type);
            break;
        }
    }
}

void CountAggregator::initiate(void* colPtr, int32_t type, uint32_t offset)
{
    switch (type)
    {
        case 1: 
        case 2: 
        case 3: {
            nonGroupState = {0};
            break; 
        }
        default: {
            DebugError("No such data type %d", type);
            break;
        }
    }
}

void CountAggregator::processNonGroup(void* colPtr, int32_t type, uint32_t offset)
{
    if (!initiated) {
        initiate(colPtr, type, offset);
        return;
    }
    switch (type)
    {
        case 1: 
        case 2: 
        case 3: {
            nonGroupState.count++;
            break; 
        }
        default: {
            DebugError("No such data type %d", type);
            break;
        }
    }
}

void CountAggregator::insert(int64_t key, void* colPtr, int32_t type, uint32_t offset)
{
    switch (type)
    {
        case 1: 
        case 2: 
        case 3: {
            GroupBySlot slot = {0};
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
    switch (type)
    {
        case 1: {
            int32_t* rowVal = reinterpret_cast<int32_t*>(colPtr) + offset;
            *(static_cast<int32_t*>(groupSlot.avgVal)) += *rowVal;
            groupSlot.avgCnt++;
            break;
        }
        case 2: {
            int64_t* rowVal = reinterpret_cast<int64_t*>(colPtr) + offset;
            *(static_cast<int64_t*>(groupSlot.avgVal)) += *rowVal;
            groupSlot.avgCnt++;
            break;
        }
        case 3: {
            double* rowVal = reinterpret_cast<double*>(colPtr) + offset;
            *(static_cast<double*>(groupSlot.avgVal)) += *rowVal;
            groupSlot.avgCnt++;
            break;
        }
        default: {
            DebugError("No such data type %d", type);
            break;
        }
    }
}

void AverageAggregator::initiate(void* colPtr, int32_t type, uint32_t offset)
{
    switch (type) 
    {
        case 1: {
            int32_t* rowVal = reinterpret_cast<int32_t*>(colPtr) + offset;
            int32_t* val = new int32_t;
            *val = *rowVal;
            nonGroupState = {{val, 1}};
            break; 
        }
        case 2: {
            int64_t* rowVal = reinterpret_cast<int64_t*>(colPtr) + offset;
            int64_t* val = new int64_t;
            *val = *rowVal;
            nonGroupState = {{val, 1}};
            break; 
        }
        case 3: {
            double* rowVal = reinterpret_cast<double*>(colPtr) + offset;
            double* val = new double;
            *val = *rowVal;
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

    switch (type)
    {
        case 1: {
            int32_t* rowVal = reinterpret_cast<int32_t*>(colPtr) + offset;
            *(static_cast<int32_t*>(nonGroupState.val)) += *rowVal;
            nonGroupState.avgCnt++;
            break;
        }
        case 2: {
            int64_t* rowVal = reinterpret_cast<int64_t*>(colPtr) + offset;
            *(static_cast<int64_t*>(nonGroupState.avgVal)) += *rowVal;
            nonGroupState.avgCnt++;
            break;
        }
        case 3: {
            double* rowVal = reinterpret_cast<double*>(colPtr) + offset;
            *(static_cast<double*>(nonGroupState.avgVal)) += *rowVal;
            nonGroupState.avgCnt++;
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
    switch (type)
    {
        case 1: {
            int32_t* rowVal = reinterpret_cast<int32_t*>(colPtr) + offset;
            int32_t* val = new int32_t;
            *val = *rowVal;
            GroupBySlot slot = {{val, 1}};
            groupState.insert({key, slot});
            break;
        }
        case 2: {
            int64_t* rowVal = reinterpret_cast<int64_t*>(colPtr) + offset;
            int64_t* val = new int64_t;
            *val = *rowVal;
            GroupBySlot slot = {{val, 1}};
            groupState.insert({key, slot});
            break;
        }
        case 3: {
            double* rowVal = reinterpret_cast<double*>(colPtr) + offset;
            double* val = new double;
            *val = *rowVal;
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

void MinAggregator::processGroup(GroupBySlot& groupSlot, void* colPtr, int32_t type, uint32_t offset)
{
    switch (type)
    {
        case 1: {
            int32_t* rowVal = reinterpret_cast<int32_t*>(colPtr) + offset;
            int32_t* leftVal = reinterpret_cast<int32_t*>(groupSlot.val);
            *leftVal = cmp_int(*leftVal, *rowVal) == -1 ? *leftVal : *rowVal;
            break;
        }
        case 2: {
            int64_t* rowVal = reinterpret_cast<int64_t*>(colPtr) + offset;
            int64_t* leftVal = reinterpret_cast<int64_t*>(groupSlot.val);
            *leftVal = cmp_int64(*leftVal, *rowVal) == -1 ? *leftVal : *rowVal;
            break;
        }
        case 3: {
            double* rowVal = reinterpret_cast<double*>(colPtr) + offset;
            double* leftVal = reinterpret_cast<double*>(groupSlot.val);
            *leftVal = cmp_double(*leftVal, *rowVal) == -1 ? *leftVal : *rowVal;
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
            *val = INT32_MAX;
            nonGroupState = {val};
            break; 
        }
        case 2: {
            int64_t* val = new int64_t;
            *val = INT64_MAX;
            nonGroupState = {val};
            break; 
        }
        case 3: {
            double* val = new double;
            *val = __DBL_MAX__;
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
            int32_t* rowVal = reinterpret_cast<int32_t*>(colPtr) + offset;
            int32_t* leftVal = reinterpret_cast<int32_t*>(nonGroupState.val);
            *leftVal = cmp_int(*leftVal, *rowVal) == -1 ? *leftVal : *rowVal;
            break;
        }
        case 2: {
            int64_t* rowVal = reinterpret_cast<int64_t*>(colPtr) + offset;
            int64_t* leftVal = reinterpret_cast<int64_t*>(nonGroupState.val);
            *leftVal = cmp_int64(*leftVal, *rowVal) == -1 ? *leftVal : *rowVal;
            break;
        }
        case 3: {
            double* rowVal = reinterpret_cast<double*>(colPtr) + offset;
            double* leftVal = reinterpret_cast<double*>(nonGroupState.val);
            *leftVal = cmp_double(*leftVal, *rowVal) == -1 ? *leftVal : *rowVal;
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
            int32_t* rowVal = reinterpret_cast<int32_t*>(colPtr) + offset;
            int32_t* leftVal = reinterpret_cast<int32_t*>(groupSlot.val);
            *leftVal = cmp_int(*leftVal, *rowVal) == 1 ? *leftVal : *rowVal;
            break;
        }
        case 2: {
            int64_t* rowVal = reinterpret_cast<int64_t*>(colPtr) + offset;
            int64_t* leftVal = reinterpret_cast<int64_t*>(groupSlot.val);
            *leftVal = cmp_int64(*leftVal, *rowVal) == 1 ? *leftVal : *rowVal;
            break;
        }
        case 3: {
            double* rowVal = reinterpret_cast<double*>(colPtr) + offset;
            double* leftVal = reinterpret_cast<double*>(groupSlot.val);
            *leftVal = cmp_double(*leftVal, *rowVal) == 1 ? *leftVal : *rowVal;
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
            *val = INT32_MIN;
            nonGroupState = {val};
            break; 
        }
        case 2: {
            int64_t* val = new int64_t;
            *val = INT64_MIN;
            nonGroupState = {val};
            break; 
        }
        case 3: {
            double* val = new double;
            *val = __DBL_MIN__;
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
            int32_t* rowVal = reinterpret_cast<int32_t*>(colPtr) + offset;
            int32_t* leftVal = reinterpret_cast<int32_t*>(nonGroupState.val);
            *leftVal = cmp_int(*leftVal, *rowVal) == 1 ? *leftVal : *rowVal;
            break;
        }
        case 2: {
            int64_t* rowVal = reinterpret_cast<int64_t*>(colPtr) + offset;
            int64_t* leftVal = reinterpret_cast<int64_t*>(nonGroupState.val);
            *leftVal = cmp_int64(*leftVal, *rowVal) == 1 ? *leftVal : *rowVal;
            break;
        }
        case 3: {
            double* rowVal = reinterpret_cast<double*>(colPtr) + offset;
            double* leftVal = reinterpret_cast<double*>(nonGroupState.val);
            *leftVal = cmp_double(*leftVal, *rowVal) == 1 ? *leftVal : *rowVal;
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
