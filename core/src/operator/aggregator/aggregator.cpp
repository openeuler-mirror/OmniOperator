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

void SumAggregator::process(uint64_t key, void* colPtr, int32_t type, uint32_t offset)
{
    switch (type)
    {
        case 1: {
            int32_t* rowVal = reinterpret_cast<int32_t*>(colPtr) + offset;
            auto value = state.find(key);
            if (value != state.end()) {
                *((int32_t*)(value->second.val)) += *rowVal;
            }else {
                int32_t* val = new int32_t;
                *val = *rowVal;
                GroupBySlot slot = {val};
                state.insert({key, slot});
            }
            break; 
        }
        case 2: {
            int64_t* rowVal = reinterpret_cast<int64_t*>(colPtr) + offset;
            auto value = state.find(key);
            if (value != state.end()) {
                *((int64_t*)(value->second.val)) += *rowVal;
            }else {
                int64_t* val = new int64_t;
                *val = *rowVal;
                GroupBySlot slot = {val};
                state.insert({key, slot});
            }
            break; 
        }
        case 3: {
            double* rowVal = reinterpret_cast<double*>(colPtr) + offset;
            auto value = state.find(key);
            if (value != state.end()) {
                *((double*)(value->second.val)) += *rowVal;
            }else {
                double* val = new double;
                *val = *rowVal;
                GroupBySlot slot = {val};
                state.insert({key, slot});
            }
            break; 
        }
        default: {
            DebugError("No such data type %d", type);
            break;
        }
    }
}

void CountAggregator::process(uint64_t key, void* colPtr, int32_t type, uint32_t offset)
{
    switch (type)
    {
        case 1: 
        case 2: 
        case 3: {
            auto value = state.find(key);
            if (value != state.end()) {
                value->second.count++;
            }else {
                GroupBySlot slot = {0};
                state.insert({key, slot});
            }
            break; 
        }
        default: {
            DebugError("No such data type %d", type);
            break;
        }
    }
}

void AverageAggregator::process(uint64_t key, void* colPtr, int32_t type, uint32_t offset)
{
    switch (type)
    {
        case 1: {
            int32_t* rowVal = reinterpret_cast<int32_t*>(colPtr) + offset;
            auto value = state.find(key);
            if (value != state.end()) {
                *(static_cast<int32_t*>(value->second.avgVal)) += *rowVal;
                value->second.avgCnt++;
            }else {
                int32_t* val = new int32_t;
                *val = *rowVal;
                GroupBySlot slot = {{val, 1}};
                state.insert({key, slot});
            }
            break;
        }
        case 2: {
            int64_t* rowVal = reinterpret_cast<int64_t*>(colPtr) + offset;
            auto value = state.find(key);
            if (value != state.end()) {
                *(static_cast<int64_t*>(value->second.avgVal)) += *rowVal;
                value->second.avgCnt++;
            }else {
                int64_t* val = new int64_t;
                *val = *rowVal;
                GroupBySlot slot = {{val, 1}};
                state.insert({key, slot});
            }
            break;
        }
        case 3: {
            double* rowVal = reinterpret_cast<double*>(colPtr) + offset;
            auto value = state.find(key);
            if (value != state.end()) {
                *(static_cast<double*>(value->second.avgVal)) += *rowVal;
                value->second.avgCnt++;
            }else {
                double* val = new double;
                *val = *rowVal;
                GroupBySlot slot = {{val, 1}};
                state.insert({key, slot});
            }
            break;
        }
        default: {
            DebugError("No such data type %d", type);
            break;
        }
    }
}

void MinAggregator::process(uint64_t key, void* colPtr, int32_t type, uint32_t offset)
{
    switch (type)
    {
        case 1: {
            int32_t* rowVal = reinterpret_cast<int32_t*>(colPtr) + offset;
            auto value = state.find(key);
            if (value != state.end()) {
                int32_t* leftVal = reinterpret_cast<int32_t*>(value->second.val);
                *leftVal = cmp_int(*leftVal, *rowVal) == -1 ? *leftVal : *rowVal;
            }else {
                int32_t* val = new int32_t;
                *val = INT32_MAX;
                GroupBySlot slot = {val};
                state.insert({key, slot});
            }
            break;
        }
        case 2: {
            int64_t* rowVal = reinterpret_cast<int64_t*>(colPtr) + offset;
            auto value = state.find(key);
            if (value != state.end()) {
                int64_t* leftVal = reinterpret_cast<int64_t*>(value->second.val);
                *leftVal = cmp_int64(*leftVal, *rowVal) == -1 ? *leftVal : *rowVal;
            }else {
                int64_t* val = new int64_t;
                *val = INT64_MAX;
                GroupBySlot slot = {val};
                state.insert({key, slot});
            }
            break;
        }
        case 3: {
            double* rowVal = reinterpret_cast<double*>(colPtr) + offset;
            auto value = state.find(key);
            if (value != state.end()) {
                double* leftVal = reinterpret_cast<double*>(value->second.val);
                *leftVal = cmp_double(*leftVal, *rowVal) == -1 ? *leftVal : *rowVal;
            }else {
                double* val = new double;
                *val = __DBL_MAX__;
                GroupBySlot slot = {val};
                state.insert({key, slot});
            }
            break;
        }
        default: {
            DebugError("No such data type %d", type);
            break;
        }
    }
}

void MaxAggregator::process(uint64_t key, void* colPtr, int32_t type, uint32_t offset)
{
    switch (type)
    {
        case 1: {
            int32_t* rowVal = reinterpret_cast<int32_t*>(colPtr) + offset;
            auto value = state.find(key);
            if (value != state.end()) {
                int32_t* leftVal = reinterpret_cast<int32_t*>(value->second.val);
                *leftVal = cmp_int(*leftVal, *rowVal) == 1 ? *leftVal : *rowVal;
            }else {
                int32_t* val = new int32_t;
                *val = INT32_MIN;
                GroupBySlot slot = {val};
                state.insert({key, slot});
            }
            break;
        }
        case 2: {
            int64_t* rowVal = reinterpret_cast<int64_t*>(colPtr) + offset;
            auto value = state.find(key);
            if (value != state.end()) {
                int64_t* leftVal = reinterpret_cast<int64_t*>(value->second.val);
                *leftVal = cmp_int64(*leftVal, *rowVal) == 1 ? *leftVal : *rowVal;
            }else {
                int64_t* val = new int64_t;
                *val = INT64_MIN;
                GroupBySlot slot = {val};
                state.insert({key, slot});
            }
            break;
        }
        case 3: {
            double* rowVal = reinterpret_cast<double*>(colPtr) + offset;
            auto value = state.find(key);
            if (value != state.end()) {
                double* leftVal = reinterpret_cast<double*>(value->second.val);
                *leftVal = cmp_double(*leftVal, *rowVal) == 1 ? *leftVal : *rowVal;
            }else {
                double* val = new double;
                *val = __DBL_MIN__;
                GroupBySlot slot = {val};
                state.insert({key, slot});
            }
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
