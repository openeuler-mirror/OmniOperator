#include "aggregator.h"
#include "../util/debug.h"
#include "../memory_pool/memory_pool.h"
 
void SumAggregator::process(uint64_t key, void* colPtr, int32_t type, uint32_t offset)
{
    switch (type)
    {
        case 1: {
            int32_t* rowVal = reinterpret_cast<int32_t*>(colPtr) + offset;
            auto value = state.find(key);
            if (value != state.end()) {
                *((int32_t*)(value->second[0].val)) += *rowVal;
            }else {
                std::vector<GroupByColumn> gbc;
                int32_t* val = new int32_t;
                *val = *rowVal;
                GroupByColumn c = {(ColumnType)type, val};
                gbc.push_back(c);
                state.insert({key, gbc});
            }
            break; 
        }
        case 2: {
            int64_t* rowVal = reinterpret_cast<int64_t*>(colPtr) + offset;
            auto value = state.find(key);
            if (value != state.end()) {
                *((int64_t*)(value->second[0].val)) += *rowVal;
            }else {
                std::vector<GroupByColumn> gbc;
                int64_t* val = new int64_t;
                *val = *rowVal;
                GroupByColumn c = {(ColumnType)type, val};
                gbc.push_back(c);
                state.insert({key, gbc});
            }
            break; 
        }
        case 3: {
            double* rowVal = reinterpret_cast<double*>(colPtr) + offset;
            auto value = state.find(key);
            if (value != state.end()) {
                *((double*)(value->second[0].val)) += *rowVal;
            }else {
                std::vector<GroupByColumn> gbc;
                double* val = new double;
                *val = *rowVal;
                GroupByColumn c = {(ColumnType)type, val};
                gbc.push_back(c);
                state.insert({key, gbc});
            }
            break; 
        }
        default:
            break;
    }
}

extern "C" void sumProcessInt32(SumAggregator* aggregator, int64_t key, void* columnPtr, int32_t offset)
{
    if (aggregator == nullptr) {
        DebugError("Null Pointer %d", offset);
    }
    auto& state = aggregator->getState();
    auto value = state.find(key);

    int32_t* rowVal = reinterpret_cast<int32_t*>(columnPtr) + offset;
    if (value != state.end()) {
        *((int32_t*)(value->second[0].val)) += *rowVal;
    }else {
        std::vector<GroupByColumn> gbc;
        // int32_t* val = (int32_t*)omni_allocate(sizeof(int32_t));
        int32_t* val = new int32_t;
        *val = *rowVal;
        GroupByColumn c = {INT32, val};
        gbc.push_back(c);
        state.insert({key, gbc});
    }      
}

extern "C" void sumProcessInt64(SumAggregator* aggregator, int64_t key, void* columnPtr, int32_t offset)
{
    if (aggregator == nullptr) {
        DebugError("Null Pointer %d", offset);
    }
    auto& state = aggregator->getState();
    auto value = state.find(key);

    int64_t* rowVal = reinterpret_cast<int64_t*>(columnPtr) + offset;
    if (value != state.end()) {
        *((int64_t*)(value->second[0].val)) += *rowVal;
    }else {
        std::vector<GroupByColumn> gbc;
        // int64_t* val = (int64_t*)omni_allocate(sizeof(int64_t));
        int64_t* val = new int64_t;
        *val = *rowVal;
        GroupByColumn c = {INT64, val};
        gbc.push_back(c);
        state.insert({key, gbc});
    }      
}

extern "C" void sumProcessDouble(SumAggregator* aggregator, int64_t key, void* columnPtr, int32_t offset)
{
    if (aggregator == nullptr) {
        DebugError("Null Pointer %d", offset);
    }
    auto& state = aggregator->getState();
    auto value = state.find(key);

    double* rowVal = reinterpret_cast<double*>(columnPtr) + offset;
    if (value != state.end()) {
        *((double*)(value->second[0].val)) += *rowVal;
    }else {
        std::vector<GroupByColumn> gbc;
        // double* val = (double*)omni_allocate(sizeof(double));
        double* val = new double;
        *val = *rowVal;
        GroupByColumn c = {DOUBLE, val};
        gbc.push_back(c);
        state.insert({key, gbc});
    }      
}