#ifndef __AGGREGATOR_H__
#define __AGGREGATOR_H__

#include "../data/table.h"

#include <stdint.h>
#include <unordered_map>

using namespace opt;

typedef enum AggregateType {
    SUM = 0,
}AggregateType;

typedef struct GroupByColumn {
    ColumnType type;
    void* val;
} GroupByColumn;

// create template aggregator with type info
class Aggregator {
public:
    Aggregator(AggregateType ty, int32_t dataTy) : type(ty), dataType(dataTy){}
    virtual ~Aggregator() 
    {
        for (auto& i : state) {
            // FIXME free data by type
            delete i.second[0].val;
        }
        state.clear();
    }
    virtual void process(uint64_t key, void* valuePtr, ColumnType type) = 0;
    virtual void process(uint64_t key, void* colPtr, int32_t type, uint32_t offset) = 0;
    AggregateType getType() {
        return type;
    }
    std::unordered_map<uint64_t, std::vector<GroupByColumn>>& getState() {
        return state;
    }
    int32_t getDataType()
    {
        return dataType;
    }
protected:
    AggregateType type;
    int32_t dataType;
    // V type defined as vector due to keep constructColumn interface uinified.
    std::unordered_map<uint64_t, std::vector<GroupByColumn>> state;
};

class SumAggregator : public Aggregator {
public:
    SumAggregator(int32_t ty) : Aggregator(SUM, ty) {}
    ~SumAggregator() {}
    void process(uint64_t key, void* colPtr, int32_t type, uint32_t offset) override;
    void process(uint64_t key, void* valuePtr, ColumnType type) override
    {}
};

extern "C" void sumProcessInt32(SumAggregator* aggregator, int64_t key, void* columnPtr, int32_t offset);
extern "C" void sumProcessInt64(SumAggregator* aggregator, int64_t key, void* columnPtr, int32_t offset);
extern "C" void sumProcessDouble(SumAggregator* aggregator, int64_t key, void* columnPtr, int32_t offset);

#endif