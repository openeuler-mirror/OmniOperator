#ifndef __AGGREGATOR_H__
#define __AGGREGATOR_H__

#include "../../vector/table.h"

#include <stdint.h>
#include <unordered_map>

namespace omniruntime {
namespace op {

typedef enum AggregateType {
    SUM = 0,
    COUNT,
    AVG,
    MAX,
    MIN,
    DNV,
} AggregateType;

typedef union GroupBySlot {
    struct {
        void* avgVal;
        int64_t avgCnt;
    };
    void* val;
    int64_t count;
} GroupBySlot;

// TODO check if it can merge subtype aggregators to one aggregator class.
class Aggregator {
public:
    // Initiate this aggregator, such as setting default values for states.
    Aggregator(AggregateType ty, int32_t dataTy) : type(ty), dataType(dataTy) { }
    virtual ~Aggregator() {
        if (type == COUNT) {
            //do nothing
        }else {
            for (auto& i : state) {
                switch (dataType)
                {
                    case 1: {
                        delete reinterpret_cast<int32_t*>(i.second.val);
                        break;
                    }
                    case 2: {
                        delete reinterpret_cast<int64_t*>(i.second.val);
                        break;
                    }
                    case 3: {
                        delete reinterpret_cast<double*>(i.second.val);
                        break;
                    }
                    default:
                        break;
                }
            }
        }
        state.clear();
    }
    virtual void process(uint64_t key, void* valuePtr, ColumnType type) = 0;
    // process input data row by row, e.g. for 'sum' aggregation function, add each input to the intermediate state.
    // TODO seperate data process from hashing in 'inloop'. Change this function to process a input batch instead of only a row.
    virtual void process(uint64_t key, void* colPtr, int32_t type, uint32_t offset) = 0;
    AggregateType getType() {
        return type;
    }
    // Provide the final state to operators. Operators can use the state to construct final output.
    std::unordered_map<uint64_t, GroupBySlot>& getState() {
        return state;
    }
    int32_t getDataType()
    {
        return dataType;
    }
protected:
    AggregateType type;
    int32_t dataType;
    std::unordered_map<uint64_t, GroupBySlot> state;
};

class SumAggregator : public Aggregator {
public:
    SumAggregator(int32_t ty) : Aggregator(SUM, ty) {}
    ~SumAggregator() { }
    void process(uint64_t key, void* colPtr, int32_t type, uint32_t offset) override;
    void process(uint64_t key, void* valuePtr, ColumnType type) override { }
};

class AverageAggregator : public Aggregator {
public:
    AverageAggregator(int32_t ty) : Aggregator(AVG, ty) {}
    ~AverageAggregator() { }
    void process(uint64_t key, void* colPtr, int32_t type, uint32_t offset) override;
    void process(uint64_t key, void* valuePtr, ColumnType type) override { }
};

class CountAggregator : public Aggregator {
public:
    CountAggregator(int32_t ty) : Aggregator(COUNT, ty) {}
    ~CountAggregator() {}
    void process(uint64_t key, void* colPtr, int32_t type, uint32_t offset) override;
    void process(uint64_t key, void* valuePtr, ColumnType type) override { }
};

class MinAggregator : public Aggregator {
public:
    MinAggregator(int32_t ty) : Aggregator(MIN, ty) {}
    ~MinAggregator() { }
    void process(uint64_t key, void* colPtr, int32_t type, uint32_t offset) override;
    void process(uint64_t key, void* valuePtr, ColumnType type) override { }
};

class MaxAggregator : public Aggregator {
public:
    MaxAggregator(int32_t ty) : Aggregator(MAX, ty) {}
    ~MaxAggregator() { }
    void process(uint64_t key, void* colPtr, int32_t type, uint32_t offset) override;
    void process(uint64_t key, void* valuePtr, ColumnType type) override { }
};

} // end of namespace op
} // end of namespace omniruntime

// class DistinctCountAggregator : public Aggregator<GroupBySlot> {
// public:
//     DistinctCountAggregator(int32_t ty) : Aggregator(DNV, ty) {}
//     ~DistinctCountAggregator() {}
//     void process(uint64_t key, void* colPtr, int32_t type, uint32_t offset) override;
//     void process(uint64_t key, void* valuePtr, ColumnType type) override { }
// };
#endif