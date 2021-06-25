#ifndef __AGGREGATOR_H__
#define __AGGREGATOR_H__

#include <stdint.h>
#include <unordered_map>
#include "../../vector/vector_type.h"

namespace omniruntime {
namespace op {

typedef struct ColumnIndex {
    uint32_t idx;
    VecType type;
} ColumnIndex;

typedef struct PrepareContext {
    uint32_t* context;
    size_t len;
} PrepareContext;

typedef enum AggregateType{
    OMNI_AGGREGATION_TYPE_SUM = 0,
    OMNI_AGGREGATION_TYPE_COUNT,
    OMNI_AGGREGATION_TYPE_AVG,
    OMNI_AGGREGATION_TYPE_MAX,
    OMNI_AGGREGATION_TYPE_MIN,
    OMNI_AGGREGATION_TYPE_DNV,
}AggregateType;

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
    Aggregator(AggregateType ty, int32_t dataTy, bool inputRaw = true, bool outputParitial = false)
    : type(ty), dataType(dataTy), initiated(false), inputRaw(inputRaw), outputPartial(outputParitial){ }
    virtual ~Aggregator() {
        if (type == OMNI_AGGREGATION_TYPE_COUNT) {
            //do nothing
        }else {
            for (auto& i : groupState) {
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
        groupState.clear();
    }
    virtual void process(void* valuePtr, VecType type) = 0;
    // process input data row by row, e.g. for 'sum' aggregation function, add each input to the intermediate state.
    // TODO seperate data process from hashing in 'inloop'. Change this function to process a input batch instead of only a row.
    virtual void processGroup(GroupBySlot& groupSlot, void* colPtr, int32_t type, uint32_t offset) = 0;
    virtual void processNonGroup(void* colPtr, int32_t type, uint32_t offset) = 0;
    virtual void insert(int64_t key, void* colPtr, int32_t type, uint32_t offset) = 0;
    virtual void initiate(void* colPtr, int32_t type, uint32_t offset) = 0;
    AggregateType getType() {
        return type;
    }
    // Provide the final state to operators. Operators can use the state to construct final output.
    std::unordered_map<uint64_t, GroupBySlot>& getGroupState() {
        return groupState;
    }
    GroupBySlot& getNonGroupState() {
        return nonGroupState;
    }
    int32_t getDataType()
    {
        return dataType;
    }
protected:
    AggregateType type;
    int32_t dataType;
    // state for grouping aggregate
    std::unordered_map<uint64_t, GroupBySlot> groupState;
    // state for non-grouping aggregate
    GroupBySlot nonGroupState;
    bool initiated;
    bool inputRaw;
    bool outputPartial;
};

class SumAggregator : public Aggregator {
public:
    SumAggregator(int32_t ty) : Aggregator(OMNI_AGGREGATION_TYPE_SUM, ty) { }
    SumAggregator(int32_t ty, bool inputRaw, bool outputPartial) : Aggregator(OMNI_AGGREGATION_TYPE_SUM, ty, inputRaw, outputPartial)
    {
        // initiate non-grouping state
        // int32_t* val = new int32_t;
        // *val = 0;
        // nonGroupState = {val};
    }
    ~SumAggregator() { }
    void processGroup(GroupBySlot& groupSlot, void* colPtr, int32_t type, uint32_t offset) override;
    void processNonGroup(void* colPtr, int32_t type, uint32_t offset) override;
    void insert(int64_t key, void* colPtr, int32_t type, uint32_t offset) override;
    void initiate(void* colPtr, int32_t type, uint32_t offset) override;
    void process(void* valuePtr, VecType type) override { }
};

class AverageAggregator : public Aggregator {
public:
    AverageAggregator(int32_t ty) : Aggregator(OMNI_AGGREGATION_TYPE_AVG, ty) { }
    AverageAggregator(int32_t ty, bool inputRaw, bool outputPartial) : Aggregator(OMNI_AGGREGATION_TYPE_AVG, ty, inputRaw, outputPartial)
    {
        // initiate non-grouping state
        // nonGroupState = {0};
    }
    ~AverageAggregator() { }
    void processGroup(GroupBySlot& groupSlot, void* colPtr, int32_t type, uint32_t offset) override;
    void processNonGroup(void* colPtr, int32_t type, uint32_t offset) override;
    void insert(int64_t key, void* colPtr, int32_t type, uint32_t offset) override;
    void initiate(void* colPtr, int32_t type, uint32_t offset) override;
    void process(void* valuePtr, VecType type) override { }
};

class CountAggregator : public Aggregator {
public:
    CountAggregator(int32_t ty) : Aggregator(OMNI_AGGREGATION_TYPE_COUNT, ty) { }
    CountAggregator(int32_t ty, bool inputRaw, bool outputPartial) : Aggregator(OMNI_AGGREGATION_TYPE_COUNT, ty, inputRaw, outputPartial)
    {
        // nonGroup = {0};
    }
    ~CountAggregator() {}
    void processGroup(GroupBySlot& groupSlot, void* colPtr, int32_t type, uint32_t offset) override;
    void processNonGroup(void* colPtr, int32_t type, uint32_t offset) override;
    void insert(int64_t key, void* colPtr, int32_t type, uint32_t offset) override;
    void initiate(void* colPtr, int32_t type, uint32_t offset) override;
    void process(void* valuePtr, VecType type) override { }
};

class MinAggregator : public Aggregator {
public:
    MinAggregator(int32_t ty) : Aggregator(OMNI_AGGREGATION_TYPE_MIN, ty) { }
    MinAggregator(int32_t ty, bool inputRaw, bool outputPartial) : Aggregator(OMNI_AGGREGATION_TYPE_MIN, ty, inputRaw, outputPartial) {}
    ~MinAggregator() { }
    void processGroup(GroupBySlot& groupSlot, void* colPtr, int32_t type, uint32_t offset) override;
    void processNonGroup(void* colPtr, int32_t type, uint32_t offset) override;
    void insert(int64_t key, void* colPtr, int32_t type, uint32_t offset) override;
    void initiate(void* colPtr, int32_t type, uint32_t offset) override;
    void process(void* valuePtr, VecType type) override { }
};

class MaxAggregator : public Aggregator {
public:
    MaxAggregator(int32_t ty) : Aggregator(OMNI_AGGREGATION_TYPE_MAX, ty) { }
    MaxAggregator(int32_t ty, bool inputRaw, bool outputPartial) : Aggregator(OMNI_AGGREGATION_TYPE_MAX, ty, inputRaw, outputPartial) {}
    ~MaxAggregator() { }
    void processGroup(GroupBySlot& groupSlot, void* colPtr, int32_t type, uint32_t offset) override;
    void processNonGroup(void* colPtr, int32_t type, uint32_t offset) override;
    void insert(int64_t key, void* colPtr, int32_t type, uint32_t offset) override;
    void initiate(void* colPtr, int32_t type, uint32_t offset) override;
    void process(void* valuePtr, VecType type) override { }
};

} // end of namespace op
} // end of namespace omniruntime

// class DistinctCountAggregator : public Aggregator<GroupBySlot> {
// public:
//     DistinctCountAggregator(int32_t ty) : Aggregator(DNV, ty) {}
//     ~DistinctCountAggregator() {}
//     void process(void* colPtr, int32_t type, uint32_t offset) override;
//     void process(void* valuePtr, VecType type) override { }
// };
#endif