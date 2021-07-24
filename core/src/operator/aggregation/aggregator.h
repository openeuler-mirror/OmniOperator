/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Inner supported aggregators header
 */
#ifndef AGGREGATOR_H
#define AGGREGATOR_H

#include <cstdint>
#include <unordered_map>
#include <memory>

#include "../../vector/vector_type.h"

namespace omniruntime {
namespace op {

const int32_t AVG_VECTOR_COUNT = 2;

using ColumnIndex = struct ColumnIndex {
    uint32_t idx;
    omniruntime::vec::VecType type;
};

using PrepareContext = struct PrepareContext {
    uint32_t *context;
    size_t len;
};

using AggregateType = enum AggregateType {
    OMNI_AGGREGATION_TYPE_SUM = 0,
    OMNI_AGGREGATION_TYPE_COUNT,
    OMNI_AGGREGATION_TYPE_AVG,
    OMNI_AGGREGATION_TYPE_MAX,
    OMNI_AGGREGATION_TYPE_MIN,
    OMNI_AGGREGATION_TYPE_DNV,
};

using GroupBySlot = union GroupBySlot {
    struct {
        void *avgVal;
        int64_t avgCnt;
    };
    void *val;
    int64_t count;
};

class AggregatorFactory;

// TODO check if it can merge subtype aggregators to one aggregator class.
class Aggregator {
public:
    // Initiate this aggregator, such as setting default values for states.
    Aggregator(AggregateType ty, int32_t dataTy, bool inputRaw = true, bool outputParitial = false)
        : type(ty), dataType(dataTy), initiated(false), inputRaw(inputRaw), outputPartial(outputParitial), nonGroupState({ nullptr })
    {}
    virtual ~Aggregator()
    {
        if (type == OMNI_AGGREGATION_TYPE_COUNT) {
            // do nothing
        } else {
            for (auto &i : groupState) {
                switch (dataType) {
                    case omniruntime::vec::OMNI_VEC_TYPE_INT: {
                        delete reinterpret_cast<int32_t *>(i.second.val);
                        break;
                    }
                    case omniruntime::vec::OMNI_VEC_TYPE_LONG: {
                        delete reinterpret_cast<int64_t *>(i.second.val);
                        break;
                    }
                    case omniruntime::vec::OMNI_VEC_TYPE_DOUBLE: {
                        delete reinterpret_cast<double *>(i.second.val);
                        break;
                    }
                    default:
                        break;
                }
            }
        }
        groupState.clear();
    }
    virtual void Process(void *valuePtr, omniruntime::vec::VecType type) = 0;
    // process input data row by row, e.g. for 'sum' aggregation function, add each input to the intermediate state.
    // TODO seperate data process from hashing in 'inloop'. Change this function to process a input batch instead of
    // only a row.
    virtual void ProcessGroup(GroupBySlot &groupSlot, void *colPtr, int32_t type, uint32_t offset) = 0;
    virtual void ProcessNonGroup(void *colPtr, int32_t type, uint32_t offset) = 0;
    virtual void Insert(int64_t key, void *colPtr, int32_t type, uint32_t offset) = 0;
    virtual void Initiate(void *colPtr, int32_t type, uint32_t offset) = 0;
    bool IsInputRaw() const;
    bool IsOutputPartial() const;
    AggregateType GetType() const
    {
        return type;
    }
    // Provide the final state to operators. Operators can use the state to construct final output.
    std::unordered_map<uint64_t, GroupBySlot> &GetGroupState()
    {
        return groupState;
    }
    const GroupBySlot &GetNonGroupState()
    {
        return nonGroupState;
    }
    int32_t GetDataType() const
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
    explicit SumAggregator(int32_t ty) : Aggregator(OMNI_AGGREGATION_TYPE_SUM, ty) {}
    SumAggregator(int32_t ty, bool inputRaw, bool outputPartial)
        : Aggregator(OMNI_AGGREGATION_TYPE_SUM, ty, inputRaw, outputPartial)
    { }
    ~SumAggregator() override {}
    void ProcessGroup(GroupBySlot &groupSlot, void *colPtr, int32_t type, uint32_t offset) override;
    void ProcessNonGroup(void *colPtr, int32_t type, uint32_t offset) override;
    void Insert(int64_t key, void *colPtr, int32_t type, uint32_t offset) override;
    void Initiate(void *colPtr, int32_t type, uint32_t offset) override;
    void Process(void *valuePtr, omniruntime::vec::VecType type) override {}
};

class AverageAggregator : public Aggregator {
public:
    explicit AverageAggregator(int32_t ty) : Aggregator(OMNI_AGGREGATION_TYPE_AVG, ty) {}
    AverageAggregator(int32_t ty, bool inputRaw, bool outputPartial)
        : Aggregator(OMNI_AGGREGATION_TYPE_AVG, ty, inputRaw, outputPartial)
    { }
    ~AverageAggregator() override {}
    void ProcessGroup(GroupBySlot &groupSlot, void *colPtr, int32_t type, uint32_t offset) override;
    void ProcessNonGroup(void *colPtr, int32_t type, uint32_t offset) override;
    void Insert(int64_t key, void *colPtr, int32_t type, uint32_t offset) override;
    void Initiate(void *colPtr, int32_t type, uint32_t offset) override;
    void Process(void *valuePtr, omniruntime::vec::VecType type) override {}
};

class CountAggregator : public Aggregator {
public:
    explicit CountAggregator(int32_t ty) : Aggregator(OMNI_AGGREGATION_TYPE_COUNT, ty) {}
    CountAggregator(int32_t ty, bool inputRaw, bool outputPartial)
        : Aggregator(OMNI_AGGREGATION_TYPE_COUNT, ty, inputRaw, outputPartial)
    {

    }
    ~CountAggregator() override {}
    void ProcessGroup(GroupBySlot &groupSlot, void *colPtr, int32_t type, uint32_t offset) override;
    void ProcessNonGroup(void *colPtr, int32_t type, uint32_t offset) override;
    void Insert(int64_t key, void *colPtr, int32_t type, uint32_t offset) override;
    void Initiate(void *colPtr, int32_t type, uint32_t offset) override;
    void Process(void *valuePtr, omniruntime::vec::VecType type) override {}
};

class MinAggregator : public Aggregator {
public:
    explicit MinAggregator(int32_t ty) : Aggregator(OMNI_AGGREGATION_TYPE_MIN, ty) {}
    MinAggregator(int32_t ty, bool inputRaw, bool outputPartial)
        : Aggregator(OMNI_AGGREGATION_TYPE_MIN, ty, inputRaw, outputPartial)
    {}
    ~MinAggregator() override {}
    void ProcessGroup(GroupBySlot &groupSlot, void *colPtr, int32_t type, uint32_t offset) override;
    void ProcessNonGroup(void *colPtr, int32_t type, uint32_t offset) override;
    void Insert(int64_t key, void *colPtr, int32_t type, uint32_t offset) override;
    void Initiate(void *colPtr, int32_t type, uint32_t offset) override;
    void Process(void *valuePtr, omniruntime::vec::VecType type) override {}
};

class MaxAggregator : public Aggregator {
public:
    explicit MaxAggregator(int32_t ty) : Aggregator(OMNI_AGGREGATION_TYPE_MAX, ty) {}
    MaxAggregator(int32_t ty, bool inputRaw, bool outputPartial)
        : Aggregator(OMNI_AGGREGATION_TYPE_MAX, ty, inputRaw, outputPartial)
    {}
    ~MaxAggregator() override {}
    void ProcessGroup(GroupBySlot &groupSlot, void *colPtr, int32_t type, uint32_t offset) override;
    void ProcessNonGroup(void *colPtr, int32_t type, uint32_t offset) override;
    void Insert(int64_t key, void *colPtr, int32_t type, uint32_t offset) override;
    void Initiate(void *colPtr, int32_t type, uint32_t offset) override;
    void Process(void *valuePtr, omniruntime::vec::VecType type) override {}
};

class AggregatorFactory {
public:
    AggregatorFactory() {}
    virtual ~AggregatorFactory() {}
    virtual std::unique_ptr<Aggregator> CreateAggregator(int32_t dataType) = 0;
};

class SumAggregatorFactory : public AggregatorFactory {
public:
    SumAggregatorFactory() {}
    ~SumAggregatorFactory() override {}
    std::unique_ptr<Aggregator> CreateAggregator(int32_t dataType) override;
};

class CountAggregatorFactory : public AggregatorFactory {
public:
    CountAggregatorFactory() {}
    ~CountAggregatorFactory() override {}
    std::unique_ptr<Aggregator> CreateAggregator(int32_t dataType) override;
};

class MinAggregatorFactory : public AggregatorFactory {
public:
    MinAggregatorFactory() {}
    ~MinAggregatorFactory() override {}
    std::unique_ptr<Aggregator> CreateAggregator(int32_t dataType) override;
};

class MaxAggregatorFactory : public AggregatorFactory {
public:
    MaxAggregatorFactory() {}
    ~MaxAggregatorFactory() override {}
    std::unique_ptr<Aggregator> CreateAggregator(int32_t dataType) override;
};

class AverageAggregatorFactory : public AggregatorFactory {
public:
    AverageAggregatorFactory() {}
    ~AverageAggregatorFactory() override {}
    std::unique_ptr<Aggregator> CreateAggregator(int32_t dataType) override;
};

} // end of namespace op
} // end of namespace omniruntime
#endif