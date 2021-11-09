/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Inner supported aggregators header
 */
#ifndef AGGREGATOR_H
#define AGGREGATOR_H

#include <memory>

#include "definitions.h"
#include "../../vector/vector_type.h"
#include "../../vector/vector.h"
#include "../../vector/vector_common.h"
#include "../execution_context.h"

namespace omniruntime {
namespace op {
using namespace omniruntime::vec;

using ColumnIndex = struct ColumnIndex {
    uint32_t idx;
    VecType input;
    VecType output;
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
    OMNI_AGGREGATION_TYPE_INVALIDE
};

using GroupBySlot = union GroupBySlot {
    void *val;
    int64_t count;
    struct {
        void *avgVal;
        int64_t avgCnt;
    };
    struct {
        uint8_t *strVal;
        int32_t strLen;
    };
};

class AggregatorFactory;

// TODO check if it can merge subtype aggregators to one aggregator class.
class Aggregator {
public:
    // Initiate this aggregator, such as setting default values for states.
    Aggregator(AggregateType ty, int32_t input, bool inputRaw = true, bool outputParitial = false)
        : type(ty),
          inputType(input),
          outputType(input),
          initiated(false),
          inputRaw(inputRaw),
          outputPartial(outputParitial),
          nonGroupState({ nullptr }),
          executionContext(std::make_unique<ExecutionContext>())
    {}
    Aggregator(AggregateType ty, int32_t input, int32_t output, bool inputRaw = true, bool outputParitial = false)
        : type(ty),
          inputType(input),
          outputType(output),
          initiated(false),
          inputRaw(inputRaw),
          outputPartial(outputParitial),
          nonGroupState({ nullptr }),
          executionContext(std::make_unique<ExecutionContext>())
    {}
    virtual ~Aggregator() {}
    virtual void Process(void *valuePtr, VecType type) = 0;
    // process input data row by row, e.g. for 'sum' aggregation function, add each input to the intermediate state.
    // TODO seperate data process from hashing in 'inloop'. Change this function to process a input batch instead of
    // only a row.
    void AggProcessGroup(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset);
    virtual void ProcessGroup(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset) = 0;
    void AggProcessNonGroup(Vector *colPtr, int32_t type, uint32_t offset);
    virtual void ProcessNonGroup(Vector *colPtr, int32_t type, uint32_t offset) = 0;
    void AggInsert(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset);
    virtual void Insert(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset) = 0;
    virtual void Initiate(Vector *colPtr, int32_t type, uint32_t offset) = 0;
    bool IsInputRaw() const;
    bool IsOutputPartial() const;

    AggregateType GetType() const
    {
        return type;
    }
    const GroupBySlot &GetNonGroupState()
    {
        return nonGroupState;
    }
    int32_t GetInputType() const
    {
        return inputType;
    }

    int32_t GetOutputType() const
    {
        return outputType;
    }

protected:
    AggregateType type;
    int32_t inputType;
    int32_t outputType;
    // state for non-grouping aggregate
    GroupBySlot nonGroupState;
    bool initiated;
    bool inputRaw;
    bool outputPartial;
    std::unique_ptr<ExecutionContext> executionContext;
};

using ProcessGroupFunc = void (*)(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset,
    std::unique_ptr<ExecutionContext> &context);
using ProcessNonGroupFunc = void (*)(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset);
using InsertFunc = void (*)(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset,
    std::unique_ptr<ExecutionContext> &context);
using InitiateFunc = void (*)(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset);
using AggFunctionByType = struct {
    VecTypeId typeId;
    InsertFunc insertFunc;
    ProcessGroupFunc processGroupFunc;
    InitiateFunc initiateFunc;
    ProcessNonGroupFunc processNonGroupFunc;
};

template <typename V, typename D>
void SumInsertImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset,
    std::unique_ptr<ExecutionContext> &context);
void SumInsertDecimalImpl(GroupBySlot &groupBySlot, Vector *colPtr, int32_t type, uint32_t offset,
    std::unique_ptr<ExecutionContext> &context);
template <typename V, typename D>
void SumProcessGroupImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset,
    std::unique_ptr<ExecutionContext> &context);
void SumProcessGroupDecimalImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset,
    std::unique_ptr<ExecutionContext> &context);
template <typename V, typename D>
void SumInitiateImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset);
void SumInitiateDecimalImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset);
template <typename V, typename D>
void SumProcessNonGroupImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset);
void SumProcessNonGroupDecimalImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset);

template <typename V, typename D>
void AvgInsertImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset,
    std::unique_ptr<ExecutionContext> &context);
void AvgInsertContainerImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset,
    std::unique_ptr<ExecutionContext> &context);
void AvgInsertDecimalImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset,
    std::unique_ptr<ExecutionContext> &context);
template <typename V, typename D>
void AvgProcessGroupImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset,
    std::unique_ptr<ExecutionContext> &context);
void AvgProcessGroupContainerImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset,
    std::unique_ptr<ExecutionContext> &context);
void AvgProcessGroupDecimalImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset,
    std::unique_ptr<ExecutionContext> &context);
template <typename V, typename D>
void AvgInitiateImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset);
void AvgInitiateDecimalImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset);
template <typename V, typename D>
void AvgProcessNonGroupImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset);
void AvgProcessNonGroupDecimalImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset);
template <typename V, typename D>
void MinInsertImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset,
    std::unique_ptr<ExecutionContext> &context);
void MinInsertVarcharImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset,
    std::unique_ptr<ExecutionContext> &context);
template <typename V, typename D>
void MinProcessGroupImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset,
    std::unique_ptr<ExecutionContext> &context);
void MinProcessGroupVarcharImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset,
    std::unique_ptr<ExecutionContext> &context);
template <typename V, typename D>
void MinInitiateImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset);
void MinInitiateVarcharImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset);
template <typename V, typename D>
void MinProcessNonGroupImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset);
void MinProcessNonGroupVarcharImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset);

template <typename V, typename D>
void MaxInsertImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset,
    std::unique_ptr<ExecutionContext> &context);
void MaxInsertVarcharImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset,
    std::unique_ptr<ExecutionContext> &context);
template <typename V, typename D>
void MaxProcessGroupImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset,
    std::unique_ptr<ExecutionContext> &context);
void MaxProcessGroupVarcharImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset,
    std::unique_ptr<ExecutionContext> &context);
template <typename V, typename D>
void MaxInitiateImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset);
void MaxInitiateVarcharImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset);
template <typename V, typename D>
void MaxProcessNonGroupImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset);
void MaxProcessNonGroupVarcharImpl(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset);

class SumAggregator : public Aggregator {
public:
    SumAggregator(int32_t in, int32_t out) : Aggregator(OMNI_AGGREGATION_TYPE_SUM, in, out) {}
    // TODO deprecate
    explicit SumAggregator(int32_t in) : Aggregator(OMNI_AGGREGATION_TYPE_SUM, in) {}
    SumAggregator(int32_t in, int32_t out, bool inputRaw, bool outputPartial)
        : Aggregator(OMNI_AGGREGATION_TYPE_SUM, in, out, inputRaw, outputPartial)
    {}
    ~SumAggregator() override {}
    void ProcessGroup(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset) override;
    void ProcessNonGroup(Vector *colPtr, int32_t type, uint32_t offset) override;
    void Insert(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset) override;
    void Initiate(Vector *colPtr, int32_t type, uint32_t offset) override;
    void Process(void *valuePtr, VecType type) override {}
    static constexpr AggFunctionByType SUM_FUNCTIONS[VEC_TYPE_MAX_COUNT] = {
        {   OMNI_VEC_TYPE_NONE, nullptr, nullptr, nullptr, nullptr},
        {
            OMNI_VEC_TYPE_INT, SumInsertImpl<IntVector, int64_t>, SumProcessGroupImpl<IntVector, int64_t>,
            SumInitiateImpl<IntVector, int64_t>, SumProcessNonGroupImpl<IntVector, int64_t>
        },
        {
            OMNI_VEC_TYPE_LONG, SumInsertImpl<LongVector, int64_t>, SumProcessGroupImpl<LongVector, int64_t>,
            SumInitiateImpl<LongVector, int64_t>, SumProcessNonGroupImpl<LongVector, int64_t>
        },
        {
            OMNI_VEC_TYPE_DOUBLE, SumInsertImpl<DoubleVector, double>, SumProcessGroupImpl<DoubleVector, double>,
            SumInitiateImpl<DoubleVector, double>, SumProcessNonGroupImpl<DoubleVector, double>
        },
        {OMNI_VEC_TYPE_BOOLEAN, nullptr, nullptr, nullptr, nullptr},
        {OMNI_VEC_TYPE_SHORT, nullptr, nullptr, nullptr, nullptr},
        {
            OMNI_VEC_TYPE_DECIMAL64, SumInsertDecimalImpl, SumProcessGroupDecimalImpl,
            SumInitiateDecimalImpl, SumProcessNonGroupDecimalImpl
        },
        {
            OMNI_VEC_TYPE_DECIMAL128, SumInsertImpl<Decimal128Vector, Decimal128>,
            SumProcessGroupImpl<Decimal128Vector, Decimal128>, SumInitiateImpl<Decimal128Vector, Decimal128>,
            SumProcessNonGroupImpl<Decimal128Vector, Decimal128>
        },
        {
            OMNI_VEC_TYPE_DATE32, SumInsertImpl<IntVector, int32_t>, SumProcessGroupImpl<IntVector, int32_t>,
            SumInitiateImpl<IntVector, int32_t>, SumProcessNonGroupImpl<IntVector, int32_t>
        },
        {OMNI_VEC_TYPE_DATE64, nullptr, nullptr, nullptr, nullptr},
        {OMNI_VEC_TYPE_TIME32, nullptr, nullptr, nullptr, nullptr},
        {OMNI_VEC_TYPE_TIME64, nullptr, nullptr, nullptr, nullptr},
        {OMNI_VEC_TYPE_TIMESTAMP, nullptr, nullptr, nullptr, nullptr},
        {OMNI_VEC_TYPE_INTERVAL_MONTHS, nullptr, nullptr, nullptr, nullptr},
        {OMNI_VEC_TYPE_INTERVAL_DAY_TIME, nullptr, nullptr, nullptr, nullptr},
        {OMNI_VEC_TYPE_VARCHAR, nullptr, nullptr, nullptr, nullptr},
        {OMNI_VEC_TYPE_DICTIONARY, nullptr, nullptr, nullptr, nullptr},
        {OMNI_VEC_TYPE_CONTAINER, nullptr, nullptr, nullptr, nullptr},
    };
};

class AverageAggregator : public Aggregator {
public:
    explicit AverageAggregator(int32_t in) : Aggregator(OMNI_AGGREGATION_TYPE_AVG, in) {}

    AverageAggregator(int32_t in, int32_t out) : Aggregator(OMNI_AGGREGATION_TYPE_AVG, in, out) {}

    AverageAggregator(int32_t in, int32_t out, bool inputRaw, bool outputPartial)
        : Aggregator(OMNI_AGGREGATION_TYPE_AVG, in, out, inputRaw, outputPartial)
    {}
    ~AverageAggregator() override {}
    void ProcessGroup(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset) override;
    void ProcessNonGroup(Vector *colPtr, int32_t type, uint32_t offset) override;
    void Insert(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset) override;
    void Initiate(Vector *colPtr, int32_t type, uint32_t offset) override;
    void Process(void *valuePtr, VecType type) override {}
    static constexpr AggFunctionByType AVG_FUNCTIONS[VEC_TYPE_MAX_COUNT] = {
        {OMNI_VEC_TYPE_NONE, nullptr, nullptr, nullptr, nullptr},
        {
            OMNI_VEC_TYPE_INT, AvgInsertImpl<IntVector, double>, AvgProcessGroupImpl<IntVector, double>,
            AvgInitiateImpl<IntVector, double>, AvgProcessNonGroupImpl<IntVector, double>
        },
        {
            OMNI_VEC_TYPE_LONG, AvgInsertImpl<LongVector, double>, AvgProcessGroupImpl<LongVector, double>,
            AvgInitiateImpl<LongVector, double>, AvgProcessNonGroupImpl<LongVector, double>
        },
        {
            OMNI_VEC_TYPE_DOUBLE, AvgInsertImpl<DoubleVector, double>, AvgProcessGroupImpl<DoubleVector, double>,
            AvgInitiateImpl<DoubleVector, double>, AvgProcessNonGroupImpl<DoubleVector, double>
        },
        {OMNI_VEC_TYPE_BOOLEAN, nullptr, nullptr, nullptr, nullptr},
        {OMNI_VEC_TYPE_SHORT, nullptr, nullptr, nullptr, nullptr},
        {
            OMNI_VEC_TYPE_DECIMAL64, AvgInsertImpl<LongVector, double>, AvgProcessGroupImpl<LongVector, double>,
            AvgInitiateImpl<LongVector, double>, AvgProcessNonGroupImpl<LongVector, double>
        },
        // TODO support decimal128 average
        {
            OMNI_VEC_TYPE_DECIMAL128, nullptr, nullptr,
            AvgInitiateImpl<Decimal128Vector, Decimal128>, AvgProcessNonGroupImpl<Decimal128Vector, Decimal128>
        },
        {
            OMNI_VEC_TYPE_DATE32, AvgInsertImpl<IntVector, double>, AvgProcessGroupImpl<IntVector, double>,
            AvgInitiateImpl<IntVector, double>, AvgProcessNonGroupImpl<IntVector, double>
        },
        {OMNI_VEC_TYPE_DATE64, nullptr, nullptr, nullptr, nullptr},
        {OMNI_VEC_TYPE_TIME32, nullptr, nullptr, nullptr, nullptr},
        {OMNI_VEC_TYPE_TIME64, nullptr, nullptr, nullptr, nullptr},
        {OMNI_VEC_TYPE_TIMESTAMP, nullptr, nullptr, nullptr, nullptr},
        {OMNI_VEC_TYPE_INTERVAL_MONTHS, nullptr, nullptr, nullptr, nullptr},
        {OMNI_VEC_TYPE_INTERVAL_DAY_TIME, nullptr, nullptr, nullptr, nullptr},
        {OMNI_VEC_TYPE_VARCHAR, nullptr, nullptr, nullptr, nullptr},
        {OMNI_VEC_TYPE_DICTIONARY, nullptr, nullptr, nullptr, nullptr},
        {OMNI_VEC_TYPE_CONTAINER, AvgInsertContainerImpl, AvgProcessGroupContainerImpl, nullptr, nullptr},
    };
};

class CountAggregator : public Aggregator {
public:
    explicit CountAggregator(int32_t in) : Aggregator(OMNI_AGGREGATION_TYPE_COUNT, in) {}
    CountAggregator(int32_t in, int32_t out) : Aggregator(OMNI_AGGREGATION_TYPE_COUNT, in, out) {}
    CountAggregator(int32_t in, int32_t out, bool inputRaw, bool outputPartial)
        : Aggregator(OMNI_AGGREGATION_TYPE_COUNT, in, out, inputRaw, outputPartial)
    {}
    ~CountAggregator() override {}
    void ProcessGroup(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset) override;
    void ProcessNonGroup(Vector *colPtr, int32_t type, uint32_t offset) override;
    void Insert(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset) override;
    void Initiate(Vector *colPtr, int32_t type, uint32_t offset) override;
    void Process(void *valuePtr, VecType type) override {}
};

class MinAggregator : public Aggregator {
public:
    explicit MinAggregator(int32_t in) : Aggregator(OMNI_AGGREGATION_TYPE_MIN, in) {}
    MinAggregator(int32_t in, int32_t out) : Aggregator(OMNI_AGGREGATION_TYPE_MIN, in, out) {}
    MinAggregator(int32_t in, int32_t out, bool inputRaw, bool outputPartial)
        : Aggregator(OMNI_AGGREGATION_TYPE_MIN, in, out, inputRaw, outputPartial)
    {}
    ~MinAggregator() override {}
    void ProcessGroup(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset) override;
    void ProcessNonGroup(Vector *colPtr, int32_t type, uint32_t offset) override;
    void Insert(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset) override;
    void Initiate(Vector *colPtr, int32_t type, uint32_t offset) override;
    void Process(void *valuePtr, VecType type) override {}
    static constexpr AggFunctionByType MIN_FUNCTIONS[VEC_TYPE_MAX_COUNT] = {
        {OMNI_VEC_TYPE_NONE, nullptr, nullptr, nullptr, nullptr},
        {
            OMNI_VEC_TYPE_INT, MinInsertImpl<IntVector, int32_t>, MinProcessGroupImpl<IntVector, int32_t>,
            MinInitiateImpl<IntVector, int32_t>, MinProcessNonGroupImpl<IntVector, int32_t>
        },
        {
            OMNI_VEC_TYPE_LONG, MinInsertImpl<LongVector, int64_t>, MinProcessGroupImpl<LongVector, int64_t>,
            MinInitiateImpl<LongVector, int64_t>, MinProcessNonGroupImpl<LongVector, int64_t>
        },
        {
            OMNI_VEC_TYPE_DOUBLE, MinInsertImpl<DoubleVector, double>, MinProcessGroupImpl<DoubleVector, double>,
            MinInitiateImpl<DoubleVector, double>, MinProcessNonGroupImpl<DoubleVector, double>
        },
        {OMNI_VEC_TYPE_BOOLEAN, nullptr, nullptr, nullptr, nullptr},
        {OMNI_VEC_TYPE_SHORT, nullptr, nullptr, nullptr, nullptr},
        {
            OMNI_VEC_TYPE_DECIMAL64, MinInsertImpl<LongVector, int64_t>, MinProcessGroupImpl<LongVector, int64_t>,
            MinInitiateImpl<LongVector, int64_t>, MinProcessNonGroupImpl<LongVector, int64_t>
        },
        {
            OMNI_VEC_TYPE_DECIMAL128, MinInsertImpl<Decimal128Vector, Decimal128>,
            MinProcessGroupImpl<Decimal128Vector, Decimal128>, MinInitiateImpl<Decimal128Vector, Decimal128>,
            MinProcessNonGroupImpl<Decimal128Vector, Decimal128>
        },
        {
            OMNI_VEC_TYPE_DATE32, MinInsertImpl<IntVector, int32_t>, MinProcessGroupImpl<IntVector, int32_t>,
            MinInitiateImpl<IntVector, int32_t>, MinProcessNonGroupImpl<IntVector, int32_t>
        },
        {OMNI_VEC_TYPE_DATE64, nullptr, nullptr, nullptr, nullptr},
        {OMNI_VEC_TYPE_TIME32, nullptr, nullptr, nullptr, nullptr},
        {OMNI_VEC_TYPE_TIME64, nullptr, nullptr, nullptr, nullptr},
        {OMNI_VEC_TYPE_TIMESTAMP, nullptr, nullptr, nullptr, nullptr},
        {OMNI_VEC_TYPE_INTERVAL_MONTHS, nullptr, nullptr, nullptr, nullptr},
        {OMNI_VEC_TYPE_INTERVAL_DAY_TIME, nullptr, nullptr, nullptr, nullptr},
        {
            OMNI_VEC_TYPE_VARCHAR, MinInsertVarcharImpl, MinProcessGroupVarcharImpl, MinInitiateVarcharImpl,
            MinProcessNonGroupVarcharImpl
        },
        {OMNI_VEC_TYPE_DICTIONARY, nullptr, nullptr, nullptr, nullptr},
        {OMNI_VEC_TYPE_CONTAINER, nullptr, nullptr, nullptr, nullptr},
    };
};

class MaxAggregator : public Aggregator {
public:
    explicit MaxAggregator(int32_t in) : Aggregator(OMNI_AGGREGATION_TYPE_MAX, in) {}
    MaxAggregator(int32_t in, int32_t out) : Aggregator(OMNI_AGGREGATION_TYPE_MAX, in, out) {}
    MaxAggregator(int32_t in, int32_t out, bool inputRaw, bool outputPartial)
        : Aggregator(OMNI_AGGREGATION_TYPE_MAX, in, out, inputRaw, outputPartial)
    {}
    ~MaxAggregator() override {}
    void ProcessGroup(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset) override;
    void ProcessNonGroup(Vector *colPtr, int32_t type, uint32_t offset) override;
    void Insert(GroupBySlot &groupSlot, Vector *colPtr, int32_t type, uint32_t offset) override;
    void Initiate(Vector *colPtr, int32_t type, uint32_t offset) override;
    void Process(void *valuePtr, VecType type) override {}
    static constexpr AggFunctionByType MAX_FUNCTIONS[VEC_TYPE_MAX_COUNT] = {
        {OMNI_VEC_TYPE_NONE, nullptr, nullptr, nullptr, nullptr},
        {
            OMNI_VEC_TYPE_INT, MaxInsertImpl<IntVector, int32_t>, MaxProcessGroupImpl<IntVector, int32_t>,
            MaxInitiateImpl<IntVector, int32_t>, MaxProcessNonGroupImpl<IntVector, int32_t>
        },
        {
            OMNI_VEC_TYPE_LONG, MaxInsertImpl<LongVector, int64_t>, MaxProcessGroupImpl<LongVector, int64_t>,
            MaxInitiateImpl<LongVector, int64_t>, MaxProcessNonGroupImpl<LongVector, int64_t>
        },
        {
            OMNI_VEC_TYPE_DOUBLE, MaxInsertImpl<DoubleVector, double>, MaxProcessGroupImpl<DoubleVector, double>,
            MaxInitiateImpl<DoubleVector, double>, MaxProcessNonGroupImpl<DoubleVector, double>
        },
        {OMNI_VEC_TYPE_BOOLEAN, nullptr, nullptr, nullptr, nullptr},
        {OMNI_VEC_TYPE_SHORT, nullptr, nullptr, nullptr, nullptr},
        {
            OMNI_VEC_TYPE_DECIMAL64, MaxInsertImpl<LongVector, int64_t>, MaxProcessGroupImpl<LongVector, int64_t>,
            MaxInitiateImpl<LongVector, int64_t>, MaxProcessNonGroupImpl<LongVector, int64_t>
        },
        {
            OMNI_VEC_TYPE_DECIMAL128, MaxInsertImpl<Decimal128Vector, Decimal128>,
            MaxProcessGroupImpl<Decimal128Vector, Decimal128>, MaxInitiateImpl<Decimal128Vector, Decimal128>,
            MaxProcessNonGroupImpl<Decimal128Vector, Decimal128>
        },
        {
            OMNI_VEC_TYPE_DATE32, MaxInsertImpl<IntVector, int32_t>, MaxProcessGroupImpl<IntVector, int32_t>,
            MaxInitiateImpl<IntVector, int32_t>, MaxProcessNonGroupImpl<IntVector, int32_t>
        },
        {OMNI_VEC_TYPE_DATE64, nullptr, nullptr, nullptr, nullptr},
        {OMNI_VEC_TYPE_TIME32, nullptr, nullptr, nullptr, nullptr},
        {OMNI_VEC_TYPE_TIME64, nullptr, nullptr, nullptr, nullptr},
        {OMNI_VEC_TYPE_TIMESTAMP, nullptr, nullptr, nullptr, nullptr},
        {OMNI_VEC_TYPE_INTERVAL_MONTHS, nullptr, nullptr, nullptr, nullptr},
        {OMNI_VEC_TYPE_INTERVAL_DAY_TIME, nullptr, nullptr, nullptr, nullptr},
        {
            OMNI_VEC_TYPE_VARCHAR, MaxInsertVarcharImpl, MaxProcessGroupVarcharImpl, MaxInitiateVarcharImpl,
            MaxProcessNonGroupVarcharImpl
        },
        {OMNI_VEC_TYPE_DICTIONARY, nullptr, nullptr, nullptr, nullptr},
        {OMNI_VEC_TYPE_CONTAINER, nullptr, nullptr, nullptr, nullptr},
    };
};

class AggregatorFactory {
public:
    AggregatorFactory() {}
    virtual ~AggregatorFactory() {}
    /* *
     * This interface is for creating aggregators. You have to specify the data type for both input and output data.
     * Also the phase of the aggregator to be created is determined by 'inputRaw' and 'outputPartial'.
     * @param inputType
     * @param outputType
     * @param inputRaw
     * @param outputPartial
     * @return
     */
    virtual std::unique_ptr<Aggregator> CreateAggregator(int32_t inputType, int32_t outputType, bool inputRaw,
        bool outputPartial) = 0;
};

class SumAggregatorFactory : public AggregatorFactory {
public:
    SumAggregatorFactory() {}
    ~SumAggregatorFactory() override {}
    std::unique_ptr<Aggregator> CreateAggregator(int32_t inputType, int32_t outputType, bool inputRaw,
        bool outputPartial) override;
};

class CountAggregatorFactory : public AggregatorFactory {
public:
    CountAggregatorFactory() {}
    ~CountAggregatorFactory() override {}
    std::unique_ptr<Aggregator> CreateAggregator(int32_t inputType, int32_t outputType, bool inputRaw,
        bool outputPartial) override;
};

class MinAggregatorFactory : public AggregatorFactory {
public:
    MinAggregatorFactory() {}
    ~MinAggregatorFactory() override {}
    std::unique_ptr<Aggregator> CreateAggregator(int32_t inputType, int32_t outputType, bool inputRaw,
        bool outputPartial) override;
};

class MaxAggregatorFactory : public AggregatorFactory {
public:
    MaxAggregatorFactory() {}
    ~MaxAggregatorFactory() override {}
    std::unique_ptr<Aggregator> CreateAggregator(int32_t inputType, int32_t outputType, bool inputRaw,
        bool outputPartial) override;
};

class AverageAggregatorFactory : public AggregatorFactory {
public:
    AverageAggregatorFactory() {}
    ~AverageAggregatorFactory() override {}
    std::unique_ptr<Aggregator> CreateAggregator(int32_t inputType, int32_t outputType, bool inputRaw,
        bool outputPartial) override;
};
} // end of namespace op
} // end of namespace omniruntime
#endif