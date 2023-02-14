/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Aggregate factories
 */
#ifndef OMNI_RUNTIME_AGGREGATOR_FACTORY_H
#define OMNI_RUNTIME_AGGREGATOR_FACTORY_H
#include "aggregator.h"
#include "operator/aggregation/aggregator/all_aggregators.h"
#include "util/config_util.h"

namespace omniruntime {
namespace op {
class AggregatorFactory {
public:
    AggregatorFactory() {}
    virtual ~AggregatorFactory() {}
    /* *
     * This interface is for creating aggregators. You have to specify the data type for both input and output data.
     * Also the phase of the aggregator to be created is determined by 'inputRaw' and 'outputPartial'.
     * @param inputTypes
     * @param outputTypes
     * @param inputRaw
     * @param outputPartial
     * @param overflowAsNull  defalut overflow will throw exception
     * @return
     */
    virtual std::unique_ptr<Aggregator> CreateAggregator(const DataTypes &inputTypes, const DataTypes &outputTypes,
        std::vector<int32_t> &channels, bool inputRaw = true, bool outputPartial = false,
        bool isOverflowAsNull = false) = 0;
};

class SumSparkAggregatorFactory : public AggregatorFactory {
public:
    SumSparkAggregatorFactory() {}
    ~SumSparkAggregatorFactory() override {}
    std::unique_ptr<Aggregator> CreateAggregator(const DataTypes &inputTypes, const DataTypes &outputTypes,
        std::vector<int32_t> &channels, bool inputRaw = true, bool outputPartial = false,
        bool isOverflowAsNull = true) override
    {
        // fetch first inputTypes id as aggregator input type and map to type
        // spark rule for sum function input type:
        //    timestamp/sting: cast as double
        //    boolean, date, binnary: not support
        auto inputTypeId = inputTypes.GetIds()[0];
        switch (inputTypeId) {
            case OMNI_SHORT: {
                return std::make_unique<SumFlatIMAggregator<ShortVector, LongVector, int64_t>>(inputTypes, outputTypes,
                    channels, inputRaw, outputPartial, isOverflowAsNull);
            }
            case OMNI_INT: {
                return std::make_unique<SumFlatIMAggregator<IntVector, LongVector, int64_t>>(inputTypes, outputTypes,
                    channels, inputRaw, outputPartial, isOverflowAsNull);
            }
            case OMNI_LONG: {
                return std::make_unique<SumFlatIMAggregator<LongVector, LongVector, int64_t>>(inputTypes, outputTypes,
                    channels, inputRaw, outputPartial, isOverflowAsNull);
            }
            case OMNI_DOUBLE: {
                return std::make_unique<SumFlatIMAggregator<DoubleVector, DoubleVector, double>>(inputTypes,
                    outputTypes, channels, inputRaw, outputPartial, isOverflowAsNull);
            }
            case OMNI_DECIMAL64:
            case OMNI_DECIMAL128: {
                return std::make_unique<SumSparkDecimalAggregator>(inputTypes, outputTypes, channels, inputRaw,
                    outputPartial, isOverflowAsNull);
            }
            default: {
                LogError("Unsupported input type %d for spark sum aggregate", inputTypeId);
                return nullptr;
            }
        }
    }
};

class SumAggregatorFactory : public AggregatorFactory {
public:
    SumAggregatorFactory() {}
    ~SumAggregatorFactory() override {}
    std::unique_ptr<Aggregator> CreateAggregator(const DataTypes &inputTypes, const DataTypes &outputTypes,
        std::vector<int32_t> &channels, bool inputRaw = true, bool outputPartial = false,
        bool isOverflowAsNull = false) override
    {
        // fetch first inputTypes id as aggregator input type and map to type
        auto inputTypeId = inputTypes.GetIds()[0];
        switch (inputTypeId) {
            case OMNI_INT:
            case OMNI_DATE32: {
                return std::make_unique<SumAggregator<IntVector, int32_t, int64_t>>(inputTypes, outputTypes, channels,
                    inputRaw, outputPartial);
            }
            case OMNI_SHORT: {
                return std::make_unique<SumAggregator<ShortVector, int16_t, int32_t>>(inputTypes, outputTypes, channels,
                    inputRaw, outputPartial);
            }
            case OMNI_LONG: {
                return std::make_unique<SumAggregator<LongVector, int64_t, int64_t>>(inputTypes, outputTypes, channels,
                    inputRaw, outputPartial);
            }
            case OMNI_DOUBLE: {
                return std::make_unique<SumAggregator<DoubleVector, double, double>>(inputTypes, outputTypes, channels,
                    inputRaw, outputPartial);
            }
            /* *                             input type
             * -------------------------------------------
             * |          | Decimal64 | Varbinary      |
             * -------------------------------------------
             * output type | Partial | Varbinary  |        /      |
             * ----------------------------------------
             * |  Final |     /       |    Decimal128 |
             */
            // OMNI_VEC_TYPE_VARCHAR is varbinary,need to optimize
            case OMNI_DECIMAL64: {
                return std::make_unique<SumShortDecimalAggregator>(inputTypes, outputTypes, channels, inputRaw,
                    outputPartial);
            }
            case OMNI_DECIMAL128: {
                return std::make_unique<SumLongDecimalAggregator>(inputTypes, outputTypes, channels, inputRaw,
                    outputPartial);
            }
            // Final stage
            case OMNI_VARCHAR: {
                return std::make_unique<SumFinalDecimalAggregator>(inputTypes, outputTypes, channels, inputRaw,
                    outputPartial);
            }
            default: {
                LogError("Unsupported input type %d for sum aggregate", inputTypeId);
                return nullptr;
            }
        }
    }
};

class AverageSparkAggregatorFactory : public AggregatorFactory {
public:
    AverageSparkAggregatorFactory() = default;
    ~AverageSparkAggregatorFactory() override = default;
    std::unique_ptr<Aggregator> CreateAggregator(const DataTypes &inputTypes, const DataTypes &outputTypes,
        std::vector<int32_t> &channels, bool inputRaw = true, bool outputPartial = false,
        bool isOverflowAsNull = true) override
    {
        // fetch first inputTypes id as aggregator input type and map to type
        // spark rule for average function input type:
        //    timestamp/sting: cast as double
        //    boolean, date, binnary: not support
        auto inputTypeId = inputTypes.GetIds()[0];
        switch (inputTypeId) {
            case OMNI_SHORT: {
                return std::make_unique<AverageFlatIMAggregator<ShortVector>>(inputTypes, outputTypes, channels,
                    inputRaw, outputPartial, isOverflowAsNull);
            }
            case OMNI_INT: {
                return std::make_unique<AverageFlatIMAggregator<IntVector>>(inputTypes, outputTypes, channels, inputRaw,
                    outputPartial, isOverflowAsNull);
            }
            case OMNI_LONG: {
                return std::make_unique<AverageFlatIMAggregator<LongVector>>(inputTypes, outputTypes, channels,
                    inputRaw, outputPartial, isOverflowAsNull);
            }
            case OMNI_DOUBLE: {
                return std::make_unique<AverageFlatIMAggregator<DoubleVector>>(inputTypes, outputTypes, channels,
                    inputRaw, outputPartial, isOverflowAsNull);
            }
            case OMNI_DECIMAL64:
            case OMNI_DECIMAL128: {
                return std::make_unique<AverageSparkDecimalAggregator>(inputTypes, outputTypes, channels, inputRaw,
                    outputPartial, isOverflowAsNull);
            }
            default: {
                LogError("Unsupported input type %d for spark average aggregate", inputTypeId);
                return nullptr;
            }
        }
    }
};

class AverageAggregatorFactory : public AggregatorFactory {
public:
    AverageAggregatorFactory() = default;
    ~AverageAggregatorFactory() override = default;
    std::unique_ptr<Aggregator> CreateAggregator(const DataTypes &inputTypes, const DataTypes &outputTypes,
        std::vector<int32_t> &channels, bool inputRaw = true, bool outputPartial = false,
        bool isOverflowAsNull = false) override
    {
        // fetch first inputTypes id as aggregator input type and map to type
        auto inputTypeId = inputTypes.GetIds()[0];
        switch (inputTypeId) {
            case OMNI_INT:
            case OMNI_DATE32:
            case OMNI_CONTAINER: {
                return std::make_unique<AverageAggregator<IntVector>>(inputTypes, outputTypes, channels, inputRaw,
                    outputPartial);
            }
            case OMNI_SHORT: {
                return std::make_unique<AverageAggregator<ShortVector>>(inputTypes, outputTypes, channels, inputRaw,
                    outputPartial);
            }
            case OMNI_LONG: {
                return std::make_unique<AverageAggregator<LongVector>>(inputTypes, outputTypes, channels, inputRaw,
                    outputPartial);
            }
            case OMNI_DOUBLE: {
                return std::make_unique<AverageAggregator<DoubleVector>>(inputTypes, outputTypes, channels, inputRaw,
                    outputPartial);
            }
            case OMNI_DECIMAL64:
            case OMNI_DECIMAL128:
            case OMNI_VARCHAR: {
                return std::make_unique<AverageDecimalAggregator>(inputTypes, outputTypes, channels, inputRaw,
                    outputPartial);
            }
            default: {
                LogError("Unsupported input type %d for average aggregate", inputTypeId);
                return nullptr;
            }
        }
    }
};

class MinAggregatorFactory : public AggregatorFactory {
public:
    MinAggregatorFactory() = default;
    ~MinAggregatorFactory() override = default;
    std::unique_ptr<Aggregator> CreateAggregator(const DataTypes &inputTypes, const DataTypes &outputTypes,
        std::vector<int32_t> &channels, bool inputRaw = true, bool outputPartial = false,
        bool isOverflowAsNull = false) override
    {
        // fetch first inputTypes/outputTypes id as aggregator type and map to type
        auto inputTypeId = inputTypes.GetIds()[0];
        auto outputTypeId = outputTypes.GetIds()[0];
        // Adapt to openLooKeng, openLooKeng converts the output type to bigint in the partial stage,
        // and reverts it to the original input type in the final stage.
        if (inputTypeId == OMNI_INT && outputTypeId == OMNI_LONG) {
            return std::make_unique<MinAggregator<IntVector, LongVector, int64_t>>(inputTypes, outputTypes, channels,
                inputRaw, outputPartial);
        }
        if (inputTypeId == OMNI_LONG && outputTypeId == OMNI_INT) {
            return std::make_unique<MinAggregator<LongVector, IntVector, int32_t>>(inputTypes, outputTypes, channels,
                inputRaw, outputPartial);
        }
        if (inputTypeId == OMNI_DATE32 && outputTypeId == OMNI_LONG) {
            return std::make_unique<MinAggregator<IntVector, LongVector, int64_t>>(inputTypes, outputTypes, channels,
                inputRaw, outputPartial);
        }
        if (inputTypeId == OMNI_LONG && outputTypeId == OMNI_DATE32) {
            return std::make_unique<MinAggregator<LongVector, IntVector, int32_t>>(inputTypes, outputTypes, channels,
                inputRaw, outputPartial);
        }
        // adapt to SQL of min(short) + group by(short)
        if (inputTypeId == OMNI_SHORT && outputTypeId == OMNI_LONG) {
            return std::make_unique<MinAggregator<ShortVector, LongVector, int64_t>>(inputTypes, outputTypes, channels,
                inputRaw, outputPartial);
        }
        if (inputTypeId == OMNI_LONG && outputTypeId == OMNI_SHORT) {
            return std::make_unique<MinAggregator<LongVector, ShortVector, int16_t>>(inputTypes, outputTypes, channels,
                inputRaw, outputPartial);
        }
        switch (inputTypeId) {
            case OMNI_INT:
            case OMNI_DATE32: {
                return std::make_unique<MinAggregator<IntVector, IntVector, int32_t>>(inputTypes, outputTypes, channels,
                    inputRaw, outputPartial);
            }
            case OMNI_SHORT: {
                return std::make_unique<MinAggregator<ShortVector, ShortVector, int16_t>>(inputTypes, outputTypes,
                    channels, inputRaw, outputPartial);
            }
            case OMNI_LONG:
            case OMNI_DECIMAL64: {
                return std::make_unique<MinAggregator<LongVector, LongVector, int64_t>>(inputTypes, outputTypes,
                    channels, inputRaw, outputPartial);
            }
            case OMNI_DOUBLE: {
                return std::make_unique<MinAggregator<DoubleVector, DoubleVector, double>>(inputTypes, outputTypes,
                    channels, inputRaw, outputPartial);
            }
            case OMNI_DECIMAL128: {
                return std::make_unique<MinAggregator<Decimal128Vector, Decimal128Vector, Decimal128>>(inputTypes,
                    outputTypes, channels, inputRaw, outputPartial);
            }
            case OMNI_VARCHAR:
            case OMNI_CHAR: {
                return std::make_unique<MinVarcharAggregator>(inputTypes, outputTypes, channels, inputRaw,
                    outputPartial);
            }
            case OMNI_BOOLEAN: {
                return std::make_unique<MinAggregator<BooleanVector, BooleanVector, bool>>(inputTypes, outputTypes,
                    channels, inputRaw, outputPartial);
            }
            default: {
                LogError("Unsupported input type %d for min aggregate", inputTypeId);
                return nullptr;
            }
        }
    }
};

class MaxAggregatorFactory : public AggregatorFactory {
public:
    MaxAggregatorFactory() = default;
    ~MaxAggregatorFactory() override = default;
    std::unique_ptr<Aggregator> CreateAggregator(const DataTypes &inputTypes, const DataTypes &outputTypes,
        std::vector<int32_t> &channels, bool inputRaw = true, bool outputPartial = false,
        bool isOverflowAsNull = false) override
    {
        // fetch first inputTypes/outputTypes id as aggregator type and map to type
        auto inputTypeId = inputTypes.GetIds()[0];
        auto outputTypeId = outputTypes.GetIds()[0];
        // Adapt to openLooKeng, openLooKeng converts the output type to bigint in the partial stage,
        // and reverts it to the original input type in the final stage.
        if (inputTypeId == OMNI_INT && outputTypeId == OMNI_LONG) {
            return std::make_unique<MaxAggregator<IntVector, LongVector, int64_t>>(inputTypes, outputTypes, channels,
                inputRaw, outputPartial);
        }
        if (inputTypeId == OMNI_LONG && outputTypeId == OMNI_INT) {
            return std::make_unique<MaxAggregator<LongVector, IntVector, int32_t>>(inputTypes, outputTypes, channels,
                inputRaw, outputPartial);
        }
        if (inputTypeId == OMNI_DATE32 && outputTypeId == OMNI_LONG) {
            return std::make_unique<MaxAggregator<IntVector, LongVector, int64_t>>(inputTypes, outputTypes, channels,
                inputRaw, outputPartial);
        }
        if (inputTypeId == OMNI_LONG && outputTypeId == OMNI_DATE32) {
            return std::make_unique<MaxAggregator<LongVector, IntVector, int32_t>>(inputTypes, outputTypes, channels,
                inputRaw, outputPartial);
        }
        // adapt to SQL of max(short) + group by(short)
        if (inputTypeId == OMNI_SHORT && outputTypeId == OMNI_LONG) {
            return std::make_unique<MaxAggregator<ShortVector, LongVector, int64_t>>(inputTypes, outputTypes, channels,
                inputRaw, outputPartial);
        }
        if (inputTypeId == OMNI_LONG && outputTypeId == OMNI_SHORT) {
            return std::make_unique<MaxAggregator<LongVector, ShortVector, int16_t>>(inputTypes, outputTypes, channels,
                inputRaw, outputPartial);
        }
        switch (inputTypeId) {
            case OMNI_INT:
            case OMNI_DATE32: {
                return std::make_unique<MaxAggregator<IntVector, IntVector, int32_t>>(inputTypes, outputTypes, channels,
                    inputRaw, outputPartial);
            }
            case OMNI_SHORT: {
                return std::make_unique<MaxAggregator<ShortVector, ShortVector, int16_t>>(inputTypes, outputTypes,
                    channels, inputRaw, outputPartial);
            }
            case OMNI_LONG:
            case OMNI_DECIMAL64: {
                return std::make_unique<MaxAggregator<LongVector, LongVector, int64_t>>(inputTypes, outputTypes,
                    channels, inputRaw, outputPartial);
            }
            case OMNI_DOUBLE: {
                return std::make_unique<MaxAggregator<DoubleVector, DoubleVector, double>>(inputTypes, outputTypes,
                    channels, inputRaw, outputPartial);
            }
            case OMNI_DECIMAL128: {
                return std::make_unique<MaxAggregator<Decimal128Vector, Decimal128Vector, Decimal128>>(inputTypes,
                    outputTypes, channels, inputRaw, outputPartial);
            }
            case OMNI_VARCHAR:
            case OMNI_CHAR: {
                return std::make_unique<MaxVarcharAggregator>(inputTypes, outputTypes, channels, inputRaw,
                    outputPartial);
            }
            case OMNI_BOOLEAN: {
                return std::make_unique<MaxAggregator<BooleanVector, BooleanVector, bool>>(inputTypes, outputTypes,
                    channels, inputRaw, outputPartial);
            }
            default: {
                LogError("Unsupported input type %d for min aggregate", inputTypeId);
                return nullptr;
            }
        }
    }
};

class CountColumnAggregatorFactory : public AggregatorFactory {
public:
    CountColumnAggregatorFactory() = default;
    ~CountColumnAggregatorFactory() override = default;
    std::unique_ptr<Aggregator> CreateAggregator(const DataTypes &inputTypes, const DataTypes &outputTypes,
        std::vector<int32_t> &channels, bool inputRaw = true, bool outputPartial = false,
        bool isOverflowAsNull = false) override
    {
        return std::make_unique<CountColumnAggregator>(outputTypes, channels, inputRaw, outputPartial);
    }
};

class CountAllAggregatorFactory : public AggregatorFactory {
public:
    CountAllAggregatorFactory() = default;
    ~CountAllAggregatorFactory() override = default;
    std::unique_ptr<Aggregator> CreateAggregator(const DataTypes &inputTypes, const DataTypes &outputTypes,
        std::vector<int32_t> &channels, bool inputRaw = true, bool outputPartial = false,
        bool isOverflowAsNull = false) override
    {
        return std::make_unique<CountAllAggregator>(outputTypes, channels, inputRaw, outputPartial);
    }
};

template <class T> class MaskAggregatorFactory : public AggregatorFactory {
public:
    explicit MaskAggregatorFactory(int32_t maskCol) : maskColumnId(maskCol), realFactory(std::make_unique<T>()) {}
    ~MaskAggregatorFactory() override = default;
    std::unique_ptr<Aggregator> CreateAggregator(const DataTypes &inputTypes, const DataTypes &outputTypes,
        std::vector<int32_t> &channels, bool inputRaw = true, bool outputPartial = false,
        bool isOverflowAsNull = false) override
    {
        std::unique_ptr<Aggregator> realAggregator =
            realFactory->CreateAggregator(inputTypes, outputTypes, channels, inputRaw, outputPartial);
        return std::make_unique<MaskColAggregator>(maskColumnId, std::move(realAggregator));
    }

private:
    int maskColumnId;
    std::unique_ptr<AggregatorFactory> realFactory;
};

class FirstAggregatorFactory : public AggregatorFactory {
public:
    explicit FirstAggregatorFactory(FunctionType aggregateType) : aggregateType(aggregateType) {}
    ~FirstAggregatorFactory() override = default;
    std::unique_ptr<Aggregator> CreateAggregator(const DataTypes &inputTypes, const DataTypes &outputTypes,
        std::vector<int32_t> &channels, bool inputRaw = true, bool outputPartial = false,
        bool isOverflowAsNull = false) override
    {
        // fetch first inputTypes id as aggregator input type and map to type
        // spark rule for first function input type:
        //    binnary/sting: run with SortAggregateExec, so current not implemented
        auto inputTypeId = inputTypes.GetIds()[0];
        switch (inputTypeId) {
            case OMNI_BOOLEAN: {
                return std::make_unique<FirstAggregator<BooleanVector, bool>>(aggregateType, inputTypes, outputTypes,
                    channels, inputRaw, outputPartial, isOverflowAsNull);
            }
            case OMNI_SHORT: {
                return std::make_unique<FirstAggregator<ShortVector, int16_t>>(aggregateType, inputTypes, outputTypes,
                    channels, inputRaw, outputPartial, isOverflowAsNull);
            }
            case OMNI_INT:
            case OMNI_DATE32: {
                return std::make_unique<FirstAggregator<IntVector, int32_t>>(aggregateType, inputTypes, outputTypes,
                    channels, inputRaw, outputPartial, isOverflowAsNull);
            }
            case OMNI_LONG:
            case OMNI_DECIMAL64: {
                return std::make_unique<FirstAggregator<LongVector, int64_t>>(aggregateType, inputTypes, outputTypes,
                    channels, inputRaw, outputPartial, isOverflowAsNull);
            }
            case OMNI_DOUBLE: {
                return std::make_unique<FirstAggregator<DoubleVector, double>>(aggregateType, inputTypes, outputTypes,
                    channels, inputRaw, outputPartial, isOverflowAsNull);
            }
            case OMNI_DECIMAL128: {
                return std::make_unique<FirstAggregator<Decimal128Vector, Decimal128>>(aggregateType, inputTypes,
                    outputTypes, channels, inputRaw, outputPartial, isOverflowAsNull);
            }
            default: {
                LogError("Unsupported input type %d for first aggregate", inputTypeId);
                return nullptr;
            }
        }
    }

private:
    FunctionType aggregateType;
};

class FirstIgnoreNullAggregatorFactory : public FirstAggregatorFactory {
public:
    explicit FirstIgnoreNullAggregatorFactory() : FirstAggregatorFactory(OMNI_AGGREGATION_TYPE_FIRST_IGNORENULL) {}
    ~FirstIgnoreNullAggregatorFactory() override = default;
};

class FirstIncludeNullAggregatorFactory : public FirstAggregatorFactory {
public:
    explicit FirstIncludeNullAggregatorFactory() : FirstAggregatorFactory(OMNI_AGGREGATION_TYPE_FIRST_INCLUDENULL) {}
    ~FirstIncludeNullAggregatorFactory() override = default;
};
// for window aggregation call
static std::unique_ptr<AggregatorFactory> CreateAggregatorFactory(FunctionType aggType)
{
    switch (aggType) {
        case OMNI_AGGREGATION_TYPE_SUM: {
            if (ConfigUtil::GetSupportContainerVecRule() == SupportContainerVecRule::NOT_SUPPORT) {
                return std::make_unique<SumSparkAggregatorFactory>();
            } else {
                return std::make_unique<SumAggregatorFactory>();
            }
        }
        case OMNI_AGGREGATION_TYPE_AVG: {
            if (ConfigUtil::GetSupportContainerVecRule() == SupportContainerVecRule::NOT_SUPPORT) {
                return std::make_unique<AverageSparkAggregatorFactory>();
            } else {
                return std::make_unique<AverageAggregatorFactory>();
            }
        }
        case OMNI_AGGREGATION_TYPE_MIN: {
            return std::make_unique<MinAggregatorFactory>();
        }
        case OMNI_AGGREGATION_TYPE_MAX: {
            return std::make_unique<MaxAggregatorFactory>();
        }
        case OMNI_AGGREGATION_TYPE_COUNT_COLUMN: {
            return std::make_unique<CountColumnAggregatorFactory>();
        }
        case OMNI_AGGREGATION_TYPE_COUNT_ALL: {
            return std::make_unique<CountAllAggregatorFactory>();
        }
        case OMNI_AGGREGATION_TYPE_FIRST_IGNORENULL: {
            return std::make_unique<FirstIgnoreNullAggregatorFactory>();
        }
        case OMNI_AGGREGATION_TYPE_FIRST_INCLUDENULL: {
            return std::make_unique<FirstIncludeNullAggregatorFactory>();
        }
        default: {
            LogError("No such aggregate type %d", aggType);
        }
    }
    return nullptr;
}
}
}
#endif // OMNI_RUNTIME_AGGREGATOR_FACTORY_H
