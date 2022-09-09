/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Aggregate factories
 */
#ifndef OMNI_RUNTIME_AGGREGATOR_FACTORY_H
#define OMNI_RUNTIME_AGGREGATOR_FACTORY_H
#include "aggregator.h"
#include "operator/aggregation/aggregator/all_aggregators.h"

namespace omniruntime {
namespace op {
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
    virtual std::unique_ptr<Aggregator> CreateAggregator(DataTypePtr inputType, DataTypePtr outputType, int32_t channel,
        bool inputRaw = true, bool outputPartial = false) = 0;
};


class SumAggregatorFactory : public AggregatorFactory {
public:
    SumAggregatorFactory() {}
    ~SumAggregatorFactory() override {}
    std::unique_ptr<Aggregator> CreateAggregator(DataTypePtr inputType, DataTypePtr outputType, int32_t channel,
        bool inputRaw = true, bool outputPartial = false) override
    {
        auto inputTypeId = inputType->GetId();
        switch (inputTypeId) {
            case OMNI_INT:
            case OMNI_DATE32: {
                return std::make_unique<SumAggregator<IntVector, int32_t, int64_t>>(std::move(inputType),
                    std::move(outputType), channel, inputRaw, outputPartial);
            }
            case OMNI_SHORT: {
                return std::make_unique<SumAggregator<ShortVector, int16_t, int32_t>>(std::move(inputType),
                    std::move(outputType), channel, inputRaw, outputPartial);
            }
            case OMNI_LONG: {
                return std::make_unique<SumAggregator<LongVector, int64_t, int64_t>>(std::move(inputType),
                    std::move(outputType), channel, inputRaw, outputPartial);
            }
            case OMNI_DOUBLE: {
                return std::make_unique<SumAggregator<DoubleVector, double, double>>(std::move(inputType),
                    std::move(outputType), channel, inputRaw, outputPartial);
            }
            /* *                             input type
             * -------------------------------------------
             * |          | Decimal64 | Varbinary      |
             * -------------------------------------------
             * output type | Partial | Varbinary  |        /      |
             * ----------------------------------------
             * |  Final |     /       |    Decimal128 |
             *                         */
            // OMNI_VEC_TYPE_VARCHAR is varbinary,need to optimize
            case OMNI_DECIMAL64: {
                return std::make_unique<SumShortDecimalAggregator>(std::move(inputType), std::move(outputType), channel,
                    inputRaw, outputPartial);
            }
            case OMNI_DECIMAL128: {
                return std::make_unique<SumLongDecimalAggregator>(std::move(inputType), std::move(outputType), channel,
                    inputRaw, outputPartial);
            }
            // Final stage
            case OMNI_VARCHAR: {
                return std::make_unique<SumFinalDecimalAggregator>(std::move(inputType), std::move(outputType), channel,
                    inputRaw, outputPartial);
            }
            default: {
                LogError("Unsupported input type %d for sum aggregate", inputTypeId);
                return nullptr;
            }
        }
    }
};

class AverageAggregatorFactory : public AggregatorFactory {
public:
    AverageAggregatorFactory() = default;
    ~AverageAggregatorFactory() override = default;
    std::unique_ptr<Aggregator> CreateAggregator(DataTypePtr inputType, DataTypePtr outputType, int32_t channel,
        bool inputRaw = true, bool outputPartial = false) override
    {
        auto inputTypeId = inputType->GetId();
        // TODO add a param to represent engine type or
        //  inputType and outputType are from physical operations
        //  use meta programming to avoid explicit Vector type in template
        switch (inputTypeId) {
            case OMNI_INT:
            case OMNI_DATE32:
            case OMNI_CONTAINER: {
                return std::make_unique<AverageAggregator<IntVector>>(std::move(inputType), std::move(outputType),
                    channel, inputRaw, outputPartial);
            }
            case OMNI_SHORT: {
                return std::make_unique<AverageAggregator<ShortVector>>(std::move(inputType), std::move(outputType),
                    channel, inputRaw, outputPartial);
            }
            case OMNI_LONG: {
                return std::make_unique<AverageAggregator<LongVector>>(std::move(inputType), std::move(outputType),
                    channel, inputRaw, outputPartial);
            }
            case OMNI_DOUBLE: {
                return std::make_unique<AverageAggregator<DoubleVector>>(std::move(inputType), std::move(outputType),
                    channel, inputRaw, outputPartial);
            }
            case OMNI_DECIMAL64:
            case OMNI_DECIMAL128:
            case OMNI_VARCHAR: {
                return std::make_unique<AverageDecimalAggregator>(std::move(inputType), std::move(outputType), channel,
                    inputRaw, outputPartial);
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
    std::unique_ptr<Aggregator> CreateAggregator(DataTypePtr inputType, DataTypePtr outputType, int32_t channel,
        bool inputRaw = true, bool outputPartial = false) override
    {
        auto inputTypeId = inputType->GetId();
        auto outputTypeId = outputType->GetId();
        // Adapt to openLooKeng, openLooKeng converts the output type to bigint in the partial stage,
        // and reverts it to the original input type in the final stage.
        if (inputTypeId == OMNI_INT && outputTypeId == OMNI_LONG) {
            return std::make_unique<MinAggregator<IntVector, LongVector, int64_t>>(std::move(inputType),
                std::move(outputType), channel, inputRaw, outputPartial);
        }
        if (inputTypeId == OMNI_LONG && outputTypeId == OMNI_INT) {
            return std::make_unique<MinAggregator<LongVector, IntVector, int32_t>>(std::move(inputType),
                std::move(outputType), channel, inputRaw, outputPartial);
        }
        if (inputTypeId == OMNI_DATE32 && outputTypeId == OMNI_LONG) {
            return std::make_unique<MinAggregator<IntVector, LongVector, int64_t>>(std::move(inputType),
                std::move(outputType), channel, inputRaw, outputPartial);
        }
        if (inputTypeId == OMNI_LONG && outputTypeId == OMNI_DATE32) {
            return std::make_unique<MinAggregator<LongVector, IntVector, int32_t>>(std::move(inputType),
                std::move(outputType), channel, inputRaw, outputPartial);
        }
        //adapt to SQL of min(short) + group by(short)
        if (inputTypeId == OMNI_SHORT && outputTypeId == OMNI_LONG) {
            return std::make_unique<MinAggregator<ShortVector, LongVector, int64_t>>(std::move(inputType),
                std::move(outputType), channel, inputRaw, outputPartial);
        }
        if (inputTypeId == OMNI_LONG && outputTypeId == OMNI_SHORT) {
            return std::make_unique<MinAggregator<LongVector, ShortVector, int16_t>>(std::move(inputType),
                std::move(outputType), channel, inputRaw, outputPartial);
        }
        switch (inputTypeId) {
            case OMNI_INT:
            case OMNI_DATE32: {
                return std::make_unique<MinAggregator<IntVector, IntVector, int32_t>>(std::move(inputType),
                    std::move(outputType), channel, inputRaw, outputPartial);
            }
            case OMNI_SHORT: {
                return std::make_unique<MinAggregator<ShortVector, ShortVector, int16_t>>(std::move(inputType),
                    std::move(outputType), channel, inputRaw, outputPartial);
            }
            case OMNI_LONG:
            case OMNI_DECIMAL64: {
                return std::make_unique<MinAggregator<LongVector, LongVector, int64_t>>(std::move(inputType),
                    std::move(outputType), channel, inputRaw, outputPartial);
            }
            case OMNI_DOUBLE: {
                return std::make_unique<MinAggregator<DoubleVector, DoubleVector, double>>(std::move(inputType),
                    std::move(outputType), channel, inputRaw, outputPartial);
            }
            case OMNI_DECIMAL128: {
                return std::make_unique<MinAggregator<Decimal128Vector, Decimal128Vector, Decimal128>>(
                    std::move(inputType), std::move(outputType), channel, inputRaw, outputPartial);
            }
            case OMNI_VARCHAR:
            case OMNI_CHAR: {
                return std::make_unique<MinVarcharAggregator>(std::move(inputType), std::move(outputType), channel,
                    inputRaw, outputPartial);
            }
            case OMNI_BOOLEAN: {
                return std::make_unique<MinAggregator<BooleanVector, BooleanVector, bool>>(std::move(inputType),
                    std::move(outputType), channel, inputRaw, outputPartial);
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
    std::unique_ptr<Aggregator> CreateAggregator(DataTypePtr inputType, DataTypePtr outputType, int32_t channel,
        bool inputRaw = true, bool outputPartial = false) override
    {
        auto inputTypeId = inputType->GetId();
        auto outputTypeId = outputType->GetId();
        // Adapt to openLooKeng, openLooKeng converts the output type to bigint in the partial stage,
        // and reverts it to the original input type in the final stage.
        if (inputTypeId == OMNI_INT && outputTypeId == OMNI_LONG) {
            return std::make_unique<MaxAggregator<IntVector, LongVector, int64_t>>(std::move(inputType),
                std::move(outputType), channel, inputRaw, outputPartial);
        }
        if (inputTypeId == OMNI_LONG && outputTypeId == OMNI_INT) {
            return std::make_unique<MaxAggregator<LongVector, IntVector, int32_t>>(std::move(inputType),
                std::move(outputType), channel, inputRaw, outputPartial);
        }
        if (inputTypeId == OMNI_DATE32 && outputTypeId == OMNI_LONG) {
            return std::make_unique<MaxAggregator<IntVector, LongVector, int64_t>>(std::move(inputType),
                std::move(outputType), channel, inputRaw, outputPartial);
        }
        if (inputTypeId == OMNI_LONG && outputTypeId == OMNI_DATE32) {
            return std::make_unique<MaxAggregator<LongVector, IntVector, int32_t>>(std::move(inputType),
                std::move(outputType), channel, inputRaw, outputPartial);
        }
        //adapt to SQL of max(short) + group by(short)
        if (inputTypeId == OMNI_SHORT && outputTypeId == OMNI_LONG) {
            return std::make_unique<MaxAggregator<ShortVector, LongVector, int64_t>>(std::move(inputType),
                std::move(outputType), channel, inputRaw, outputPartial);
        }
        if (inputTypeId == OMNI_LONG && outputTypeId == OMNI_SHORT) {
            return std::make_unique<MaxAggregator<LongVector, ShortVector, int16_t>>(std::move(inputType),
                std::move(outputType), channel, inputRaw, outputPartial);
        }
        switch (inputTypeId) {
            case OMNI_INT:
            case OMNI_DATE32: {
                return std::make_unique<MaxAggregator<IntVector, IntVector, int32_t>>(std::move(inputType),
                    std::move(outputType), channel, inputRaw, outputPartial);
            }
            case OMNI_SHORT: {
                return std::make_unique<MaxAggregator<ShortVector, ShortVector, int16_t>>(std::move(inputType),
                    std::move(outputType), channel, inputRaw, outputPartial);
            }
            case OMNI_LONG:
            case OMNI_DECIMAL64: {
                return std::make_unique<MaxAggregator<LongVector, LongVector, int64_t>>(std::move(inputType),
                    std::move(outputType), channel, inputRaw, outputPartial);
            }
            case OMNI_DOUBLE: {
                return std::make_unique<MaxAggregator<DoubleVector, DoubleVector, double>>(std::move(inputType),
                    std::move(outputType), channel, inputRaw, outputPartial);
            }
            case OMNI_DECIMAL128: {
                return std::make_unique<MaxAggregator<Decimal128Vector, Decimal128Vector, Decimal128>>(
                    std::move(inputType), std::move(outputType), channel, inputRaw, outputPartial);
            }
            case OMNI_VARCHAR:
            case OMNI_CHAR: {
                return std::make_unique<MaxVarcharAggregator>(std::move(inputType), std::move(outputType), channel,
                    inputRaw, outputPartial);
            }
            case OMNI_BOOLEAN: {
                return std::make_unique<MaxAggregator<BooleanVector, BooleanVector, bool>>(std::move(inputType),
                    std::move(outputType), channel, inputRaw, outputPartial);
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
    std::unique_ptr<Aggregator> CreateAggregator(DataTypePtr inputType, DataTypePtr outputType, int32_t channel,
        bool inputRaw = true, bool outputPartial = false) override
    {
        return std::make_unique<CountColumnAggregator>(std::move(outputType), channel, inputRaw, outputPartial);
    }
};

class CountAllAggregatorFactory : public AggregatorFactory {
public:
    CountAllAggregatorFactory() = default;
    ~CountAllAggregatorFactory() override = default;
    std::unique_ptr<Aggregator> CreateAggregator(DataTypePtr inputType, DataTypePtr outputType, int32_t channel,
        bool inputRaw = true, bool outputPartial = false) override
    {
        return std::make_unique<CountAllAggregator>(std::move(outputType), inputRaw, outputPartial);
    }
};

template <class T> class MaskAggregatorFactory : public AggregatorFactory {
public:
    explicit MaskAggregatorFactory(int32_t maskCol) : maskColumnId(maskCol), realFactory(std::make_unique<T>()) {}
    ~MaskAggregatorFactory() override = default;
    std::unique_ptr<Aggregator> CreateAggregator(DataTypePtr inputType, DataTypePtr outputType, int32_t inputChannel,
        bool inputRaw = true, bool outputPartial = false) override
    {
        std::unique_ptr<Aggregator> realAggregator = realFactory->CreateAggregator(std::move(inputType),
            std::move(outputType), inputChannel, inputRaw, outputPartial);
        return std::make_unique<MaskColAggregator>(maskColumnId, std::move(realAggregator));
    }

private:
    int maskColumnId;
    std::unique_ptr<AggregatorFactory> realFactory;
};

static std::unique_ptr<AggregatorFactory> CreateAggregatorFactory(FunctionType aggType)
{
    switch (aggType) {
        case OMNI_AGGREGATION_TYPE_SUM: {
            return std::make_unique<SumAggregatorFactory>();
        }
        case OMNI_AGGREGATION_TYPE_AVG: {
            return std::make_unique<AverageAggregatorFactory>();
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
        default: {
            LogError("No such aggregate type %d", aggType);
        }
    }
    return nullptr;
}
}
}
#endif // OMNI_RUNTIME_AGGREGATOR_FACTORY_H
