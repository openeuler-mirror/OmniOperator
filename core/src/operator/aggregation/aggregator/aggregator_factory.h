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
    virtual std::unique_ptr<Aggregator> CreateAggregator(int32_t inputType, int32_t outputType, int32_t channel,
        bool inputRaw = true, bool outputPartial = false) = 0;
};


class SumAggregatorFactory : public AggregatorFactory {
public:
    SumAggregatorFactory() {}
    ~SumAggregatorFactory() override {}
    std::unique_ptr<Aggregator> CreateAggregator(int32_t inputType, int32_t outputType, int32_t channel,
        bool inputRaw = true, bool outputPartial = false) override
    {
        switch (inputType) {
            case OMNI_VEC_TYPE_INT:
            case OMNI_VEC_TYPE_DATE32: {
                return std::make_unique<SumAggregator<IntVector, int32_t, int64_t>>(inputType, outputType, channel,
                    inputRaw, outputPartial);
            }
            case OMNI_VEC_TYPE_LONG: {
                return std::make_unique<SumAggregator<LongVector, int64_t, int64_t>>(inputType, outputType, channel,
                    inputRaw, outputPartial);
            }
            case OMNI_VEC_TYPE_DOUBLE: {
                return std::make_unique<SumAggregator<DoubleVector, double, double>>(inputType, outputType, channel,
                    inputRaw, outputPartial);
            }
            /* *                             input type
             * -------------------------------------------
             * |          | Decimal64 | Varbinary      |
             * -------------------------------------------
             * output type | Partial | Varbinary  |        /      |
             * ----------------------------------------
             * |  Final |     /       |    Decimal128 |
             *      */
            case OMNI_VEC_TYPE_DECIMAL64: {
                // TODO SumShortDecimalAggregator for olk
                return std::make_unique<SumShortDecimalAggregator>(inputType, outputType, channel, inputRaw,
                    outputPartial);
            }
            case OMNI_VEC_TYPE_DECIMAL128: {
                // TODO SumLongDecimalAggregator for olk
                return std::make_unique<SumLongDecimalAggregator>(inputType, outputType, channel, inputRaw,
                    outputPartial);
            }
            default: {
                LogError("Unsupported input type %d for sum aggregate", inputType);
                return nullptr;
            }
        }
    }
};

class AverageAggregatorFactory : public AggregatorFactory {
public:
    AverageAggregatorFactory() {}
    ~AverageAggregatorFactory() override {}
    std::unique_ptr<Aggregator> CreateAggregator(int32_t inputType, int32_t outputType, int32_t channel,
        bool inputRaw = true, bool outputPartial = false) override
    {
        // TODO add a param to represent engine type or
        //  inputType and outputType are from physical operations
        //  use meta programming to avoid explicit Vector type in template
        // if (inputType == OMNI_VEC_TYPE_DECIMAL64 && outputType == OMNI_VEC_TYPE_VARCHAR) {...}
        switch (inputType) {
            case OMNI_VEC_TYPE_INT:
            case OMNI_VEC_TYPE_DATE32:
            case OMNI_VEC_TYPE_CONTAINER: {
                return std::make_unique<AverageAggregator<IntVector>>(inputType, outputType, channel, inputRaw,
                    outputPartial);
            }
            case OMNI_VEC_TYPE_LONG: {
                return std::make_unique<AverageAggregator<LongVector>>(inputType, outputType, channel, inputRaw,
                    outputPartial);
            }
            case OMNI_VEC_TYPE_DOUBLE: {
                return std::make_unique<AverageAggregator<DoubleVector>>(inputType, outputType, channel, inputRaw,
                    outputPartial);
            }
                // TODO AverageShortDecimalAggregator for olk
            case OMNI_VEC_TYPE_DECIMAL64: {
                return std::make_unique<AverageShortDecimalAggregator>(inputType, outputType, channel, inputRaw,
                    outputPartial);
            }
                // TODO AverageLongDecimalAggregator for olk
            case OMNI_VEC_TYPE_DECIMAL128: {
                return std::make_unique<AverageLongDecimalAggregator>(inputType, outputType, channel, inputRaw,
                    outputPartial);
            }
            default: {
                LogError("Unsupported input type %d for average aggregate", inputType);
                return nullptr;
            }
        }
    }
};

class MinAggregatorFactory : public AggregatorFactory {
public:
    MinAggregatorFactory() {}
    ~MinAggregatorFactory() override {}
    std::unique_ptr<Aggregator> CreateAggregator(int32_t inputType, int32_t outputType, int32_t channel,
        bool inputRaw = true, bool outputPartial = false) override
    {
        switch (inputType) {
            case OMNI_VEC_TYPE_INT:
            case OMNI_VEC_TYPE_DATE32: {
                return std::make_unique<MinAggregator<IntVector, int32_t>>(inputType, outputType, channel, inputRaw,
                    outputPartial);
            }
            case OMNI_VEC_TYPE_LONG:
            case OMNI_VEC_TYPE_DECIMAL64: {
                return std::make_unique<MinAggregator<LongVector, int64_t>>(inputType, outputType, channel, inputRaw,
                    outputPartial);
            }
            case OMNI_VEC_TYPE_DOUBLE: {
                return std::make_unique<MinAggregator<DoubleVector, double>>(inputType, outputType, channel, inputRaw,
                    outputPartial);
            }
            case OMNI_VEC_TYPE_DECIMAL128: {
                return std::make_unique<MinAggregator<Decimal128Vector, Decimal128>>(inputType, outputType, channel,
                    inputRaw, outputPartial);
            }
            case OMNI_VEC_TYPE_VARCHAR:
            case OMNI_VEC_TYPE_CHAR: {
                return std::make_unique<MinVarcharAggregator>(inputType, outputType, channel, inputRaw, outputPartial);
            }
            default: {
                LogError("Unsupported input type %d for min aggregate", inputType);
                return nullptr;
            }
        }
    }
};

class MaxAggregatorFactory : public AggregatorFactory {
public:
    MaxAggregatorFactory() {}
    ~MaxAggregatorFactory() override {}
    std::unique_ptr<Aggregator> CreateAggregator(int32_t inputType, int32_t outputType, int32_t channel,
        bool inputRaw = true, bool outputPartial = false) override
    {
        switch (inputType) {
            case OMNI_VEC_TYPE_INT:
            case OMNI_VEC_TYPE_DATE32: {
                return std::make_unique<MaxAggregator<IntVector, int32_t>>(inputType, outputType, channel, inputRaw,
                    outputPartial);
            }
            case OMNI_VEC_TYPE_LONG:
            case OMNI_VEC_TYPE_DECIMAL64: {
                return std::make_unique<MaxAggregator<LongVector, int64_t>>(inputType, outputType, channel, inputRaw,
                    outputPartial);
            }
            case OMNI_VEC_TYPE_DOUBLE: {
                return std::make_unique<MaxAggregator<DoubleVector, double>>(inputType, outputType, channel, inputRaw,
                    outputPartial);
            }
            case OMNI_VEC_TYPE_DECIMAL128: {
                return std::make_unique<MaxAggregator<Decimal128Vector, Decimal128>>(inputType, outputType, channel,
                    inputRaw, outputPartial);
            }
            case OMNI_VEC_TYPE_VARCHAR:
            case OMNI_VEC_TYPE_CHAR: {
                return std::make_unique<MaxVarcharAggregator>(inputType, outputType, channel, inputRaw, outputPartial);
            }
            default: {
                LogError("Unsupported input type %d for min aggregate", inputType);
                return nullptr;
            }
        }
    }
};

class CountAggregatorFactory : public AggregatorFactory {
public:
    CountAggregatorFactory() {}
    ~CountAggregatorFactory() override {}
    std::unique_ptr<Aggregator> CreateAggregator(int32_t inputType, int32_t outputType, int32_t channel,
        bool inputRaw = true, bool outputPartial = false) override
    {
        return std::make_unique<CountAggregator>(inputType, outputType, channel, inputRaw, outputPartial);
    }
};

template <class T> class MaskAggregatorFactory : public AggregatorFactory {
public:
    MaskAggregatorFactory(int32_t maskCol) : maskColumnId(maskCol), realFactory(std::make_unique<T>()) {}
    ~MaskAggregatorFactory() override {}
    std::unique_ptr<Aggregator> CreateAggregator(int32_t inputType, int32_t outputType, int32_t inputChannel,
        bool inputRaw = true, bool outputPartial = false) override
    {
        std::unique_ptr<Aggregator> realAggregator =
            realFactory->CreateAggregator(inputType, outputType, inputChannel, inputRaw, outputPartial);
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
        case OMNI_AGGREGATION_TYPE_COUNT: {
            return std::make_unique<CountAggregatorFactory>();
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
