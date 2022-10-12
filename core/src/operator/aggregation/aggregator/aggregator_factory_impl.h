#pragma once

/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Aggregate factories
 */

#include "aggregator_factory.h"
#include "all_aggregators.h"
#include "util/config_util.h"
#include "aggregator_factory_sum.h"
#include "aggregator_factory_average.h"
#include "aggregator_factory_min_max.h"
#include "aggregator_factory_count.h"
#include "aggregator_factory_mask.h"

namespace omniruntime {
namespace op {
class SumSparkAggregatorFactory : public AggregatorFactory {
public:
    SumSparkAggregatorFactory() = default;
    ~SumSparkAggregatorFactory() override = default;
    std::unique_ptr<Aggregator> CreateAggregator(DataTypesPtr inputTypes, DataTypesPtr outputTypes,
        std::vector<int32_t> &channels, bool inputRaw = true, bool outputPartial = false,
        bool isOverflowAsNull = true) override
    {
        // fetch first inputTypes id as aggregator input type and map to type
        // spark rule for sum function input type:
        //    timestamp/sting: cast as double
        //    boolean, date, binnary: not support
        auto inputTypeId = inputTypes->GetIds()[0];
        switch (inputTypeId) {
            case OMNI_SHORT: {
                return std::make_unique<SumFlatIMAggregator<ShortVector, LongVector, int64_t>>(std::move(inputTypes),
                    std::move(outputTypes), channels, inputRaw, outputPartial, isOverflowAsNull);
            }
            case OMNI_INT: {
                return std::make_unique<SumFlatIMAggregator<IntVector, LongVector, int64_t>>(std::move(inputTypes),
                    std::move(outputTypes), channels, inputRaw, outputPartial, isOverflowAsNull);
            }
            case OMNI_LONG: {
                return std::make_unique<SumFlatIMAggregator<LongVector, LongVector, int64_t>>(std::move(inputTypes),
                    std::move(outputTypes), channels, inputRaw, outputPartial, isOverflowAsNull);
            }
            case OMNI_DOUBLE: {
                return std::make_unique<SumFlatIMAggregator<DoubleVector, DoubleVector, double>>(std::move(inputTypes),
                    std::move(outputTypes), channels, inputRaw, outputPartial, isOverflowAsNull);
            }
            case OMNI_DECIMAL64:
            case OMNI_DECIMAL128: {
                return std::make_unique<SumSparkDecimalAggregator>(std::move(inputTypes), std::move(outputTypes),
                    channels, inputRaw, outputPartial, isOverflowAsNull);
            }
            default: {
                LogError("Unsupported input type %d for spark sum aggregate", inputTypeId);
                return nullptr;
            }
        }
    }
};

class AverageSparkAggregatorFactory : public AggregatorFactory {
public:
    AverageSparkAggregatorFactory() = default;
    ~AverageSparkAggregatorFactory() override = default;
    std::unique_ptr<Aggregator> CreateAggregator(DataTypesPtr inputTypes, DataTypesPtr outputTypes,
        std::vector<int32_t> &channels, bool inputRaw = true, bool outputPartial = false,
        bool isOverflowAsNull = true) override
    {
        // fetch first inputTypes id as aggregator input type and map to type
        // spark rule for average function input type:
        //    timestamp/sting: cast as double
        //    boolean, date, binnary: not support
        auto inputTypeId = inputTypes->GetIds()[0];
        switch (inputTypeId) {
            case OMNI_SHORT: {
                return std::make_unique<AverageFlatIMAggregator<ShortVector>>(std::move(inputTypes),
                    std::move(outputTypes), channels, inputRaw, outputPartial, isOverflowAsNull);
            }
            case OMNI_INT: {
                return std::make_unique<AverageFlatIMAggregator<IntVector>>(std::move(inputTypes),
                    std::move(outputTypes), channels, inputRaw, outputPartial, isOverflowAsNull);
            }
            case OMNI_LONG: {
                return std::make_unique<AverageFlatIMAggregator<LongVector>>(std::move(inputTypes),
                    std::move(outputTypes), channels, inputRaw, outputPartial, isOverflowAsNull);
            }
            case OMNI_DOUBLE: {
                return std::make_unique<AverageFlatIMAggregator<DoubleVector>>(std::move(inputTypes),
                    std::move(outputTypes), channels, inputRaw, outputPartial, isOverflowAsNull);
            }
            case OMNI_DECIMAL64:
            case OMNI_DECIMAL128: {
                return std::make_unique<AverageSparkDecimalAggregator>(std::move(inputTypes), std::move(outputTypes),
                    channels, inputRaw, outputPartial, isOverflowAsNull);
            }
            default: {
                LogError("Unsupported input type %d for spark average aggregate", inputTypeId);
                return nullptr;
            }
        }
    }
};

class FirstAggregatorFactory : public AggregatorFactory {
public:
    explicit FirstAggregatorFactory(FunctionType aggregateType) : aggregateType(aggregateType) {}
    ~FirstAggregatorFactory() override = default;
    std::unique_ptr<Aggregator> CreateAggregator(DataTypesPtr inputTypes, DataTypesPtr outputTypes,
        std::vector<int32_t> &channels, bool inputRaw = true, bool outputPartial = false,
        bool isOverflowAsNull = false) override
    {
        // fetch first inputTypes id as aggregator input type and map to type
        // spark rule for first function input type:
        //    binnary/sting: run with SortAggregateExec, so current not implemented
        auto inputTypeId = inputTypes->GetIds()[0];
        switch (inputTypeId) {
            case OMNI_BOOLEAN: {
                return std::make_unique<FirstAggregator<BooleanVector, bool>>(aggregateType, std::move(inputTypes),
                    std::move(outputTypes), channels, inputRaw, outputPartial, isOverflowAsNull);
            }
            case OMNI_SHORT: {
                return std::make_unique<FirstAggregator<ShortVector, int16_t>>(aggregateType, std::move(inputTypes),
                    std::move(outputTypes), channels, inputRaw, outputPartial, isOverflowAsNull);
            }
            case OMNI_INT:
            case OMNI_DATE32: {
                return std::make_unique<FirstAggregator<IntVector, int32_t>>(aggregateType, std::move(inputTypes),
                    std::move(outputTypes), channels, inputRaw, outputPartial, isOverflowAsNull);
            }
            case OMNI_LONG:
            case OMNI_DECIMAL64: {
                return std::make_unique<FirstAggregator<LongVector, int64_t>>(aggregateType, std::move(inputTypes),
                    std::move(outputTypes), channels, inputRaw, outputPartial, isOverflowAsNull);
            }
            case OMNI_DOUBLE: {
                return std::make_unique<FirstAggregator<DoubleVector, double>>(aggregateType, std::move(inputTypes),
                    std::move(outputTypes), channels, inputRaw, outputPartial, isOverflowAsNull);
            }
            case OMNI_DECIMAL128: {
                return std::make_unique<FirstAggregator<Decimal128Vector, Decimal128>>(aggregateType,
                    std::move(inputTypes), std::move(outputTypes), channels, inputRaw, outputPartial, isOverflowAsNull);
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
            if (ConfigUtil::GetFlatDataTypesRule() == SupportContainerVecRule::NotSupport) {
                return std::make_unique<SumSparkAggregatorFactory>();
            } else {
                return std::make_unique<SumAggregatorFactory>();
            }
        }
        case OMNI_AGGREGATION_TYPE_AVG: {
            if (ConfigUtil::GetFlatDataTypesRule() == SupportContainerVecRule::NotSupport) {
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
