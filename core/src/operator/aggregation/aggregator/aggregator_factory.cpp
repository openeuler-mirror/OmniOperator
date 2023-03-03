/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 */

#include "aggregator_factory.h"

namespace omniruntime {
namespace op {
std::unique_ptr<AggregatorFactory> CreateAggregatorFactory(FunctionType aggType)
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

template <template <bool, bool, typename...> class T, typename... Args>
std::unique_ptr<Aggregator> CreateAggregatorHelper(const DataTypes &inputTypes, const DataTypes &outputTypes,
    std::vector<int32_t> &channels, bool inputRaw, bool outputPartial, bool isOverflowAsNull)
{
    if (inputRaw) {
        if (outputPartial) {
            return std::make_unique<T<true, true, Args...>>(inputTypes, outputTypes, channels, inputRaw, outputPartial,
                isOverflowAsNull);
        } else {
            return std::make_unique<T<true, false, Args...>>(inputTypes, outputTypes, channels, inputRaw, outputPartial,
                isOverflowAsNull);
        }
    } else {
        if (outputPartial) {
            return std::make_unique<T<false, true, Args...>>(inputTypes, outputTypes, channels, inputRaw, outputPartial,
                isOverflowAsNull);
        } else {
            return std::make_unique<T<false, false, Args...>>(inputTypes, outputTypes, channels, inputRaw,
                outputPartial, isOverflowAsNull);
        }
    }
}

std::unique_ptr<Aggregator> SumSparkAggregatorFactory::CreateAggregator(const DataTypes &inputTypes,
    const DataTypes &outputTypes, std::vector<int32_t> &channels, bool inputRaw, bool outputPartial,
    bool isOverflowAsNull)
{
    auto inputTypeId = inputTypes.GetIds()[0];
    switch (inputTypeId) {
        case OMNI_SHORT: {
            return CreateAggregatorHelper<SumFlatIMAggregator, int16_t , int64_t>(inputTypes,
                outputTypes, channels, inputRaw, outputPartial, isOverflowAsNull);
        }
        case OMNI_INT: {
            return CreateAggregatorHelper<SumFlatIMAggregator, int32_t, int64_t>(inputTypes, outputTypes,
                channels, inputRaw, outputPartial, isOverflowAsNull);
        }
        case OMNI_LONG: {
            return CreateAggregatorHelper<SumFlatIMAggregator, int64_t, int64_t>(inputTypes, outputTypes,
                channels, inputRaw, outputPartial, isOverflowAsNull);
        }
        case OMNI_DOUBLE: {
            return CreateAggregatorHelper<SumFlatIMAggregator, double, double>(inputTypes,
                outputTypes, channels, inputRaw, outputPartial, isOverflowAsNull);
        }
        case OMNI_DECIMAL64:
        case OMNI_DECIMAL128: {
            return CreateAggregatorHelper<SumSparkDecimalAggregator>(inputTypes, outputTypes, channels, inputRaw,
                outputPartial, isOverflowAsNull);
        }
        default: {
            LogError("Unsupported input type %d for spark sum aggregate", inputTypeId);
            return nullptr;
        }
    }
}

std::unique_ptr<Aggregator> AverageSparkAggregatorFactory::CreateAggregator(const DataTypes &inputTypes,
    const DataTypes &outputTypes, std::vector<int32_t> &channels, bool inputRaw, bool outputPartial,
    bool isOverflowAsNull)
{
    // fetch first inputTypes id as aggregator input type and map to type
    // spark rule for average function input type:
    //    timestamp/sting: cast as double
    //    boolean, date, binnary: not support
    auto inputTypeId = inputTypes.GetIds()[0];
    switch (inputTypeId) {
        case OMNI_SHORT: {
            return CreateAggregatorHelper<AverageFlatIMAggregator, int16_t>(inputTypes, outputTypes, channels,
                inputRaw, outputPartial, isOverflowAsNull);
        }
        case OMNI_INT: {
            return CreateAggregatorHelper<AverageFlatIMAggregator, int32_t>(inputTypes, outputTypes, channels,
                inputRaw, outputPartial, isOverflowAsNull);
        }
        case OMNI_LONG: {
            return CreateAggregatorHelper<AverageFlatIMAggregator, int64_t>(inputTypes, outputTypes, channels,
                inputRaw, outputPartial, isOverflowAsNull);
        }
        case OMNI_DOUBLE: {
            return CreateAggregatorHelper<AverageFlatIMAggregator, double>(inputTypes, outputTypes, channels,
                inputRaw, outputPartial, isOverflowAsNull);
        }
        case OMNI_DECIMAL64:
        case OMNI_DECIMAL128: {
            return CreateAggregatorHelper<AverageSparkDecimalAggregator>(inputTypes, outputTypes, channels, inputRaw,
                outputPartial, isOverflowAsNull);
        }
        default: {
            LogError("Unsupported input type %d for spark average aggregate", inputTypeId);
            return nullptr;
        }
    }
}


template <typename InputType>
std::unique_ptr<Aggregator> FirstAggregatorFactory::CreateFirstAggregatorHelper(FunctionType aggregateType,
    const DataTypes &inputTypes, const DataTypes &outputTypes, std::vector<int32_t> &channels, bool inputRaw,
    bool outputPartial, bool isOverflowAsNull)
{
    if (inputRaw) {
        if (outputPartial) {
            return std::make_unique<FirstAggregator<true, true, InputType>>(aggregateType, inputTypes,
                outputTypes, channels, inputRaw, outputPartial, isOverflowAsNull);
        } else {
            return std::make_unique<FirstAggregator<true, false, InputType>>(aggregateType, inputTypes,
                outputTypes, channels, inputRaw, outputPartial, isOverflowAsNull);
        }
    } else {
        if (outputPartial) {
            return std::make_unique<FirstAggregator<false, true, InputType>>(aggregateType, inputTypes,
                outputTypes, channels, inputRaw, outputPartial, isOverflowAsNull);
        } else {
            return std::make_unique<FirstAggregator<false, false, InputType>>(aggregateType, inputTypes,
                outputTypes, channels, inputRaw, outputPartial, isOverflowAsNull);
        }
    }
}

std::unique_ptr<Aggregator> FirstAggregatorFactory::CreateAggregator(const DataTypes &inputTypes,
    const DataTypes &outputTypes, std::vector<int32_t> &channels, bool inputRaw, bool outputPartial,
    bool isOverflowAsNull)
{
    // fetch first inputTypes id as aggregator input type and map to type
    // spark rule for first function input type:
    //    binnary/sting: run with SortAggregateExec, so current not implemented
    auto inputTypeId = inputTypes.GetIds()[0];
    switch (inputTypeId) {
        case OMNI_BOOLEAN: {
            return CreateFirstAggregatorHelper<bool>(aggregateType, inputTypes, outputTypes, channels,
                inputRaw, outputPartial, isOverflowAsNull);
        }
        case OMNI_SHORT: {
            return CreateFirstAggregatorHelper<int16_t>(aggregateType, inputTypes, outputTypes, channels,
                inputRaw, outputPartial, isOverflowAsNull);
        }
        case OMNI_INT:
        case OMNI_DATE32: {
            return CreateFirstAggregatorHelper<int32_t>(aggregateType, inputTypes, outputTypes, channels,
                inputRaw, outputPartial, isOverflowAsNull);
        }
        case OMNI_LONG:
        case OMNI_DECIMAL64: {
            return CreateFirstAggregatorHelper<int64_t>(aggregateType, inputTypes, outputTypes, channels,
                inputRaw, outputPartial, isOverflowAsNull);
        }
        case OMNI_DOUBLE: {
            return CreateFirstAggregatorHelper<double>(aggregateType, inputTypes, outputTypes, channels,
                inputRaw, outputPartial, isOverflowAsNull);
        }
        case OMNI_DECIMAL128: {
            return CreateFirstAggregatorHelper<Decimal128>(aggregateType, inputTypes, outputTypes,
                channels, inputRaw, outputPartial, isOverflowAsNull);
        }
        default: {
            LogError("Unsupported input type %d for first aggregate", inputTypeId);
            return nullptr;
        }
    }
}
}
}
