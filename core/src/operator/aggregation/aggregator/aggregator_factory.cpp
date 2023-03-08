/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 */

#include "aggregator_factory.h"

namespace omniruntime {
namespace op {
template <template <DataTypeId, DataTypeId> class T>
std::unique_ptr<Aggregator> TypedAggregatorFactory<T>::CreateAggregatorInternal(const DataTypes &inputTypes,
    const DataTypes &outputTypes, std::vector<int32_t> &channels,
    bool inputRaw, bool outputPartial, bool isOverflowAsNull)
{
    auto outputTypeId = outputTypes.GetType(0)->GetId();
    switch (outputTypeId) {
        case OMNI_BOOLEAN:
            return FromKnownOutput<OMNI_BOOLEAN>(std::move(inputTypes), std::move(outputTypes), channels, inputRaw,
                outputPartial, isOverflowAsNull);
        case OMNI_SHORT:
            return FromKnownOutput<OMNI_SHORT>(std::move(inputTypes), std::move(outputTypes), channels, inputRaw,
                outputPartial, isOverflowAsNull);
        case OMNI_DATE32:
        case OMNI_TIME32:
        case OMNI_INT:
            return FromKnownOutput<OMNI_INT>(std::move(inputTypes), std::move(outputTypes), channels, inputRaw,
                outputPartial, isOverflowAsNull);
        case OMNI_LONG:
        case OMNI_DATE64:
        case OMNI_TIME64:
        case OMNI_TIMESTAMP:
            return FromKnownOutput<OMNI_LONG>(std::move(inputTypes), std::move(outputTypes), channels, inputRaw,
                outputPartial, isOverflowAsNull);
        case OMNI_DOUBLE:
            return FromKnownOutput<OMNI_DOUBLE>(std::move(inputTypes), std::move(outputTypes), channels, inputRaw,
                outputPartial, isOverflowAsNull);
        case OMNI_DECIMAL64:
            return FromKnownOutput<OMNI_DECIMAL64>(std::move(inputTypes), std::move(outputTypes), channels, inputRaw,
                outputPartial, isOverflowAsNull);
        case OMNI_DECIMAL128:
            return FromKnownOutput<OMNI_DECIMAL128>(std::move(inputTypes), std::move(outputTypes), channels, inputRaw,
                outputPartial, isOverflowAsNull);
        case OMNI_CONTAINER:
            return FromKnownOutput<OMNI_CONTAINER>(std::move(inputTypes), std::move(outputTypes), channels, inputRaw,
                outputPartial, isOverflowAsNull);
        case OMNI_VARCHAR:
            return FromKnownOutput<OMNI_VARCHAR>(std::move(inputTypes), std::move(outputTypes), channels, inputRaw,
                outputPartial, isOverflowAsNull);
        case OMNI_CHAR:
            return FromKnownOutput<OMNI_CHAR>(std::move(inputTypes), std::move(outputTypes), channels, inputRaw,
                outputPartial, isOverflowAsNull);
        default:
            LogError("Unsupported output type %s", TypeUtil::TypeToStringLog(outputTypeId).c_str());
            return nullptr;
    }
}

template <template <DataTypeId, DataTypeId> class T>
template <DataTypeId OUT_ID>
std::unique_ptr<Aggregator> TypedAggregatorFactory<T>::FromKnownOutput(const DataTypes &inputTypes,
    const DataTypes &outputTypes, std::vector<int32_t> &channels,
    bool inputRaw, bool outputPartial, bool isOverflowAsNull)
{
    auto inputTypeId = inputTypes.GetType(0)->GetId();
    switch (inputTypeId) {
        case OMNI_BOOLEAN:
            return T<OMNI_BOOLEAN, OUT_ID>::Create(std::move(inputTypes), std::move(outputTypes), channels, inputRaw,
                outputPartial, isOverflowAsNull);
        case OMNI_SHORT:
            return T<OMNI_SHORT, OUT_ID>::Create(std::move(inputTypes), std::move(outputTypes), channels, inputRaw,
                outputPartial, isOverflowAsNull);
        case OMNI_DATE32:
        case OMNI_TIME32:
        case OMNI_INT:
            return T<OMNI_INT, OUT_ID>::Create(std::move(inputTypes), std::move(outputTypes), channels, inputRaw,
                outputPartial, isOverflowAsNull);
        case OMNI_LONG:
        case OMNI_DATE64:
        case OMNI_TIME64:
        case OMNI_TIMESTAMP:
            return T<OMNI_LONG, OUT_ID>::Create(std::move(inputTypes), std::move(outputTypes), channels, inputRaw,
                outputPartial, isOverflowAsNull);
        case OMNI_DOUBLE:
            return T<OMNI_DOUBLE, OUT_ID>::Create(std::move(inputTypes), std::move(outputTypes), channels, inputRaw,
                outputPartial, isOverflowAsNull);
        case OMNI_DECIMAL64:
            return T<OMNI_DECIMAL64, OUT_ID>::Create(std::move(inputTypes), std::move(outputTypes), channels, inputRaw,
                outputPartial, isOverflowAsNull);
        case OMNI_DECIMAL128:
            return T<OMNI_DECIMAL128, OUT_ID>::Create(std::move(inputTypes), std::move(outputTypes), channels, inputRaw,
                outputPartial, isOverflowAsNull);
        case OMNI_CONTAINER:
            return T<OMNI_CONTAINER, OUT_ID>::Create(std::move(inputTypes), std::move(outputTypes), channels, inputRaw,
                outputPartial, isOverflowAsNull);
        case OMNI_VARCHAR:
            return T<OMNI_VARCHAR, OUT_ID>::Create(std::move(inputTypes), std::move(outputTypes), channels, inputRaw,
                outputPartial, isOverflowAsNull);
        case OMNI_CHAR:
            return T<OMNI_CHAR, OUT_ID>::Create(std::move(inputTypes), std::move(outputTypes), channels, inputRaw,
                outputPartial, isOverflowAsNull);
        case OMNI_NONE:
            return T<OMNI_NONE, OUT_ID>::Create(std::move(inputTypes), std::move(outputTypes), channels, inputRaw,
                outputPartial, isOverflowAsNull);
        case OMNI_INVALID:
            return T<OMNI_INVALID, OUT_ID>::Create(std::move(inputTypes), std::move(outputTypes), channels, inputRaw,
                outputPartial, isOverflowAsNull);
        default:
            LogError("Unsupported input type %s", TypeUtil::TypeToStringLog(inputTypeId).c_str());
            return nullptr;
    }
}

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
            return CreateAggregatorHelper<SumFlatIMAggregator, ShortVector, LongVector, int64_t>(inputTypes,
                outputTypes, channels, inputRaw, outputPartial, isOverflowAsNull);
        }
        case OMNI_INT: {
            return CreateAggregatorHelper<SumFlatIMAggregator, IntVector, LongVector, int64_t>(inputTypes, outputTypes,
                channels, inputRaw, outputPartial, isOverflowAsNull);
        }
        case OMNI_LONG: {
            return CreateAggregatorHelper<SumFlatIMAggregator, LongVector, LongVector, int64_t>(inputTypes, outputTypes,
                channels, inputRaw, outputPartial, isOverflowAsNull);
        }
        case OMNI_DOUBLE: {
            return CreateAggregatorHelper<SumFlatIMAggregator, DoubleVector, DoubleVector, double>(inputTypes,
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
            return CreateAggregatorHelper<AverageFlatIMAggregator, ShortVector>(inputTypes, outputTypes, channels,
                inputRaw, outputPartial, isOverflowAsNull);
        }
        case OMNI_INT: {
            return CreateAggregatorHelper<AverageFlatIMAggregator, IntVector>(inputTypes, outputTypes, channels,
                inputRaw, outputPartial, isOverflowAsNull);
        }
        case OMNI_LONG: {
            return CreateAggregatorHelper<AverageFlatIMAggregator, LongVector>(inputTypes, outputTypes, channels,
                inputRaw, outputPartial, isOverflowAsNull);
        }
        case OMNI_DOUBLE: {
            return CreateAggregatorHelper<AverageFlatIMAggregator, DoubleVector>(inputTypes, outputTypes, channels,
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


template <typename InputVecType, typename InputType>
std::unique_ptr<Aggregator> FirstAggregatorFactory::CreateFirstAggregatorHelper(FunctionType aggregateType,
    const DataTypes &inputTypes, const DataTypes &outputTypes, std::vector<int32_t> &channels, bool inputRaw,
    bool outputPartial, bool isOverflowAsNull)
{
    if (inputRaw) {
        if (outputPartial) {
            return std::make_unique<FirstAggregator<true, true, InputVecType, InputType>>(aggregateType, inputTypes,
                outputTypes, channels, inputRaw, outputPartial, isOverflowAsNull);
        } else {
            return std::make_unique<FirstAggregator<true, false, InputVecType, InputType>>(aggregateType, inputTypes,
                outputTypes, channels, inputRaw, outputPartial, isOverflowAsNull);
        }
    } else {
        if (outputPartial) {
            return std::make_unique<FirstAggregator<false, true, InputVecType, InputType>>(aggregateType, inputTypes,
                outputTypes, channels, inputRaw, outputPartial, isOverflowAsNull);
        } else {
            return std::make_unique<FirstAggregator<false, false, InputVecType, InputType>>(aggregateType, inputTypes,
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
            return CreateFirstAggregatorHelper<BooleanVector, bool>(aggregateType, inputTypes, outputTypes, channels,
                inputRaw, outputPartial, isOverflowAsNull);
        }
        case OMNI_SHORT: {
            return CreateFirstAggregatorHelper<ShortVector, int16_t>(aggregateType, inputTypes, outputTypes, channels,
                inputRaw, outputPartial, isOverflowAsNull);
        }
        case OMNI_INT:
        case OMNI_DATE32: {
            return CreateFirstAggregatorHelper<IntVector, int32_t>(aggregateType, inputTypes, outputTypes, channels,
                inputRaw, outputPartial, isOverflowAsNull);
        }
        case OMNI_LONG:
        case OMNI_DECIMAL64: {
            return CreateFirstAggregatorHelper<LongVector, int64_t>(aggregateType, inputTypes, outputTypes, channels,
                inputRaw, outputPartial, isOverflowAsNull);
        }
        case OMNI_DOUBLE: {
            return CreateFirstAggregatorHelper<DoubleVector, double>(aggregateType, inputTypes, outputTypes, channels,
                inputRaw, outputPartial, isOverflowAsNull);
        }
        case OMNI_DECIMAL128: {
            return CreateFirstAggregatorHelper<Decimal128Vector, Decimal128>(aggregateType, inputTypes, outputTypes,
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
