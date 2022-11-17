/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Aggregate factories
 */
#ifndef OMNI_RUNTIME_AGGREGATOR_FACTORY_H
#define OMNI_RUNTIME_AGGREGATOR_FACTORY_H

#include "all_aggregators.h"
#include "util/config_util.h"

namespace omniruntime {
namespace op {
class AggregatorFactory {
public:
    AggregatorFactory() = default;
    virtual ~AggregatorFactory() = default;
    /* *
     * This interface is for creating aggregators. You have to specify the data type for both input and output data.
     * Also the phase of the aggregator to be created is determined by 'inputRaw' and 'outputPartial'.
     * @param inputTypes
     * @param outputTypes
     * @param inputRaw
     * @param outputPartial
     * @param overflowAsNull  default overflow will throw exception
     * @return
     */
    virtual std::unique_ptr<Aggregator> CreateAggregator(
        const DataTypes &inputTypes, const DataTypes &outputTypes, std::vector<int32_t> &channels,
        bool inputRaw, bool outputPartial, bool isOverflowAsNull) = 0;
};

template <template <bool, bool, bool, DataTypeId, DataTypeId> class T>
class TypedAggregatorFactory : public AggregatorFactory {
public:
    TypedAggregatorFactory() = default;
    ~TypedAggregatorFactory() override = default;

    std::unique_ptr<Aggregator> CreateAggregator(
        const DataTypes &inputTypes, const DataTypes &outputTypes, std::vector<int32_t> &channels,
        bool inputRaw, bool outputPartial, bool isOverflowAsNull) override
    {
        if (inputRaw) {
            if (outputPartial) {
                if (isOverflowAsNull) {
                    return CreateAggregator<true, true, true>(inputTypes, outputTypes, channels);
                } else {
                    return CreateAggregator<true, true, false>(inputTypes, outputTypes, channels);
                }
            } else {
                if (isOverflowAsNull) {
                    return CreateAggregator<true, false, true>(inputTypes, outputTypes, channels);
                } else {
                    return CreateAggregator<true, false, false>(inputTypes, outputTypes, channels);
                }
            }
        } else {
            if (outputPartial) {
                if (isOverflowAsNull) {
                    return CreateAggregator<false, true, true>(inputTypes, outputTypes, channels);
                } else {
                    return CreateAggregator<false, true, false>(inputTypes, outputTypes, channels);
                }
            } else {
                if (isOverflowAsNull) {
                    return CreateAggregator<false, false, true>(inputTypes, outputTypes, channels);
                } else {
                    return CreateAggregator<false, false, false>(inputTypes, outputTypes, channels);
                }
            }
        }
    }

protected:
    template <bool RAW_IN, bool PARTIAL_OUT, bool NULL_OVERFLOW>
    std::unique_ptr<Aggregator> CreateAggregator(
            const DataTypes &inputTypes, const DataTypes &outputTypes, std::vector<int32_t> &channels)
    {
        auto outputTypeId = outputTypes.GetType(0)->GetId();
        switch (outputTypeId) {
            case OMNI_BOOLEAN:
                return fromKnownOutput<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, OMNI_BOOLEAN>(
                    std::move(inputTypes), std::move(outputTypes), channels);
            case OMNI_SHORT:
                return fromKnownOutput<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, OMNI_SHORT>(
                    std::move(inputTypes), std::move(outputTypes), channels);
            case OMNI_DATE32:
            case OMNI_TIME32:
            case OMNI_INT:
                return fromKnownOutput<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, OMNI_INT>(
                    std::move(inputTypes), std::move(outputTypes), channels);
            case OMNI_LONG:
            case OMNI_DATE64:
            case OMNI_TIME64:
            case OMNI_TIMESTAMP:
                return fromKnownOutput<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, OMNI_LONG>(
                    std::move(inputTypes), std::move(outputTypes), channels);
            case OMNI_DOUBLE:
                return fromKnownOutput<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, OMNI_DOUBLE>(
                    std::move(inputTypes), std::move(outputTypes), channels);
            case OMNI_DECIMAL64:
                return fromKnownOutput<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, OMNI_DECIMAL64>(
                    std::move(inputTypes), std::move(outputTypes), channels);
            case OMNI_DECIMAL128:
                return fromKnownOutput<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, OMNI_DECIMAL128>(
                    std::move(inputTypes), std::move(outputTypes), channels);
            case OMNI_CONTAINER:
                return fromKnownOutput<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, OMNI_CONTAINER>(
                    std::move(inputTypes), std::move(outputTypes), channels);
            case OMNI_VARCHAR:
                return fromKnownOutput<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, OMNI_VARCHAR>(
                    std::move(inputTypes), std::move(outputTypes), channels);
            case OMNI_CHAR:
                return fromKnownOutput<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, OMNI_CHAR>(
                    std::move(inputTypes), std::move(outputTypes), channels);
            default:
                LogError("Unsupported output type %s", TypeUtil::TypeToString(outputTypeId).c_str());
                return nullptr;
        }
    }

    template <bool RAW_IN, bool PARTIAL_OUT, bool NULL_OVERFLOW, DataTypeId OUT_ID>
    std::unique_ptr<Aggregator> fromKnownOutput(
            const DataTypes &inputTypes, const DataTypes &outputTypes, std::vector<int32_t> &channels)
    {
        auto inputTypeId = inputTypes.GetType(0)->GetId();
        switch (inputTypeId) {
            case OMNI_BOOLEAN:
                return T<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, OMNI_BOOLEAN, OUT_ID>::Create(
                    std::move(inputTypes), std::move(outputTypes), channels);
            case OMNI_SHORT:
                return T<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, OMNI_SHORT, OUT_ID>::Create(
                    std::move(inputTypes), std::move(outputTypes), channels);
            case OMNI_DATE32:
            case OMNI_TIME32:
            case OMNI_INT:
                return T<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, OMNI_INT, OUT_ID>::Create(
                    std::move(inputTypes), std::move(outputTypes), channels);
            case OMNI_LONG:
            case OMNI_DATE64:
            case OMNI_TIME64:
            case OMNI_TIMESTAMP:
                return T<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, OMNI_LONG, OUT_ID>::Create(
                    std::move(inputTypes), std::move(outputTypes), channels);
            case OMNI_DOUBLE:
                return T<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, OMNI_DOUBLE, OUT_ID>::Create(
                    std::move(inputTypes), std::move(outputTypes), channels);
            case OMNI_DECIMAL64:
                return T<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, OMNI_DECIMAL64, OUT_ID>::Create(
                    std::move(inputTypes), std::move(outputTypes), channels);
            case OMNI_DECIMAL128:
                return T<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, OMNI_DECIMAL128, OUT_ID>::Create(
                    std::move(inputTypes), std::move(outputTypes), channels);
            case OMNI_CONTAINER:
                return T<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, OMNI_CONTAINER, OUT_ID>::Create(
                    std::move(inputTypes), std::move(outputTypes), channels);
            case OMNI_VARCHAR:
                return T<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, OMNI_VARCHAR, OUT_ID>::Create(
                    std::move(inputTypes), std::move(outputTypes), channels);
            case OMNI_CHAR:
                return T<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, OMNI_CHAR, OUT_ID>::Create(
                    std::move(inputTypes), std::move(outputTypes), channels);
            case OMNI_NONE:
                return T<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, OMNI_NONE, OUT_ID>::Create(
                    std::move(inputTypes), std::move(outputTypes), channels);
            case OMNI_INVALID:
                return T<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, OMNI_INVALID, OUT_ID>::Create(
                    std::move(inputTypes), std::move(outputTypes), channels);
            default:
                LogError("Unsupported input type %s", TypeUtil::TypeToString(inputTypeId).c_str());
                return nullptr;
        }
    }
};

// Implementation of Aggregator factories
class SumSparkAggregatorFactory : public AggregatorFactory {
public:
    SumSparkAggregatorFactory() = default;
    ~SumSparkAggregatorFactory() override = default;
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
                return std::make_unique<SumFlatIMAggregator<ShortVector, LongVector, int64_t>>(inputTypes,
                    outputTypes, channels, inputRaw, outputPartial, isOverflowAsNull);
            }
            case OMNI_INT: {
                return std::make_unique<SumFlatIMAggregator<IntVector, LongVector, int64_t>>(inputTypes,
                    outputTypes, channels, inputRaw, outputPartial, isOverflowAsNull);
            }
            case OMNI_LONG: {
                return std::make_unique<SumFlatIMAggregator<LongVector, LongVector, int64_t>>(inputTypes,
                    outputTypes, channels, inputRaw, outputPartial, isOverflowAsNull);
            }
            case OMNI_DOUBLE: {
                return std::make_unique<SumFlatIMAggregator<DoubleVector, DoubleVector, double>>(inputTypes,
                    outputTypes, channels, inputRaw, outputPartial, isOverflowAsNull);
            }
            case OMNI_DECIMAL64:
            case OMNI_DECIMAL128: {
                return std::make_unique<SumSparkDecimalAggregator>(inputTypes, outputTypes,
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
                return std::make_unique<AverageFlatIMAggregator<ShortVector>>(inputTypes,
                    outputTypes, channels, inputRaw, outputPartial, isOverflowAsNull);
            }
            case OMNI_INT: {
                return std::make_unique<AverageFlatIMAggregator<IntVector>>(inputTypes,
                    outputTypes, channels, inputRaw, outputPartial, isOverflowAsNull);
            }
            case OMNI_LONG: {
                return std::make_unique<AverageFlatIMAggregator<LongVector>>(inputTypes,
                    outputTypes, channels, inputRaw, outputPartial, isOverflowAsNull);
            }
            case OMNI_DOUBLE: {
                return std::make_unique<AverageFlatIMAggregator<DoubleVector>>(inputTypes,
                    outputTypes, channels, inputRaw, outputPartial, isOverflowAsNull);
            }
            case OMNI_DECIMAL64:
            case OMNI_DECIMAL128: {
                return std::make_unique<AverageSparkDecimalAggregator>(inputTypes, outputTypes,
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
                return std::make_unique<FirstAggregator<BooleanVector, bool>>(aggregateType, inputTypes,
                    outputTypes, channels, inputRaw, outputPartial, isOverflowAsNull);
            }
            case OMNI_SHORT: {
                return std::make_unique<FirstAggregator<ShortVector, int16_t>>(aggregateType, inputTypes,
                    outputTypes, channels, inputRaw, outputPartial, isOverflowAsNull);
            }
            case OMNI_INT:
            case OMNI_DATE32: {
                return std::make_unique<FirstAggregator<IntVector, int32_t>>(aggregateType, inputTypes,
                    outputTypes, channels, inputRaw, outputPartial, isOverflowAsNull);
            }
            case OMNI_LONG:
            case OMNI_DECIMAL64: {
                return std::make_unique<FirstAggregator<LongVector, int64_t>>(aggregateType, inputTypes,
                    outputTypes, channels, inputRaw, outputPartial, isOverflowAsNull);
            }
            case OMNI_DOUBLE: {
                return std::make_unique<FirstAggregator<DoubleVector, double>>(aggregateType, inputTypes,
                    outputTypes, channels, inputRaw, outputPartial, isOverflowAsNull);
            }
            case OMNI_DECIMAL128: {
                return std::make_unique<FirstAggregator<Decimal128Vector, Decimal128>>(aggregateType,
                    inputTypes, outputTypes, channels, inputRaw, outputPartial, isOverflowAsNull);
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

class AverageAggregatorFactory : public TypedAggregatorFactory<AverageAggregator> {
public:
    AverageAggregatorFactory() : TypedAggregatorFactory<AverageAggregator>()
    {}
    ~AverageAggregatorFactory() override = default;
};

class SumAggregatorFactory : public TypedAggregatorFactory<SumAggregator> {
public:
    SumAggregatorFactory() : TypedAggregatorFactory<SumAggregator>()
    {}
    ~SumAggregatorFactory() override = default;
};

class MinAggregatorFactory : public TypedAggregatorFactory<MinAggregator> {
public:
    MinAggregatorFactory() : TypedAggregatorFactory<MinAggregator>()
    {}
    ~MinAggregatorFactory() override = default;
};

class MaxAggregatorFactory : public TypedAggregatorFactory<MaxAggregator> {
public:
    MaxAggregatorFactory() : TypedAggregatorFactory<MaxAggregator>()
    {}
    ~MaxAggregatorFactory() override = default;
};

class CountColumnAggregatorFactory : public TypedAggregatorFactory<CountColumnAggregator> {
public:
    CountColumnAggregatorFactory() : TypedAggregatorFactory<CountColumnAggregator>()
    {}
    ~CountColumnAggregatorFactory() override = default;
};

class CountAllAggregatorFactory : public TypedAggregatorFactory<CountAllAggregator> {
public:
    CountAllAggregatorFactory() : TypedAggregatorFactory<CountAllAggregator>()
    {}
    ~CountAllAggregatorFactory() override = default;
};

template <class T>
class MaskAggregatorFactory : public AggregatorFactory {
public:
    explicit MaskAggregatorFactory(int32_t maskCol) : maskColumnId(maskCol), realFactory(std::make_unique<T>()) {}
    ~MaskAggregatorFactory() override = default;

    std::unique_ptr<Aggregator> CreateAggregator(
        const DataTypes &inputTypes, const DataTypes &outputTypes, std::vector<int32_t> &channels,
        bool inputRaw, bool outputPartial, bool isOverflowAsNull) override
    {
        std::unique_ptr<Aggregator> realAggregator = realFactory->CreateAggregator(
            std::move(inputTypes), std::move(outputTypes), channels, inputRaw, outputPartial, isOverflowAsNull);
        if (realAggregator == nullptr) {
            LogError("Error in mask aggregate: Real aggregator is null");
            return nullptr;
        }
        if (realAggregator->IsTypedAggregator()) {
            if (realAggregator->IsInputRaw()) {
                if (realAggregator->IsOutputPartial()) {
                    if (realAggregator->IsOverflowAsNull()) {
                        return TypedMaskColAggregator<true, true, true>::Create(maskColumnId, std::move(realAggregator));
                    } else {
                        return TypedMaskColAggregator<true, true, false>::Create(maskColumnId, std::move(realAggregator));
                    }
                } else {
                    if (realAggregator->IsOverflowAsNull()) {
                        return TypedMaskColAggregator<true, false, true>::Create(maskColumnId, std::move(realAggregator));
                    } else {
                        return TypedMaskColAggregator<true, false, false>::Create(maskColumnId, std::move(realAggregator));
                    }
                }
            } else {
                if (realAggregator->IsOutputPartial()) {
                    if (realAggregator->IsOverflowAsNull()) {
                        return TypedMaskColAggregator<false, true, true>::Create(maskColumnId, std::move(realAggregator));
                    } else {
                        return TypedMaskColAggregator<false, true, false>::Create(maskColumnId, std::move(realAggregator));
                    }
                } else {
                    if (realAggregator->IsOverflowAsNull()) {
                        return TypedMaskColAggregator<false, false, true>::Create(maskColumnId, std::move(realAggregator));
                    } else {
                        return TypedMaskColAggregator<false, false, false>::Create(maskColumnId, std::move(realAggregator));
                    }
                }
            }
        } else {
            return std::make_unique<MaskColAggregator>(maskColumnId, std::move(realAggregator));
        }
    }

private:
    int maskColumnId;
    std::unique_ptr<AggregatorFactory> realFactory;
};

// for window aggregation call
static std::unique_ptr<AggregatorFactory> CreateAggregatorFactory(FunctionType aggType)
{
    switch (aggType) {
        case OMNI_AGGREGATION_TYPE_SUM: {
            if (ConfigUtil::GetSupportContainerVecRule() == SupportContainerVecRule::NotSupport) {
                return std::make_unique<SumSparkAggregatorFactory>();
            } else {
                return std::make_unique<SumAggregatorFactory>();
            }
        }
        case OMNI_AGGREGATION_TYPE_AVG: {
            if (ConfigUtil::GetSupportContainerVecRule() == SupportContainerVecRule::NotSupport) {
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
