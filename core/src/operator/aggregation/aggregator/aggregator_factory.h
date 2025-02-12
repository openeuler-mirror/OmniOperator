/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2023. All rights reserved.
 * Description: Aggregate factories
 */
#ifndef OMNI_RUNTIME_AGGREGATOR_FACTORY_H
#define OMNI_RUNTIME_AGGREGATOR_FACTORY_H

#include "all_aggregators.h"
#include "util/config_util.h"
#include "operator/util/function_type.h"

namespace omniruntime {
namespace op {
template <template <DataTypeId, DataTypeId> class T> class TypedAggregatorFactory : public AggregatorFactory {
public:
    TypedAggregatorFactory() = default;
    ~TypedAggregatorFactory() override = default;

    std::unique_ptr<Aggregator> CreateAggregator(const DataTypes &inputTypes, const DataTypes &outputTypes,
        std::vector<int32_t> &channels, bool inputRaw, bool outputPartial, bool isOverflowAsNull) override
    {
        return CreateAggregatorInternal(inputTypes, outputTypes, channels, inputRaw, outputPartial, isOverflowAsNull);
    }

protected:
    std::unique_ptr<Aggregator> CreateAggregatorInternal(const DataTypes &inputTypes, const DataTypes &outputTypes,
        std::vector<int32_t> &channels, bool inputRaw, bool outputPartial, bool isOverflowAsNull)
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
                return FromKnownOutput<OMNI_LONG>(std::move(inputTypes), std::move(outputTypes), channels,
                    inputRaw, outputPartial, isOverflowAsNull);
            case OMNI_DOUBLE:
                return FromKnownOutput<OMNI_DOUBLE>(std::move(inputTypes), std::move(outputTypes), channels, inputRaw,
                    outputPartial, isOverflowAsNull);
            case OMNI_DECIMAL64:
                return FromKnownOutput<OMNI_DECIMAL64>(std::move(inputTypes), std::move(outputTypes), channels,
                    inputRaw, outputPartial, isOverflowAsNull);
            case OMNI_DECIMAL128:
                return FromKnownOutput<OMNI_DECIMAL128>(std::move(inputTypes), std::move(outputTypes), channels,
                    inputRaw, outputPartial, isOverflowAsNull);
            case OMNI_CONTAINER:
                return FromKnownOutput<OMNI_CONTAINER>(std::move(inputTypes), std::move(outputTypes), channels,
                    inputRaw, outputPartial, isOverflowAsNull);
            case OMNI_VARCHAR:
                return FromKnownOutput<OMNI_VARCHAR>(std::move(inputTypes), std::move(outputTypes), channels, inputRaw,
                    outputPartial, isOverflowAsNull);
            case OMNI_CHAR:
                return FromKnownOutput<OMNI_CHAR>(std::move(inputTypes), std::move(outputTypes), channels, inputRaw,
                    outputPartial, isOverflowAsNull);
            default:
                std::string omniExceptionInfo =
                    "In function CreateAggregatorInternal, no such input type " + std::to_string(outputTypeId);
                throw omniruntime::exception::OmniException("UNSUPPORTED_ERROR", omniExceptionInfo);
        }
    }

    template <DataTypeId OUT_ID>
    std::unique_ptr<Aggregator> FromKnownOutput(const DataTypes &inputTypes, const DataTypes &outputTypes,
        std::vector<int32_t> &channels, bool inputRaw, bool outputPartial, bool isOverflowAsNull)
    {
        auto inputTypeId = inputTypes.GetType(0)->GetId();
        switch (inputTypeId) {
            case OMNI_BOOLEAN:
                return T<OMNI_BOOLEAN, OUT_ID>::Create(std::move(inputTypes), std::move(outputTypes), channels,
                    inputRaw, outputPartial, isOverflowAsNull);
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
                return T<OMNI_DECIMAL64, OUT_ID>::Create(std::move(inputTypes), std::move(outputTypes), channels,
                    inputRaw, outputPartial, isOverflowAsNull);
            case OMNI_DECIMAL128:
                return T<OMNI_DECIMAL128, OUT_ID>::Create(std::move(inputTypes), std::move(outputTypes), channels,
                    inputRaw, outputPartial, isOverflowAsNull);
            case OMNI_CONTAINER:
                return T<OMNI_CONTAINER, OUT_ID>::Create(std::move(inputTypes), std::move(outputTypes), channels,
                    inputRaw, outputPartial, isOverflowAsNull);
            case OMNI_VARCHAR:
                return T<OMNI_VARCHAR, OUT_ID>::Create(std::move(inputTypes), std::move(outputTypes), channels,
                    inputRaw, outputPartial, isOverflowAsNull);
            case OMNI_CHAR:
                return T<OMNI_CHAR, OUT_ID>::Create(std::move(inputTypes), std::move(outputTypes), channels, inputRaw,
                    outputPartial, isOverflowAsNull);
            case OMNI_NONE:
                return T<OMNI_NONE, OUT_ID>::Create(std::move(inputTypes), std::move(outputTypes), channels, inputRaw,
                    outputPartial, isOverflowAsNull);
            case OMNI_INVALID:
                return T<OMNI_INVALID, OUT_ID>::Create(std::move(inputTypes), std::move(outputTypes), channels,
                    inputRaw, outputPartial, isOverflowAsNull);
            default:
                std::string omniExceptionInfo =
                    "In function FromKnownOutput, no such input type " + std::to_string(inputTypeId);
                throw omniruntime::exception::OmniException("UNSUPPORTED_ERROR", omniExceptionInfo);
        }
    }
};

template <template <bool, bool, typename...> class T, typename... Args>
std::unique_ptr<Aggregator> CreateAggregatorHelper(const DataTypes &inputTypes, const DataTypes &outputTypes,
    std::vector<int32_t> &channels, bool inputRaw = true, bool outputPartial = false, bool isOverflowAsNull = true);

// Implementation of Aggregator factories
class SumSparkAggregatorFactory : public AggregatorFactory {
public:
    SumSparkAggregatorFactory() = default;
    ~SumSparkAggregatorFactory() override = default;

    std::unique_ptr<Aggregator> CreateAggregator(const DataTypes &inputTypes, const DataTypes &outputTypes,
        std::vector<int32_t> &channels, bool inputRaw = true, bool outputPartial = false,
        bool isOverflowAsNull = true) override;
};

class TrySumSparkAggregatorFactory : public AggregatorFactory {
public:
    TrySumSparkAggregatorFactory() = default;
    ~TrySumSparkAggregatorFactory() override = default;

    std::unique_ptr<Aggregator> CreateAggregator(const DataTypes &inputTypes, const DataTypes &outputTypes,
         std::vector<int32_t> &channels, bool inputRaw = true, bool outputPartial = false,
        bool isOverflowAsNull = true) override;
};

class AverageSparkAggregatorFactory : public AggregatorFactory {
public:
    AverageSparkAggregatorFactory() = default;
    ~AverageSparkAggregatorFactory() override = default;
    std::unique_ptr<Aggregator> CreateAggregator(const DataTypes &inputTypes, const DataTypes &outputTypes,
        std::vector<int32_t> &channels, bool inputRaw = true, bool outputPartial = false,
        bool isOverflowAsNull = true) override;
};

class TryAverageSparkAggregatorFactory : public AggregatorFactory {
public:
    TryAverageSparkAggregatorFactory() = default;
    ~TryAverageSparkAggregatorFactory() override = default;
    std::unique_ptr<Aggregator> CreateAggregator(const DataTypes &inputTypes, const DataTypes &outputTypes,
         std::vector<int32_t> &channels, bool inputRaw = true, bool outputPartial = false,
         bool isOverflowAsNull = true) override;
};


class StddevSampSparkAggregatorFactory : public AggregatorFactory {
public:
    StddevSampSparkAggregatorFactory() = default;
    ~StddevSampSparkAggregatorFactory() override = default;
    std::unique_ptr<Aggregator> CreateAggregator(const DataTypes &inputTypes, const DataTypes &outputTypes,
        std::vector<int32_t> &channels, bool inputRaw = true, bool outputPartial = false,
        bool isOverflowAsNull = true) override;
};

class FirstAggregatorFactory : public AggregatorFactory {
public:
    explicit FirstAggregatorFactory(FunctionType aggregateType) : aggregateType(aggregateType) {}
    ~FirstAggregatorFactory() override = default;
    template <typename InputType>
    std::unique_ptr<Aggregator> CreateFirstAggregatorHelper(FunctionType aggregateType, const DataTypes &inputTypes,
        const DataTypes &outputTypes, std::vector<int32_t> &channels, bool inputRaw = true, bool outputPartial = false,
        bool isOverflowAsNull = true);

    std::unique_ptr<Aggregator> CreateAggregator(const DataTypes &inputTypes, const DataTypes &outputTypes,
        std::vector<int32_t> &channels, bool inputRaw = true, bool outputPartial = false,
        bool isOverflowAsNull = false) override;

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
    AverageAggregatorFactory() : TypedAggregatorFactory<AverageAggregator>() {}
    ~AverageAggregatorFactory() override = default;
};

class SumAggregatorFactory : public TypedAggregatorFactory<SumAggregator> {
public:
    SumAggregatorFactory() : TypedAggregatorFactory<SumAggregator>() {}
    ~SumAggregatorFactory() override = default;
};

class MinAggregatorFactory : public TypedAggregatorFactory<MinAggregator> {
public:
    MinAggregatorFactory() : TypedAggregatorFactory<MinAggregator>() {}
    ~MinAggregatorFactory() override = default;
};

class MaxAggregatorFactory : public TypedAggregatorFactory<MaxAggregator> {
public:
    MaxAggregatorFactory() : TypedAggregatorFactory<MaxAggregator>() {}
    ~MaxAggregatorFactory() override = default;
};

class CountColumnAggregatorFactory : public TypedAggregatorFactory<CountColumnAggregator> {
public:
    CountColumnAggregatorFactory() : TypedAggregatorFactory<CountColumnAggregator>() {}
    ~CountColumnAggregatorFactory() override = default;
};

class CountAllAggregatorFactory : public TypedAggregatorFactory<CountAllAggregator> {
public:
    CountAllAggregatorFactory() : TypedAggregatorFactory<CountAllAggregator>() {}
    ~CountAllAggregatorFactory() override = default;
};

template <class T> class MaskAggregatorFactory : public AggregatorFactory {
public:
    explicit MaskAggregatorFactory(int32_t maskCol) : maskColumnId(maskCol), realFactory(std::make_unique<T>()) {}
    ~MaskAggregatorFactory() override = default;

    std::unique_ptr<Aggregator> CreateAggregator(const DataTypes &inputTypes, const DataTypes &outputTypes,
        std::vector<int32_t> &channels, bool inputRaw, bool outputPartial, bool isOverflowAsNull) override
    {
        std::unique_ptr<Aggregator> realAggregator = realFactory->CreateAggregator(std::move(inputTypes),
            std::move(outputTypes), channels, inputRaw, outputPartial, isOverflowAsNull);
        if (realAggregator == nullptr) {
            LogError("Error in mask aggregate: Real aggregator is null");
            return nullptr;
        }
        if (realAggregator->IsTypedAggregator()) {
            return TypedMaskColAggregator::Create(maskColumnId, std::move(realAggregator));
        } else {
            return std::make_unique<MaskColAggregator>(maskColumnId, std::move(realAggregator));
        }
    }

private:
    int maskColumnId;
    std::unique_ptr<AggregatorFactory> realFactory;
};

// for window aggregation call
std::unique_ptr<AggregatorFactory> CreateAggregatorFactory(FunctionType aggType);
}
}
#endif // OMNI_RUNTIME_AGGREGATOR_FACTORY_H
