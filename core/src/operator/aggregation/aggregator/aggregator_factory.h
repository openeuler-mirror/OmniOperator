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
template <template <DataTypeId, DataTypeId> class T>
class TypedAggregatorFactory : public AggregatorFactory {
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
        std::vector<int32_t> &channels, bool inputRaw, bool outputPartial, bool isOverflowAsNull);

    template <DataTypeId OUT_ID>
    std::unique_ptr<Aggregator> FromKnownOutput(const DataTypes &inputTypes, const DataTypes &outputTypes,
        std::vector<int32_t> &channels, bool inputRaw, bool outputPartial, bool isOverflowAsNull);
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

class AverageSparkAggregatorFactory : public AggregatorFactory {
public:
    AverageSparkAggregatorFactory() = default;
    ~AverageSparkAggregatorFactory() override = default;
    std::unique_ptr<Aggregator> CreateAggregator(const DataTypes &inputTypes, const DataTypes &outputTypes,
        std::vector<int32_t> &channels, bool inputRaw = true, bool outputPartial = false,
        bool isOverflowAsNull = true) override;
};

class FirstAggregatorFactory : public AggregatorFactory {
public:
    explicit FirstAggregatorFactory(FunctionType aggregateType) : aggregateType(aggregateType) {}
    ~FirstAggregatorFactory() override = default;
    template <typename InputVecType, typename InputType>
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
            return TypedMaskColAggregator::Create(maskColumnId,std::move(realAggregator));
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
