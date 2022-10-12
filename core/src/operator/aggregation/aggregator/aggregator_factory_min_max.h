#pragma once

/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Aggregate factories
 */

#include "aggregator.h"

namespace omniruntime {
namespace op {
class MinAggregatorFactory : public AggregatorFactory {
public:
    MinAggregatorFactory() = default;
    ~MinAggregatorFactory() override = default;

    std::unique_ptr<Aggregator> CreateAggregator(
        DataTypesPtr inputTypes, DataTypesPtr outputTypes, std::vector<int32_t> &channels,
        bool inputRaw = true, bool outputPartial = false, bool isOverflowAsNull = false) override
    {
        if (inputRaw) {
            if (outputPartial) {
                if (isOverflowAsNull) {
                    return CreateAggregator<true, true, true>(std::move(inputTypes), std::move(outputTypes), channels);
                } else {
                    return CreateAggregator<true, true, false>(std::move(inputTypes), std::move(outputTypes), channels);
                }
            } else {
                if (isOverflowAsNull) {
                    return CreateAggregator<true, false, true>(std::move(inputTypes), std::move(outputTypes), channels);
                } else {
                    return CreateAggregator<true, false, false>(std::move(inputTypes), std::move(outputTypes), channels);
                }
            }
        } else {
            if (outputPartial) {
                if (isOverflowAsNull) {
                    return CreateAggregator<false, true, true>(std::move(inputTypes), std::move(outputTypes), channels);
                } else {
                    return CreateAggregator<false, true, false>(std::move(inputTypes), std::move(outputTypes), channels);
                }
            } else {
                if (isOverflowAsNull) {
                    return CreateAggregator<false, false, true>(std::move(inputTypes), std::move(outputTypes), channels);
                } else {
                    return CreateAggregator<false, false, false>(std::move(inputTypes), std::move(outputTypes), channels);
                }
            }
        }
    }

private:
    template <bool RAW_IN, bool PARTIAL_OUT, bool NULL_OVERFLOW>
    std::unique_ptr<Aggregator> CreateAggregator(
        DataTypesPtr inputTypes, DataTypesPtr outputTypes, std::vector<int32_t> &channels)
    {
        auto inputTypeId = inputTypes->GetIds()[0];
        auto outputTypeId = outputTypes->GetIds()[0];
        switch (inputTypeId) {
            case OMNI_SHORT:
                return fromKnownInput<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, OMNI_SHORT>(
                    std::move(inputTypes), std::move(outputTypes), channels);
            case OMNI_INT:
            case OMNI_DATE32:
            case OMNI_TIME32:
                return fromKnownInput<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, OMNI_INT>(
                    std::move(inputTypes), std::move(outputTypes), channels);
            case OMNI_LONG:
            case OMNI_DATE64:
            case OMNI_TIME64:
            case OMNI_TIMESTAMP:
                return fromKnownInput<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, OMNI_LONG>(
                    std::move(inputTypes), std::move(outputTypes), channels);
            case OMNI_DOUBLE:
                return fromKnownInput<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, OMNI_DOUBLE>(
                    std::move(inputTypes), std::move(outputTypes), channels);
            case OMNI_DECIMAL128:
                return fromKnownInput<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, OMNI_DECIMAL128>(
                    std::move(inputTypes), std::move(outputTypes), channels);
            case OMNI_DECIMAL64:
                return fromKnownInput<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, OMNI_DECIMAL64>(
                    std::move(inputTypes), std::move(outputTypes), channels);
            case OMNI_BOOLEAN:
                return fromKnownInput<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, OMNI_BOOLEAN>(
                    std::move(inputTypes), std::move(outputTypes), channels);
            case OMNI_VARCHAR:
                return std::make_unique<MinVarcharAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, OMNI_VARCHAR, OMNI_VARCHAR>>(
                    std::move(inputTypes), std::move(outputTypes), channels);
            case OMNI_CHAR:
                return std::make_unique<MinVarcharAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, OMNI_CHAR, OMNI_CHAR>>(
                    std::move(inputTypes), std::move(outputTypes), channels);
            default:
                LogError("Unsupported output type %d for min aggregate with input type %d", outputTypeId, inputTypeId);
                return nullptr;
        }
    }

    template <bool RAW_IN, bool PARTIAL_OUT, bool NULL_OVERFLOW, DataTypeId IN_ID>
    std::unique_ptr<Aggregator> fromKnownInput(
        DataTypesPtr inputTypes, DataTypesPtr outputTypes, std::vector<int32_t> &channels)
    {
        auto inputTypeId = inputTypes->GetIds()[0];
        auto outputTypeId = outputTypes->GetIds()[0];
        switch (outputTypeId) {
            case OMNI_SHORT:
                return std::make_unique<MinAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, IN_ID, OMNI_SHORT>>(
                    std::move(inputTypes), std::move(outputTypes), channels);
            case OMNI_INT:
            case OMNI_DATE32:
            case OMNI_TIME32:
                return std::make_unique<MinAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, IN_ID, OMNI_INT>>(
                    std::move(inputTypes), std::move(outputTypes), channels);
            case OMNI_LONG:
            case OMNI_DATE64:
            case OMNI_TIME64:
            case OMNI_TIMESTAMP:
                return std::make_unique<MinAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, IN_ID, OMNI_LONG>>(
                    std::move(inputTypes), std::move(outputTypes), channels);
            case OMNI_DOUBLE:
                return std::make_unique<MinAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, IN_ID, OMNI_DOUBLE>>(
                    std::move(inputTypes), std::move(outputTypes), channels);
            case OMNI_DECIMAL128:
                return std::make_unique<MinAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, IN_ID, OMNI_DECIMAL128>>(
                    std::move(inputTypes), std::move(outputTypes), channels);
            case OMNI_DECIMAL64:
                return std::make_unique<MinAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, IN_ID, OMNI_DECIMAL64>>(
                    std::move(inputTypes), std::move(outputTypes), channels);
            case OMNI_BOOLEAN:
                return std::make_unique<MinAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, IN_ID, OMNI_BOOLEAN>>(
                    std::move(inputTypes), std::move(outputTypes), channels);
            default:
                LogError("Unsupported output type %d for min aggregate with input type %d", outputTypeId, inputTypeId);
                return nullptr;
        }
    }
};

class MaxAggregatorFactory : public AggregatorFactory {
public:
    MaxAggregatorFactory() = default;
    ~MaxAggregatorFactory() override = default;

    std::unique_ptr<Aggregator> CreateAggregator(
        DataTypesPtr inputTypes, DataTypesPtr outputTypes, std::vector<int32_t> &channels,
        bool inputRaw = true, bool outputPartial = false, bool isOverflowAsNull = false) override
    {
        if (inputRaw) {
            if (outputPartial) {
                if (isOverflowAsNull) {
                    return CreateAggregator<true, true, true>(std::move(inputTypes), std::move(outputTypes), channels);
                } else {
                    return CreateAggregator<true, true, false>(std::move(inputTypes), std::move(outputTypes), channels);
                }
            } else {
                if (isOverflowAsNull) {
                    return CreateAggregator<true, false, true>(std::move(inputTypes), std::move(outputTypes), channels);
                } else {
                    return CreateAggregator<true, false, false>(std::move(inputTypes), std::move(outputTypes), channels);
                }
            }
        } else {
            if (outputPartial) {
                if (isOverflowAsNull) {
                    return CreateAggregator<false, true, true>(std::move(inputTypes), std::move(outputTypes), channels);
                } else {
                    return CreateAggregator<false, true, false>(std::move(inputTypes), std::move(outputTypes), channels);
                }
            } else {
                if (isOverflowAsNull) {
                    return CreateAggregator<false, false, true>(std::move(inputTypes), std::move(outputTypes), channels);
                } else {
                    return CreateAggregator<false, false, false>(std::move(inputTypes), std::move(outputTypes), channels);
                }
            }
        }
    }

private:
    template <bool RAW_IN, bool PARTIAL_OUT, bool NULL_OVERFLOW>
    std::unique_ptr<Aggregator> CreateAggregator(
        DataTypesPtr inputTypes, DataTypesPtr outputTypes, std::vector<int32_t> &channels)
    {
        auto inputTypeId = inputTypes->GetIds()[0];
        auto outputTypeId = outputTypes->GetIds()[0];
        switch (inputTypeId) {
            case OMNI_SHORT:
                return fromKnownInput<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, OMNI_SHORT>(
                    std::move(inputTypes), std::move(outputTypes), channels);
            case OMNI_INT:
            case OMNI_DATE32:
            case OMNI_TIME32:
                return fromKnownInput<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, OMNI_INT>(
                    std::move(inputTypes), std::move(outputTypes), channels);
            case OMNI_LONG:
            case OMNI_DATE64:
            case OMNI_TIME64:
            case OMNI_TIMESTAMP:
                return fromKnownInput<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, OMNI_LONG>(
                    std::move(inputTypes), std::move(outputTypes), channels);
            case OMNI_DOUBLE:
                return fromKnownInput<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, OMNI_DOUBLE>(
                    std::move(inputTypes), std::move(outputTypes), channels);
            case OMNI_DECIMAL128:
                return fromKnownInput<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, OMNI_DECIMAL128>(
                    std::move(inputTypes), std::move(outputTypes), channels);
            case OMNI_DECIMAL64:
                return fromKnownInput<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, OMNI_DECIMAL64>(
                    std::move(inputTypes), std::move(outputTypes), channels);
            case OMNI_BOOLEAN:
                return fromKnownInput<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, OMNI_BOOLEAN>(
                    std::move(inputTypes), std::move(outputTypes), channels);
            case OMNI_VARCHAR:
                return std::make_unique<MaxVarcharAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, OMNI_VARCHAR, OMNI_VARCHAR>>(
                    std::move(inputTypes), std::move(outputTypes), channels);
            case OMNI_CHAR:
                return std::make_unique<MaxVarcharAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, OMNI_CHAR, OMNI_CHAR>>(
                    std::move(inputTypes), std::move(outputTypes), channels);
            default:
                LogError("Unsupported output type %d for max aggregate with input type %d", outputTypeId, inputTypeId);
                return nullptr;
        }
    }

    template <bool RAW_IN, bool PARTIAL_OUT, bool NULL_OVERFLOW, DataTypeId IN_ID>
    std::unique_ptr<Aggregator> fromKnownInput(
        DataTypesPtr inputTypes, DataTypesPtr outputTypes, std::vector<int32_t> &channels)
    {
        auto inputTypeId = inputTypes->GetIds()[0];
        auto outputTypeId = outputTypes->GetIds()[0];
        switch (outputTypeId) {
            case OMNI_SHORT:
                return std::make_unique<MaxAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, IN_ID, OMNI_SHORT>>(
                    std::move(inputTypes), std::move(outputTypes), channels);
            case OMNI_INT:
            case OMNI_DATE32:
            case OMNI_TIME32:
                return std::make_unique<MaxAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, IN_ID, OMNI_INT>>(
                    std::move(inputTypes), std::move(outputTypes), channels);
            case OMNI_LONG:
            case OMNI_DATE64:
            case OMNI_TIME64:
            case OMNI_TIMESTAMP:
                return std::make_unique<MaxAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, IN_ID, OMNI_LONG>>(
                    std::move(inputTypes), std::move(outputTypes), channels);
            case OMNI_DOUBLE:
                return std::make_unique<MaxAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, IN_ID, OMNI_DOUBLE>>(
                    std::move(inputTypes), std::move(outputTypes), channels);
            case OMNI_DECIMAL128:
                return std::make_unique<MaxAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, IN_ID, OMNI_DECIMAL128>>(
                    std::move(inputTypes), std::move(outputTypes), channels);
            case OMNI_DECIMAL64:
                return std::make_unique<MaxAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, IN_ID, OMNI_DECIMAL64>>(
                    std::move(inputTypes), std::move(outputTypes), channels);
            case OMNI_BOOLEAN:
                return std::make_unique<MinAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, IN_ID, OMNI_BOOLEAN>>(
                    std::move(inputTypes), std::move(outputTypes), channels);
            default:
                LogError("Unsupported output type %d for max aggregate with input type %d", outputTypeId, inputTypeId);
                return nullptr;
        }
    }
};
}
}
