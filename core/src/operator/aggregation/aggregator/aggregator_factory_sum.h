#pragma once

/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Aggregate factories
 */

#include "aggregator.h"

namespace omniruntime {
namespace op {
class SumAggregatorFactory : public AggregatorFactory {
public:
    SumAggregatorFactory() = default;
    ~SumAggregatorFactory() override = default;

    std::unique_ptr<Aggregator> CreateAggregator(
        DataTypesPtr inputTypes, DataTypesPtr outputTypes, std::vector<int32_t> &channels,
        bool inputRaw = true, bool outputPartial = false, bool isOverflowAsNull = false) override
    {
        ProcessDataTypes(inputTypes);
        ProcessDataTypes(outputTypes);

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
    static void ProcessDataTypes(DataTypesPtr in)
    {
        int32_t partialWidth = sizeof(DecimalPartialResult);
        for (DataTypePtr dataType : in->Get()) {
            if (dataType->GetId() == OMNI_VARCHAR) {
                static_cast<VarcharDataType *>(dataType.get())->SetWidth(partialWidth);
            } else if (dataType->GetId() == OMNI_CHAR) {
                static_cast<CharDataType *>(dataType.get())->SetWidth(partialWidth);
            }
        }
    }

    template <bool RAW_IN, bool PARTIAL_OUT, bool NULL_OVERFLOW>
    std::unique_ptr<Aggregator> CreateAggregator(
        DataTypesPtr inputTypes, DataTypesPtr outputTypes, std::vector<int32_t> &channels)
    {
        auto inputTypeId = inputTypes->GetIds()[0];
        switch (inputTypeId) {
            case OMNI_SHORT:
                return fromKnownInput<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, OMNI_SHORT, int64_t>(
                    std::move(inputTypes), std::move(outputTypes), channels);
            case OMNI_DATE32:
            case OMNI_TIME32:
            case OMNI_INT:
                return fromKnownInput<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, OMNI_INT, int64_t>(
                    std::move(inputTypes), std::move(outputTypes), channels);
            case OMNI_DATE64:
            case OMNI_TIME64:
            case OMNI_TIMESTAMP:
            case OMNI_LONG:
                return fromKnownInput<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, OMNI_LONG, int64_t>(
                    std::move(inputTypes), std::move(outputTypes), channels);
            case OMNI_DOUBLE:
                return fromKnownInput<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, OMNI_DOUBLE, double>(
                    std::move(inputTypes), std::move(outputTypes), channels);
            case OMNI_DECIMAL128:
                return createSumDecimalRawInput<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, OMNI_DECIMAL128>(
                    std::move(inputTypes), std::move(outputTypes), channels);
            case OMNI_DECIMAL64:
                return createSumDecimalRawInput<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, OMNI_DECIMAL64>(
                    std::move(inputTypes), std::move(outputTypes), channels);
            case OMNI_VARCHAR:
                return createSumDecimalPartialInput<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW>(
                    std::move(inputTypes), std::move(outputTypes), channels);
            default:
                LogError("Unsupported input type %d for sum aggregate", inputTypeId);
                return nullptr;
        }
    }

    template <bool RAW_IN, bool PARTIAL_OUT, bool NULL_OVERFLOW, DataTypeId IN_ID, typename ResultType>
    std::unique_ptr<Aggregator> fromKnownInput(
        DataTypesPtr inputTypes, DataTypesPtr outputTypes, std::vector<int32_t> &channels)
    {
        auto inputTypeId = inputTypes->GetIds()[0];
        auto outputTypeId = outputTypes->GetIds()[0];
        switch (outputTypeId) {
            case OMNI_SHORT:
                return std::make_unique<SumAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, IN_ID, OMNI_SHORT, ResultType>>(
                    std::move(inputTypes), std::move(outputTypes), channels);
            case OMNI_INT:
            case OMNI_DATE32:
            case OMNI_TIME32:
                return std::make_unique<SumAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, IN_ID, OMNI_INT, ResultType>>(
                    std::move(inputTypes), std::move(outputTypes), channels);
            case OMNI_LONG:
            case OMNI_DATE64:
            case OMNI_TIME64:
            case OMNI_TIMESTAMP:
                return std::make_unique<SumAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, IN_ID, OMNI_LONG, ResultType>>(
                    std::move(inputTypes), std::move(outputTypes), channels);
            case OMNI_DOUBLE:
                return std::make_unique<SumAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, IN_ID, OMNI_DOUBLE, ResultType>>(
                    std::move(inputTypes), std::move(outputTypes), channels);
            default:
                LogError("Unsupported output type %d for sum aggregate with input type %d", outputTypeId, inputTypeId);
                return nullptr;
        }
    }

    template <bool RAW_IN, bool PARTIAL_OUT, bool NULL_OVERFLOW>
    std::unique_ptr<Aggregator> createSumDecimalPartialInput(
        DataTypesPtr inputTypes, DataTypesPtr outputTypes, std::vector<int32_t> &channels)
    {
        auto inputTypeId = inputTypes->GetIds()[0];
        auto outputTypeId = outputTypes->GetIds()[0];

        if (inputTypeId != OMNI_VARCHAR) {
            LogError("Expecting Varchar input type for createSumDecimalPartialInput");
            return nullptr;
        }
        if (RAW_IN) {
            LogError("RawInput for average decimal aggregator with varchar input");
            return nullptr;
        }
        if (PARTIAL_OUT) {
            LogError("Average decimal aggregator with varchar input cannot have partialOutput");
            return nullptr;
        }

        if (outputTypeId == OMNI_DECIMAL128) {
            return std::make_unique<SumDecimalAggregator<false, false, NULL_OVERFLOW, OMNI_VARCHAR, OMNI_DECIMAL128>>(
                std::move(inputTypes), std::move(outputTypes), channels);
        } else if (outputTypeId == OMNI_DECIMAL64) {
            return std::make_unique<SumDecimalAggregator<false, false, NULL_OVERFLOW, OMNI_VARCHAR, OMNI_DECIMAL64>>(
                std::move(inputTypes), std::move(outputTypes), channels);
        } else {
            LogError("Unsupported output type %d for average aggregate with input type %d", outputTypeId, inputTypeId);
            return nullptr;
        }
    }

    template <bool RAW_IN, bool PARTIAL_OUT, bool NULL_OVERFLOW, DataTypeId IN_ID>
    std::unique_ptr<Aggregator> createSumDecimalRawInput(
        DataTypesPtr inputTypes, DataTypesPtr outputTypes, std::vector<int32_t> &channels)
    {
        static_assert(IN_ID == OMNI_DECIMAL64 || IN_ID == OMNI_DECIMAL128,
            "Expecting Decimal input type for createSumDecimalRawInput");

        auto outputTypeId = outputTypes->GetIds()[0];
        if (!RAW_IN) {
            LogError("Not rawInput for sum decimal aggregator with input %d", IN_ID);
            return nullptr;
        }
        if (PARTIAL_OUT) {
            if (outputTypeId != OMNI_VARCHAR) {
                LogError("Sum decimal aggregator with partialOutput shoud have varchar output");
                return nullptr;
            }
        } else {
            if (outputTypeId == OMNI_VARCHAR) {
                LogError("Sum decimal aggregator with partialOutput=false cannot have varchar output");
                return nullptr;
            }
        }

        if (outputTypeId == OMNI_VARCHAR) {
            return std::make_unique<SumDecimalAggregator<true, true, NULL_OVERFLOW, IN_ID, OMNI_VARCHAR>>(
                std::move(inputTypes), std::move(outputTypes), channels);
        } else if (outputTypeId == OMNI_DECIMAL128) {
            return std::make_unique<SumDecimalAggregator<true, false, NULL_OVERFLOW, IN_ID, OMNI_DECIMAL128>>(
                std::move(inputTypes), std::move(outputTypes), channels);
        } else if (outputTypeId == OMNI_DECIMAL64) {
            return std::make_unique<SumDecimalAggregator<true, false, NULL_OVERFLOW, IN_ID, OMNI_DECIMAL64>>(
                std::move(inputTypes), std::move(outputTypes), channels);
        } else {
            LogError("Unsupported output type %d for sum decimal aggregate with input type %d", outputTypeId, IN_ID);
            return nullptr;
        }
    }
};
}
}
