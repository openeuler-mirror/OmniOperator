#pragma once

/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Aggregate factories
 */

#include "aggregator_factory.h"
#include "average_aggregator.h"
#include "average_decimal_aggregator.h"

namespace omniruntime {
namespace op {
class AverageAggregatorFactory : public AggregatorFactory {
public:
    AverageAggregatorFactory() = default;
    ~AverageAggregatorFactory() override = default;

    std::unique_ptr<Aggregator> CreateAggregator(
        DataTypesPtr inputTypes, DataTypesPtr outputTypes, std::vector<int32_t> &channels,
        bool inputRaw = true, bool outputPartial = false, bool isOverflowAsNull = false) override
    {
        // average aggregator output can only be one of following:
        // 1. ContainerType<Double, Long> when output is partial
        // 2. DoubleType when output is not partial
        // note: we do handle Spark here, so oututTypes should contain a single type
        ValidateOutputType(inputTypes, outputTypes, inputRaw, outputPartial);

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

    static void ValidateOutputType(DataTypesPtr inputTypes, DataTypesPtr outputTypes, 
        const bool isInputRaw, const bool isOutputPartial)
    {
        if (outputTypes->GetSize() != 1) {
            throw OmniException("Invalid Parameter",
                "Average aggregator expected single output data type. "
                + std::to_string(outputTypes->GetSize()) + " provided.");
        }
        if (inputTypes->GetSize() != 1) {
            throw OmniException("Invalid Parameter",
                "Average aggregator expected single input data type. "
                + std::to_string(inputTypes->GetSize()) + " provided.");
        }
        auto inputTypeId = inputTypes->GetType(0)->GetId();
        auto outputTypeId = outputTypes->GetType(0)->GetId();
        std::string error = "";
        if (isOutputPartial) {
            if (isInputRaw) {
                if (inputTypeId == OMNI_DECIMAL64 || inputTypeId == OMNI_DECIMAL128) {
                    if (outputTypeId != OMNI_VARCHAR) {
                        error = "Average aggregator expected varchat partial output for decimal raw input. Got "
                            + std::to_string(outputTypeId) + " (" + TypeUtil::TypeToString(outputTypeId) + ")";
                    }
                } else {
                    if (outputTypeId != OMNI_CONTAINER) {
                        error = "Average aggregator with container partial output. Got"
                            + std::to_string(outputTypeId) + " (" + TypeUtil::TypeToString(outputTypeId) + ")";
                    } else {
                        CheckContainerType(static_cast<ContainerDataType *>(outputTypes->GetType(0).get()));
                    }
                }
            } else {
                error = "Average aggregator cannot have partialOutput and not raw input";
            }
        } else {
            if (outputTypeId != OMNI_DOUBLE && outputTypeId != OMNI_DECIMAL64 && outputTypeId != OMNI_DECIMAL128) {
                error = "Average aggregator expected double, decimal64 or decimal128 for not partial output. Got "
                    + std::to_string(outputTypeId) + " (" + TypeUtil::TypeToString(outputTypeId) + ")";
            } else if (!isInputRaw) {
                if (inputTypeId == OMNI_CONTAINER) {
                    CheckContainerType(static_cast<ContainerDataType *>(inputTypes->GetType(0).get()));
                } else if (inputTypeId != OMNI_VARCHAR) {
                    error = "Average aggregator expected varchar or container for not raw input. Got "
                        + std::to_string(inputTypeId) + " (" + TypeUtil::TypeToString(inputTypeId) + ")";
                }
            }
        }
        if (error.length() > 0) {
            throw OmniException("Invalid Parameter", error);
        }
    }

    static void CheckContainerType(ContainerDataType *containerType)
    {
        if (containerType->GetSize() != 2) {
            throw OmniException("Invalid Parameter",
                "Average aggregator expecting container output type of size 2. Got "
                + std::to_string(containerType->GetSize()));
        }

        if (containerType->GetFieldType(0)->GetId() != OMNI_DOUBLE) {
            throw OmniException("Invalid Parameter",
                "Average aggregator expecting double type for first field in container. Got "
                + std::to_string(containerType->GetFieldType(0)->GetId()) + " (" 
                + TypeUtil::TypeToString(containerType->GetFieldType(0)->GetId()) + ")");
        }

        if (containerType->GetFieldType(1)->GetId() != OMNI_LONG) {
            throw OmniException("Invalid Parameter",
                "Average aggregator expecting long type for second field in container. Got "
                    + std::to_string(containerType->GetFieldType(1)->GetId()) + " ("
                    + TypeUtil::TypeToString(containerType->GetFieldType(1)->GetId()) + ")");
        }
    }

    template <bool RAW_IN, bool PARTIAL_OUT, bool NULL_OVERFLOW>
    std::unique_ptr<Aggregator> CreateAggregator(
        DataTypesPtr inputTypes, DataTypesPtr outputTypes, std::vector<int32_t> &channels)
    {
        auto inputTypeId = inputTypes->GetIds()[0];
        switch (inputTypeId) {
            case OMNI_SHORT:
                return std::make_unique<AverageAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, OMNI_SHORT, OMNI_DOUBLE, int64_t>>(
                    std::move(inputTypes), std::move(outputTypes), channels);
            case OMNI_DATE32:
            case OMNI_TIME32:
            case OMNI_INT:
                return std::make_unique<AverageAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, OMNI_INT, OMNI_DOUBLE, int64_t>>(
                    std::move(inputTypes), std::move(outputTypes), channels);
            case OMNI_LONG:
                return std::make_unique<AverageAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, OMNI_LONG, OMNI_DOUBLE, int64_t>>(
                    std::move(inputTypes), std::move(outputTypes), channels);
            case OMNI_CONTAINER:
            case OMNI_DOUBLE:
                return std::make_unique<AverageAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, OMNI_DOUBLE, OMNI_DOUBLE, double>>(
                    std::move(inputTypes), std::move(outputTypes), channels);
            case OMNI_DECIMAL64:
                return createAvgDecimalRawInput<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, OMNI_DECIMAL64>(
                    std::move(inputTypes), std::move(outputTypes), channels);
            case OMNI_DECIMAL128:
                return createAvgDecimalRawInput<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, OMNI_DECIMAL128>(
                    std::move(inputTypes), std::move(outputTypes), channels);
            case OMNI_VARCHAR:
                return createAvgDecimalPartialInput<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW>(
                    std::move(inputTypes), std::move(outputTypes), channels);
            default:
                LogError("Unsupported input type %d for average aggregate", inputTypeId);
                return nullptr;
        }
    }

    template <bool RAW_IN, bool PARTIAL_OUT, bool NULL_OVERFLOW>
    std::unique_ptr<Aggregator> createAvgDecimalPartialInput(
        DataTypesPtr inputTypes, DataTypesPtr outputTypes, std::vector<int32_t> &channels)
    {
        auto inputTypeId = inputTypes->GetIds()[0];
        auto outputTypeId = outputTypes->GetIds()[0];

        if (inputTypeId != OMNI_VARCHAR) {
            LogError("Expecting Varchar input type for createAvgDecimalPartialInput");
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
            return std::make_unique<AverageDecimalAggregator<false, false, NULL_OVERFLOW, OMNI_VARCHAR, OMNI_DECIMAL128>>(
                std::move(inputTypes), std::move(outputTypes), channels);
        } else if (outputTypeId == OMNI_DECIMAL64) {
            return std::make_unique<AverageDecimalAggregator<false, false, NULL_OVERFLOW, OMNI_VARCHAR, OMNI_DECIMAL64>>(
                std::move(inputTypes), std::move(outputTypes), channels);
        } else {
            LogError("Unsupported output type %d for average aggregate with input type %d", outputTypeId, inputTypeId);
            return nullptr;
        }
    }

    template <bool RAW_IN, bool PARTIAL_OUT, bool NULL_OVERFLOW, DataTypeId IN_ID>
    std::unique_ptr<Aggregator> createAvgDecimalRawInput(
        DataTypesPtr inputTypes, DataTypesPtr outputTypes, std::vector<int32_t> &channels)
    {
        static_assert(IN_ID == OMNI_DECIMAL64 || IN_ID == OMNI_DECIMAL128,
            "Expecting Decimal input type for createAvgDecimalRawInput");

        auto outputTypeId = outputTypes->GetIds()[0];
        if (!RAW_IN) {
            LogError("Not rawInput for average decimal aggregator with input %d", IN_ID);
            return nullptr;
        }
        if (PARTIAL_OUT) {
            if (outputTypeId != OMNI_VARCHAR) {
                LogError("Average decimal aggregator with partialOutput shoud have varchar output");
                return nullptr;
            }
        } else {
            if (outputTypeId == OMNI_VARCHAR) {
                LogError("Average decimal aggregator with partialOutput=false cannot have varchar output");
                return nullptr;
            }
        }

        if (outputTypeId == OMNI_VARCHAR) {
            return std::make_unique<AverageDecimalAggregator<true, true, NULL_OVERFLOW, IN_ID, OMNI_VARCHAR>>(
                std::move(inputTypes), std::move(outputTypes), channels);
        } else if (outputTypeId == OMNI_DECIMAL128) {
            return std::make_unique<AverageDecimalAggregator<true, false, NULL_OVERFLOW, IN_ID, OMNI_DECIMAL128>>(
                std::move(inputTypes), std::move(outputTypes), channels);
        } else if (outputTypeId == OMNI_DECIMAL64) {
            return std::make_unique<AverageDecimalAggregator<true, false, NULL_OVERFLOW, IN_ID, OMNI_DECIMAL64>>(
                std::move(inputTypes), std::move(outputTypes), channels);
        } else {
            LogError("Unsupported output type %d for average decimal aggregate with input type %d",
                outputTypeId, IN_ID);
            return nullptr;
        }
    }
};
}
}
