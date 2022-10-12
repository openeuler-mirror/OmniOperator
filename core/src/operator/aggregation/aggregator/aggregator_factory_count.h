#pragma once

/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Aggregate factories
 */

#include "aggregator_factory.h"
#include "count_all_aggregator.h"
#include "count_column_aggregator.h"

namespace omniruntime {
namespace op {
class CountColumnAggregatorFactory : public AggregatorFactory {
public:
    CountColumnAggregatorFactory() = default;
    ~CountColumnAggregatorFactory() override = default;

    std::unique_ptr<Aggregator> CreateAggregator(
        DataTypesPtr inputTypes, DataTypesPtr outputTypes, std::vector<int32_t> &channels,
        bool inputRaw = true, bool outputPartial = false, bool isOverflowAsNull = false) override
    {
        if (inputRaw) {
            if (outputPartial) {
                if (isOverflowAsNull) {
                    return std::make_unique<CountColumnAggregator<true, true, true>>(
                        std::move(outputTypes), channels);
                } else {
                    return std::make_unique<CountColumnAggregator<true, true, false>>(
                        std::move(outputTypes), channels);
                }
            } else {
                if (isOverflowAsNull) {
                    return std::make_unique<CountColumnAggregator<true, false, true>>(
                        std::move(outputTypes), channels);
                } else {
                    return std::make_unique<CountColumnAggregator<true, false, false>>(
                        std::move(outputTypes), channels);
                }
            }
        } else {
            if (outputPartial) {
                if (isOverflowAsNull) {
                    return std::make_unique<CountColumnAggregator<false, true, true>>(
                        std::move(outputTypes), channels);
                } else {
                    return std::make_unique<CountColumnAggregator<false, true, false>>(
                        std::move(outputTypes), channels);
                }
            } else {
                if (isOverflowAsNull) {
                    return std::make_unique<CountColumnAggregator<false, false, true>>(
                        std::move(outputTypes), channels);
                } else {
                    return std::make_unique<CountColumnAggregator<false, false, false>>(
                        std::move(outputTypes), channels);
                }
            }
        }
    }
};

class CountAllAggregatorFactory : public AggregatorFactory {
public:
    CountAllAggregatorFactory() = default;
    ~CountAllAggregatorFactory() override = default;

    std::unique_ptr<Aggregator> CreateAggregator(
        DataTypesPtr inputTypes, DataTypesPtr outputTypes, std::vector<int32_t> &channels,
        bool inputRaw = true, bool outputPartial = false, bool isOverflowAsNull = false) override
    {
        if (inputRaw) {
            if (outputPartial) {
                if (isOverflowAsNull) {
                    return std::make_unique<CountAllAggregator<true, true, true>>(
                        std::move(outputTypes), channels);
                } else {
                    return std::make_unique<CountAllAggregator<true, true, false>>(
                        std::move(outputTypes), channels);
                }
            } else {
                if (isOverflowAsNull) {
                    return std::make_unique<CountAllAggregator<true, false, true>>(
                        std::move(outputTypes), channels);
                } else {
                    return std::make_unique<CountAllAggregator<true, false, false>>(
                        std::move(outputTypes), channels);
                }
            }
        } else {
            if (outputPartial) {
                if (isOverflowAsNull) {
                    return std::make_unique<CountAllAggregator<false, true, true>>(
                        std::move(outputTypes), channels);
                } else {
                    return std::make_unique<CountAllAggregator<false, true, false>>(
                        std::move(outputTypes), channels);
                }
            } else {
                if (isOverflowAsNull) {
                    return std::make_unique<CountAllAggregator<false, false, true>>(
                        std::move(outputTypes), channels);
                } else {
                    return std::make_unique<CountAllAggregator<false, false, false>>(
                        std::move(outputTypes), channels);
                }
            }
        }
    }
};
}
}
