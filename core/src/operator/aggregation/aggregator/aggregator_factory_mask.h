#pragma once

/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Aggregate factories
 */

#include "aggregator.h"

namespace omniruntime {
namespace op {
template <class T>
class MaskAggregatorFactory : public AggregatorFactory {
public:
    explicit MaskAggregatorFactory(int32_t maskCol) : maskColumnId(maskCol), realFactory(std::make_unique<T>()) {}
    ~MaskAggregatorFactory() override = default;

    std::unique_ptr<Aggregator> CreateAggregator(
        DataTypesPtr inputTypes, DataTypesPtr outputTypes, std::vector<int32_t> &channels,
        bool inputRaw = true, bool outputPartial = false, bool isOverflowAsNull = false) override
    {
        std::unique_ptr<Aggregator> realAggregator = realFactory->CreateAggregator(
            std::move(inputTypes), std::move(outputTypes), channels, inputRaw, outputPartial, isOverflowAsNull);
        if (realAggregator->IsTypedAggregator()) {
            if (realAggregator->IsInputRaw()) {
                if (realAggregator->IsOutputPartial()) {
                    if (realAggregator->IsOverflowAsNull()) {
                        return std::make_unique<TypedMaskColAggregator<true, true, true>>(
                            maskColumnId, std::move(realAggregator));
                    } else {
                        return std::make_unique<TypedMaskColAggregator<true, true, false>>(
                            maskColumnId, std::move(realAggregator));
                    }
                } else {
                    if (realAggregator->IsOverflowAsNull()) {
                        return std::make_unique<TypedMaskColAggregator<true, false, true>>(
                            maskColumnId, std::move(realAggregator));
                    } else {
                        return std::make_unique<TypedMaskColAggregator<true, false, false>>(
                            maskColumnId, std::move(realAggregator));
                    }
                }
            } else {
                if (realAggregator->IsOutputPartial()) {
                    if (realAggregator->IsOverflowAsNull()) {
                        return std::make_unique<TypedMaskColAggregator<false, true, true>>(
                            maskColumnId, std::move(realAggregator));
                    } else {
                        return std::make_unique<TypedMaskColAggregator<false, true, false>>(
                            maskColumnId, std::move(realAggregator));
                    }
                } else {
                    if (realAggregator->IsOverflowAsNull()) {
                        return std::make_unique<TypedMaskColAggregator<false, false, true>>(
                            maskColumnId, std::move(realAggregator));
                    } else {
                        return std::make_unique<TypedMaskColAggregator<false, false, false>>(
                            maskColumnId, std::move(realAggregator));
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
}
}
