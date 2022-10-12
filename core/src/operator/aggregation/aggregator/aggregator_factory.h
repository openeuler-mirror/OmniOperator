/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Aggregate factories
 */
#ifndef OMNI_RUNTIME_AGGREGATOR_FACTORY_H
#define OMNI_RUNTIME_AGGREGATOR_FACTORY_H

#include "aggregator.h"

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
        DataTypesPtr inputTypes, DataTypesPtr outputTypes, std::vector<int32_t> &channels,
        bool inputRaw = true, bool outputPartial = false, bool isOverflowAsNull = false) = 0;
};
}
}
#endif // OMNI_RUNTIME_AGGREGATOR_FACTORY_H
