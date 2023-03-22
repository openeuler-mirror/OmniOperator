/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 */
#ifndef OMNI_RUNTIME_ONLY_AGGREGATOR_FACTORY_H
#define OMNI_RUNTIME_ONLY_AGGREGATOR_FACTORY_H
#include <vector>
#include <type/data_types.h>
#include "aggregator.h"
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
    virtual std::unique_ptr<Aggregator> CreateAggregator(const type::DataTypes &inputTypes,
        const type::DataTypes &outputTypes, std::vector<int32_t> &channels, bool inputRaw, bool outputPartial,
        bool isOverflowAsNull) = 0;
};
}
}
#endif // OMNI_RUNTIME_ONLY_AGGREGATOR_FACTORY_H
