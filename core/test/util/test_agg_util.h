/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Description: Test AGG Util
 */
#ifndef __TEST_AGG_UTIL_H__
#define __TEST_AGG_UTIL_H__

#include <ctime>
#include <gtest/gtest.h>
#include "type/data_types.h"
#include "operator/aggregation/group_aggregation.h"
#include "operator/aggregation/non_group_aggregation.h"
#include "operator/aggregation/aggregator/aggregator_util.h"
#include "type/data_type.h"

namespace omniruntime::TestUtil {
using namespace omniruntime::expressions;

inline std::unique_ptr<HashAggregationOperatorFactory> CreateHashAggregationOperatorFactory(
    const std::vector<uint32_t> &groupByColumns_, const std::vector<DataTypePtr> &groupTypes,
    const std::vector<uint32_t> &aggFuncTypes_, const std::vector<uint32_t> &aggInputCols,
    const std::vector<DataTypePtr> &aggInputTypes, const std::vector<DataTypePtr> &aggOutputTypes,
    const std::vector<uint32_t> &aggMask_, const bool inputRaw, const bool outputPartial, const bool nullWhenOverflow)
{
    EXPECT_EQ(groupByColumns_.size(), groupTypes.size());
    auto numAgg = aggFuncTypes_.size();
    EXPECT_EQ(numAgg, aggInputCols.size());
    EXPECT_EQ(numAgg, aggInputTypes.size());
    EXPECT_EQ(numAgg, aggOutputTypes.size());
    if (aggMask_.size() != 0) {
        EXPECT_EQ(numAgg, aggMask_.size());
    }

    auto aggInputColsWrap = AggregatorUtil::WrapWithVector(aggInputCols);
    auto aggInputTypesWrap = AggregatorUtil::WrapWithVector(DataTypes(aggInputTypes));
    auto aggOutputTypesWrap = AggregatorUtil::WrapWithVector(DataTypes(aggOutputTypes));
    std::vector<uint32_t> aggMask;
    if (aggMask_.size() == 0) {
        aggMask.reserve(numAgg);
        for (size_t i = 0; i < numAgg; ++i) {
            aggMask.push_back(static_cast<uint32_t>(-1));
        }
    } else {
        aggMask = std::vector<uint32_t>(aggMask_);
    }
    auto inputRawWrap = std::vector<bool>(numAgg, inputRaw);
    auto outputPartialWrap = std::vector<bool>(numAgg, outputPartial);

    auto groupByColumns = std::vector<uint32_t>(groupByColumns_);
    auto aggFuncTypes = std::vector<uint32_t>(aggFuncTypes_);
    auto hashAggOpFactory = std::make_unique<HashAggregationOperatorFactory>(groupByColumns, DataTypes(groupTypes),
        aggInputColsWrap, aggInputTypesWrap, aggOutputTypesWrap, aggFuncTypes, aggMask, inputRawWrap, outputPartialWrap,
        nullWhenOverflow);
    hashAggOpFactory->Init();
    return hashAggOpFactory;
}

inline std::unique_ptr<AggregationOperatorFactory> CreateAggregationOperatorFactory(const std::vector<uint32_t> &aggFuncTypes_,
    const std::vector<uint32_t> &aggInputCols, const std::vector<DataTypePtr> &aggInputTypes,
    const std::vector<DataTypePtr> &aggOutputTypes, const std::vector<uint32_t> &aggMask_, const bool inputRaw,
    const bool outputPartial, const bool nullWhenOverflow)
{
    auto numAgg = aggFuncTypes_.size();
    EXPECT_EQ(numAgg, aggInputCols.size());
    EXPECT_EQ(numAgg, aggInputTypes.size());
    EXPECT_EQ(numAgg, aggOutputTypes.size());
    if (aggMask_.size() != 0) {
        EXPECT_EQ(numAgg, aggMask_.size());
    }
    auto aggInputColsWrap = AggregatorUtil::WrapWithVector(aggInputCols);
    auto aggOutputTypesWrap = AggregatorUtil::WrapWithVector(DataTypes(aggOutputTypes));
    std::vector<uint32_t> aggMask;
    if (aggMask_.size() == 0) {
        aggMask.reserve(numAgg);
        for (size_t i = 0; i < numAgg; ++i) {
            aggMask.push_back(static_cast<uint32_t>(-1));
        }
    } else {
        aggMask = std::vector<uint32_t>(aggMask_);
    }
    auto inputRawWrap = std::vector<bool>(numAgg, inputRaw);
    auto outputPartialWrap = std::vector<bool>(numAgg, outputPartial);

    auto aggFuncTypes = std::vector<uint32_t>(aggFuncTypes_);
    DataTypes inputTypes(aggInputTypes);
    auto aggOpFactory = std::make_unique<AggregationOperatorFactory>(inputTypes, aggFuncTypes, aggInputColsWrap,
        aggMask, aggOutputTypesWrap, inputRawWrap, outputPartialWrap, nullWhenOverflow);
    aggOpFactory->Init();
    return aggOpFactory;
}

}
#endif // __TEST_AGG_UTIL_H__