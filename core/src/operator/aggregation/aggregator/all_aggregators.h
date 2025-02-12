/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2023. All rights reserved.
 * Description: Including all aggregator headers
 */
#ifndef OMNI_RUNTIME_ALL_AGGREGATORS_H
#define OMNI_RUNTIME_ALL_AGGREGATORS_H
#include "operator/aggregation/aggregator/only_aggregator_factory.h"
#include "operator/aggregation/aggregator/sum_aggregator.h"
#include "operator/aggregation/aggregator/sum_flatim_aggregator.h"
#include "operator/aggregation/aggregator/sum_spark_decimal_aggregator.h"
#include "operator/aggregation/aggregator/average_aggregator.h"
#include "operator/aggregation/aggregator/average_flatim_aggregator.h"
#include "operator/aggregation/aggregator/average_spark_decimal_aggregator.h"
#include "operator/aggregation/aggregator/stddev_samp_aggregator.h"
#include "operator/aggregation/aggregator/min_aggregator.h"
#include "operator/aggregation/aggregator/min_varchar_aggregator.h"
#include "operator/aggregation/aggregator/max_aggregator.h"
#include "operator/aggregation/aggregator/max_varchar_aggregator.h"
#include "operator/aggregation/aggregator/count_column_aggregator.h"
#include "operator/aggregation/aggregator/count_all_aggregator.h"
#include "operator/aggregation/aggregator/mask_column_assistant_aggregator.h"
#include "operator/aggregation/aggregator/typed_mask_column_assistant_aggregator.h"
#include "operator/aggregation/aggregator/first_aggregator.h"
#include "operator/aggregation/aggregator/try_sum_flatim_aggregator.h"
#endif // OMNI_RUNTIME_ALL_AGGREGATORS_H
