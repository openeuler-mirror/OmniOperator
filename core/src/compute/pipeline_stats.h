/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2024. All rights reserved.
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef PIPELINE_STATS_H
#define PIPELINE_STATS_H

#pragma once

#include <vector>

#include "operator_stats.h"

namespace omniruntime::compute {

    /// Stores execution stats per pipeline.
struct PipelineStats {
    /// Cumulative OperatorStats for finished Drivers. The subscript is the
    /// operator id, which is the initial ordinal position of the operator in the
    /// DriverFactory.
    std::vector<OperatorStats> operatorStats;

    /// True if contains the source node for the task.
    bool inputPipeline;

    /// True if contains the sync node for the task.
    bool outputPipeline;

    /// Id of current Pipeline
    int32_t pipelineId;

    explicit PipelineStats() = default;

    PipelineStats(bool _inputPipeline, bool _outputPipeline)
        : inputPipeline{_inputPipeline}, outputPipeline{_outputPipeline} {}
};

} // namespace omniruntime::compute

#endif
