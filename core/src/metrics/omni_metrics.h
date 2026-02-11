/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <memory>

namespace omniruntime {

struct OmniMetrics {
    unsigned int numMetrics = 0;
    long omniToArrow = 0;

    // The underlying memory buffer.
    std::unique_ptr<long[]> array;

    // Point to array.get() after the above unique_ptr created.
    long* arrayRawPtr = nullptr;

    enum TYPE {
        // Begin from 0.
        kBegin = 0,

        kInputRows = kBegin,
        kNumInputVecBatches,
        kInputBytes,

        kAddInputTime,
        kAddInputCpuCount,

        kOutputRows,
        kNumOutputVecBatches,
        kOutputBytes,

        kGetOutputTime,
        kGetOutputCpuCount,

        kRawInputRows,
        kRawInputBytes,

        // CpuWallTiming.
        kCpuCount,
        kWallNanos,
        kCpuNanos,

        kPeakMemoryBytes,
        kNumMemoryAllocations,

        // Spill.
        kSpilledInputBytes,
        kSpilledBytes,
        kSpilledRows,
        kSpilledPartitions,
        kSpilledFiles,

        // For BHJ/SHJ
        kBuildInputRows,
        kBuildNumInputVecBatches,
        kBuildAddInputTime,
        kBuildGetOutputTime,

        kLookupInputRows,
        kLookupNumInputVecBatches,
        kLookupOutputRows,
        kLookupNumOutputVecBatches,
        kLookupAddInputTime,
        kLookupGetOutputTime,

        // Runtime OmniMetrics.
        kNumDynamicFiltersProduced,
        kNumDynamicFiltersAccepted,
        kNumReplacedWithDynamicFilterRows,
        kFlushRowCount,
        kLoadedToValueHook,
        kScanTime,
        kSkippedSplits,
        kProcessedSplits,
        kSkippedStrides,
        kProcessedStrides,
        kRemainingFilterTime,
        kIoWaitTime,
        kStorageReadBytes,
        kLocalReadBytes,
        kRamReadBytes,
        kPreloadSplits,

        // Write OmniMetrics.
        kPhysicalWrittenBytes,
        kWriteIOTime,
        kNumWrittenFiles,

        // The end of enum items.
        kEnd,
        kNum = kEnd - kBegin
    };

    explicit OmniMetrics(const unsigned int numMetrics) : numMetrics(numMetrics), array(new long[numMetrics * kNum])
    {
        memset(array.get(), 0, numMetrics * kNum * sizeof(long));
        arrayRawPtr = array.get();
    }

    OmniMetrics(const OmniMetrics&) = delete;
    OmniMetrics(OmniMetrics&&) = delete;
    OmniMetrics& operator=(const OmniMetrics&) = delete;
    OmniMetrics& operator=(OmniMetrics&&) = delete;

    long* get(TYPE type)
    {
        auto offset = (static_cast<int>(type) - static_cast<int>(kBegin)) * numMetrics;
        return &arrayRawPtr[offset];
    }
};
} // omniruntime
