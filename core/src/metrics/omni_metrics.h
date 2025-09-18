/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
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
