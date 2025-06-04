/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
 
#ifndef OPERATOR_STATS_H
#define OPERATOR_STATS_H

#pragma once

#include <functional>
#include <optional>
 
#include "cpuWall_timer.h"

namespace omniruntime::compute {
using namespace std;
using PlanNodeId = std::string;
 
struct OperatorStats {
    /// Initial ordinal position in the operator's pipeline.
    int32_t operatorId{0};
    int32_t pipelineId{0};
    PlanNodeId planNodeId;
 
    /// Some operators perform the logic describe in multiple consecutive plan
    /// nodes. For example, FilterProject operator maps to Filter node followed by
    /// Project node. In this case, runtime stats are collected for the combined
    /// operator and attached to the "main" plan node ID chosen by the operator.
    /// (Project node ID in case of FilterProject operator.) The operator can then
    /// provide a function to split the stats among all plan nodes that are being
    /// represented. For example, FilterProject would split the stats but moving
    /// cardinality reduction to Filter and making Project cardinality neutral.
    using StatsSplitter = std::function<std::vector<OperatorStats>(
        const OperatorStats& combinedStats)>;
 
    std::optional<StatsSplitter> statsSplitter;
 
    /// Name for reporting. We use Presto compatible names set at
    /// construction of the Operator where applicable.
    std::string operatorType;
 
    /// Number of splits (or chunks of work). Split can be a part of data file to
    /// read.
    int64_t numSplits{0};
 
    CpuWallTiming isBlockedTiming;
 
    /// Bytes read from raw source, e.g. compressed file or network connection.
    uint64_t rawInputBytes{0};
    uint64_t rawInputPositions{0};
 
    CpuWallTiming addInputTiming;
 
    /// Bytes of input in terms of retained size of input vectors.
    uint64_t inputBytes{0};
    uint64_t inputPositions{0};
 
    /// Number of input batches / vectors. Allows to compute an average batch
    /// size.
    uint64_t inputVectors{0};
 
    CpuWallTiming getOutputTiming;
 
    /// Bytes of output in terms of retained size of vectors.
    uint64_t outputBytes{0};
    uint64_t outputPositions{0};
 
    /// Number of output batches / vectors. Allows to compute an average batch
    /// size.
    uint64_t outputVectors{0};
 
    uint64_t physicalWrittenBytes{0};
 
    uint64_t blockedWallNanos{0};
 
    CpuWallTiming finishTiming;
 
    // CPU time spent on background activities (activities that are not
    // running on driver threads). Operators are responsible to report background
    // CPU time at a reasonable time granularity.
    CpuWallTiming backgroundTiming;
 
    // Total bytes in memory for spilling
    uint64_t spilledInputBytes{0};
 
    // Total bytes written to file for spilling.
    uint64_t spilledBytes{0};
 
    // Total rows written for spilling.
    uint64_t spilledRows{0};
 
    // Total spilled partitions.
    uint32_t spilledPartitions{0};
 
    // Total current spilled files.
    uint32_t spilledFiles{0};
 
    // Last recorded values for lazy loading times for loads triggered by 'this'.
    int64_t lastLazyCpuNanos{0};
    int64_t lastLazyWallNanos{0};
    int64_t lastLazyInputBytes{0};
 
    // Total null keys processed by the operator.
    // Currently populated only by HashJoin/HashBuild.
    // HashProbe doesn't populate numNullKeys when build side is empty.
    int64_t numNullKeys{0};

    int numDrivers = 0;
 
    OperatorStats() = default;
 
    OperatorStats(
        int32_t _operatorId,
        int32_t _pipelineId,
        PlanNodeId _planNodeId,
        std::string _operatorType)
        : operatorId(_operatorId),
          pipelineId(_pipelineId),
          planNodeId(std::move(_planNodeId)),
          operatorType(std::move(_operatorType)) {}
 
    void setStatSplitter(StatsSplitter splitter)
    {
        statsSplitter = std::move(splitter);
    }
 
    void addInputVector(uint64_t bytes, uint64_t positions)
    {
        inputBytes += bytes;
        inputPositions += positions;
        inputVectors += 1;
    }
 
    void AddOutputVector(uint64_t bytes, uint64_t positions)
    {
        outputBytes += bytes;
        outputPositions += positions;
        outputVectors += 1;
    }
 
    void Add(const OperatorStats& other)
    {
        numSplits += other.numSplits;
        rawInputBytes += other.rawInputBytes;
        rawInputPositions += other.rawInputPositions;
 
        addInputTiming.Add(other.addInputTiming);
        inputBytes += other.inputBytes;
        inputPositions += other.inputPositions;
        inputVectors += other.inputVectors;
 
        getOutputTiming.Add(other.getOutputTiming);
        outputBytes += other.outputBytes;
        outputPositions += other.outputPositions;
        outputVectors += other.outputVectors;
 
        physicalWrittenBytes += other.physicalWrittenBytes;
 
        blockedWallNanos += other.blockedWallNanos;
 
        finishTiming.Add(other.finishTiming);
 
        isBlockedTiming.Add(other.isBlockedTiming);
 
        backgroundTiming.Add(other.backgroundTiming);
 
        numDrivers += other.numDrivers;
        spilledInputBytes += other.spilledInputBytes;
        spilledBytes += other.spilledBytes;
        spilledRows += other.spilledRows;
        spilledPartitions += other.spilledPartitions;
        spilledFiles += other.spilledFiles;
 
        numNullKeys += other.numNullKeys;
    }
 
    void Clear()
    {
        numSplits = 0;
        rawInputBytes = 0;
        rawInputPositions = 0;
 
        addInputTiming.Clear();
        inputBytes = 0;
        inputPositions = 0;
 
        getOutputTiming.Clear();
        outputBytes = 0;
        outputPositions = 0;
 
        physicalWrittenBytes = 0;
 
        blockedWallNanos = 0;
 
        finishTiming.Clear();
 
        backgroundTiming.Clear();
 
        numDrivers = 0;
        spilledInputBytes = 0;
        spilledBytes = 0;
        spilledRows = 0;
        spilledPartitions = 0;
        spilledFiles = 0;
    }
};
} // omniruntime::compute
#endif
