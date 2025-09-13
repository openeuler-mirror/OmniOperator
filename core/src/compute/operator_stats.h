/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
 
#ifndef OPERATOR_STATS_H
#define OPERATOR_STATS_H

#pragma once

#include <functional>
#include <optional>
#include <string>
#include <sstream>

#include "cpuWall_timer.h"
#include "metrics/metrics_config.h"

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
 
    /// For Scan
    uint64_t rawInputBytes{0};
    uint64_t rawInputRows{0};
 
    /// Bytes of input in terms of retained size of input vectors.
    uint64_t inputRows{0};
    uint64_t inputBytes{0};
    uint64_t numInputVecBatches{0};

    CpuWallTiming addInputTime;
 
    /// Bytes of output in terms of retained size of vectors.
    uint64_t outputBytes{0};
    uint64_t outputRows{0};
    uint64_t numOutputVecBatches{0};

    CpuWallTiming getOutputTime;
 
    // Total bytes written to file for spilling.
    uint64_t spilledBytes{0};
 
    // Total rows written for spilling.
    uint64_t spilledRows{0};
 
    // Total spilled partitions.
    uint32_t spilledPartitions{0};
 
    // Total current spilled files.
    uint32_t spilledFiles{0};

    CpuWallTiming finishTiming;

    int numDrivers = 0;

    // For BHJ/SHJ
    uint64_t buildInputRows;
    uint64_t buildNumInputVecBatches;
    CpuWallTiming buildAddInputTime;
    CpuWallTiming buildGetOutputTime;

    uint64_t lookupInputRows;
    uint64_t lookupNumInputVecBatches;
    uint64_t lookupOutputRows;
    uint64_t lookupNumOutputVecBatches;
    CpuWallTiming lookupAddInputTime;
    CpuWallTiming lookupGetOutputTime;
 
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
 
    void AddInputVector(uint64_t bytes, uint64_t inputVecBatches, uint64_t rowCount)
    {
        inputBytes += bytes;
        numInputVecBatches += 1;
        inputRows += rowCount;
    }
 
    void AddOutputVector(uint64_t bytes, uint64_t outputVecBatches, uint64_t rowCount)
    {
        outputBytes += bytes;
        numOutputVecBatches += 1;
        outputRows += rowCount;
    }

    void HashJoinOperator(const OperatorStats& stats)
    {
        const std::string& opType = stats.operatorType;

        if (opType == opNameForHashBuilder) {
            buildInputRows += stats.inputRows;
            buildAddInputTime.Add(stats.addInputTime);
            buildGetOutputTime.Add(stats.getOutputTime);
            buildNumInputVecBatches += stats.numInputVecBatches;
        }

        if (opType == opNameForLookUpJoin) {
            lookupInputRows += stats.inputRows;
            lookupOutputRows += stats.outputRows;
            lookupAddInputTime.Add(stats.addInputTime);
            lookupGetOutputTime.Add(stats.getOutputTime);
            lookupNumInputVecBatches += stats.numInputVecBatches;
            lookupNumOutputVecBatches += stats.numOutputVecBatches;
        }
    }

    void Add(const OperatorStats& other)
    {
        HashJoinOperator(other);

        operatorType = other.operatorType;
        planNodeId = other.planNodeId;
        numSplits += other.numSplits;
        rawInputBytes += other.rawInputBytes;
        rawInputRows += other.rawInputRows;
 
        addInputTime.Add(other.addInputTime);
        inputBytes += other.inputBytes;
        inputRows += other.inputRows;
        numInputVecBatches += other.numInputVecBatches;
 
        getOutputTime.Add(other.getOutputTime);
        outputBytes += other.outputBytes;
        numOutputVecBatches += other.numOutputVecBatches;
        outputRows += other.outputRows;
 
        isBlockedTiming.Add(other.isBlockedTiming);

        numDrivers += other.numDrivers;
        spilledBytes += other.spilledBytes;
        spilledRows += other.spilledRows;
        spilledPartitions += other.spilledPartitions;
        spilledFiles += other.spilledFiles;

        finishTiming.Add(other.finishTiming);
    }
 
    void Clear()
    {
        numSplits = 0;
        rawInputBytes = 0;
        rawInputRows = 0;
 
        addInputTime.Clear();
        inputBytes = 0;
        inputRows = 0;
        numInputVecBatches = 0;
 
        getOutputTime.Clear();
        outputBytes = 0;
        outputRows = 0;
        numOutputVecBatches = 0;
 
        numDrivers = 0;
        spilledBytes = 0;
        spilledRows = 0;
        spilledPartitions = 0;
        spilledFiles = 0;

        finishTiming.Clear();
    }
};
} // omniruntime::compute
#endif
