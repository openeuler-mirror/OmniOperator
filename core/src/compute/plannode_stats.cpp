/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: This file declares for plannode_stats.cpp
 */
 
#include "plannode_stats.h"
#include "compute/task.h"
#include "plannode/planNode.h"
#include "util/format.h"

#include <unordered_map>

namespace omniruntime::compute {
PlanNodeStats& PlanNodeStats::operator+=(const PlanNodeStats& another)
{
    inputRows += another.inputRows;
    inputBytes += another.inputBytes;
    inputVectors += another.inputVectors;

    rawInputRows += another.inputRows;
    rawInputBytes += another.rawInputBytes;

    outputRows += another.outputRows;
    outputBytes += another.outputBytes;
    outputVectors += another.outputVectors;

    isBlockedTiming.Add(another.isBlockedTiming);
    addInputTiming.Add(another.addInputTiming);
    getOutputTiming.Add(another.getOutputTiming);
    finishTiming.Add(another.finishTiming);
    cpuWallTiming.Add(another.addInputTiming);
    cpuWallTiming.Add(another.getOutputTiming);
    cpuWallTiming.Add(another.finishTiming);
    cpuWallTiming.Add(another.isBlockedTiming);

    backgroundTiming.Add(another.backgroundTiming);

    blockedWallNanos += another.blockedWallNanos;

    physicalWrittenBytes += another.physicalWrittenBytes;

    // Populating number of drivers for plan nodes with multiple operators is not
    // useful. Each operator could have been executed in different pipelines with
    // different number of drivers.
    if (!IsMultiOperatorTypeNode()) {
        numDrivers += another.numDrivers;
    } else {
        numDrivers = 0;
    }

    numSplits += another.numSplits;

    spilledInputBytes += another.spilledInputBytes;
    spilledBytes += another.spilledBytes;
    spilledRows += another.spilledRows;
    spilledPartitions += another.spilledPartitions;
    spilledFiles += another.spilledFiles;

    return *this;
}

void PlanNodeStats::Add(const OperatorStats& stats)
{
    auto it = operatorStats.find(stats.operatorType);
    if (it != operatorStats.end()) {
        it->second->AddTotals(stats);
    } else {
        auto opStats = std::make_unique<PlanNodeStats>();
        opStats->AddTotals(stats);
        operatorStats.emplace(stats.operatorType, std::move(opStats));
    }
    AddTotals(stats);
}

void PlanNodeStats::AddTotals(const OperatorStats& stats)
{
    inputRows += stats.inputPositions;
    inputBytes += stats.inputBytes;
    inputVectors += stats.inputVectors;

    rawInputRows += stats.rawInputPositions;
    rawInputBytes += stats.rawInputBytes;

    outputRows += stats.outputPositions;
    outputBytes += stats.outputBytes;
    outputVectors += stats.outputVectors;

    isBlockedTiming.Add(stats.isBlockedTiming);
    addInputTiming.Add(stats.addInputTiming);
    getOutputTiming.Add(stats.getOutputTiming);
    finishTiming.Add(stats.finishTiming);
    cpuWallTiming.Add(stats.addInputTiming);
    cpuWallTiming.Add(stats.getOutputTiming);
    cpuWallTiming.Add(stats.finishTiming);
    cpuWallTiming.Add(stats.isBlockedTiming);

    backgroundTiming.Add(stats.backgroundTiming);

    blockedWallNanos += stats.blockedWallNanos;

    physicalWrittenBytes += stats.physicalWrittenBytes;

    // Populating number of drivers for plan nodes with multiple operators is not
    // useful. Each operator could have been executed in different pipelines with
    // different number of drivers.
    if (!IsMultiOperatorTypeNode()) {
        numDrivers += stats.numDrivers;
    } else {
        numDrivers = 0;
    }

    numSplits += stats.numSplits;

    spilledInputBytes += stats.spilledInputBytes;
    spilledBytes += stats.spilledBytes;
    spilledRows += stats.spilledRows;
    spilledPartitions += stats.spilledPartitions;
    spilledFiles += stats.spilledFiles;
}

void appendOperatorStats(
    const OperatorStats& stats,
    std::unordered_map<PlanNodeId, PlanNodeStats>& planStats)
{
    const auto& planNodeId = stats.planNodeId;
    auto it = planStats.find(planNodeId);
    if (it != planStats.end()) {
        it->second.Add(stats);
    } else {
        PlanNodeStats nodeStats;
        nodeStats.Add(stats);
        planStats.emplace(planNodeId, std::move(nodeStats));
    }
}

std::string PlanNodeStats::ToString(
    bool includeInputStats) const
{
    std::stringstream out;
    if (includeInputStats) {
        out << "Input: " << inputRows << " rows (" << inputBytes
            << ", " << inputVectors << " batches), ";
        if ((rawInputRows > 0) && (rawInputRows != inputRows)) {
            out << "Raw Input: " << rawInputRows << " rows ("
                << rawInputBytes << "), ";
        }
    }
    out << "Output: " << outputRows << " rows (" << outputBytes
        << ", " << outputVectors << " batches)";
    if (physicalWrittenBytes > 0) {
        out << ", Physical written output: " << physicalWrittenBytes;
    }
    out << ", Cpu time: " << cpuWallTiming.cpuNanos
        << ", Wall time: " << cpuWallTiming.wallNanos
        << ", Blocked wall time: " << blockedWallNanos
        << ", Peak memory: " << peakMemoryBytes
        << ", Memory allocations: " << numMemoryAllocations
        << ", Threads: " << numDrivers
        << ", Splits: " << numSplits
        <<", Spilled: " << spilledRows << " rows ("
            << spilledBytes << ", " << spilledFiles << " files)";
    out << ", CPU breakdown: B/I/O/F "
        << Format(
            "({}/{}/{}/{})", isBlockedTiming.cpuNanos, addInputTiming.cpuNanos,
            getOutputTiming.cpuNanos, finishTiming.cpuNanos);
    return out.str();
}

}