/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 * Description: Metrics header
 */

#ifndef OMNI_RUNTIME_METRICS_H
#define OMNI_RUNTIME_METRICS_H
#include <chrono>
#include <ctime>
#include <iomanip>
#include <sstream>
#include <string>
#include "type/data_types.h"
#include "metrics_memory_info.h"
#include "metrics_row_counter.h"
#include "metrics_spill_info.h"
#include "util/global_log.h"
#include "memory/simple_arena_allocator.h"
namespace omniruntime {
namespace op {
const std::string metricsNameHashAgg = "hashAggregation";
const std::string metricsNameHashBuilder = "hashBuilder";
const std::string metricsNameFilter = "filter";
const std::string metricsNameLookUpJoin = "lookUpJoin";
const std::string metricsNameSort = "sort";
const std::string metricsNameWindow = "window";
const std::string metricsNameNestedLoopJoinBuilder = "nestedLoopJoinBuilder";
const std::string metricsNameNestedLoopJoinLookup = "nestedLoopJoinLookup";

class Metrics {
public:
    Metrics() : isDebugEnabled(IsDebugEnable()), tid(std::to_string(pthread_self())), pid(std::to_string(getpid())) {}

    ~Metrics() = default;

    void SetOperatorName(const std::string &operatorName)
    {
        this->operatorName = operatorName;
    }

    void UpdateAddInputInfo(int32_t rowCount,
        const std::unique_ptr<omniruntime::op::ExecutionContext> &executionContext)
    {
        metricsRowCounter.UpdateAddInputInfo(rowCount);
        SetRowInfo();
        SetSpillInfo();
        SetMemoryInfo(executionContext);
        std::string allInfo = "In operator:" + operatorName + "," + rowInfoStr + spillInfoStr + memoryInfoStr;
        LogDebug("%s", allInfo.c_str());
    }

    void UpdateGetOutputInfo(int32_t rowCount,
        const std::unique_ptr<omniruntime::op::ExecutionContext> &executionContext)
    {
        metricsRowCounter.UpdateGetOutputInfo(rowCount);
        SetRowInfo();
        SetSpillInfo();
        SetMemoryInfo(executionContext);
        std::string allInfo = "In operator:" + operatorName + "," + rowInfoStr + spillInfoStr + memoryInfoStr;
        LogDebug("%s", allInfo.c_str());
    }

    void UpdateSpillFileInfo(int32_t fileCount,
        const std::unique_ptr<omniruntime::op::ExecutionContext> &executionContext)
    {
        metricsSpillInfo.UpdateSpillFileInfo(fileCount);
        SetRowInfo();
        SetSpillInfo();
        SetMemoryInfo(executionContext);
        std::string allInfo = "In operator:" + operatorName + "," + rowInfoStr + spillInfoStr + memoryInfoStr;
        LogDebug("%s", allInfo.c_str());
    }

    void UpdateSpillTimesInfo(const std::unique_ptr<omniruntime::op::ExecutionContext> &executionContext)
    {
        metricsSpillInfo.UpdateSpillTimesInfo();
        SetRowInfo();
        SetSpillInfo();
        SetMemoryInfo(executionContext);
        std::string allInfo = "In operator:" + operatorName + "," + rowInfoStr + spillInfoStr + memoryInfoStr;
        LogDebug("%s", allInfo.c_str());
    }

    void UpdateCloseInfo(const std::unique_ptr<omniruntime::op::ExecutionContext> &executionContext)
    {
        SetRowInfo();
        SetSpillInfo();
        SetMemoryInfo(executionContext);
        std::string allInfo = "In operator:" + operatorName + "," + rowInfoStr + spillInfoStr + memoryInfoStr;
        LogDebug("%s", allInfo.c_str());
    }

private:
    bool isDebugEnabled = false;
    std::string rowInfoStr;
    std::string memoryInfoStr;
    std::string spillInfoStr;
    MetricsMemoryInfo metricsMemoryInfo;
    MetricsRowCounter metricsRowCounter;
    MetricsSpillInfo metricsSpillInfo;
    const std::string pid;
    const std::string tid;
    std::string operatorName;

    void SetRowInfo()
    {
        rowInfoStr = " pid=" + pid + ",tid=" + tid + "." + metricsRowCounter.GetRowCounterInfo();
    }

    void SetMemoryInfo(const std::unique_ptr<omniruntime::op::ExecutionContext> &executionContext)
    {
        memoryInfoStr = metricsMemoryInfo.SetMemoryInfo(executionContext);
    }

    void SetSpillInfo()
    {
        spillInfoStr = metricsSpillInfo.GetSpillInfo();
    }
};
}
}
#endif // OMNI_RUNTIME_METRICS_H
