/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 * Description: Metrics row info header
 */

#ifndef OMNI_RUNTIME_METRICS_ROW_COUNTER_H
#define OMNI_RUNTIME_METRICS_ROW_COUNTER_H
#include "type/data_types.h"
namespace omniruntime {
namespace op {
class MetricsRowCounter {
public:
    MetricsRowCounter() {}

    ~MetricsRowCounter() = default;

    void UpdateAddInputInfo(int32_t rowCount)
    {
        addInputTimes++;
        addInputRowCount += rowCount;
    }

    void UpdateGetOutputInfo(int32_t rowCount)
    {
        getOutputTimes++;
        getOutputRowCount += rowCount;
    }

    std::string &GetRowCounterInfo()
    {
        rowInfoStr.clear();
        rowInfoStr = "AddInput:Times=" + std::to_string(addInputTimes) +
            ",RowCount=" + std::to_string(addInputRowCount) + ",GetOutput:Times=" + std::to_string(getOutputTimes) +
            ",RowCount=" + std::to_string(getOutputRowCount) + ".";
        return rowInfoStr;
    }

private:
    std::string rowInfoStr = "";
    int64_t addInputTimes = 0;
    int64_t addInputRowCount = 0;
    int64_t getOutputTimes = 0;
    int64_t getOutputRowCount = 0;
};
}
}

#endif // OMNI_RUNTIME_METRICS_ROW_COUNTER_H
