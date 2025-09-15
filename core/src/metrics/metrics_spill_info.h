/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 * Description: Metrics spill info header
 */

#ifndef OMNI_RUNTIME_METRICS_SPILL_INFO_H
#define OMNI_RUNTIME_METRICS_SPILL_INFO_H

#include "type/data_types.h"

namespace omniruntime {
namespace op {
class MetricsSpillInfo {
public:
    MetricsSpillInfo(){};
    ~MetricsSpillInfo() = default;
    void UpdateSpillFileInfo(uint32_t fileCount)
    {
        spillFileCount += fileCount;
    }

    void UpdateSpillTimesInfo()
    {
        spillTimes++;
    }
    std::string &GetSpillInfo()
    {
        rowInfoStr.clear();
        rowInfoStr = "Spill:Times=" + std::to_string(spillTimes) + ",FileNum=" + std::to_string(spillFileCount) + ".";
        return rowInfoStr;
    }

private:
    std::string rowInfoStr = "";
    int64_t spillFileCount = 0;
    int64_t spillTimes = 0;
};
}
}

#endif // OMNI_RUNTIME_METRICS_SPILL_INFO_H
