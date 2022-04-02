/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

#ifndef OMNI_RUNTIME_MEMORY_STATISTIC_H
#define OMNI_RUNTIME_MEMORY_STATISTIC_H

#include <map>
#include <shared_mutex>
#include <thread>

namespace omniruntime {
namespace mem {
class MemoryStatistic {
public:
    MemoryStatistic();
    ~MemoryStatistic();
    void RecordSize(int size);

private:
    void Print();
    std::map<uint64_t, uint64_t> sizeStatistic = {};
    std::shared_timed_mutex mutex;
    std::thread thread;
    bool running = true;
};
}
}
#endif // OMNI_RUNTIME_MEMORY_STATISTIC_H
