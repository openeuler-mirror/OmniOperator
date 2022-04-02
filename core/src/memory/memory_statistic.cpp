/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

#include <iostream>
#include <unistd.h>
#include "memory_statistic.h"

namespace omniruntime {
namespace mem {
MemoryStatistic::MemoryStatistic() = default;

void MemoryStatistic::RecordSize(int size)
{
    std::shared_lock<std::shared_timed_mutex> lock(mutex);
    sizeStatistic[size]++;
}

void MemoryStatistic::Print()
{
    while (running) {
        std::shared_lock<std::shared_timed_mutex> lock(mutex);
        auto it = sizeStatistic.cbegin();
        for (; it != sizeStatistic.cend(); it++) {
            std::cout << "[" << it->first << "]\t" << it->second << std::endl;
        }
        lock.unlock();
        sleep(10); // sleep for 10 seconds
    }
}

MemoryStatistic::~MemoryStatistic()
{
    running = false;
}
}
}