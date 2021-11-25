/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

#include <iostream>
#include "memory_statistic.h"

MemoryStatistic::MemoryStatistic() {}

void MemoryStatistic::RecordSize(int size)
{
    std::shared_lock<std::shared_timed_mutex> lock(mutex);
    sizeStatistic[size]++;
}

void MemoryStatistic::Print()
{
    while (running) {
        std::shared_lock<std::shared_timed_mutex> lock(mutex);
        std::map<uint64_t, uint64_t>::iterator it = sizeStatistic.begin();
        for (; it != sizeStatistic.end(); it++) {
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
