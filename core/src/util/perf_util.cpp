/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Inner supported aggregators source file
 */
#include "perf_util.h"
#include <huawei_secure_c/include/securec.h>
#include <cstdio>
#include <unistd.h>
#include <sys/ioctl.h>
#include <sys/syscall.h>
#include <linux/perf_event.h>

void PerfUtil::Init()
{
    struct perf_event_attr pe {};
    memset_s(&pe, sizeof(struct perf_event_attr), 0, sizeof(struct perf_event_attr));
    pe.type = PERF_TYPE_HARDWARE;
    pe.size = sizeof(struct perf_event_attr);
    pe.config = PERF_COUNT_HW_INSTRUCTIONS;
    // can also enable other perf event
    // branch miss event: PERF_COUNT_HW_BRANCH_MISSES;
    pe.disabled = 1;
    pe.exclude_kernel = 1;
    pe.exclude_hv = 1;

    this->fd = syscall(SYS_perf_event_open, &pe, 0, -1, -1, 0);
    if (fd == -1) {
        perror("Error initializing PerfUtil: ");
        return;
    }
    this->initialized = true;
}

void PerfUtil::Start() const
{
    if (initialized) {
        ioctl(fd, PERF_EVENT_IOC_ENABLE, 0);
    }
}

void PerfUtil::Stop() const
{
    if (initialized) {
        ioctl(fd, PERF_EVENT_IOC_DISABLE, 0);
    }
}

void PerfUtil::Reset() const
{
    if (initialized) {
        ioctl(fd, PERF_EVENT_IOC_RESET, 0);
    }
}

long long PerfUtil::GetData() const
{
    if (!initialized) {
        return -1;
    }

    long long count;
    if (!read(fd, &count, sizeof(long long))) {
        return count;
    }
    return -1;
}
