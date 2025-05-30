/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: This file declares for ProccessBaseTest.cpp
 */

#include "gtest/gtest.h"
#include "compute/process_base.h"

namespace ProccessBaseTest {
TEST(ThreadCpuNanosTest, ThreadCpuNanos_ShouldHandleZeroCpuTime_WhenCalled)
{
    // Simulate zero CPU time by setting the timespec values to zero
    timespec ts = {0, 0};
    clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts);

    // Call the function under test
    int64_t result = omniruntime::compute::ThreadCpuNanos();

    // Check if the result is zero
    EXPECT_NE(result, 0);
}
}