/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#include "gtest/gtest.h"
#include "thread_memory_manager.h"

namespace omniruntime::vec {
const static int64_t THRESHOLD = 1 * 1024 * 1024;

// test: report memory usage but not reach the untrackedMemoryThreshold.
TEST(ThreadMemoryManager, testReportMemoryUsageNotReachThreshold)
{
    auto threadMemoryManager = mem::ThreadMemoryManager::GetThreadMemoryManager();
    threadMemoryManager->Clear();
    int64_t size = THRESHOLD;
    threadMemoryManager->ReportMemoryUsage(size);
    int64_t threadAccount = threadMemoryManager->GetThreadAccountedMemory();
    int64_t threadUntracked = threadMemoryManager->GetUntrackedMemory();
    EXPECT_EQ(threadAccount, 0);
    EXPECT_EQ(threadUntracked, size);
}

// test: reclaim memory usage but not reach the untrackedMemoryThreshold.
TEST(ThreadMemoryManager, testReclaimMemoryUsageNotReachThreshold)
{
    auto threadMemoryManager = mem::ThreadMemoryManager::GetThreadMemoryManager();
    threadMemoryManager->Clear();
    int64_t size = THRESHOLD;
    threadMemoryManager->ReclaimMemoryUsage(size);
    int64_t threadAccount = threadMemoryManager->GetThreadAccountedMemory();
    int64_t threadUntracked = threadMemoryManager->GetUntrackedMemory();
    EXPECT_EQ(threadAccount, 0);
    EXPECT_EQ(threadUntracked, -size);
}

// test: report memory usage and reach the untrackedMemoryThreshold->
TEST(ThreadMemoryManager, testReportMemoryUsageReachThreshold)
{
    auto threadMemoryManager = mem::ThreadMemoryManager::GetThreadMemoryManager();
    threadMemoryManager->Clear();
    int64_t size = THRESHOLD + 1;

    threadMemoryManager->ReportMemoryUsage(size);
    int64_t threadAccount = threadMemoryManager->GetThreadAccountedMemory();
    EXPECT_EQ(threadAccount, size);
}

// test: reclaim memory usage and reach the untrackedMemoryThreshold->
TEST(ThreadMemoryManager, testReclaimMemoryUsageReachThreshold)
{
    auto threadMemoryManager = mem::ThreadMemoryManager::GetThreadMemoryManager();
    threadMemoryManager->Clear();
    int64_t size = THRESHOLD + 1;

    threadMemoryManager->ReclaimMemoryUsage(size);
    int64_t threadAccount = threadMemoryManager->GetThreadAccountedMemory();
    EXPECT_EQ(threadAccount, -size);
}
}
