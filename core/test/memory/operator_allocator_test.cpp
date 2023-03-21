/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#include "gtest/gtest.h"
#include "memory/base_allocator.h"
namespace omniruntime {
using namespace omniruntime::mem;

const int ALLOC_TIMES = 10;
const int ALLOC_BASIC_SIZE = 1024;

int64_t GetUnTrackedBytes(BaseAllocator *curAllocator)
{
    std::vector<BaseAllocator *> childs = curAllocator->GetChildAllocators();
    if (childs.empty()) {
        return curAllocator->GetUnTrackedBytes();
    }

    int64_t childsTotalUnTrackedBytes = 0;
    for (auto child : childs) {
        childsTotalUnTrackedBytes += GetUnTrackedBytes(child);
    }

    return childsTotalUnTrackedBytes + curAllocator->GetUnTrackedBytes();
}

void FreeAllocators(BaseAllocator *curAllocator)
{
    std::vector<BaseAllocator *> childs = curAllocator->GetChildAllocators();
    if (childs.empty() && curAllocator->GetParentAllocator() != nullptr) {
        curAllocator->ReleaseBytes(curAllocator->GetAllocatedMemory());
        delete curAllocator;
        return;
    }

    for (auto child : childs) {
        FreeAllocators(child);
    }

    return;
}

BaseAllocator *NewAllocators()
{
    BaseAllocator *rootAllocator = GetProcessRootAllocator();
    BaseAllocator *globalAllocator = rootAllocator->NewChildAllocator("___GLOBAL_SCOPE___");
    BaseAllocator *taskAllocator1 = globalAllocator->NewChildAllocator("Task Level");
    EXPECT_TRUE(taskAllocator1 != nullptr);
    BaseAllocator *operatorAllocator11 = taskAllocator1->NewChildAllocator("Operator Level");
    EXPECT_TRUE(operatorAllocator11 != nullptr);
    for (int i = 0; i < ALLOC_TIMES; ++i) {
        operatorAllocator11->TryAllocate(ALLOC_BASIC_SIZE + i);
    }

    BaseAllocator *operatorAllocator12 = taskAllocator1->NewChildAllocator("Operator Level");
    EXPECT_TRUE(operatorAllocator12 != nullptr);
    for (int i = 0; i < ALLOC_TIMES; ++i) {
        operatorAllocator12->TryAllocate(ALLOC_BASIC_SIZE * ALLOC_BASIC_SIZE + i);
    }

    BaseAllocator *taskAllocator2 = globalAllocator->NewChildAllocator("Task Level");
    EXPECT_TRUE(taskAllocator2 != nullptr);
    BaseAllocator *operatorAllocator21 = taskAllocator2->NewChildAllocator("Operator Level");
    EXPECT_TRUE(operatorAllocator21 != nullptr);
    for (int i = 0; i < ALLOC_TIMES; ++i) {
        operatorAllocator21->TryAllocate(ALLOC_BASIC_SIZE + i);
    }

    BaseAllocator *operatorAllocator22 = taskAllocator2->NewChildAllocator("Operator Level");
    EXPECT_TRUE(operatorAllocator22 != nullptr);
    for (int i = 0; i < ALLOC_TIMES; ++i) {
        operatorAllocator22->TryAllocate(ALLOC_BASIC_SIZE * ALLOC_BASIC_SIZE + i);
    }

    return globalAllocator;
}

TEST(OperatorAllocator, testAllocatorUnderLimit)
{
    BaseAllocator *globalAllocator = NewAllocators();
    EXPECT_EQ(globalAllocator->GetAllocatedMemory(), 20971610);

    int64_t totalBytes = globalAllocator->GetAllocatedMemory() + GetUnTrackedBytes(globalAllocator);
    EXPECT_EQ(totalBytes, 20992180);

    FreeAllocators(globalAllocator);
    EXPECT_EQ(globalAllocator->GetAllocatedMemory(), 0);
}


TEST(OperatorAllocator, testAllocatorOverLimit)
{
    BaseAllocator *globalAllocator = NewAllocators();

    int64_t oldLimit = globalAllocator->GetLimit();
    globalAllocator->SetLimit(20000000);

    EXPECT_ANY_THROW(globalAllocator->alloc(1));

    FreeAllocators(globalAllocator);
    EXPECT_EQ(globalAllocator->GetAllocatedMemory(), 0);

    globalAllocator->SetLimit(oldLimit);
}
}
