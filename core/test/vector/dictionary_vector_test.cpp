/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

#include "gtest/gtest.h"
#include <dictionary_vector.h>
#include <long_vector.h>
#include <vector_allocator_manager.h>

using namespace omniruntime::vec;

TEST(DictionaryVector, appendVector)
{
    VectorAllocatorManager manager = VectorAllocatorManager::GetInstance();
    VectorAllocator *allocator = manager.GetOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);
    int *ids1 = new int[5];
    int *ids2 = new int[5];
    int *ids = new int[10];
    LongVector *dictionary = new LongVector(allocator, 100);
    for (int32_t i = 0; i < 5; i++) {
        ids1[i] = i;
        ids2[i] = i + 5;
    }

    DictionaryVector *src1 = new DictionaryVector(dictionary, ids1, 5);
    DictionaryVector *src2 = new DictionaryVector(dictionary, ids2, 5);

    DictionaryVector *dest = new DictionaryVector(dictionary, ids, 10);
    dest->Append(src1, 0, 5);
    dest->Append(src2, 5, 5);
    for (int i = 0; i < 5; i++) {
        EXPECT_EQ(dest->GetIds()[i], i);
    }
    delete dictionary;
    delete src1;
    delete src2;
    delete dest;
    delete[] ids1;
    delete[] ids2;
    delete[] ids;
    manager.DeleteAllocator(&allocator);
    EXPECT_TRUE(allocator == nullptr);
}