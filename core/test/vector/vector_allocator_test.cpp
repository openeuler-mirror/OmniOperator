//
// Created by root on 6/11/21.
//
#include <long_vector.h>
#include "gtest/gtest.h"
#include "vector_allocator.h"
#include "vector_allocator_manager.h"

using namespace omniruntime::vec;

TEST(VectorAllocatorManager, getOrCreateAllocator) {
    VectorAllocatorManager manager = VectorAllocatorManager::GetInstance();

// create one scope allocator
    VectorAllocator *allocator1 = manager.GetOrCreateAllocator("test1");
    EXPECT_TRUE(allocator1 != nullptr);
// get the same scope allocator
    VectorAllocator *allocator2 = manager.GetOrCreateAllocator("test1");
    EXPECT_TRUE(allocator2 != nullptr);
    EXPECT_TRUE(allocator2 == allocator1);
// get different scope allocator
    VectorAllocator *allocator3 = manager.GetOrCreateAllocator("test3");
    EXPECT_TRUE(allocator3 != nullptr);
    EXPECT_TRUE(allocator2 != allocator3);
// free allocator2
    manager.DeleteAllocator(&allocator1);
    EXPECT_TRUE(allocator1 == nullptr);
    // TODO: Freeing the same allocator causes a segfault.
    // manager.DeleteAllocator(&allocator2);
    // EXPECT_TRUE(allocator2 == nullptr);
    manager.DeleteAllocator(&allocator3);
    EXPECT_TRUE(allocator3 == nullptr);
}

TEST(VectorAllocator, newVector) {
    VectorAllocatorManager manager = VectorAllocatorManager::GetInstance();
    VectorAllocator *allocator = manager.GetOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);
    LongVector *vector = new LongVector(allocator, 256);
    EXPECT_EQ(vector->GetSize(), 256);
    EXPECT_EQ(vector->GetPositionOffset(), 0);
    EXPECT_EQ(vector->GetReference()->GetCapacityInBytes(), 2048);
    EXPECT_EQ(vector->GetReference()->GetType(), OMNI_VEC_TYPE_LONG);
    delete vector;

    manager.DeleteAllocator(&allocator);
    EXPECT_TRUE(allocator == nullptr);
}
