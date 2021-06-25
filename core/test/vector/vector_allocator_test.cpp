//
// Created by root on 6/11/21.
//
#include <long_vector.h>
#include "gtest/gtest.h"
#include "vector_allocator.h"
#include "vector_allocator_manager.h"

TEST(VectorAllocatorManager, getOrCreateAllocator) {
    VectorAllocatorManager manager = VectorAllocatorManager::getInstance();

// create one scope allocator
    VectorAllocator *allocator1 = manager.getOrCreateAllocator("test1");
    EXPECT_TRUE(allocator1 != nullptr);

// get the same scope allocator
    VectorAllocator *allocator2 = manager.getOrCreateAllocator("test1");
    EXPECT_TRUE(allocator2 != nullptr);
    EXPECT_TRUE(allocator2 == allocator1);

// get different scope allocator
    VectorAllocator *allocator3 = manager.getOrCreateAllocator("test3");
    EXPECT_TRUE(allocator3 != nullptr);
    EXPECT_TRUE(allocator2 != allocator3);

// free allocator2
    manager.deleteAllocator(&allocator1);
    EXPECT_TRUE(allocator1 == nullptr);
    manager.deleteAllocator(&allocator2);
    EXPECT_TRUE(allocator2 == nullptr);
    manager.deleteAllocator(&allocator3);
    EXPECT_TRUE(allocator3 == nullptr);
}

TEST(VectorAllocator, newVector) {
    VectorAllocatorManager manager = VectorAllocatorManager::getInstance();
    VectorAllocator *allocator = manager.getOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);
    LongVector *vector = new LongVector(allocator, 256);
    EXPECT_EQ(vector->getSize(), 256);
    EXPECT_EQ(vector->getPositionOffset(), 0);
    EXPECT_EQ(vector->getReference()->getCapacityInBytes(), 2048);
    EXPECT_EQ(vector->getReference()->getType(), OMNI_VEC_TYPE_LONG);
    delete vector;

    manager.deleteAllocator(&allocator);
    EXPECT_TRUE(allocator == nullptr);
}
