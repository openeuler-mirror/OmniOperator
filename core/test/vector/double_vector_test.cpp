//
// Created by root on 6/2/21.
//

#include "gtest/gtest.h"
#include "vector.h"
#include "vector_allocator.h"
#include "vector_allocator_manager.h"
#include "double_vector.h"

TEST(DoubleVector, newVector) {
    VectorAllocatorManager manager = VectorAllocatorManager::getInstance();
    VectorAllocator *allocator = manager.getOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);
    DoubleVector *vector = new DoubleVector(allocator, 256);
    EXPECT_EQ(vector->getSize(), 256);
    EXPECT_EQ(vector->getPositionOffset(), 0);
    EXPECT_EQ(vector->getReference()->getCapacityInBytes(), 2048);
    EXPECT_EQ(vector->getReference()->getType(), OMNI_VEC_TYPE_DOUBLE);
    delete vector;

    manager.deleteAllocator(&allocator);
    EXPECT_TRUE(allocator == nullptr);
}

TEST(DoubleVector, sliceVector) {
    VectorAllocatorManager manager = VectorAllocatorManager::getInstance();
    VectorAllocator *allocator = manager.getOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);

    DoubleVector *vector = new DoubleVector(allocator, 256);

    DoubleVector *sliceVector = vector->slice(10, 20);
    EXPECT_EQ(sliceVector->getPositionOffset(), 10);
    EXPECT_EQ(sliceVector->getSize(), 20);
    EXPECT_EQ(sliceVector->getReference()->getRef(), 2);

    delete vector;

    EXPECT_EQ(sliceVector->getReference()->getRef(), 1);

    delete sliceVector;

    manager.deleteAllocator(&allocator);
}

// Test set/get
TEST(DoubleVector, setAndGetValue) {
    VectorAllocatorManager manager = VectorAllocatorManager::getInstance();
    VectorAllocator *allocator = manager.getOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);

    DoubleVector *vector = new DoubleVector(allocator, 256);
    for (int i = 0; i < 256; i++) {
        vector->setValue(i, i * 2.3);
    }

    for (int i = 0; i < 256; i++) {
        EXPECT_EQ(vector->getValue(i), i * 2.3);
    }
    delete vector;
    manager.deleteAllocator(&allocator);
}

// Test out of bounds
#ifdef DEBUG
TEST(DoubleVector, setValueOutOfBounds1) {
    VectorAllocatorManager manager = VectorAllocatorManager::getInstance();
    VectorAllocator *allocator = manager.getOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);

    DoubleVector *vector = new DoubleVector(allocator, 256);
    EXPECT_THROW(vector->setValue(256, 256), runtime_error);

    delete vector;
    manager.deleteAllocator(&allocator);
}
#endif

// Test out of bounds
#ifdef DEBUG
TEST(DoubleVector, setValueOutOfBounds2) {
    VectorAllocatorManager manager = VectorAllocatorManager::getInstance();
    VectorAllocator *allocator = manager.getOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);

    DoubleVector *vector = new DoubleVector(allocator, 256);
    EXPECT_THROW(vector->setValue(-1, 256), runtime_error);

    delete vector;
    manager.deleteAllocator(&allocator);
}
#endif

// Test setValues/get
TEST(DoubleVector, setValuesWithoutOffset) {
    VectorAllocatorManager manager = VectorAllocatorManager::getInstance();
    VectorAllocator *allocator = manager.getOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);

    DoubleVector *vector = new DoubleVector(allocator, 256);
    double *value = new double[256];
    for (int i = 0; i < 256; i++) {
        value[i] = i * 2.3;
    }
    vector->setValues(0, value, 256);
    for (int i = 0; i < 256; i++) {
        EXPECT_EQ(vector->getValue(i), i * 2.3);
    }

    delete[] value;
    delete vector;
    manager.deleteAllocator(&allocator);
}

// Test setValues/get with offset
TEST(DoubleVector, setValuesWithOffset) {
    VectorAllocatorManager manager = VectorAllocatorManager::getInstance();
    VectorAllocator *allocator = manager.getOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);

    DoubleVector *vector = new DoubleVector(allocator, 256);
    double *value = new double[256];
    for (int i = 0; i < 256; i++) {
        value[i] = i * 2.3;
    }
    vector->setValues(128, &value[128], 128);
    for (int i = 128; i < 256; i++) {
        EXPECT_EQ(vector->getValue(i), i * 2.3);
    }

    delete[] value;
    delete vector;
    manager.deleteAllocator(&allocator);
}

// Test out of bounds
#ifdef DEBUG
TEST(DoubleVector, setValuesWithoutOffsetOutOfBounds) {
    VectorAllocatorManager manager = VectorAllocatorManager::getInstance();
    VectorAllocator *allocator = manager.getOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);

    DoubleVector *vector = new DoubleVector(allocator, 256);
    double *value = new double[257];
    for (int i = 0; i < 257; i++) {
        value[i] = i * 2.3;
    }

    EXPECT_THROW(vector->setValues(0, value, 257), runtime_error);

    delete[] value;
    delete vector;
    manager.deleteAllocator(&allocator);
}
#endif

// Test is null
TEST(DoubleVector, setValueNull) {
    VectorAllocatorManager manager = VectorAllocatorManager::getInstance();
    VectorAllocator *allocator = manager.getOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);

    DoubleVector *vector = new DoubleVector(allocator, 256);
    for (int i = 0; i < 256; i++) {
        if (i % 5 == 0) {
            vector->setValueNull(i);
        }
    }
    for (int i = 0; i < 256; i++) {
        if (i % 5 == 0) {
            EXPECT_TRUE(vector->isValueNull(i));
        } else {
            EXPECT_FALSE(vector->isValueNull(i));
        }
    }
    delete vector;
    manager.deleteAllocator(&allocator);
}

// Test is not writable

// Test multi thread

