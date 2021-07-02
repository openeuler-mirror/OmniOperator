//
// Created by root on 6/2/21.
//

#include "gtest/gtest.h"
#include "vector.h"
#include "vector_allocator.h"
#include "vector_allocator_manager.h"
#include "boolean_vector.h"

TEST(BooleanVector, newVector) {
    VectorAllocatorManager manager = VectorAllocatorManager::getInstance();
    VectorAllocator *allocator = manager.getOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);
    BooleanVector *vector = new BooleanVector(allocator, 256);
    EXPECT_EQ(vector->getSize(), 256);
    EXPECT_EQ(vector->getPositionOffset(), 0);
    EXPECT_EQ(vector->getReference()->getCapacityInBytes(), 256);
    EXPECT_EQ(vector->getReference()->getType(), OMNI_VEC_TYPE_BOOLEAN);
    delete vector;

    BooleanVector *vector1 = new BooleanVector(allocator, 251);
    EXPECT_EQ(vector1->getSize(), 251);
    EXPECT_EQ(vector1->getPositionOffset(), 0);
    EXPECT_EQ(vector1->getReference()->getCapacityInBytes(), 251);
    EXPECT_EQ(vector1->getReference()->getType(), OMNI_VEC_TYPE_BOOLEAN);
    delete vector1;

    manager.deleteAllocator(&allocator);
    EXPECT_TRUE(allocator == nullptr);
}

TEST(BooleanVector, sliceVector) {
    VectorAllocatorManager manager = VectorAllocatorManager::getInstance();
    VectorAllocator *allocator = manager.getOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);

    BooleanVector *originalVector = new BooleanVector(allocator, 10);
    for (int i = 0; i < originalVector->getSize(); i++) {
        originalVector->setValue(i, i % 2 == 0);
    }

    int offset = 3;
    BooleanVector *slice1 = originalVector->slice(offset, 4);
    EXPECT_EQ(slice1->getPositionOffset(), offset);
    EXPECT_EQ(slice1->getSize(), 4);
    EXPECT_EQ(slice1->getReference()->getRef(), 2);
    for (int i = 0; i < slice1->getSize(); i++) {
        EXPECT_EQ(slice1->getValue(i), originalVector->getValue(i + offset));
    }

    BooleanVector *slice2 = slice1->slice(1, 2);
    for (int i = 0; i < slice2->getSize(); i++) {
        EXPECT_EQ(slice2->getValue(i), originalVector->getValue(i + offset + 1));
    }

    delete originalVector;
    EXPECT_EQ(slice1->getReference()->getRef(), 2);

    delete slice1;
    EXPECT_EQ(slice2->getReference()->getRef(), 1);
    delete slice2;

    manager.deleteAllocator(&allocator);
}

// Test set/get
TEST(BooleanVector, setAndGetValue) {
    VectorAllocatorManager manager = VectorAllocatorManager::getInstance();
    VectorAllocator *allocator = manager.getOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);

    BooleanVector *vector = new BooleanVector(allocator, 256);
    for (int i = 0; i < 256; i++) {
        vector->setValue(i, i % 2);
    }

    for (int i = 0; i < 256; i++) {
        EXPECT_EQ(vector->getValue(i), i % 2);
    }
    delete vector;
    manager.deleteAllocator(&allocator);
}

// Test setValues
TEST(BooleanVector, setValues) {
    VectorAllocatorManager manager = VectorAllocatorManager::getInstance();
    VectorAllocator *allocator = manager.getOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);

    const int size = 5;
    bool values[size] = {1, 0, 1, 0, 0};
    bool *p = values;
    BooleanVector *boolVector1 = new BooleanVector(allocator, size);
    boolVector1->setValues(0, p, size);
    for (int i = 0; i < size; i++) {
        EXPECT_EQ(boolVector1->getValue(i), values[i]);
    }
    
    BooleanVector *boolVector2 = new BooleanVector(allocator, size);
    boolVector2->setValues(1, p + 2, 3);
    for (int i = 0; i < 3; i++) {
        EXPECT_EQ(boolVector2->getValue(i + 1), values[i + 2]);
    }

    delete boolVector1;
    delete boolVector2;
    manager.deleteAllocator(&allocator);
}

// Test is null
TEST(BooleanVector, setValueNull) {
    VectorAllocatorManager manager = VectorAllocatorManager::getInstance();
    VectorAllocator *allocator = manager.getOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);

    BooleanVector *vector = new BooleanVector(allocator, 256);
    for (int i = 0; i < 256; i++) {
        if (i % 5 == 0) {
            vector->setValueNull(i);
        } else {
            vector->setValue(i, i);
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

// Test is copyPosition
TEST(BooleanVector, copyPositions) {
    VectorAllocatorManager manager = VectorAllocatorManager::getInstance();
    VectorAllocator *allocator = manager.getOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);

    BooleanVector *originalVector = new BooleanVector(allocator, 4);
    for (int i = 0; i < originalVector->getSize(); i++) {
        originalVector->setValue(i, i % 2);
    }

    int *possions = new int[2];
    possions[0] = 1;
    possions[1] = 3;
    BooleanVector* copyPostionVector = originalVector->copyPositions(possions, 0, 2);

    for (int i = 0; i < copyPostionVector->getSize(); i++) {
        EXPECT_EQ(copyPostionVector->getValue(i), originalVector->getValue(possions[i]));
    }

    delete originalVector;
    delete copyPostionVector;
    manager.deleteAllocator(&allocator);
}

// Test is copyRegion
TEST(BooleanVector, copyRegion) {
    VectorAllocatorManager manager = VectorAllocatorManager::getInstance();
    VectorAllocator *allocator = manager.getOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);

    BooleanVector *originalVector = new BooleanVector(allocator, 4);
    for (int i = 0; i < 4; i++) {
        originalVector->setValue(i, i % 2);
    }

    BooleanVector *copyRegionVector = originalVector->copyRegion(2, 2);

    for (int i = 0; i < copyRegionVector->getSize(); i++) {
        EXPECT_EQ(copyRegionVector->getValue(i), originalVector->getValue(i + 2));
    }

    delete originalVector;
    delete copyRegionVector;
    manager.deleteAllocator(&allocator);
}

TEST(BooleanVector, jniFreeVector) {
    VectorAllocatorManager manager = VectorAllocatorManager::getInstance();
    VectorAllocator *allocator = manager.getOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);

   BooleanVector *oritianlVector = new BooleanVector(allocator, 256);
    Vector *vector = (Vector *) oritianlVector;
    delete vector;
}

