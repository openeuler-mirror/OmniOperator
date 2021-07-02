//
// Created by root on 6/2/21.
//

#include "gtest/gtest.h"
#include "vector.h"
#include "vector_allocator.h"
#include "vector_allocator_manager.h"
#include "int_vector.h"

TEST(IntVector, newVector) {
    VectorAllocatorManager manager = VectorAllocatorManager::getInstance();
    VectorAllocator *allocator = manager.getOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);
    IntVector *vector = new IntVector(allocator, 256);
    EXPECT_EQ(vector->getSize(), 256);
    EXPECT_EQ(vector->getPositionOffset(), 0);
    EXPECT_EQ(vector->getReference()->getCapacityInBytes(), 1024);
    EXPECT_EQ(vector->getReference()->getType(), OMNI_VEC_TYPE_INT);
    delete vector;

    manager.deleteAllocator(&allocator);
    EXPECT_TRUE(allocator == nullptr);
}

TEST(IntVector, sliceVector) {
    VectorAllocatorManager manager = VectorAllocatorManager::getInstance();
    VectorAllocator *allocator = manager.getOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);

    IntVector *originalVector = new IntVector(allocator, 10);
    for (int i = 0; i < originalVector->getSize(); i++) {
        originalVector->setValue(i, i * 2);
    }

    int offset = 3;
    IntVector *slice1 = originalVector->slice(offset, 4);
    EXPECT_EQ(slice1->getPositionOffset(), offset);
    EXPECT_EQ(slice1->getSize(), 4);
    EXPECT_EQ(slice1->getReference()->getRef(), 2);
    for (int i = 0; i < slice1->getSize(); i++) {
        EXPECT_EQ(slice1->getValue(i), originalVector->getValue(i + offset));
    }

    IntVector *slice2 = slice1->slice(1, 2);
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
TEST(IntVector, setAndGetValue) {
    VectorAllocatorManager manager = VectorAllocatorManager::getInstance();
    VectorAllocator *allocator = manager.getOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);

    IntVector *vector = new IntVector(allocator, 256);
    for (int i = 0; i < 256; i++) {
        vector->setValue(i, i * 2);
    }

    for (int i = 0; i < 256; i++) {
        EXPECT_EQ(vector->getValue(i), i * 2);
    }
    delete vector;
    manager.deleteAllocator(&allocator);
}

// Test setValues
TEST(IntVector, setValues) {
    VectorAllocatorManager manager = VectorAllocatorManager::getInstance();
    VectorAllocator *allocator = manager.getOrCreateAllocator("test");
    EXPECT_TRUE(allocator != NULL);

    const int size = 5;
    int32_t values[size] = {1, 3, 4, 6, 7};
    int32_t *p = values;
    IntVector *intVector1 = new IntVector(allocator, size);
    intVector1->setValues(0, p, size);
    for (int i = 0; i < size; i++) {
        EXPECT_EQ(intVector1->getValue(i), values[i]);
    }
    
    IntVector *intVector2 = new IntVector(allocator, size);
    intVector2->setValues(1, p + 2, 3);
    for (int i = 0; i < 3; i++) {
        EXPECT_EQ(intVector2->getValue(i + 1), values[i + 2]);
    }

    delete intVector1;
    delete intVector2;
    manager.deleteAllocator(&allocator);
}

// Test out of bounds
#ifdef DEBUG
TEST(IntVector, setValueOutOfBounds1) {
    VectorAllocatorManager manager = VectorAllocatorManager::getInstance();
    VectorAllocator *allocator = manager.getOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);

    IntVector *vector = new IntVector(allocator, 256);
    EXPECT_THROW(vector->setValue(256, 256), runtime_error);

    delete vector;
    manager.deleteAllocator(&allocator);
}
#endif

// Test out of bounds
#ifdef DEBUG
TEST(IntVector, setValueOutOfBounds2) {
    VectorAllocatorManager manager = VectorAllocatorManager::getInstance();
    VectorAllocator *allocator = manager.getOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);

    IntVector *vector = new IntVector(allocator, 256);
    EXPECT_THROW(vector->setValue(-1, 256), runtime_error);

    delete vector;
    manager.deleteAllocator(&allocator);
}
#endif

// Test setValues/get
TEST(IntVector, setValuesWithoutOffset) {
    VectorAllocatorManager manager = VectorAllocatorManager::getInstance();
    VectorAllocator *allocator = manager.getOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);

    IntVector *vector = new IntVector(allocator, 256);
    int32_t *value = new int32_t[256];
    for (int i = 0; i < 256; i++) {
        value[i] = i * 2;
    }
    vector->setValues(0, value, 256);
    for (int i = 0; i < 256; i++) {
        EXPECT_EQ(vector->getValue(i), i * 2);
    }

    delete[] value;
    delete vector;
    manager.deleteAllocator(&allocator);
}

// Test setValues/get with offset
TEST(IntVector, setValuesWithOffset) {
    VectorAllocatorManager manager = VectorAllocatorManager::getInstance();
    VectorAllocator *allocator = manager.getOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);

    IntVector *vector = new IntVector(allocator, 256);
    int32_t *value = new int32_t[256];
    for (int i = 0; i < 256; i++) {
        value[i] = i * 2;
    }
    vector->setValues(128, &value[128], 128);
    for (int i = 128; i < 256; i++) {
        EXPECT_EQ(vector->getValue(i), i * 2);
    }

    delete[] value;
    delete vector;
    manager.deleteAllocator(&allocator);
}

// Test out of bounds
#ifdef DEBUG
TEST(IntVector, setValuesWithoutOffsetOutOfBounds) {
    VectorAllocatorManager manager = VectorAllocatorManager::getInstance();
    VectorAllocator *allocator = manager.getOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);

    IntVector *vector = new IntVector(allocator, 256);
    int32_t *value = new int32_t[257];
    for (int i = 0; i < 257; i++) {
        value[i] = i * 2;
    }

    EXPECT_THROW(vector->setValues(0, value, 257), runtime_error);

    delete[] value;
    delete vector;
    manager.deleteAllocator(&allocator);
}
#endif

// Test is null
TEST(IntVector, setValueNull) {
    VectorAllocatorManager manager = VectorAllocatorManager::getInstance();
    VectorAllocator *allocator = manager.getOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);

    IntVector *vector = new IntVector(allocator, 256);
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
TEST(IntVector, copyPositions) {
    VectorAllocatorManager manager = VectorAllocatorManager::getInstance();
    VectorAllocator *allocator = manager.getOrCreateAllocator("intVector");
    EXPECT_TRUE(allocator != nullptr);

    IntVector *originalVector = new IntVector(allocator, 4);
    for (int i = 0; i < originalVector->getSize(); i++) {
        originalVector->setValue(i, i);
    }

    int *possions = new int[2];
    possions[0] = 1;
    possions[1] = 3;
    IntVector* copyPostionVector = originalVector->copyPositions(possions, 0, 2);

    for (int i = 0; i < copyPostionVector->getSize(); i++) {
        EXPECT_EQ(copyPostionVector->getValue(i), originalVector->getValue(possions[i]));
    }

    delete originalVector;
    delete copyPostionVector;
    manager.deleteAllocator(&allocator);
}

// Test is copyRegion
TEST(IntVector, copyRegion) {
    VectorAllocatorManager manager = VectorAllocatorManager::getInstance();
    VectorAllocator *allocator = manager.getOrCreateAllocator("intVector");
    EXPECT_TRUE(allocator != NULL);

    IntVector *originalVector = new IntVector(allocator, 4);
    for (int i = 0; i < 4; i++) {
        originalVector->setValue(i, i * 2);
    }

    IntVector *copyRegionVector = originalVector->copyRegion(2, 2);

    for (int i = 0; i < copyRegionVector->getSize(); i++) {
        EXPECT_EQ(copyRegionVector->getValue(i), originalVector->getValue(i + 2));
    }

    delete originalVector;
    delete copyRegionVector;
    manager.deleteAllocator(&allocator);
}

TEST(IntVector, jniFreeVector) {
    VectorAllocatorManager manager = VectorAllocatorManager::getInstance();
    VectorAllocator *allocator = manager.getOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);

   IntVector *oritianlVector = new IntVector(allocator, 256);
    Vector *vector = (Vector *) oritianlVector;
    delete vector;
}

