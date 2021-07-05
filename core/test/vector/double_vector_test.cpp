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

    DoubleVector *originalVector = new DoubleVector(allocator, 10);
    for (int i = 0; i < originalVector->getSize(); i++) {
        originalVector->setValue(i, (double)i / 3);
    }

    int offset = 3;
    DoubleVector *slice1 = originalVector->slice(offset, 4);
    EXPECT_EQ(slice1->getSize(), 4);
    EXPECT_EQ(slice1->getReference()->getRef(), 2);
    for (int i = 0; i < slice1->getSize(); i++) {
        EXPECT_EQ(slice1->getValue(i), originalVector->getValue(i + offset));
    }

    DoubleVector *slice2 = slice1->slice(1, 2);
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

// Test setValues
TEST(DoubleVector, setValues) {
    VectorAllocatorManager manager = VectorAllocatorManager::getInstance();
    VectorAllocator *allocator = manager.getOrCreateAllocator("test");
    EXPECT_TRUE(allocator != NULL);

    const int size = 5;
    double values[size] = {1.1, 3.3, 4.5, 6.6, 7.7};
    double *p = values;
    DoubleVector *doubleVector1 = new DoubleVector(allocator, size);
    doubleVector1->setValues(0, p, size);
    for (int i = 0; i < size; i++) {
        EXPECT_EQ(doubleVector1->getValue(i), values[i]);
    }
    
    DoubleVector *doubleVector2 = new DoubleVector(allocator, size);
    doubleVector2->setValues(1, p + 2, 3);
    for (int i = 0; i < 3; i++) {
        EXPECT_EQ(doubleVector2->getValue(i + 1), values[i + 2]);
    }

    delete doubleVector1;
    delete doubleVector2;
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
        } else {
            vector->setValue(i, i * 2.3);
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
TEST(DoubleVector, copyPositions) {
    VectorAllocatorManager manager = VectorAllocatorManager::getInstance();
    VectorAllocator *allocator = manager.getOrCreateAllocator("doubleVector");
    EXPECT_TRUE(allocator != nullptr);

    DoubleVector *originalVector = new DoubleVector(allocator, 4);
    for (int i = 0; i < originalVector->getSize(); i++) {
        originalVector->setValue(i, i * 2.3);
    }

    int *possions = new int[2];
    possions[0] = 1;
    possions[1] = 3;
    DoubleVector* copyPostionVector = originalVector->copyPositions(possions, 0, 2);

    for (int i = 0; i < copyPostionVector->getSize(); i++) {
        EXPECT_EQ(copyPostionVector->getValue(i), originalVector->getValue(possions[i]));
    }

    delete originalVector;
    delete copyPostionVector;
    manager.deleteAllocator(&allocator);
}

// Test is copyRegion
TEST(DoubleVector, copyRegion) {
    VectorAllocatorManager manager = VectorAllocatorManager::getInstance();
    VectorAllocator *allocator = manager.getOrCreateAllocator("doubleVector");
    EXPECT_TRUE(allocator != NULL);

    DoubleVector *originalVector = new DoubleVector(allocator, 4);
    for (int i = 0; i < 4; i++) {
        originalVector->setValue(i, i * 3.3);
    }

    DoubleVector *copyRegionVector = originalVector->copyRegion(2, 2);

    for (int i = 0; i < copyRegionVector->getSize(); i++) {
        EXPECT_EQ(copyRegionVector->getValue(i), originalVector->getValue(i + 2));
    }

    delete originalVector;
    delete copyRegionVector;
    manager.deleteAllocator(&allocator);
}

TEST(DoubleVector, jniFreeVector) {
    VectorAllocatorManager manager = VectorAllocatorManager::getInstance();
    VectorAllocator *allocator = manager.getOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);
    
    DoubleVector *oritianlVector = new DoubleVector(allocator, 256);
    Vector *vector = (Vector *) oritianlVector;
    delete vector;
}

