//
// Created by root on 6/2/21.
//

#include "gtest/gtest.h"
#include "vector_allocator.h"
#include "vector_allocator_manager.h"
#include "long_vector.h"
#include "../util/test_util.h"

VectorAllocatorManager manager = VectorAllocatorManager::getInstance();

TEST(LongVector, sliceVector) {
    VectorAllocator *allocator = manager.getOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);

    LongVector *originalVector = new LongVector(allocator, 10);
    for (int i = 0; i < originalVector->getSize(); i++) {
        originalVector->setValue(i, i * 2);
    }

    int offset = 3;
    LongVector *slice1 = originalVector->slice(offset, 4);
    EXPECT_EQ(slice1->getPositionOffset(), offset);
    EXPECT_EQ(slice1->getSize(), 4);
    EXPECT_EQ(slice1->getReference()->getRef(), 2);
    for (int i = 0; i < slice1->getSize(); i++) {
        EXPECT_EQ(slice1->getValue(i), originalVector->getValue(i + offset));
    }

    LongVector *slice2 = slice1->slice(1, 2);
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
TEST(LongVector, setAndGetValue) {
    VectorAllocator *allocator = manager.getOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);

    LongVector *vector = new LongVector(allocator, 256);
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
TEST(LongVector, setValues) {
    VectorAllocator *allocator = manager.getOrCreateAllocator("test");
    EXPECT_TRUE(allocator != NULL);

    const int size = 5;
    int64_t values[size] = {1, 3, 4, 6, 7};
    int64_t *p = values;
    LongVector *longVector1 = new LongVector(allocator, size);
    longVector1->setValues(0, p, size);
    for (int i = 0; i < size; i++) {
        EXPECT_EQ(longVector1->getValue(i), values[i]);
    }
        
    LongVector *longVector2 = new LongVector(allocator, size);
    longVector2->setValues(1, p + 2, 3);
    for (int i = 0; i < 3; i++) {
        EXPECT_EQ(longVector2->getValue(i + 1), values[i + 2]);
    }

    delete longVector1;
    delete longVector2;
    manager.deleteAllocator(&allocator);
}

// Test out of bounds
#ifdef DEBUG
TEST(LongVector, setValueOutOfBounds1) {
    VectorAllocator *allocator = manager.getOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);

    LongVector *vector = new LongVector(allocator, 256);
    EXPECT_THROW(vector->setValue(256, 256), runtime_error);

    delete vector;
    manager.deleteAllocator(&allocator);
}
#endif

// Test out of bounds
#ifdef DEBUG
TEST(LongVector, setValueOutOfBounds2) {
    VectorAllocator *allocator = manager.getOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);

    LongVector *vector = new LongVector(allocator, 256);
    EXPECT_THROW(vector->setValue(-1, 256), runtime_error);

    delete vector;
    manager.deleteAllocator(&allocator);
}
#endif

// Test setValues/get
TEST(LongVector, setValuesWithoutOffset) {
    VectorAllocator *allocator = manager.getOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);

    LongVector *vector = new LongVector(allocator, 256);
    long *value = new long[256];
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
TEST(LongVector, setValuesWithOffset) {
    VectorAllocator *allocator = manager.getOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);

    LongVector *vector = new LongVector(allocator, 256);
    long *value = new long[256];
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
TEST(LongVector, setValuesWithoutOffsetOutOfBounds) {
    VectorAllocator *allocator = manager.getOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);

    LongVector *vector = new LongVector(allocator, 256);
    long *value = new long[257];
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
TEST(LongVector, setValueNull) {
    VectorAllocator *allocator = manager.getOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);

    LongVector *vector = new LongVector(allocator, 256);
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
TEST(LongVector, copyPositions) {
    VectorAllocatorManager manager = VectorAllocatorManager::getInstance();
    VectorAllocator *allocator = manager.getOrCreateAllocator("longVector");
    EXPECT_TRUE(allocator != nullptr);

    LongVector *originalVector = new LongVector(allocator, 4);
    for (int i = 0; i < originalVector->getSize(); i++) {
        originalVector->setValue(i, i);
    }

    int *possions = new int[2];
    possions[0] = 1;
    possions[1] = 3;
    LongVector* copyPostionVector = originalVector->copyPositions(possions, 0, 2);

    for (int i = 0; i < copyPostionVector->getSize(); i++) {
        EXPECT_EQ(copyPostionVector->getValue(i), originalVector->getValue(possions[i]));
    }

    delete originalVector;
    delete copyPostionVector;
    manager.deleteAllocator(&allocator);
}

// Test is copyRegion
TEST(LongVector, copyRegion) {
    VectorAllocatorManager manager = VectorAllocatorManager::getInstance();
    VectorAllocator *allocator = manager.getOrCreateAllocator("longVector");
    EXPECT_TRUE(allocator != NULL);

    LongVector *originalVector = new LongVector(allocator, 4);
    for (int i = 0; i < 4; i++) {
        originalVector->setValue(i, i * 2);
    }

    LongVector *copyRegionVector = originalVector->copyRegion(2, 2);

    for (int i = 0; i < copyRegionVector->getSize(); i++) {
        EXPECT_EQ(copyRegionVector->getValue(i), originalVector->getValue(i + 2));
    }

    delete originalVector;
    delete copyRegionVector;
    manager.deleteAllocator(&allocator);
}

TEST(Vector, jniFreeVector) {
    VectorAllocator *allocator = manager.getOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);

    LongVector *longVector = new LongVector(allocator, 256);
    Vector *vector = (Vector *) longVector;
    delete vector;
}

class LongVectorTest {
public:
    LongVectorTest() {
        values = new long[100000000];
    }

    void setValue(int index, int64_t value) {
        ((int64_t *) values)[index] = value;
    }

    int64_t getValue(int index) {
        return ((int64_t *) values)[index];
    }

private:
    void *values;
};

// Performance test
TEST(LongVector, performanceCompare) {
    VectorAllocator *allocator = manager.getOrCreateAllocator("test");
    int ROW_COUNT = 100000000;

    Timer timer;

    // test long vector set value
    auto *vectorTest2 = new LongVectorTest();
    timer.start("point test vector set value");
    for (int i = 0; i < ROW_COUNT; ++i) {
        vectorTest2->setValue(i, i);
    }
    timer.end();

    // test long vector set value
    LongVectorTest vectorTest1;
    timer.start("stack test vector set value");
    for (int i = 0; i < ROW_COUNT; ++i) {
        vectorTest1.setValue(i, i);
    }
    timer.end();

    // test long vector get value
    timer.start("point test vector get value");
    for (int i = 0; i < ROW_COUNT; ++i) {
        vectorTest2->getValue(i);
    }
    timer.end();

    // test long vector get value
    timer.start("stack test vector get value");
    for (int i = 0; i < ROW_COUNT; ++i) {
        vectorTest1.getValue(i);
    }
    timer.end();

    // vector set value
    LongVector longVector(allocator, ROW_COUNT);
    timer.start("vector set value");
    for (int i = 0; i < ROW_COUNT; ++i) {
        longVector.setValue(i, i);
    }
    timer.end();

    // original set value
    void *longVector2 = new long[ROW_COUNT];
    timer.start("original set value");
    for (int i = 0; i < ROW_COUNT; ++i) {
        ((long *) longVector2)[i] = i;
    }
    timer.end();

    // vector get value
    timer.start("vector get value");
    for (int i = 0; i < ROW_COUNT; ++i) {
        long value = longVector.getValue(i);
    }
    timer.end();

    // original get value
    timer.start("original get value");
    for (int i = 0; i < ROW_COUNT; ++i) {
        long value = *((int64_t *) (longVector2) + i);
    }
    timer.end();

//    delete longVector;
    delete[] (long *) longVector2;
    delete vectorTest2;
}

// Test is not writable

// Test multi thread

