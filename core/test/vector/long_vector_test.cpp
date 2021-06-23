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
    /**
     * LongVector *vector = new LongVector(allocator, 1024, 256, OMNI_VEC_TYPE_LONG);
     * vector->setValue(0, 1);
     * long value = vector->getValue(0);
     * delete vector;
     */
    VectorAllocator *allocator = manager.getOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);

    LongVector *vector = new LongVector(allocator, 256);

    LongVector *sliceVector = vector->slice(10, 20);
    EXPECT_EQ(sliceVector->getPositionOffset(), 10);
    EXPECT_EQ(sliceVector->getSize(), 20);
    EXPECT_EQ(sliceVector->getReference()->getRef(), 2);

    delete vector;

    EXPECT_EQ(sliceVector->getReference()->getRef(), 1);

    delete sliceVector;

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

