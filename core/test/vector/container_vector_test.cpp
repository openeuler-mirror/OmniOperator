//
// Created by root on 7/7/21.
//

#include "gtest/gtest.h"
#include "vector_allocator.h"
#include "vector_allocator_manager.h"
#include "container_vector.h"
#include "../util/test_util.h"

using namespace omniruntime::vec;

const int32_t POSITION_COUNT = 100;
const int32_t VECTOR_COUNT = 2;
const VecType VECTOR_TYPES[] = {OMNI_VEC_TYPE_DOUBLE, OMNI_VEC_TYPE_LONG};

TEST(ContainerVector, sliceVector) {
    VectorAllocatorManager manager = VectorAllocatorManager::GetInstance();
    VectorAllocator *allocator = manager.GetOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);

    DoubleVector* doubleVector = new DoubleVector(allocator, POSITION_COUNT);
    LongVector* longVector = new LongVector(allocator, POSITION_COUNT);
    Vector** vectorAddresses = new Vector*[VECTOR_COUNT];
    vectorAddresses[0] = doubleVector;
    vectorAddresses[1] = longVector;
    ContainerVector *originalVector = new ContainerVector(allocator, POSITION_COUNT, vectorAddresses, VECTOR_COUNT,
                                                          const_cast<VecType *>(VECTOR_TYPES));
    for (int i = 0; i < originalVector->GetSize(); i++) {
        originalVector->setValue(i, i * 2);
    }

    int offset = 0;
    ContainerVector *slice1 = originalVector->Slice(offset, 2);
    EXPECT_EQ(slice1->GetPositionOffset(), offset);
    EXPECT_EQ(slice1->GetSize(), 2);
    EXPECT_EQ(slice1->GetReference()->GetRef(), 2);
    for (int i = 0; i < slice1->GetSize(); i++) {
        EXPECT_EQ(slice1->getValue(i), originalVector->getValue(i + offset));
    }

    delete originalVector;
    EXPECT_EQ(slice1->GetReference()->GetRef(), 1);

    delete slice1;

    manager.DeleteAllocator(&allocator);
}

// Test set/get
TEST(ContainerVector, setAndGetValue) {
    VectorAllocatorManager manager = VectorAllocatorManager::GetInstance();
    VectorAllocator *allocator = manager.GetOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);

    DoubleVector* doubleVector = new DoubleVector(allocator, POSITION_COUNT);
    LongVector* longVector = new LongVector(allocator, POSITION_COUNT);
    Vector** vectorAddresses = new Vector*[VECTOR_COUNT];
    vectorAddresses[0] = doubleVector;
    vectorAddresses[1] = longVector;
    ContainerVector *vector = new ContainerVector(allocator, POSITION_COUNT, vectorAddresses, VECTOR_COUNT, const_cast<VecType*>(VECTOR_TYPES));
    for (int i = 0; i < POSITION_COUNT; i++) {
        vector->setValue(i, i * 2);
    }

    for (int i = 0; i < POSITION_COUNT; i++) {
        EXPECT_EQ(vector->getValue(i), i * 2);
    }
    delete vector;
    manager.DeleteAllocator(&allocator);
}

// Test out of bounds
#ifdef DEBUG
TEST(ContainerVector, setValueOutOfBounds1) {
    VectorAllocator *allocator = manager.getOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);

    ContainerVector *vector = new ContainerVector(allocator, 256);
    EXPECT_THROW(vector->setValue(256, 256), runtime_error);

    delete vector;
    manager.deleteAllocator(&allocator);
}
#endif

// Test out of bounds
#ifdef DEBUG
TEST(ContainerVector, setValueOutOfBounds2) {
    VectorAllocator *allocator = manager.getOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);

    ContainerVector *vector = new ContainerVector(allocator, 256);
    EXPECT_THROW(vector->setValue(-1, 256), runtime_error);

    delete vector;
    manager.deleteAllocator(&allocator);
}
#endif

// Test out of bounds
#ifdef DEBUG
TEST(ContainerVector, setValuesWithoutOffsetOutOfBounds) {
    VectorAllocator *allocator = manager.getOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);

    ContainerVector *vector = new ContainerVector(allocator, 256);
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
//
//// Test is null
//TEST(ContainerVector, setValueNull) {
//    VectorAllocator *allocator = manager.getOrCreateAllocator("test");
//    EXPECT_TRUE(allocator != nullptr);
//
//    ContainerVector *vector = new ContainerVector(allocator, 256);
//    for (int i = 0; i < 256; i++) {
//        if (i % 5 == 0) {
//            vector->setValueNull(i);
//        } else {
//            vector->setValue(i, i);
//        }
//    }
//    for (int i = 0; i < 256; i++) {
//        if (i % 5 == 0) {
//            EXPECT_TRUE(vector->isValueNull(i));
//        } else {
//            EXPECT_FALSE(vector->isValueNull(i));
//        }
//    }
//    delete vector;
//    manager.deleteAllocator(&allocator);
//}

// Test is copyPosition
TEST(ContainerVector, copyPositions) {
    VectorAllocatorManager manager = VectorAllocatorManager::GetInstance();
    VectorAllocator *allocator = manager.GetOrCreateAllocator("longVector");
    EXPECT_TRUE(allocator != nullptr);

    DoubleVector* doubleVector = new DoubleVector(allocator, POSITION_COUNT);
    LongVector* longVector = new LongVector(allocator, POSITION_COUNT);
    Vector** vectorAddresses = new Vector*[VECTOR_COUNT];
    vectorAddresses[0] = doubleVector;
    vectorAddresses[1] = longVector;
    ContainerVector *vector = new ContainerVector(allocator, POSITION_COUNT, vectorAddresses, VECTOR_COUNT, const_cast<VecType*>(VECTOR_TYPES));

    int *positions = new int[1];
    positions[0] = 1;
    ContainerVector* copyPostionVector = vector->CopyPositions(positions, 0, 1);

    for (int i = 0; i < copyPostionVector->GetSize(); i++) {
        EXPECT_EQ(copyPostionVector->getValue(i), vector->getValue(positions[i]));
    }

    delete vector;
    delete copyPostionVector;
    manager.DeleteAllocator(&allocator);
}

// Test is copyRegion
TEST(ContainerVector, copyRegion) {
    VectorAllocatorManager manager = VectorAllocatorManager::GetInstance();
    VectorAllocator *allocator = manager.GetOrCreateAllocator("longVector");
    EXPECT_TRUE(allocator != NULL);

    DoubleVector* doubleVector = new DoubleVector(allocator, POSITION_COUNT);
    LongVector* longVector = new LongVector(allocator, POSITION_COUNT);
    Vector** vectorAddresses = new Vector*[VECTOR_COUNT];
    vectorAddresses[0] = doubleVector;
    vectorAddresses[1] = longVector;
    ContainerVector *vector = new ContainerVector(allocator, POSITION_COUNT, vectorAddresses, VECTOR_COUNT, const_cast<VecType*>(VECTOR_TYPES));

    ContainerVector *copyRegionVector = vector->CopyRegion(0, 2);

    for (int i = 0; i < copyRegionVector->GetSize(); i++) {
        EXPECT_EQ(copyRegionVector->getValue(i), vector->getValue(i));
    }

    delete vector;
    delete copyRegionVector;
    manager.DeleteAllocator(&allocator);
}

TEST(ContainerVector, jniFreeVector) {
    VectorAllocatorManager manager = VectorAllocatorManager::GetInstance();
    VectorAllocator *allocator = manager.GetOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);

    DoubleVector* doubleVector = new DoubleVector(allocator, POSITION_COUNT);
    LongVector* longVector = new LongVector(allocator, POSITION_COUNT);
    Vector** vectorAddresses = new Vector*[VECTOR_COUNT];
    vectorAddresses[0] = doubleVector;
    vectorAddresses[1] = longVector;
    ContainerVector *vector = new ContainerVector(allocator, POSITION_COUNT, vectorAddresses, VECTOR_COUNT, const_cast<VecType*>(VECTOR_TYPES));
    Vector *vec = (Vector *) vector;
    delete vec;
}

TEST(ContainerVector, getVectorAllocator) {
    DoubleVector* doubleVector = new DoubleVector(nullptr, POSITION_COUNT);
    LongVector* longVector = new LongVector(nullptr, POSITION_COUNT);
    Vector** vectorAddresses = new Vector*[VECTOR_COUNT];
    vectorAddresses[0] = doubleVector;
    vectorAddresses[1] = longVector;
    ContainerVector *vector = new ContainerVector(nullptr, POSITION_COUNT, vectorAddresses, VECTOR_COUNT, const_cast<VecType*>(VECTOR_TYPES));

    int64_t doubleVecAddr = vector->getValue(0);
    auto doubleVec = reinterpret_cast<Vector*>(doubleVecAddr);
    auto allocator = doubleVec->GetAllocator();

    delete vector;
}
// Test is not writable

// Test multi thread

