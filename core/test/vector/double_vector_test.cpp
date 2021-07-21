//
// Created by root on 6/2/21.
//

#include "gtest/gtest.h"
#include "vector.h"
#include "vector_allocator.h"
#include "vector_allocator_manager.h"
#include "double_vector.h"

TEST(DoubleVector, newVector) {
    VectorAllocatorManager manager = VectorAllocatorManager::GetInstance();
    VectorAllocator *allocator = manager.GetOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);
    DoubleVector *vector = new DoubleVector(allocator, 256);
    EXPECT_EQ(vector->GetSize(), 256);
    EXPECT_EQ(vector->GetPositionOffset(), 0);
    EXPECT_EQ(vector->GetReference()->GetCapacityInBytes(), 2048);
    EXPECT_EQ(vector->GetReference()->GetType(), OMNI_VEC_TYPE_DOUBLE);
    delete vector;

    manager.DeleteAllocator(&allocator);
    EXPECT_TRUE(allocator == nullptr);
}

TEST(DoubleVector, sliceVector) {
    VectorAllocatorManager manager = VectorAllocatorManager::GetInstance();
    VectorAllocator *allocator = manager.GetOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);

    DoubleVector *originalVector = new DoubleVector(allocator, 10);
    for (int i = 0; i < originalVector->GetSize(); i++) {
        originalVector->SetValue(i, (double)i / 3);
    }

    int offset = 3;
    DoubleVector *slice1 = originalVector->Slice(offset, 4);
    EXPECT_EQ(slice1->GetSize(), 4);
    EXPECT_EQ(slice1->GetReference()->GetRef(), 2);
    for (int i = 0; i < slice1->GetSize(); i++) {
        EXPECT_EQ(slice1->GetValue(i), originalVector->GetValue(i + offset));
    }

    DoubleVector *slice2 = slice1->Slice(1, 2);
    for (int i = 0; i < slice2->GetSize(); i++) {
        EXPECT_EQ(slice2->GetValue(i), originalVector->GetValue(i + offset + 1));
    }

    delete originalVector;
    EXPECT_EQ(slice1->GetReference()->GetRef(), 2);

    delete slice1;
    EXPECT_EQ(slice2->GetReference()->GetRef(), 1);
    delete slice2;

    manager.DeleteAllocator(&allocator);
}

// Test set/get
TEST(DoubleVector, setAndGetValue) {
    VectorAllocatorManager manager = VectorAllocatorManager::GetInstance();
    VectorAllocator *allocator = manager.GetOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);

    DoubleVector *vector = new DoubleVector(allocator, 256);
    for (int i = 0; i < 256; i++) {
        vector->SetValue(i, i * 2.3);
    }

    for (int i = 0; i < 256; i++) {
        EXPECT_EQ(vector->GetValue(i), i * 2.3);
    }
    delete vector;
    manager.DeleteAllocator(&allocator);
}

// Test SetValues
TEST(DoubleVector, SetValues) {
    VectorAllocatorManager manager = VectorAllocatorManager::GetInstance();
    VectorAllocator *allocator = manager.GetOrCreateAllocator("test");
    EXPECT_TRUE(allocator != NULL);

    const int size = 5;
    double values[size] = {1.1, 3.3, 4.5, 6.6, 7.7};
    double *p = values;
    DoubleVector *doubleVector1 = new DoubleVector(allocator, size);
    doubleVector1->SetValues(0, p, size);
    for (int i = 0; i < size; i++) {
        EXPECT_EQ(doubleVector1->GetValue(i), values[i]);
    }
    
    DoubleVector *doubleVector2 = new DoubleVector(allocator, size);
    doubleVector2->SetValues(1, p + 2, 3);
    for (int i = 0; i < 3; i++) {
        EXPECT_EQ(doubleVector2->GetValue(i + 1), values[i + 2]);
    }

    delete doubleVector1;
    delete doubleVector2;
    manager.DeleteAllocator(&allocator);
}

// Test out of bounds
#ifdef DEBUG
TEST(DoubleVector, SetValueOutOfBounds1) {
    VectorAllocatorManager manager = VectorAllocatorManager::getInstance();
    VectorAllocator *allocator = manager.GetOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);

    DoubleVector *vector = new DoubleVector(allocator, 256);
    EXPECT_THROW(vector->SetValue(256, 256), runtime_error);

    delete vector;
    manager.DeleteAllocator(&allocator);
}
#endif

// Test out of bounds
#ifdef DEBUG
TEST(DoubleVector, SetValueOutOfBounds2) {
    VectorAllocatorManager manager = VectorAllocatorManager::getInstance();
    VectorAllocator *allocator = manager.GetOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);

    DoubleVector *vector = new DoubleVector(allocator, 256);
    EXPECT_THROW(vector->SetValue(-1, 256), runtime_error);

    delete vector;
    manager.DeleteAllocator(&allocator);
}
#endif

// Test SetValues/get
TEST(DoubleVector, SetValuesWithoutOffset) {
    VectorAllocatorManager manager = VectorAllocatorManager::GetInstance();
    VectorAllocator *allocator = manager.GetOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);

    DoubleVector *vector = new DoubleVector(allocator, 256);
    double *value = new double[256];
    for (int i = 0; i < 256; i++) {
        value[i] = i * 2.3;
    }
    vector->SetValues(0, value, 256);
    for (int i = 0; i < 256; i++) {
        EXPECT_EQ(vector->GetValue(i), i * 2.3);
    }

    delete[] value;
    delete vector;
    manager.DeleteAllocator(&allocator);
}

// Test SetValues/get with offset
TEST(DoubleVector, SetValuesWithOffset) {
    VectorAllocatorManager manager = VectorAllocatorManager::GetInstance();
    VectorAllocator *allocator = manager.GetOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);

    DoubleVector *vector = new DoubleVector(allocator, 256);
    double *value = new double[256];
    for (int i = 0; i < 256; i++) {
        value[i] = i * 2.3;
    }
    vector->SetValues(128, &value[128], 128);
    for (int i = 128; i < 256; i++) {
        EXPECT_EQ(vector->GetValue(i), i * 2.3);
    }

    delete[] value;
    delete vector;
    manager.DeleteAllocator(&allocator);
}

// Test out of bounds
#ifdef DEBUG
TEST(DoubleVector, SetValuesWithoutOffsetOutOfBounds) {
    VectorAllocatorManager manager = VectorAllocatorManager::getInstance();
    VectorAllocator *allocator = manager.GetOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);

    DoubleVector *vector = new DoubleVector(allocator, 256);
    double *value = new double[257];
    for (int i = 0; i < 257; i++) {
        value[i] = i * 2.3;
    }

    EXPECT_THROW(vector->SetValues(0, value, 257), runtime_error);

    delete[] value;
    delete vector;
    manager.DeleteAllocator(&allocator);
}
#endif

// Test is null
TEST(DoubleVector, SetValueNull) {
    VectorAllocatorManager manager = VectorAllocatorManager::GetInstance();
    VectorAllocator *allocator = manager.GetOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);

    DoubleVector *vector = new DoubleVector(allocator, 256);
    for (int i = 0; i < 256; i++) {
        if (i % 5 == 0) {
            vector->SetValueNull(i);
        } else {
            vector->SetValue(i, i * 2.3);
        }
    }
    for (int i = 0; i < 256; i++) {
        if (i % 5 == 0) {
            EXPECT_TRUE(vector->IsValueNull(i));
        } else {
            EXPECT_FALSE(vector->IsValueNull(i));
        }
    }
    delete vector;
    manager.DeleteAllocator(&allocator);
}

// Test is copyPosition
TEST(DoubleVector, copyPositions) {
    VectorAllocatorManager manager = VectorAllocatorManager::GetInstance();
    VectorAllocator *allocator = manager.GetOrCreateAllocator("doubleVector");
    EXPECT_TRUE(allocator != nullptr);

    DoubleVector *originalVector = new DoubleVector(allocator, 4);
    for (int i = 0; i < originalVector->GetSize(); i++) {
        originalVector->SetValue(i, i * 2.3);
    }

    int *possions = new int[2];
    possions[0] = 1;
    possions[1] = 3;
    DoubleVector* copyPostionVector = originalVector->CopyPositions(possions, 0, 2);

    for (int i = 0; i < copyPostionVector->GetSize(); i++) {
        EXPECT_EQ(copyPostionVector->GetValue(i), originalVector->GetValue(possions[i]));
    }

    delete originalVector;
    delete copyPostionVector;
    manager.DeleteAllocator(&allocator);
}

// Test is copyRegion
TEST(DoubleVector, copyRegion) {
    VectorAllocatorManager manager = VectorAllocatorManager::GetInstance();
    VectorAllocator *allocator = manager.GetOrCreateAllocator("doubleVector");
    EXPECT_TRUE(allocator != NULL);

    DoubleVector *originalVector = new DoubleVector(allocator, 4);
    for (int i = 0; i < 4; i++) {
        originalVector->SetValue(i, i * 3.3);
    }

    DoubleVector *copyRegionVector = originalVector->CopyRegion(2, 2);

    for (int i = 0; i < copyRegionVector->GetSize(); i++) {
        EXPECT_EQ(copyRegionVector->GetValue(i), originalVector->GetValue(i + 2));
    }

    delete originalVector;
    delete copyRegionVector;
    manager.DeleteAllocator(&allocator);
}

TEST(DoubleVector, jniFreeVector) {
    VectorAllocatorManager manager = VectorAllocatorManager::GetInstance();
    VectorAllocator *allocator = manager.GetOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);
    
    DoubleVector *oritianlVector = new DoubleVector(allocator, 256);
    Vector *vector = (Vector *) oritianlVector;
    delete vector;
}

