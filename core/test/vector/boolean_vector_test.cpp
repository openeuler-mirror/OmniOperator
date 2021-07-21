//
// Created by root on 6/2/21.
//

#include "gtest/gtest.h"
#include "vector.h"
#include "vector_allocator.h"
#include "vector_allocator_manager.h"
#include "boolean_vector.h"

TEST(BooleanVector, newVector) {
    VectorAllocatorManager manager = VectorAllocatorManager::GetInstance();
    VectorAllocator *allocator = manager.GetOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);
    BooleanVector *vector = new BooleanVector(allocator, 256);
    EXPECT_EQ(vector->GetSize(), 256);
    EXPECT_EQ(vector->GetPositionOffset(), 0);
    EXPECT_EQ(vector->GetReference()->GetCapacityInBytes(), 256);
    EXPECT_EQ(vector->GetReference()->GetType(), OMNI_VEC_TYPE_BOOLEAN);
    delete vector;

    BooleanVector *vector1 = new BooleanVector(allocator, 251);
    EXPECT_EQ(vector1->GetSize(), 251);
    EXPECT_EQ(vector1->GetPositionOffset(), 0);
    EXPECT_EQ(vector1->GetReference()->GetCapacityInBytes(), 251);
    EXPECT_EQ(vector1->GetReference()->GetType(), OMNI_VEC_TYPE_BOOLEAN);
    delete vector1;

    manager.DeleteAllocator(&allocator);
    EXPECT_TRUE(allocator == nullptr);
}

TEST(BooleanVector, sliceVector) {
    VectorAllocatorManager manager = VectorAllocatorManager::GetInstance();
    VectorAllocator *allocator = manager.GetOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);

    BooleanVector *originalVector = new BooleanVector(allocator, 10);
    for (int i = 0; i < originalVector->GetSize(); i++) {
        originalVector->SetValue(i, i % 2 == 0);
    }

    int offset = 3;
    BooleanVector *slice1 = originalVector->Slice(offset, 4);
    EXPECT_EQ(slice1->GetPositionOffset(), offset);
    EXPECT_EQ(slice1->GetSize(), 4);
    EXPECT_EQ(slice1->GetReference()->GetRef(), 2);
    for (int i = 0; i < slice1->GetSize(); i++) {
        EXPECT_EQ(slice1->GetValue(i), originalVector->GetValue(i + offset));
    }

    BooleanVector *slice2 = slice1->Slice(1, 2);
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
TEST(BooleanVector, setAndGetValue) {
    VectorAllocatorManager manager = VectorAllocatorManager::GetInstance();
    VectorAllocator *allocator = manager.GetOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);

    BooleanVector *vector = new BooleanVector(allocator, 256);
    for (int i = 0; i < 256; i++) {
        vector->SetValue(i, i % 2);
    }

    for (int i = 0; i < 256; i++) {
        EXPECT_EQ(vector->GetValue(i), i % 2);
    }
    delete vector;
    manager.DeleteAllocator(&allocator);
}

// Test setValues
TEST(BooleanVector, setValues) {
    VectorAllocatorManager manager = VectorAllocatorManager::GetInstance();
    VectorAllocator *allocator = manager.GetOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);

    const int size = 5;
    bool values[size] = {1, 0, 1, 0, 0};
    bool *p = values;
    BooleanVector *boolVector1 = new BooleanVector(allocator, size);
    boolVector1->SetValues(0, p, size);
    for (int i = 0; i < size; i++) {
        EXPECT_EQ(boolVector1->GetValue(i), values[i]);
    }
    
    BooleanVector *boolVector2 = new BooleanVector(allocator, size);
    boolVector2->SetValues(1, p + 2, 3);
    for (int i = 0; i < 3; i++) {
        EXPECT_EQ(boolVector2->GetValue(i + 1), values[i + 2]);
    }

    delete boolVector1;
    delete boolVector2;
    manager.DeleteAllocator(&allocator);
}

// Test is null
TEST(BooleanVector, setValueNull) {
    VectorAllocatorManager manager = VectorAllocatorManager::GetInstance();
    VectorAllocator *allocator = manager.GetOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);

    BooleanVector *vector = new BooleanVector(allocator, 256);
    for (int i = 0; i < 256; i++) {
        if (i % 5 == 0) {
            vector->SetValueNull(i);
        } else {
            vector->SetValue(i, i);
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
TEST(BooleanVector, copyPositions) {
    VectorAllocatorManager manager = VectorAllocatorManager::GetInstance();
    VectorAllocator *allocator = manager.GetOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);

    BooleanVector *originalVector = new BooleanVector(allocator, 4);
    for (int i = 0; i < originalVector->GetSize(); i++) {
        originalVector->SetValue(i, i % 2);
    }

    int *possions = new int[2];
    possions[0] = 1;
    possions[1] = 3;
    BooleanVector* copyPostionVector = originalVector->CopyPositions(possions, 0, 2);

    for (int i = 0; i < copyPostionVector->GetSize(); i++) {
        EXPECT_EQ(copyPostionVector->GetValue(i), originalVector->GetValue(possions[i]));
    }

    delete originalVector;
    delete copyPostionVector;
    manager.DeleteAllocator(&allocator);
}

// Test is copyRegion
TEST(BooleanVector, copyRegion) {
    VectorAllocatorManager manager = VectorAllocatorManager::GetInstance();
    VectorAllocator *allocator = manager.GetOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);

    BooleanVector *originalVector = new BooleanVector(allocator, 4);
    for (int i = 0; i < 4; i++) {
        originalVector->SetValue(i, i % 2);
    }

    BooleanVector *copyRegionVector = originalVector->CopyRegion(2, 2);

    for (int i = 0; i < copyRegionVector->GetSize(); i++) {
        EXPECT_EQ(copyRegionVector->GetValue(i), originalVector->GetValue(i + 2));
    }

    delete originalVector;
    delete copyRegionVector;
    manager.DeleteAllocator(&allocator);
}

TEST(BooleanVector, jniFreeVector) {
    VectorAllocatorManager manager = VectorAllocatorManager::GetInstance();
    VectorAllocator *allocator = manager.GetOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);

   BooleanVector *oritianlVector = new BooleanVector(allocator, 256);
    Vector *vector = (Vector *) oritianlVector;
    delete vector;
}

