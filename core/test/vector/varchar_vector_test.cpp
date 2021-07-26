//
// Created by root on 6/2/21.
//

#include <sstream>
#include "gtest/gtest.h"
#include "vector.h"
#include "vector_allocator.h"
#include "vector_allocator_manager.h"
#include "varchar_vector.h"

using namespace omniruntime::vec;

TEST(VarcharVector, newVector) {
    VectorAllocatorManager manager = VectorAllocatorManager::GetInstance();
    VectorAllocator *allocator = manager.GetOrCreateAllocator("varchar");
    EXPECT_TRUE(allocator != nullptr);
    VarcharVector *vector = new VarcharVector(allocator,1024, 256);
    EXPECT_EQ(vector->GetSize(), 256);
    EXPECT_EQ(vector->GetPositionOffset(), 0);
    EXPECT_EQ(vector->GetReference()->GetCapacityInBytes(), 1024);
    EXPECT_EQ(vector->GetReference()->GetType(), OMNI_VEC_TYPE_VARCHAR);
    delete vector;

    manager.DeleteAllocator(&allocator);
    EXPECT_TRUE(allocator == nullptr);
}

TEST(VarcharVector, sliceVector) {
    VectorAllocatorManager manager = VectorAllocatorManager::GetInstance();
    VectorAllocator *allocator = manager.GetOrCreateAllocator("varchar");
    EXPECT_TRUE(allocator != nullptr);

    int size = 10;
    VarcharVector *vector = new VarcharVector(allocator, 1024, size);
    std::string s = "testvarchar";
    for (int i = 0; i < size; i++) {
        std::string str(s, 0, i);
        str.append(std::to_string(i));
        vector->SetValue(i, str.c_str(), str.length());
    }

    int offset = 3;
    VarcharVector *sliceVector1 = vector->Slice(offset, 4);
    EXPECT_EQ(sliceVector1->GetPositionOffset(), offset);
    EXPECT_EQ(sliceVector1->GetSize(), 4);
    EXPECT_EQ(sliceVector1->GetReference()->GetRef(), 2);

    for (int i = 0; i < sliceVector1->GetSize(); i++) {
        std::string str(s, 0, i + 3);
        str.append(std::to_string(i + 3));
        char *actualChar;
        int len = sliceVector1->GetValue(i, &actualChar);
        std::string actualStr(actualChar, 0, len);
        EXPECT_EQ(actualStr, str);
        delete[] actualChar;
    }

    VarcharVector *sliceVector2 = sliceVector1->Slice(1, 2);
    EXPECT_EQ(sliceVector2->GetPositionOffset(), offset + 1);
    EXPECT_EQ(sliceVector2->GetSize(), 2);
    EXPECT_EQ(sliceVector2->GetReference()->GetRef(), 3);

    for (int i = 0; i < sliceVector2->GetSize(); i++) {
        std::string str(s, 0, i + 4);
        str.append(std::to_string(i + 4));
        char *actualChar;
        int len = sliceVector2->GetValue(i, &actualChar);
        std::string actualStr(actualChar, 0, len);
        EXPECT_EQ(actualStr, str);
        delete[] actualChar;
    }

    delete vector;
    EXPECT_EQ(sliceVector1->GetReference()->GetRef(), 2);

    delete sliceVector1;
    EXPECT_EQ(sliceVector2->GetReference()->GetRef(), 1);

    delete sliceVector2;
    manager.DeleteAllocator(&allocator);
}

// Test set/get
TEST(VarcharVector, setAndGetValue) {
    VectorAllocatorManager manager = VectorAllocatorManager::GetInstance();
    VectorAllocator *allocator = manager.GetOrCreateAllocator("varchar");
    EXPECT_TRUE(allocator != nullptr);

    VarcharVector *vector = new VarcharVector(allocator, 1024, 4);
    std::string s = "test";
    for (int i = 0; i < 4; i++) {
        std::string str(s, 0, i);
        str.append(std::to_string(i));
        vector->SetValue(i, str.c_str(), str.length());
    }

    for (int i = 0; i < 4; i++) {
        std::string str(s, 0, i);
        str.append(std::to_string(i));
        char *actualChar;
        int len = vector->GetValue(i, &actualChar);
        std::string actualStr(actualChar, 0, len);
        EXPECT_EQ(actualStr, str);
        delete[] actualChar;
    }

    delete vector;
    manager.DeleteAllocator(&allocator);
}

// Test is null
TEST(VarcharVector, SetValueNull) {
    VectorAllocatorManager manager = VectorAllocatorManager::GetInstance();
    VectorAllocator *allocator = manager.GetOrCreateAllocator("varchar");
    EXPECT_TRUE(allocator != nullptr);

    VarcharVector *vector = new VarcharVector(allocator,1024, 256);
    std::string s = "test";
    for (int i = 0; i < 256; i++) {
        if (i % 5 == 0) {
            vector->SetValueNull(i);
        } else {
            vector->SetValue(i,s.c_str(), s.length());
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
TEST(VarcharVector, CopyPositions) {
    VectorAllocatorManager manager = VectorAllocatorManager::GetInstance();
    VectorAllocator *allocator = manager.GetOrCreateAllocator("varchar");
    EXPECT_TRUE(allocator != nullptr);

    VarcharVector *vector = new VarcharVector(allocator, 1024, 4);
    std::string s = "test";
    for (int i = 0; i < 4; i++) {
        std::string str(s, 0, i);
        str.append(std::to_string(i));
        vector->SetValue(i, str.c_str(), str.length());
    }

    int *positions = new int[2];
    positions[0] = 1;
    positions[1] = 3;
    VarcharVector* copyPostionVector = vector->CopyPositions(positions, 0, 2);

    for (int i = 0; i < copyPostionVector->GetSize(); i++) {
        char *expectedChar;
        int len = vector->GetValue(positions[i], &expectedChar);
        std::string expectedStr(expectedChar, 0, len);

        char *actualChar;
        int len1 = copyPostionVector->GetValue(i, &actualChar);
        std::string actualStr(actualChar, 0, len1);
        EXPECT_EQ(actualStr, expectedStr);
        delete[] actualChar;
        delete[] actualChar;
    }

    delete vector;
    manager.DeleteAllocator(&allocator);
}

// Test is CopyRegion
TEST(VarcharVector, CopyRegion) {
    VectorAllocatorManager manager = VectorAllocatorManager::GetInstance();
    VectorAllocator *allocator = manager.GetOrCreateAllocator("varchar");
    EXPECT_TRUE(allocator != nullptr);

    VarcharVector *vector = new VarcharVector(allocator, 1024, 4);
    std::string s = "test";
    for (int i = 0; i < 4; i++) {
        std::string str(s, 0, i);
        str.append(std::to_string(i));
        vector->SetValue(i, str.c_str(), str.length());
    }

    VarcharVector *copyRegionVector = vector->CopyRegion(2, 2);

    for (int i = 0; i < copyRegionVector->GetSize(); i++) {
        char *expectedChar;
        int len = vector->GetValue(i + 2, &expectedChar);
        std::string expectedStr(expectedChar, 0, len);

        char *actualChar;
        int len1 = copyRegionVector->GetValue(i, &actualChar);
        std::string actualStr(actualChar, 0, len1);
        EXPECT_EQ(actualStr, expectedStr);
        delete[] actualChar;
        delete[] expectedChar;
    }

    delete vector;
    manager.DeleteAllocator(&allocator);
}

TEST(VarcharVector, jniFreeVector) {
    VectorAllocatorManager manager = VectorAllocatorManager::GetInstance();
    VectorAllocator *allocator = manager.GetOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);

   VarcharVector *oritianlVector = new VarcharVector(allocator, 1024, 256);
    Vector *vector = (Vector *) oritianlVector;
    delete vector;
}