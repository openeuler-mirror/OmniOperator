//
// Created by root on 6/2/21.
//

#include <sstream>
#include "gtest/gtest.h"
#include "vector.h"
#include "vector_allocator.h"
#include "vector_allocator_manager.h"
#include "varchar_vector.h"

TEST(VarcharVector, newVector) {
    VectorAllocatorManager manager = VectorAllocatorManager::getInstance();
    VectorAllocator *allocator = manager.getOrCreateAllocator("varchar");
    EXPECT_TRUE(allocator != nullptr);
    VarcharVector *vector = new VarcharVector(allocator,1024, 256);
    EXPECT_EQ(vector->getSize(), 256);
    EXPECT_EQ(vector->getPositionOffset(), 0);
    EXPECT_EQ(vector->getReference()->getCapacityInBytes(), 1024);
    EXPECT_EQ(vector->getReference()->getType(), OMNI_VEC_TYPE_VARCHAR);
    delete vector;

    manager.deleteAllocator(&allocator);
    EXPECT_TRUE(allocator == nullptr);
}

TEST(VarcharVector, sliceVector) {
    VectorAllocatorManager manager = VectorAllocatorManager::getInstance();
    VectorAllocator *allocator = manager.getOrCreateAllocator("varchar");
    EXPECT_TRUE(allocator != nullptr);

    int size = 10;
    VarcharVector *vector = new VarcharVector(allocator, 1024, size);
    std::string s = "testvarchar";
    for (int i = 0; i < size; i++) {
        std::string str(s, 0, i);
        str.append(to_string(i));
        vector->setValue(i, str.c_str(), str.length());
    }

    int offset = 3;
    VarcharVector *sliceVector1 = vector->slice(offset, 4);
    EXPECT_EQ(sliceVector1->getPositionOffset(), offset);
    EXPECT_EQ(sliceVector1->getSize(), 4);
    EXPECT_EQ(sliceVector1->getReference()->getRef(), 2);

    for (int i = 0; i < sliceVector1->getSize(); i++) {
        std::string str(s, 0, i + 3);
        str.append(to_string(i + 3));
        char *actualChar;
        int len = sliceVector1->getValue(i, &actualChar);
        std::string actualStr(actualChar, 0, len);
        EXPECT_EQ(actualStr, str);
        delete[] actualChar;
    }

    VarcharVector *sliceVector2 = sliceVector1->slice(1, 2);
    EXPECT_EQ(sliceVector2->getPositionOffset(), offset + 1);
    EXPECT_EQ(sliceVector2->getSize(), 2);
    EXPECT_EQ(sliceVector2->getReference()->getRef(), 3);

    for (int i = 0; i < sliceVector2->getSize(); i++) {
        std::string str(s, 0, i + 4);
        str.append(to_string(i + 4));
        char *actualChar;
        int len = sliceVector2->getValue(i, &actualChar);
        std::string actualStr(actualChar, 0, len);
        EXPECT_EQ(actualStr, str);
        delete[] actualChar;
    }

    delete vector;
    EXPECT_EQ(sliceVector1->getReference()->getRef(), 2);

    delete sliceVector1;
    EXPECT_EQ(sliceVector2->getReference()->getRef(), 1);

    delete sliceVector2;
    manager.deleteAllocator(&allocator);
}

// Test set/get
TEST(VarcharVector, setAndGetValue) {
    VectorAllocatorManager manager = VectorAllocatorManager::getInstance();
    VectorAllocator *allocator = manager.getOrCreateAllocator("varchar");
    EXPECT_TRUE(allocator != nullptr);

    VarcharVector *vector = new VarcharVector(allocator, 1024, 4);
    std::string s = "test";
    for (int i = 0; i < 4; i++) {
        std::string str(s, 0, i);
        str.append(to_string(i));
        vector->setValue(i, str.c_str(), str.length());
    }

    for (int i = 0; i < 4; i++) {
        std::string str(s, 0, i);
        str.append(to_string(i));
        char *actualChar;
        int len = vector->getValue(i, &actualChar);
        std::string actualStr(actualChar, 0, len);
        EXPECT_EQ(actualStr, str);
        delete[] actualChar;
    }

    delete vector;
    manager.deleteAllocator(&allocator);
}

// Test is null
TEST(VarcharVector, setValueNull) {
    VectorAllocatorManager manager = VectorAllocatorManager::getInstance();
    VectorAllocator *allocator = manager.getOrCreateAllocator("varchar");
    EXPECT_TRUE(allocator != nullptr);

    VarcharVector *vector = new VarcharVector(allocator,1024, 256);
    std::string s = "test";
    for (int i = 0; i < 256; i++) {
        if (i % 5 == 0) {
            vector->setValueNull(i);
        } else {
            vector->setValue(i,s.c_str(), s.length());
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
TEST(VarcharVector, copyPositions) {
    VectorAllocatorManager manager = VectorAllocatorManager::getInstance();
    VectorAllocator *allocator = manager.getOrCreateAllocator("varchar");
    EXPECT_TRUE(allocator != nullptr);

    VarcharVector *vector = new VarcharVector(allocator, 1024, 4);
    std::string s = "test";
    for (int i = 0; i < 4; i++) {
        std::string str(s, 0, i);
        str.append(to_string(i));
        vector->setValue(i, str.c_str(), str.length());
    }

    int *positions = new int[2];
    positions[0] = 1;
    positions[1] = 3;
    VarcharVector* copyPostionVector = vector->copyPositions(positions, 0, 2);

    for (int i = 0; i < copyPostionVector->getSize(); i++) {
        char *expectedChar;
        int len = vector->getValue(positions[i], &expectedChar);
        std::string expectedStr(expectedChar, 0, len);

        char *actualChar;
        int len1 = copyPostionVector->getValue(i, &actualChar);
        std::string actualStr(actualChar, 0, len1);
        EXPECT_EQ(actualStr, expectedStr);
        delete[] actualChar;
        delete[] actualChar;
    }

    delete vector;
    manager.deleteAllocator(&allocator);
}

// Test is copyRegion
TEST(VarcharVector, copyRegion) {
    VectorAllocatorManager manager = VectorAllocatorManager::getInstance();
    VectorAllocator *allocator = manager.getOrCreateAllocator("varchar");
    EXPECT_TRUE(allocator != nullptr);

    VarcharVector *vector = new VarcharVector(allocator, 1024, 4);
    std::string s = "test";
    for (int i = 0; i < 4; i++) {
        std::string str(s, 0, i);
        str.append(to_string(i));
        vector->setValue(i, str.c_str(), str.length());
    }

    VarcharVector *copyRegionVector = vector->copyRegion(2, 2);

    for (int i = 0; i < copyRegionVector->getSize(); i++) {
        char *expectedChar;
        int len = vector->getValue(i + 2, &expectedChar);
        std::string expectedStr(expectedChar, 0, len);

        char *actualChar;
        int len1 = copyRegionVector->getValue(i, &actualChar);
        std::string actualStr(actualChar, 0, len1);
        EXPECT_EQ(actualStr, expectedStr);
        delete[] actualChar;
        delete[] expectedChar;
    }

    delete vector;
    manager.deleteAllocator(&allocator);
}

TEST(VarcharVector, jniFreeVector) {
    VectorAllocatorManager manager = VectorAllocatorManager::getInstance();
    VectorAllocator *allocator = manager.getOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);

   VarcharVector *oritianlVector = new VarcharVector(allocator, 1024, 256);
    Vector *vector = (Vector *) oritianlVector;
    delete vector;
}