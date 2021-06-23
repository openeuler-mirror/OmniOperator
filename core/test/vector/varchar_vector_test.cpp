//
// Created by root on 6/2/21.
//

#include <sstream>
#include "gtest/gtest.h"
#include "vector.h"
#include "vector_allocator.h"
#include "vector_allocator_manager.h"
#include "int_vector.h"
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
        vector->setValue(i, const_cast<char *>(str.c_str()), str.length());
    }

    VarcharVector *sliceVector = vector->slice(3, 4);
    EXPECT_EQ(sliceVector->getPositionOffset(), 3);
    EXPECT_EQ(sliceVector->getSize(), 4);
    EXPECT_EQ(sliceVector->getReference()->getRef(), 2);

    for (int i = 3; i < 6; i++) {
        std::string str(s, 0, i);
        str.append(to_string(i));
        char *actualChar;
        int len = vector->getValue(i, &actualChar);
        std::string actualStr(actualChar, 0, len);
        EXPECT_EQ(actualStr, str);
        delete[] actualChar;
    }

    delete vector;
    EXPECT_EQ(sliceVector->getReference()->getRef(), 1);

    delete sliceVector;

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
        vector->setValue(i, const_cast<char *>(str.c_str()), str.length());
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
        vector->setValue(i, const_cast<char *>(str.c_str()), str.length());
    }

    int *possions = new int[2];
    possions[0] = 1;
    possions[1] = 3;
    VarcharVector* copyPostionVector = vector->copyPositions(possions, 0, 2);

    for (int i = 0; i < copyPostionVector->getSize(); i++) {
        char *actualChar;
        int len = vector->getValue(possions[i], &actualChar);
        std::string actualStr(actualChar, 0, len);

        char *actualChar1;
        int len1 = copyPostionVector->getValue(i, &actualChar1);
        std::string actualStr1(actualChar1, 0, len1);
        EXPECT_EQ(actualStr, actualStr1);
        delete[] actualChar;
        delete[] actualChar1;
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
        vector->setValue(i, const_cast<char *>(str.c_str()), str.length());
    }

    VarcharVector *copyRegionVector = vector->copyRegion(2, 2);

    for (int i = 0; i < copyRegionVector->getSize(); i++) {
        char *actualChar;
        int len = vector->getValue(i + 2, &actualChar);
        std::string actualStr(actualChar, 0, len);

        char *actualChar1;
        int len1 = copyRegionVector->getValue(i, &actualChar1);
        std::string actualStr1(actualChar1, 0, len1);
        EXPECT_EQ(actualStr, actualStr1);
        delete[] actualChar;
        delete[] actualChar1;
    }

    delete vector;
    manager.deleteAllocator(&allocator);
}