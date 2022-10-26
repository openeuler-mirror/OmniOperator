/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

#include <sstream>
#include <gtest/gtest.h>
#include "vector_common.h"
#include "util/test_util.h"

using namespace omniruntime::vec;
using namespace TestUtil;

namespace VacharVectorTest {
TEST(VarcharVector, newVector)
{
    VectorAllocator *allocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("VarcharVector_newVector");
    EXPECT_TRUE(allocator != nullptr);
    VarcharVector *vector = new VarcharVector(allocator, 1024, 256);
    EXPECT_EQ(vector->GetSize(), 256);
    EXPECT_EQ(vector->GetPositionOffset(), 0);
    EXPECT_EQ(vector->GetCapacityInBytes(), 1024);
    EXPECT_EQ(vector->GetTypeId(), OMNI_VARCHAR);
    delete vector;

    delete allocator;
}

TEST(VarcharVector, sliceVector)
{
    VectorAllocator *allocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("VarcharVector_sliceVector");
    EXPECT_TRUE(allocator != nullptr);

    int size = 10;
    VarcharVector *vector = new VarcharVector(allocator, 1024, size);
    std::string s = "testvarchar";
    for (int i = 0; i < size; i++) {
        std::string str(s, 0, i);
        str.append(std::to_string(i));
        vector->SetValue(i, reinterpret_cast<const uint8_t *>(str.c_str()), str.length());
    }

    int offset = 3;
    VarcharVector *sliceVector1 = vector->Slice(offset, 4);
    EXPECT_EQ(sliceVector1->GetPositionOffset(), offset);
    EXPECT_EQ(sliceVector1->GetSize(), 4);
    EXPECT_EQ(sliceVector1->GetReference(), 2);

    for (int i = 0; i < sliceVector1->GetSize(); i++) {
        std::string str(s, 0, i + 3);
        str.append(std::to_string(i + 3));
        uint8_t *actualChar = nullptr;
        int len = sliceVector1->GetValue(i, &actualChar);
        std::string actualStr(reinterpret_cast<char *>(actualChar), len);
        EXPECT_EQ(actualStr, str);
    }

    VarcharVector *sliceVector2 = sliceVector1->Slice(1, 2);
    EXPECT_EQ(sliceVector2->GetPositionOffset(), offset + 1);
    EXPECT_EQ(sliceVector2->GetSize(), 2);
    EXPECT_EQ(sliceVector2->GetReference(), 3);

    for (int i = 0; i < sliceVector2->GetSize(); i++) {
        std::string str(s, 0, i + 4);
        str.append(std::to_string(i + 4));
        uint8_t *actualChar = nullptr;
        int len = sliceVector2->GetValue(i, &actualChar);
        std::string actualStr(reinterpret_cast<char *>(actualChar), len);
        EXPECT_EQ(actualStr, str);
    }

    delete vector;
    EXPECT_EQ(sliceVector1->GetReference(), 2);

    delete sliceVector1;
    EXPECT_EQ(sliceVector2->GetReference(), 1);

    delete sliceVector2;
    delete allocator;
}

// Test set/get
TEST(VarcharVector, setAndGetValue)
{
    VectorAllocator *allocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("VarcharVector_setAndGetValue");
    EXPECT_TRUE(allocator != nullptr);

    VarcharVector *vector = new VarcharVector(allocator, 1024, 4);
    std::string s = "test";
    for (int i = 0; i < 4; i++) {
        std::string str(s, 0, i);
        str.append(std::to_string(i));
        vector->SetValue(i, reinterpret_cast<const uint8_t *>(str.c_str()), str.length());
    }

    for (int i = 0; i < 4; i++) {
        std::string str(s, 0, i);
        str.append(std::to_string(i));
        uint8_t *actualChar = nullptr;
        int len = vector->GetValue(i, &actualChar);
        std::string actualStr(reinterpret_cast<char *>(actualChar), len);
        EXPECT_EQ(actualStr, str);
    }

    delete vector;
    delete allocator;
}

// Test is null
TEST(VarcharVector, setValueNull)
{
    VectorAllocator *allocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("VarcharVector_setValueNull");
    EXPECT_TRUE(allocator != nullptr);

    VarcharVector *vector = new VarcharVector(allocator, 1024, 256);
    for (int i = 0; i < 256; i++) {
        if (i % 5 == 0) {
            vector->SetValueNull(i);
        } else {
            std::string str = std::to_string(i);
            vector->SetValue(i, reinterpret_cast<const uint8_t *>(str.c_str()), str.length());
        }
    }

    for (int i = 0; i < 256; i++) {
        if (i % 5 == 0) {
            EXPECT_TRUE(vector->IsValueNull(i));
        } else {
            uint8_t *actual = nullptr;
            int len = vector->GetValue(i, &actual);
            std::string actualStr(reinterpret_cast<char *>(actual), len);
            EXPECT_EQ(actualStr, std::to_string(i));
        }
    }
    delete vector;
    delete allocator;
}

// Test is copyPosition
TEST(VarcharVector, copyPositions)
{
    VectorAllocator *allocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("VarcharVector_copyPositions");
    EXPECT_TRUE(allocator != nullptr);

    VarcharVector *vector = new VarcharVector(allocator, 1024, 4);
    std::string s = "test";
    for (int i = 0; i < 4; i++) {
        std::string str(s, 0, i);
        str.append(std::to_string(i));
        vector->SetValue(i, reinterpret_cast<const uint8_t *>(str.c_str()), str.length());
    }

    int *positions = new int[2];
    positions[0] = 1;
    positions[1] = 3;
    VarcharVector *copyPostionVector = vector->CopyPositions(positions, 0, 2);

    for (int i = 0; i < copyPostionVector->GetSize(); i++) {
        uint8_t *expectedChar = nullptr;
        int len = vector->GetValue(positions[i], &expectedChar);
        std::string expectedStr(reinterpret_cast<char *>(expectedChar), len);

        uint8_t *actualChar = nullptr;
        int len1 = copyPostionVector->GetValue(i, &actualChar);
        std::string actualStr(reinterpret_cast<char *>(actualChar), len1);
        EXPECT_EQ(actualStr, expectedStr);
    }

    delete vector;
    delete copyPostionVector;
    delete[] positions;
    delete allocator;
}

// Test is CopyRegion
TEST(VarcharVector, copyRegion)
{
    VectorAllocator *allocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("VarcharVector_copyRegion");
    EXPECT_TRUE(allocator != nullptr);

    VarcharVector *vector = new VarcharVector(allocator, 1024, 4);
    std::string s = "test";
    for (int i = 0; i < 4; i++) {
        std::string str(s, 0, i);
        str.append(std::to_string(i));
        vector->SetValue(i, reinterpret_cast<const uint8_t *>(str.c_str()), str.length());
    }

    VarcharVector *copyRegionVector = vector->CopyRegion(2, 2);

    for (int i = 0; i < copyRegionVector->GetSize(); i++) {
        uint8_t *expectedChar = nullptr;
        int len = vector->GetValue(i + 2, &expectedChar);
        std::string expectedStr(reinterpret_cast<char *>(expectedChar), len);

        uint8_t *actualChar = nullptr;
        int len1 = copyRegionVector->GetValue(i, &actualChar);
        std::string actualStr(reinterpret_cast<char *>(actualChar), len1);
        EXPECT_EQ(actualStr, expectedStr);
    }

    delete vector;
    delete copyRegionVector;
    delete allocator;
}

TEST(VarcharVector, emptyString)
{
    VectorAllocator *allocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("VarcharVector_emptyString");
    EXPECT_TRUE(allocator != nullptr);

    std::vector<std::string> data = { "e", "abc", "", "hg", "" };
    int size = 5;
    VarcharVector *original = new VarcharVector(allocator, 1024, size);
    for (int i = 0; i < size; i++) {
        original->SetValue(i, reinterpret_cast<const uint8_t *>(data[i].c_str()), data[i].length());
    }

    for (int i = 0; i < size; i++) {
        uint8_t *actualChar = nullptr;
        int len = original->GetValue(i, &actualChar);
        std::string actualStr(actualChar, actualChar + len);
        EXPECT_EQ(actualStr, data[i]);
    }

    delete original;
    delete allocator;
}

TEST(VarcharVector, appendVector)
{
    VectorAllocator *allocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("VarcharVector_appendVector");
    EXPECT_TRUE(allocator != nullptr);

    auto *src1 = new VarcharVector(allocator, 1024, 5);
    auto *src2 = new VarcharVector(allocator, 1024, 5);
    for (int i = 0; i < 5; i++) {
        std::string str = std::to_string(i + 1);
        src1->SetValue(i, reinterpret_cast<const uint8_t *>(str.c_str()), str.length());
        std::string str1 = std::to_string(i + 6);
        src2->SetValue(i, reinterpret_cast<const uint8_t *>(str1.c_str()), str1.length());
    }

    auto *appended = new VarcharVector(allocator, 1024, 13);
    appended->Append(src1, 0, 5);
    appended->Append(src2, 5, 5);
    int32_t ids[3] = {1, 2, 3};
    auto *dictionaryVector = new DictionaryVector(src1, ids, 3);
    appended->Append(dictionaryVector, 10, 3);

    for (int i = 0; i < 10; i++) {
        uint8_t *actualChar = nullptr;
        int32_t len = appended->GetValue(i, &actualChar);
        std::string result(actualChar, actualChar + len);
        EXPECT_EQ(result, std::to_string(i + 1));
    }

    for (int i = 10; i < 13; i++) {
        uint8_t *actualChar = nullptr;
        int32_t len = appended->GetValue(i, &actualChar);
        uint8_t *actualChar1 = nullptr;
        int32_t len1 = dictionaryVector->GetVarchar(i % 10, &actualChar1);
        std::string result(actualChar, actualChar + len);
        std::string result1(actualChar1, actualChar1 + len1);
        EXPECT_EQ(result, result1);
    }

    delete src1;
    delete src2;
    delete dictionaryVector;
    delete appended;
    delete allocator;
}

TEST(VarcharVector, setValueExpand)
{
    VectorAllocator *allocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("VarcharVector_setValueExpand");
    EXPECT_TRUE(allocator != nullptr);

    // specify initial capacity expansion
    int32_t rows = 4;
    auto *vector = new VarcharVector(allocator, 1, rows);
    std::string s = "test";
    for (int i = 0; i < rows; i++) {
        std::string str(s, 0, i);
        str.append(std::to_string(i));
        vector->SetValue(i, reinterpret_cast<const uint8_t *>(str.c_str()), str.length());
    }

    const int32_t expectedExpandedCapacity = 16;
    EXPECT_EQ(vector->GetCapacityInBytes(), expectedExpandedCapacity);

    for (int i = 0; i < rows; i++) {
        std::string str(s, 0, i);
        str.append(std::to_string(i));
        uint8_t *actualChar = nullptr;
        int len = vector->GetValue(i, &actualChar);
        std::string actualStr(reinterpret_cast<char *>(actualChar), len);
        EXPECT_EQ(actualStr, str);
    }

    // no capacity specified when created
    rows = 8000;
    auto *vector1 = new VarcharVector(allocator, rows);
    for (int i = 0; i < rows; i++) {
        std::string str(s, 0, s.length());
        str.append(std::to_string(i));
        vector1->SetValue(i, reinterpret_cast<const uint8_t *>(str.c_str()), str.length());
    }

    // init capacity is 32K, expansion to 64k at a time
    const int32_t expectedExpandedCapacity1 = 32 * 1024 * 2;
    EXPECT_EQ(vector1->GetCapacityInBytes(), expectedExpandedCapacity1);

    for (int i = 0; i < rows; i++) {
        std::string str(s, 0, s.length());
        str.append(std::to_string(i));
        uint8_t *actualChar = nullptr;
        int len = vector1->GetValue(i, &actualChar);
        std::string actualStr(reinterpret_cast<char *>(actualChar), len);
        EXPECT_EQ(actualStr, str);
    }

    // initial capacity is zero
    auto *iniZeroCapacityVector = new VarcharVector(allocator, 0, 1);
    std::string emptyStr = "";
    iniZeroCapacityVector->SetValue(0, reinterpret_cast<const uint8_t *>(emptyStr.c_str()), 0);
    EXPECT_EQ(iniZeroCapacityVector->GetCapacityInBytes(), 0);

    iniZeroCapacityVector->SetValue(0, reinterpret_cast<const uint8_t *>(s.c_str()), s.length());
    EXPECT_EQ(iniZeroCapacityVector->GetCapacityInBytes(), 32 * 1024);
    uint8_t *actualChar = nullptr;
    int len = iniZeroCapacityVector->GetValue(0, &actualChar);
    std::string actualStr(reinterpret_cast<char *>(actualChar), len);
    EXPECT_EQ(actualStr, s);

    delete vector;
    delete vector1;
    delete iniZeroCapacityVector;
    delete allocator;
}

TEST(VarcharVector, appendExpand)
{
    VectorAllocator *allocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("VarcharVec_appendExpand");
    EXPECT_TRUE(allocator != nullptr);

    auto *src1 = new VarcharVector(allocator, 1024, 5);
    auto *src2 = new VarcharVector(allocator, 1024, 5);

    for (int i = 0; i < 5; i++) {
        std::string str = std::to_string(i + 1);
        src1->SetValue(i, reinterpret_cast<const uint8_t *>(str.c_str()), str.length());
        std::string str1 = std::to_string(i + 6);
        src2->SetValue(i, reinterpret_cast<const uint8_t *>(str1.c_str()), str1.length());
    }

    auto *appended = new VarcharVector(allocator, 10, 10);
    appended->Append(src1, 0, 5);
    appended->Append(src2, 5, 5);

    const int32_t expectedExpandCapacity = 20;
    EXPECT_EQ(appended->GetCapacityInBytes(), expectedExpandCapacity);
    for (int i = 0; i < 10; i++) {
        uint8_t *actualChar = nullptr;
        int32_t len = appended->GetValue(i, &actualChar);
        std::string result(actualChar, actualChar + len);
        EXPECT_EQ(result, std::to_string(i + 1));
    }

    delete src1;
    delete src2;
    delete appended;
    delete allocator;
}

TEST(VarcharVector, testNullFlagWithSet)
{
    VectorAllocator *allocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("testNullFlagWithSet");
    // no null value
    auto *noNull = new VarcharVector(allocator, 10);
    EXPECT_FALSE(noNull->MayHaveNull());
    EXPECT_EQ(noNull->GetNullCount(), 0);
    delete noNull;

    // has null value
    std::vector<std::string> values = { "1", "", "3", "", "5", "", "7", "", "9", "" };
    std::vector<bool> nulls = { false, true, false, true, false, true, false, true, false, true };
    auto *hasNulls = CreateVarcharVector(values, nulls);
    EXPECT_TRUE(hasNulls->MayHaveNull());
    EXPECT_EQ(hasNulls->GetNullCount(), 5);
    auto *expected = CreateVarcharVector(values, nulls);
    ColumnMatch(hasNulls, expected);
    delete expected;
    delete hasNulls;

    delete allocator;
}

TEST(VarcharVector, testNullFlagWithCopyPosition)
{
    VectorAllocator *allocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("testNullFlagWithCopyPosition");
    // has null value
    std::vector<std::string> values = { "1", "2", "", "", "5", "", "7", "", "9", "" };
    std::vector<bool> nulls = { false, false, true, true, false, true, false, true, false, true };
    auto *hasNulls = CreateVarcharVector(values, nulls);
    EXPECT_TRUE(hasNulls->MayHaveNull());
    EXPECT_EQ(hasNulls->GetNullCount(), 5);

    std::vector<int32_t> positions = { 0, 1 };
    VarcharVector *copyPositionNoNull = hasNulls->CopyPositions(positions.data(), 0, 2);
    EXPECT_FALSE(copyPositionNoNull->MayHaveNull());
    EXPECT_EQ(copyPositionNoNull->GetNullCount(), 0);

    std::vector<std::string> expectedValue1 = { "1", "2" };
    std::vector<bool> expectedNull1 = { false, false };
    VarcharVector *expected1 = CreateVarcharVector(expectedValue1, expectedNull1);
    ColumnMatch(copyPositionNoNull, expected1);
    delete expected1;
    delete copyPositionNoNull;

    positions = { 1, 2, 3, 4 };
    VarcharVector *copyPositionHasNull = hasNulls->CopyPositions(positions.data(), 0, 4);
    EXPECT_TRUE(copyPositionHasNull->MayHaveNull());
    EXPECT_EQ(copyPositionHasNull->GetNullCount(), 2);
    std::vector<std::string> expectedValue2 = { "1", "2", "", "" };
    std::vector<bool> expectedNull2 = { false, false, true, true };
    VarcharVector *expected2 = CreateVarcharVector(expectedValue2, expectedNull2);
    ColumnMatch(copyPositionHasNull, expected2);
    delete expected2;
    delete copyPositionHasNull;

    delete hasNulls;
    delete allocator;
}

TEST(VarcharVector, testNullFlagWithSlice)
{
    VectorAllocator *allocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("testNullFlagWithSlice");
    // has null value
    std::vector<std::string> values = { "1", "2", "", "", "5", "", "7", "", "9", "" };
    std::vector<bool> nulls = { false, false, true, true, false, true, false, true, false, true };
    auto *hasNulls = CreateVarcharVector(values, nulls);
    EXPECT_TRUE(hasNulls->MayHaveNull());
    EXPECT_EQ(hasNulls->GetNullCount(), 5);

    VarcharVector *sliceNoNull = hasNulls->Slice(0, 1);
    EXPECT_TRUE(sliceNoNull->MayHaveNull());
    EXPECT_EQ(sliceNoNull->GetNullCount(), 0);
    std::vector<std::string> expectedValues1 = { "1" };
    std::vector<bool> expectedNulls1 = { false };
    VarcharVector *expected1 = CreateVarcharVector(expectedValues1, expectedNulls1);
    ColumnMatch(sliceNoNull, expected1);
    delete expected1;
    delete sliceNoNull;


    VarcharVector *sliceHasNull = hasNulls->Slice(1, 4);
    EXPECT_TRUE(sliceHasNull->MayHaveNull());
    EXPECT_EQ(sliceHasNull->GetNullCount(), 2);
    std::vector<std::string> expectedValues2 = { "2", "", "", "5" };
    std::vector<bool> expectedNulls2 = { false, true, true, false };
    VarcharVector *expected2 = CreateVarcharVector(expectedValues2, expectedNulls2);
    ColumnMatch(sliceHasNull, expected2);
    delete expected2;
    delete sliceHasNull;

    delete hasNulls;
    delete allocator;
}

TEST(VarcharVector, testNullFlagWithCopyRegion)
{
    VectorAllocator *allocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("testNullFlagWithCopyRegion");
    // has null value
    std::vector<std::string> values = { "1", "2", "", "", "5", "", "7", "", "9", "" };
    std::vector<bool> nulls = { false, false, true, true, false, true, false, true, false, true };
    auto *hasNulls = CreateVarcharVector(values, nulls);
    EXPECT_TRUE(hasNulls->MayHaveNull());
    EXPECT_EQ(hasNulls->GetNullCount(), 5);

    VarcharVector *copyRegionNoNull = hasNulls->CopyRegion(0, 2);
    EXPECT_FALSE(copyRegionNoNull->MayHaveNull());
    EXPECT_EQ(copyRegionNoNull->GetNullCount(), 0);
    std::vector<std::string> expectedValues1 = { "1", "2" };
    std::vector<bool> expectedNulls1 = { false, false };
    VarcharVector *expected1 = CreateVarcharVector(expectedValues1, expectedNulls1);
    ColumnMatch(copyRegionNoNull, expected1);
    delete expected1;
    delete copyRegionNoNull;

    VarcharVector *copyRegionHasNull = hasNulls->CopyRegion(1, 4);
    EXPECT_TRUE(copyRegionHasNull->MayHaveNull());
    EXPECT_EQ(copyRegionHasNull->GetNullCount(), 2);
    std::vector<std::string> expectedValues2 = { "2", "", "", "5" };
    std::vector<bool> expectedNulls2 = { false, true, true, false };
    VarcharVector *expected2 = CreateVarcharVector(expectedValues2, expectedNulls2);
    ColumnMatch(copyRegionHasNull, expected2);
    delete expected2;
    delete copyRegionHasNull;

    delete hasNulls;
    delete allocator;
}

TEST(VarcharVector, testNullFlagWithAppend)
{
    VectorAllocator *allocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("testNullFlagWithAppend");
    int rowCount = 5;
    auto *src = new VarcharVector(allocator, rowCount);

    for (int i = 0; i < rowCount; i++) {
        std::string str = std::to_string(i + 1);
        src->SetValue(i, reinterpret_cast<const uint8_t *>(str.c_str()), str.length());
    }

    auto *appended = new VarcharVector(allocator, 15);
    appended->Append(src, 0, rowCount);
    delete src;
    EXPECT_FALSE(appended->MayHaveNull());
    EXPECT_EQ(appended->GetNullCount(), 0);

    std::vector<std::string> values = { "1", "", "", "3", "" };
    std::vector<bool> nulls = { false, true, true, false, true };
    VarcharVector *withNull = CreateVarcharVector(values, nulls);
    appended->Append(withNull, 5, rowCount);
    EXPECT_TRUE(appended->MayHaveNull());
    EXPECT_EQ(appended->GetNullCount(), 3);

    appended->Append(withNull, 10, rowCount);
    EXPECT_TRUE(appended->MayHaveNull());
    EXPECT_EQ(appended->GetNullCount(), 6);
    std::vector<std::string> expectedValues = { "1", "2", "3", "4", "5", "1", "", "", "3", "", "1", "", "", "3", "" };
    std::vector<bool> expectedNulls = { false, false, false, false, false, false, true, true,
        false, true,  false, true,  true,  false, true };
    VarcharVector *expected = CreateVarcharVector(values, nulls);
    ColumnMatch(appended, expected);
    delete expected;
    delete withNull;

    delete appended;
    delete allocator;
}
}
