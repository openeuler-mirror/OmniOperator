/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
* Description: Unit tests for comparison expressions equal (= / ==) and notEqual (!=)
*              Support types: boolean, integer, float/double, string, DATE, timestamp, binary.
*/
#include <gtest/gtest.h>
#include <vector>
#include <string>
#include "test/util/test_util.h"
#include "vectorization/registration/Register.h"
#include "vectorization/VectorFunction.h"
#include "vectorization/functions/Comparisons.h"
#include "codegen/func_signature.h"
#include "type/data_type.h"
#include "vector/vector_helper.h"
#include "vector/large_string_container.h"

using namespace omniruntime;
using namespace omniruntime::vec;
using namespace omniruntime::vectorization;
using namespace omniruntime::op;
using namespace omniruntime::type;
using namespace omniruntime::codegen;

template <typename T, DataTypeId typeId>
void TestComparisonOp(const std::string& functionName,
                     const std::vector<T>& leftData,
                     const std::vector<T>& rightData,
                     const std::vector<bool>& expectedResults) {
    int32_t rowSize = static_cast<int32_t>(leftData.size());
    ASSERT_EQ(rightData.size(), leftData.size());
    ASSERT_EQ(expectedResults.size(), leftData.size());

    BaseVector* leftVec = VectorHelper::CreateFlatVector(typeId, rowSize);
    auto* leftVector = static_cast<Vector<T>*>(leftVec);
    for (int32_t i = 0; i < rowSize; ++i) {
        leftVector->SetValue(i, leftData[i]);
        leftVector->SetNotNull(i);
    }

    BaseVector* rightVec = VectorHelper::CreateFlatVector(typeId, rowSize);
    auto* rightVector = static_cast<Vector<T>*>(rightVec);
    for (int32_t i = 0; i < rowSize; ++i) {
        rightVector->SetValue(i, rightData[i]);
        rightVector->SetNotNull(i);
    }

    std::vector<DataTypeId> argTypes = {typeId, typeId};
    auto signature = std::make_shared<FunctionSignature>(functionName, argTypes, OMNI_BOOLEAN);
    auto vectorFunction = VectorFunction::Find(signature);
    ASSERT_NE(vectorFunction, nullptr) << "Function " << functionName << " not found for type " << static_cast<int>(typeId);

    ExecutionContext context;
    context.SetResultRowSize(rowSize);

    std::stack<BaseVector*> args;
    args.push(leftVec);
    args.push(rightVec);

    BaseVector* result = nullptr;
    auto resultType = std::make_shared<DataType>(OMNI_BOOLEAN);
    vectorFunction->Apply(args, resultType, result, &context);

    auto* resultVector = static_cast<Vector<bool>*>(result);
    ASSERT_NE(resultVector, nullptr);
    for (int32_t i = 0; i < rowSize; ++i) {
        EXPECT_EQ(resultVector->GetValue(i), expectedResults[i])
            << "Mismatch at index " << i << " for " << functionName
            << " expected=" << expectedResults[i];
    }
    delete result;
}

// String (VARCHAR) and binary (VARBINARY) use LargeStringContainer<std::string_view>
void TestComparisonOpStringLike(DataTypeId typeId,
                                const std::string& functionName,
                                const std::vector<std::string>& leftData,
                                const std::vector<std::string>& rightData,
                                const std::vector<bool>& expectedResults) {
    int32_t rowSize = static_cast<int32_t>(leftData.size());
    ASSERT_EQ(rightData.size(), leftData.size());
    ASSERT_EQ(expectedResults.size(), leftData.size());

    using VarcharVector = Vector<LargeStringContainer<std::string_view>>;
    BaseVector* leftVec = VectorHelper::CreateFlatVector(typeId, rowSize);
    auto* leftVector = static_cast<VarcharVector*>(leftVec);
    for (int32_t i = 0; i < rowSize; ++i) {
        std::string_view lsv(leftData[i]);
        leftVector->SetValue(i, lsv);
        leftVector->SetNotNull(i);
    }

    BaseVector* rightVec = VectorHelper::CreateFlatVector(typeId, rowSize);
    auto* rightVector = static_cast<VarcharVector*>(rightVec);
    for (int32_t i = 0; i < rowSize; ++i) {
        std::string_view rsv(rightData[i]);
        rightVector->SetValue(i, rsv);
        rightVector->SetNotNull(i);
    }

    std::vector<DataTypeId> argTypes = {typeId, typeId};
    auto signature = std::make_shared<FunctionSignature>(functionName, argTypes, OMNI_BOOLEAN);
    auto vectorFunction = VectorFunction::Find(signature);
    ASSERT_NE(vectorFunction, nullptr) << "Function " << functionName << " not found for type " << static_cast<int>(typeId);

    ExecutionContext context;
    context.SetResultRowSize(rowSize);

    std::stack<BaseVector*> args;
    args.push(leftVec);
    args.push(rightVec);

    BaseVector* result = nullptr;
    auto resultType = std::make_shared<DataType>(OMNI_BOOLEAN);
    vectorFunction->Apply(args, resultType, result, &context);

    auto* resultVector = static_cast<Vector<bool>*>(result);
    ASSERT_NE(resultVector, nullptr);
    for (int32_t i = 0; i < rowSize; ++i) {
        EXPECT_EQ(resultVector->GetValue(i), expectedResults[i])
            << "Mismatch at index " << i << " for " << functionName
            << " left=" << leftData[i] << " right=" << rightData[i]
            << " expected=" << expectedResults[i];
    }
    delete result;
}

class ComparisonTest : public ::testing::Test {
protected:
    void SetUp() override {
        RegisterFunctions::Register();
    }
};

// equal: = or == (both map to "equal")
TEST_F(ComparisonTest, EqualBoolean) {
    std::vector<bool> left  = {true,  true,  false, false};
    std::vector<bool> right = {true,  false, true,  false};
    std::vector<bool> expected = {true, false, false, true};
    TestComparisonOp<bool, OMNI_BOOLEAN>("equal", left, right, expected);
}

TEST_F(ComparisonTest, EqualShort) {
    std::vector<int16_t> left  = {1, 2, 3, -1, 0};
    std::vector<int16_t> right = {1, 3, 3, -2, 0};
    std::vector<bool> expected = {true, false, true, false, true};
    TestComparisonOp<int16_t, OMNI_SHORT>("equal", left, right, expected);
}

TEST_F(ComparisonTest, EqualInt) {
    std::vector<int32_t> left  = {0, 1, -1, 100, 100};
    std::vector<int32_t> right = {0, 2, -1, 100, 99};
    std::vector<bool> expected = {true, false, true, true, false};
    TestComparisonOp<int32_t, OMNI_INT>("equal", left, right, expected);
}

TEST_F(ComparisonTest, EqualDouble) {
    std::vector<double> left  = {0.0, 1.5, -1.0, 3.14, 3.14};
    std::vector<double> right = {0.0, 1.6, -1.0, 3.14, 2.71};
    std::vector<bool> expected = {true, false, true, true, false};
    TestComparisonOp<double, OMNI_DOUBLE>("equal", left, right, expected);
}

TEST_F(ComparisonTest, EqualVarchar) {
    std::vector<std::string> left  = {"a", "bb", "ccc", "same", "same"};
    std::vector<std::string> right = {"a", "bc", "ccc", "same", "diff"};
    std::vector<bool> expected = {true, false, true, true, false};
    TestComparisonOpStringLike(OMNI_VARCHAR, "equal", left, right, expected);
}

TEST_F(ComparisonTest, EqualDate32) {
    // DATE32: days since epoch (int32_t)
    std::vector<int32_t> left  = {0, 100, 19723, 19723, 1};
    std::vector<int32_t> right = {0, 101, 19723, 19724, 1};
    std::vector<bool> expected = {true, false, true, false, true};
    TestComparisonOp<int32_t, OMNI_DATE32>("equal", left, right, expected);
}

TEST_F(ComparisonTest, EqualTimestamp) {
    // TIMESTAMP: int64_t (e.g. micros)
    std::vector<int64_t> left  = {0, 1000, 1000000, 1000000, 1};
    std::vector<int64_t> right = {0, 1001, 1000000, 2000000, 1};
    std::vector<bool> expected = {true, false, true, false, true};
    TestComparisonOp<int64_t, OMNI_TIMESTAMP>("equal", left, right, expected);
}

TEST_F(ComparisonTest, EqualVarbinary) {
    std::vector<std::string> left  = {"\x00\x01", "ab", "bin", "x", "x"};
    std::vector<std::string> right = {"\x00\x01", "ac", "bin", "x", "y"};
    std::vector<bool> expected = {true, false, true, true, false};
    TestComparisonOpStringLike(OMNI_VARBINARY, "equal", left, right, expected);
}

TEST_F(ComparisonTest, EqualDecimal64) {
    // DECIMAL64 stored as int64_t (unscaled)
    std::vector<int64_t> left  = {0, 100, -100, 12345, 12345};
    std::vector<int64_t> right = {0, 101, -100, 12345, 12346};
    std::vector<bool> expected = {true, false, true, true, false};
    TestComparisonOp<int64_t, OMNI_DECIMAL64>("equal", left, right, expected);
}

TEST_F(ComparisonTest, EqualDecimal128) {
    std::vector<Decimal128> left  = {Decimal128(0), Decimal128(100), Decimal128(-1, 0), Decimal128("123"), Decimal128("123")};
    std::vector<Decimal128> right = {Decimal128(0), Decimal128(101), Decimal128(-1, 0), Decimal128("123"), Decimal128("456")};
    std::vector<bool> expected = {true, false, true, true, false};
    TestComparisonOp<Decimal128, OMNI_DECIMAL128>("equal", left, right, expected);
}