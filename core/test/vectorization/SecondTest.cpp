/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: Second function unit tests
 */

#include <gtest/gtest.h>
#include <iostream>
#include <vector>
#include <ctime>

#include "test/util/test_util.h"
#include "vectorization/registration/Register.h"
#include "vectorization/functions/Second.h"
#include "vectorization/VectorFunction.h"
#include "codegen/func_signature.h"
#include "vector/vector_helper.h"
#include "vector/vector.h"
#include "type/Timestamp.h"

using namespace omniruntime;
using namespace omniruntime::vec;
using namespace omniruntime::vectorization;
using namespace omniruntime::op;
using namespace omniruntime::type;
using namespace omniruntime::codegen;
using namespace omniruntime::TestUtil;

// Initialize function registration before running tests
class SecondTestEnvironment : public ::testing::Environment {
public:
    void SetUp() override {
        RegisterFunctions::RegisterAllFunctions("");
    }
};

::testing::Environment* const second_test_env = ::testing::AddGlobalTestEnvironment(new SecondTestEnvironment);

class SecondFunctionTestHelper {
public:
    static void ValidateResult(BaseVector* result, const std::vector<int32_t>& expected, int rowSize) {
        auto* resultVec = dynamic_cast<Vector<int32_t>*>(result);
        ASSERT_NE(resultVec, nullptr) << "Result vector type mismatch";
        
        for (int i = 0; i < rowSize; ++i) {
            if (result->IsNull(i)) {
                std::cout << "Row " << i << ": NULL" << std::endl;
                continue;
            }
            int32_t actualValue = resultVec->GetValue(i);
            int32_t expectedValue = expected[i];
            std::cout << "Row " << i << ": Expected=" << expectedValue << ", Actual=" << actualValue << std::endl;
            EXPECT_EQ(actualValue, expectedValue) << "Row " << i << " value mismatch";
        }
    }
    
    static BaseVector* CreateTimestampVector(const std::vector<int64_t>& values) {
        BaseVector* vec = VectorHelper::CreateFlatVector(OMNI_TIMESTAMP, values.size());
        auto* typedVec = static_cast<Vector<int64_t>*>(vec);
        for (size_t i = 0; i < values.size(); ++i) {
            typedVec->SetValue(i, values[i]);
        }
        return vec;
    }
    
    static void ExecuteSecond(BaseVector* inputVec, DataTypeId inputTypeId, BaseVector*& result) {
        auto signature = std::make_shared<FunctionSignature>("second", 
            std::vector<DataTypeId>{inputTypeId}, OMNI_INT);
        auto function = VectorFunction::Find(signature);
        ASSERT_NE(function, nullptr) << "Second function not found for signature";
        
        auto outputType = std::make_shared<DataType>(OMNI_INT);
        ExecutionContext context;
        context.SetResultRowSize(inputVec->GetSize());
        std::stack<BaseVector*> args;
        args.push(inputVec);
        
        ASSERT_NO_THROW(function->Apply(args, outputType, result, &context))
            << "Second function threw an exception";
    }
    
    // Helper to convert timestamp components to microseconds since epoch
    static int64_t TimestampToMicros(int year, int month, int day, int hour, int minute, int second) {
        std::tm tm = {};
        tm.tm_year = year - 1900;
        tm.tm_mon = month - 1;
        tm.tm_mday = day;
        tm.tm_hour = hour;
        tm.tm_min = minute;
        tm.tm_sec = second;
        tm.tm_isdst = -1;
        
        std::time_t time = std::mktime(&tm);
        if (time == -1) {
            return 0;
        }
        return static_cast<int64_t>(time) * 1000000;
    }
};

// Test: Second from timestamp - basic cases
TEST(SecondTest, TimestampBasic) {
    std::cout << "=== Test: Second from TIMESTAMP - basic cases ===" << std::endl;
    
    // Create timestamps: 2024-01-01 12:30:45, 2024-01-01 15:45:20, 2024-01-01 00:00:00
    std::vector<int64_t> timestampValues = {
        SecondFunctionTestHelper::TimestampToMicros(2024, 1, 1, 12, 30, 45),
        SecondFunctionTestHelper::TimestampToMicros(2024, 1, 1, 15, 45, 20),
        SecondFunctionTestHelper::TimestampToMicros(2024, 1, 1, 0, 0, 0)
    };
    std::vector<int32_t> expected = {45, 20, 0};
    
    BaseVector* inputVec = SecondFunctionTestHelper::CreateTimestampVector(timestampValues);
    BaseVector* resultVec = nullptr;
    SecondFunctionTestHelper::ExecuteSecond(inputVec, OMNI_TIMESTAMP, resultVec);
    SecondFunctionTestHelper::ValidateResult(resultVec, expected, timestampValues.size());

    delete resultVec;
}

// Test: Second from timestamp - edge cases (59 seconds)
TEST(SecondTest, TimestampEdgeCases) {
    std::cout << "=== Test: Second from TIMESTAMP - edge cases ===" << std::endl;
    
    // Create timestamps with 59 seconds
    std::vector<int64_t> timestampValues = {
        SecondFunctionTestHelper::TimestampToMicros(2024, 1, 1, 10, 30, 59),
        SecondFunctionTestHelper::TimestampToMicros(2024, 1, 1, 23, 59, 59),
        SecondFunctionTestHelper::TimestampToMicros(2024, 1, 1, 0, 0, 1)
    };
    std::vector<int32_t> expected = {59, 59, 1};
    
    BaseVector* inputVec = SecondFunctionTestHelper::CreateTimestampVector(timestampValues);
    BaseVector* resultVec = nullptr;
    SecondFunctionTestHelper::ExecuteSecond(inputVec, OMNI_TIMESTAMP, resultVec);
    SecondFunctionTestHelper::ValidateResult(resultVec, expected, timestampValues.size());

    delete resultVec;
}

// Test: Second from timestamp with NULL values
TEST(SecondTest, TimestampWithNull) {
    std::cout << "=== Test: Second from TIMESTAMP with NULL values ===" << std::endl;
    
    std::vector<int64_t> timestampValues = {
        SecondFunctionTestHelper::TimestampToMicros(2024, 1, 1, 12, 30, 45),
        SecondFunctionTestHelper::TimestampToMicros(2024, 1, 1, 15, 45, 20),
        SecondFunctionTestHelper::TimestampToMicros(2024, 1, 1, 0, 0, 0)
    };
    
    BaseVector* inputVec = SecondFunctionTestHelper::CreateTimestampVector(timestampValues);
    // Set middle value to NULL
    inputVec->SetNull(1);
    
    BaseVector* resultVec = nullptr;
    SecondFunctionTestHelper::ExecuteSecond(inputVec, OMNI_TIMESTAMP, resultVec);
    
    // First and third should have values, second should be NULL
    EXPECT_FALSE(resultVec->IsNull(0)) << "Row 0 should not be NULL";
    EXPECT_TRUE(resultVec->IsNull(1)) << "Row 1 should be NULL";
    EXPECT_FALSE(resultVec->IsNull(2)) << "Row 2 should not be NULL";
    
    // Validate non-null values
    auto* resultVecTyped = dynamic_cast<Vector<int32_t>*>(resultVec);
    EXPECT_EQ(resultVecTyped->GetValue(0), 45) << "Row 0 second should be 45";
    EXPECT_EQ(resultVecTyped->GetValue(2), 0) << "Row 2 second should be 0";

    delete resultVec;
}

// Test: Second from timestamp - all seconds (0-59)
TEST(SecondTest, TimestampAllSeconds) {
    std::cout << "=== Test: Second from TIMESTAMP - all seconds (0-59) ===" << std::endl;
    
    std::vector<int64_t> timestampValues;
    std::vector<int32_t> expected;
    
    for (int s = 0; s < 60; ++s) {
        timestampValues.push_back(SecondFunctionTestHelper::TimestampToMicros(2024, 1, 1, 12, 30, s));
        expected.push_back(s);
    }
    
    BaseVector* inputVec = SecondFunctionTestHelper::CreateTimestampVector(timestampValues);
    BaseVector* resultVec = nullptr;
    SecondFunctionTestHelper::ExecuteSecond(inputVec, OMNI_TIMESTAMP, resultVec);
    SecondFunctionTestHelper::ValidateResult(resultVec, expected, timestampValues.size());

    delete resultVec;
}
