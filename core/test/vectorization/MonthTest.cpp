/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: Month function unit tests
 */

#include <gtest/gtest.h>
#include <iostream>
#include <vector>
#include <ctime>

#include "test/util/test_util.h"
#include "vectorization/registration/Register.h"
#include "vectorization/functions/Month.h"
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
class MonthTestEnvironment : public ::testing::Environment {
public:
    void SetUp() override {
        RegisterFunctions::RegisterAllFunctions("");
    }
};

::testing::Environment* const month_test_env = ::testing::AddGlobalTestEnvironment(new MonthTestEnvironment);

class MonthFunctionTestHelper {
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
    
    static BaseVector* CreateDate32Vector(const std::vector<int32_t>& values) {
        BaseVector* vec = VectorHelper::CreateFlatVector(OMNI_DATE32, values.size());
        auto* typedVec = static_cast<Vector<int32_t>*>(vec);
        for (size_t i = 0; i < values.size(); ++i) {
            typedVec->SetValue(i, values[i]);
        }
        return vec;
    }
    
    static void ExecuteMonth(BaseVector* inputVec, DataTypeId inputTypeId, BaseVector*& result) {
        auto signature = std::make_shared<FunctionSignature>("month", 
            std::vector<DataTypeId>{inputTypeId}, OMNI_INT);
        auto function = VectorFunction::Find(signature);
        ASSERT_NE(function, nullptr) << "Month function not found for signature";
        
        auto outputType = std::make_shared<DataType>(OMNI_INT);
        ExecutionContext context;
        context.SetResultRowSize(inputVec->GetSize());
        std::stack<BaseVector*> args;
        args.push(inputVec);
        
        ASSERT_NO_THROW(function->Apply(args, outputType, result, &context))
            << "Month function threw an exception";
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
    
    // Helper to convert date components to days since epoch
    static int32_t DateToDays(int year, int month, int day) {
        std::tm tm = {};
        tm.tm_year = year - 1900;
        tm.tm_mon = month - 1;
        tm.tm_mday = day;
        tm.tm_hour = 0;
        tm.tm_min = 0;
        tm.tm_sec = 0;
        tm.tm_isdst = -1;
        
        std::time_t time = std::mktime(&tm);
        if (time == -1) {
            return 0;
        }
        // Convert to days since epoch (1970-01-01)
        return static_cast<int32_t>(time / 86400);
    }
};

// Test: Month from timestamp - basic cases
TEST(MonthTest, TimestampBasic) {
    std::cout << "=== Test: Month from TIMESTAMP - basic cases ===" << std::endl;
    
    // Create timestamps: 2024-01-15, 2024-06-20, 2024-12-31
    std::vector<int64_t> timestampValues = {
        MonthFunctionTestHelper::TimestampToMicros(2024, 1, 15, 12, 30, 45),
        MonthFunctionTestHelper::TimestampToMicros(2024, 6, 20, 15, 45, 20),
        MonthFunctionTestHelper::TimestampToMicros(2024, 12, 31, 0, 0, 0)
    };
    std::vector<int32_t> expected = {1, 6, 12};
    
    BaseVector* inputVec = MonthFunctionTestHelper::CreateTimestampVector(timestampValues);
    BaseVector* resultVec = nullptr;
    MonthFunctionTestHelper::ExecuteMonth(inputVec, OMNI_TIMESTAMP, resultVec);
    MonthFunctionTestHelper::ValidateResult(resultVec, expected, timestampValues.size());
    
    delete inputVec;
    delete resultVec;
}

// Test: Month from DATE32 - basic cases
TEST(MonthTest, Date32Basic) {
    std::cout << "=== Test: Month from DATE32 - basic cases ===" << std::endl;
    
    // Create dates: 2024-01-15, 2024-06-20, 2024-12-31
    std::vector<int32_t> dateValues = {
        MonthFunctionTestHelper::DateToDays(2024, 1, 15),
        MonthFunctionTestHelper::DateToDays(2024, 6, 20),
        MonthFunctionTestHelper::DateToDays(2024, 12, 31)
    };
    std::vector<int32_t> expected = {1, 6, 12};
    
    BaseVector* inputVec = MonthFunctionTestHelper::CreateDate32Vector(dateValues);
    BaseVector* resultVec = nullptr;
    MonthFunctionTestHelper::ExecuteMonth(inputVec, OMNI_DATE32, resultVec);
    MonthFunctionTestHelper::ValidateResult(resultVec, expected, dateValues.size());
    
    delete inputVec;
    delete resultVec;
}

// Test: Month from timestamp - all months (1-12)
TEST(MonthTest, TimestampAllMonths) {
    std::cout << "=== Test: Month from TIMESTAMP - all months (1-12) ===" << std::endl;
    
    std::vector<int64_t> timestampValues;
    std::vector<int32_t> expected;
    
    for (int m = 1; m <= 12; ++m) {
        timestampValues.push_back(MonthFunctionTestHelper::TimestampToMicros(2024, m, 15, 12, 30, 45));
        expected.push_back(m);
    }
    
    BaseVector* inputVec = MonthFunctionTestHelper::CreateTimestampVector(timestampValues);
    BaseVector* resultVec = nullptr;
    MonthFunctionTestHelper::ExecuteMonth(inputVec, OMNI_TIMESTAMP, resultVec);
    MonthFunctionTestHelper::ValidateResult(resultVec, expected, timestampValues.size());
    
    delete inputVec;
    delete resultVec;
}

// Test: Month from timestamp with NULL values
TEST(MonthTest, TimestampWithNull) {
    std::cout << "=== Test: Month from TIMESTAMP with NULL values ===" << std::endl;
    
    std::vector<int64_t> timestampValues = {
        MonthFunctionTestHelper::TimestampToMicros(2024, 1, 15, 12, 30, 45),
        MonthFunctionTestHelper::TimestampToMicros(2024, 6, 20, 15, 45, 20),
        MonthFunctionTestHelper::TimestampToMicros(2024, 12, 31, 0, 0, 0)
    };
    
    BaseVector* inputVec = MonthFunctionTestHelper::CreateTimestampVector(timestampValues);
    // Set middle value to NULL
    inputVec->SetNull(1);
    
    BaseVector* resultVec = nullptr;
    MonthFunctionTestHelper::ExecuteMonth(inputVec, OMNI_TIMESTAMP, resultVec);
    
    // First and third should have values, second should be NULL
    EXPECT_FALSE(resultVec->IsNull(0)) << "Row 0 should not be NULL";
    EXPECT_TRUE(resultVec->IsNull(1)) << "Row 1 should be NULL";
    EXPECT_FALSE(resultVec->IsNull(2)) << "Row 2 should not be NULL";
    
    // Validate non-null values
    auto* resultVecTyped = dynamic_cast<Vector<int32_t>*>(resultVec);
    EXPECT_EQ(resultVecTyped->GetValue(0), 1) << "Row 0 month should be 1";
    EXPECT_EQ(resultVecTyped->GetValue(2), 12) << "Row 2 month should be 12";
    
    delete inputVec;
    delete resultVec;
}

// Test: Month from DATE32 with NULL values
TEST(MonthTest, Date32WithNull) {
    std::cout << "=== Test: Month from DATE32 with NULL values ===" << std::endl;
    
    std::vector<int32_t> dateValues = {
        MonthFunctionTestHelper::DateToDays(2024, 1, 15),
        MonthFunctionTestHelper::DateToDays(2024, 6, 20),
        MonthFunctionTestHelper::DateToDays(2024, 12, 31)
    };
    
    BaseVector* inputVec = MonthFunctionTestHelper::CreateDate32Vector(dateValues);
    // Set middle value to NULL
    inputVec->SetNull(1);
    
    BaseVector* resultVec = nullptr;
    MonthFunctionTestHelper::ExecuteMonth(inputVec, OMNI_DATE32, resultVec);
    
    // First and third should have values, second should be NULL
    EXPECT_FALSE(resultVec->IsNull(0)) << "Row 0 should not be NULL";
    EXPECT_TRUE(resultVec->IsNull(1)) << "Row 1 should be NULL";
    EXPECT_FALSE(resultVec->IsNull(2)) << "Row 2 should not be NULL";
    
    // Validate non-null values
    auto* resultVecTyped = dynamic_cast<Vector<int32_t>*>(resultVec);
    EXPECT_EQ(resultVecTyped->GetValue(0), 1) << "Row 0 month should be 1";
    EXPECT_EQ(resultVecTyped->GetValue(2), 12) << "Row 2 month should be 12";
    
    delete inputVec;
    delete resultVec;
}
