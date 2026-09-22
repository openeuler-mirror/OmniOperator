/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: Minute function unit tests
 */

#include <gtest/gtest.h>
#include <iostream>
#include <vector>
#include <ctime>
#include <string>
#include <unordered_map>

#include "test/util/test_util.h"
#include "util/config/QueryConfig.h"
#include "vectorization/registration/Register.h"
#include "vectorization/functions/Minute.h"
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
class MinuteTestEnvironment : public ::testing::Environment {
public:
    void SetUp() override {
        RegisterFunctions::RegisterAllFunctions("");
    }
};

::testing::Environment* const minute_test_env = ::testing::AddGlobalTestEnvironment(new MinuteTestEnvironment);

class MinuteFunctionTestHelper {
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
    
    static void ExecuteMinute(BaseVector* inputVec, DataTypeId inputTypeId, BaseVector*& result) {
        ExecuteMinuteInZone(inputVec, inputTypeId, "", result);
    }

    /// Same as ExecuteMinute, but runs with the given session timezone.
    /// An empty name leaves the session timezone unset.
    static void ExecuteMinuteInZone(BaseVector* inputVec, DataTypeId inputTypeId,
                                     const std::string& sessionTimezone, BaseVector*& result) {
        auto signature = std::make_shared<FunctionSignature>("minute", 
            std::vector<DataTypeId>{inputTypeId}, OMNI_INT);
        auto function = VectorFunction::Find(signature);
        ASSERT_NE(function, nullptr) << "Minute function not found for signature";
        
        auto outputType = std::make_shared<DataType>(OMNI_INT);
        ExecutionContext context;
        context.SetResultRowSize(inputVec->GetSize());
        if (!sessionTimezone.empty()) {
            context.SetConfig(config::QueryConfig(std::unordered_map<std::string, std::string>{
                {config::QueryConfig::kSessionTimezone, sessionTimezone}}));
        }
        std::stack<BaseVector*> args;
        args.push(inputVec);
        
        ASSERT_NO_THROW(function->Apply(args, outputType, result, &context))
            << "Minute function threw an exception";
    }
    
    /// Builds a microsecond timestamp for a UTC wall clock reading. Unlike
    /// TimestampToMicros this does not depend on the system timezone.
    static int64_t TimestampUtcToMicros(int year, int month, int day, int hour, int minute, int second) {
        std::tm tm = {};
        tm.tm_year = year - 1900;
        tm.tm_mon = month - 1;
        tm.tm_mday = day;
        tm.tm_hour = hour;
        tm.tm_min = minute;
        tm.tm_sec = second;
        return Timestamp::calendarUtcToEpoch(tm) * 1000000LL;
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

// Test: Minute from timestamp - basic cases
TEST(MinuteTest, TimestampBasic) {
    std::cout << "=== Test: Minute from TIMESTAMP - basic cases ===" << std::endl;
    
    // Create timestamps: 2024-01-01 12:30:45, 2024-01-01 15:45:20, 2024-01-01 00:00:00
    std::vector<int64_t> timestampValues = {
        MinuteFunctionTestHelper::TimestampToMicros(2024, 1, 1, 12, 30, 45),
        MinuteFunctionTestHelper::TimestampToMicros(2024, 1, 1, 15, 45, 20),
        MinuteFunctionTestHelper::TimestampToMicros(2024, 1, 1, 0, 0, 0)
    };
    std::vector<int32_t> expected = {30, 45, 0};
    
    BaseVector* inputVec = MinuteFunctionTestHelper::CreateTimestampVector(timestampValues);
    BaseVector* resultVec = nullptr;
    MinuteFunctionTestHelper::ExecuteMinute(inputVec, OMNI_TIMESTAMP, resultVec);
    MinuteFunctionTestHelper::ValidateResult(resultVec, expected, timestampValues.size());

    delete resultVec;
}

// Test: Minute from timestamp - edge cases (59 minutes)
TEST(MinuteTest, TimestampEdgeCases) {
    std::cout << "=== Test: Minute from TIMESTAMP - edge cases ===" << std::endl;
    
    // Create timestamps with 59 minutes
    std::vector<int64_t> timestampValues = {
        MinuteFunctionTestHelper::TimestampToMicros(2024, 1, 1, 10, 59, 0),
        MinuteFunctionTestHelper::TimestampToMicros(2024, 1, 1, 23, 59, 59),
        MinuteFunctionTestHelper::TimestampToMicros(2024, 1, 1, 0, 1, 0)
    };
    std::vector<int32_t> expected = {59, 59, 1};
    
    BaseVector* inputVec = MinuteFunctionTestHelper::CreateTimestampVector(timestampValues);
    BaseVector* resultVec = nullptr;
    MinuteFunctionTestHelper::ExecuteMinute(inputVec, OMNI_TIMESTAMP, resultVec);
    MinuteFunctionTestHelper::ValidateResult(resultVec, expected, timestampValues.size());

    delete resultVec;
}

// Test: Minute from timestamp with NULL values
TEST(MinuteTest, TimestampWithNull) {
    std::cout << "=== Test: Minute from TIMESTAMP with NULL values ===" << std::endl;
    
    std::vector<int64_t> timestampValues = {
        MinuteFunctionTestHelper::TimestampToMicros(2024, 1, 1, 12, 30, 45),
        MinuteFunctionTestHelper::TimestampToMicros(2024, 1, 1, 15, 45, 20),
        MinuteFunctionTestHelper::TimestampToMicros(2024, 1, 1, 0, 0, 0)
    };
    
    BaseVector* inputVec = MinuteFunctionTestHelper::CreateTimestampVector(timestampValues);
    // Set middle value to NULL
    inputVec->SetNull(1);
    
    BaseVector* resultVec = nullptr;
    MinuteFunctionTestHelper::ExecuteMinute(inputVec, OMNI_TIMESTAMP, resultVec);
    
    // First and third should have values, second should be NULL
    EXPECT_FALSE(resultVec->IsNull(0)) << "Row 0 should not be NULL";
    EXPECT_TRUE(resultVec->IsNull(1)) << "Row 1 should be NULL";
    EXPECT_FALSE(resultVec->IsNull(2)) << "Row 2 should not be NULL";
    
    // Validate non-null values
    auto* resultVecTyped = dynamic_cast<Vector<int32_t>*>(resultVec);
    EXPECT_EQ(resultVecTyped->GetValue(0), 30) << "Row 0 minute should be 30";
    EXPECT_EQ(resultVecTyped->GetValue(2), 0) << "Row 2 minute should be 0";

    delete resultVec;
}

// Test: the minute of hour comes from the local clock, not from UTC.
// Asia/Shanghai uses the LMT offset +08:05:43 before 1901, so local midnight
// sits at 15:54:17 UTC and reading tm_min off the UTC clock yields 54.
TEST(MinuteTest, TimestampLmtOffset) {
    std::vector<int64_t> timestampValues = {
        // 1900-11-11 00:00:00 Asia/Shanghai
        MinuteFunctionTestHelper::TimestampUtcToMicros(1900, 11, 10, 15, 54, 17),
        // 1900-11-11 09:30:00 Asia/Shanghai
        MinuteFunctionTestHelper::TimestampUtcToMicros(1900, 11, 11, 1, 24, 17),
        // 1901-01-02 00:00:00 Asia/Shanghai, past the switch to +08:00
        MinuteFunctionTestHelper::TimestampUtcToMicros(1901, 1, 1, 16, 0, 0)
    };
    std::vector<int32_t> expected = {0, 30, 0};

    BaseVector* inputVec = MinuteFunctionTestHelper::CreateTimestampVector(timestampValues);
    BaseVector* resultVec = nullptr;
    MinuteFunctionTestHelper::ExecuteMinuteInZone(inputVec, OMNI_TIMESTAMP, "Asia/Shanghai", resultVec);
    MinuteFunctionTestHelper::ValidateResult(resultVec, expected, timestampValues.size());

    delete resultVec;
}

// Test: half-hour zone offsets shift the minute of hour in every era.
TEST(MinuteTest, TimestampHalfHourOffset) {
    std::vector<int64_t> timestampValues = {
        // 2024-06-15 12:45:30 Asia/Kolkata (+05:30)
        MinuteFunctionTestHelper::TimestampUtcToMicros(2024, 6, 15, 7, 15, 30),
        // 2024-06-15 12:15:30 Asia/Kolkata
        MinuteFunctionTestHelper::TimestampUtcToMicros(2024, 6, 15, 6, 45, 30)
    };
    std::vector<int32_t> expected = {45, 15};

    BaseVector* inputVec = MinuteFunctionTestHelper::CreateTimestampVector(timestampValues);
    BaseVector* resultVec = nullptr;
    MinuteFunctionTestHelper::ExecuteMinuteInZone(inputVec, OMNI_TIMESTAMP, "Asia/Kolkata", resultVec);
    MinuteFunctionTestHelper::ValidateResult(resultVec, expected, timestampValues.size());

    delete resultVec;
}

// Test: Minute from timestamp - all minutes (0-59)
TEST(MinuteTest, TimestampAllMinutes) {
    std::cout << "=== Test: Minute from TIMESTAMP - all minutes (0-59) ===" << std::endl;
    
    std::vector<int64_t> timestampValues;
    std::vector<int32_t> expected;
    
    for (int m = 0; m < 60; ++m) {
        timestampValues.push_back(MinuteFunctionTestHelper::TimestampToMicros(2024, 1, 1, 12, m, 0));
        expected.push_back(m);
    }
    
    BaseVector* inputVec = MinuteFunctionTestHelper::CreateTimestampVector(timestampValues);
    BaseVector* resultVec = nullptr;
    MinuteFunctionTestHelper::ExecuteMinute(inputVec, OMNI_TIMESTAMP, resultVec);
    MinuteFunctionTestHelper::ValidateResult(resultVec, expected, timestampValues.size());

    delete resultVec;
}
