/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: Greatest function unit tests
 */

#include <gtest/gtest.h>
#include <iostream>
#include <string>
#include <vector>
#include <limits>
#include <cmath>

#include "test/util/test_util.h"
#include "vectorization/registration/Register.h"
#include "vectorization/functions/Greatest.h"
#include "vectorization/registration/SimpleFunctionRegistry.h"
#include "vectorization/VectorFunction.h"
#include "codegen/func_signature.h"
#include "vector/vector_helper.h"
#include "vector/vector.h"

using namespace omniruntime;
using namespace omniruntime::vec;
using namespace omniruntime::vectorization;
using namespace omniruntime::mem;
using namespace omniruntime::op;
using namespace omniruntime::type;
using namespace omniruntime::codegen;
using namespace omniruntime::TestUtil;

// Initialize function registration before running tests
class GreatestTestEnvironment : public ::testing::Environment {
public:
    void SetUp() override {
        RegisterFunctions::RegisterAllFunctions("");
    }
};

::testing::Environment* const greatest_test_env = ::testing::AddGlobalTestEnvironment(new GreatestTestEnvironment);

class GreatestFunctionTestHelper {
public:
    template<typename T>
    static void ValidateNumericResult(BaseVector* result, const std::vector<T>& expected, 
                                      const std::vector<bool>& expectedNull, int rowSize) {
        auto* resultVec = dynamic_cast<Vector<T>*>(result);
        ASSERT_NE(resultVec, nullptr) << "Result vector type mismatch";
        
        for (int i = 0; i < rowSize; ++i) {
            if (expectedNull[i]) {
                EXPECT_TRUE(result->IsNull(i)) << "Row " << i << " should be NULL";
                std::cout << "Row " << i << ": NULL (expected)" << std::endl;
                continue;
            }
            
            EXPECT_FALSE(result->IsNull(i)) << "Row " << i << " should not be NULL";
            T actualValue = resultVec->GetValue(i);
            T expectedValue = expected[i];
            std::cout << "Row " << i << ": Expected=" << expectedValue << ", Actual=" << actualValue << std::endl;
            
            if constexpr (std::is_floating_point_v<T>) {
                if (std::isnan(expectedValue)) {
                    EXPECT_TRUE(std::isnan(actualValue)) << "Row " << i << " should be NaN";
                } else {
                    EXPECT_NEAR(actualValue, expectedValue, 1e-6) 
                        << "Row " << i << " value mismatch";
                }
            } else {
                EXPECT_EQ(actualValue, expectedValue) 
                    << "Row " << i << " value mismatch";
            }
        }
    }
    
    static void ValidateStringResult(BaseVector* result, const std::vector<std::string>& expected, 
                                     const std::vector<bool>& expectedNull, int rowSize) {
        auto* resultVec = dynamic_cast<Vector<LargeStringContainer<std::string_view>>*>(result);
        ASSERT_NE(resultVec, nullptr) << "Result vector is not string type";
        
        for (int i = 0; i < rowSize; ++i) {
            if (expectedNull[i]) {
                EXPECT_TRUE(result->IsNull(i)) << "Row " << i << " should be NULL";
                std::cout << "Row " << i << ": NULL (expected)" << std::endl;
                continue;
            }
            
            EXPECT_FALSE(result->IsNull(i)) << "Row " << i << " should not be NULL";
            std::string_view actualValue = resultVec->GetValue(i);
            std::string expectedValue = expected[i];
            std::cout << "Row " << i << ": Expected=\"" << expectedValue 
                    << "\", Actual=\"" << actualValue << "\"" << std::endl;
            EXPECT_EQ(actualValue, expectedValue) << "Row " << i << " value mismatch";
        }
    }
    
    static void ValidateBooleanResult(BaseVector* result, const std::vector<bool>& expected, 
                                      const std::vector<bool>& expectedNull, int rowSize) {
        auto* resultVec = dynamic_cast<Vector<bool>*>(result);
        ASSERT_NE(resultVec, nullptr) << "Result vector is not boolean type";
        
        for (int i = 0; i < rowSize; ++i) {
            if (expectedNull[i]) {
                EXPECT_TRUE(result->IsNull(i)) << "Row " << i << " should be NULL";
                std::cout << "Row " << i << ": NULL (expected)" << std::endl;
                continue;
            }
            
            EXPECT_FALSE(result->IsNull(i)) << "Row " << i << " should not be NULL";
            bool actualValue = resultVec->GetValue(i);
            bool expectedValue = expected[i];
            std::cout << "Row " << i << ": Expected=" << (expectedValue ? "true" : "false") 
                    << ", Actual=" << (actualValue ? "true" : "false") << std::endl;
            EXPECT_EQ(actualValue, expectedValue) << "Row " << i << " value mismatch";
        }
    }
    
    template<typename T>
    static BaseVector* CreateNumericVector(const std::vector<T>& values, DataTypeId typeId) {
        BaseVector* vec = VectorHelper::CreateFlatVector(typeId, values.size());
        auto* typedVec = static_cast<Vector<T>*>(vec);
        for (size_t i = 0; i < values.size(); ++i) {
            typedVec->SetValue(i, values[i]);
        }
        return vec;
    }
    
    static BaseVector* CreateStringVector(const std::vector<std::string>& values) {
        BaseVector* vec = VectorHelper::CreateStringVector(values.size());
        auto* typedVec = dynamic_cast<Vector<LargeStringContainer<std::string_view>>*>(vec);
        for (size_t i = 0; i < values.size(); ++i) {
            std::string_view sv(values[i]);
            typedVec->SetValue(i, sv);
        }
        return vec;
    }
    
    static void ExecuteGreatest(const std::vector<BaseVector*>& inputs, DataTypeId outputTypeId, BaseVector*& result) {
        std::vector<DataTypeId> inputTypeIds;
        for (auto* input : inputs) {
            inputTypeIds.push_back(input->GetTypeId());
        }
        
        auto signature = std::make_shared<FunctionSignature>("Greatest", inputTypeIds, outputTypeId);
        auto function = VectorFunction::Find(signature);
        ASSERT_NE(function, nullptr) << "Greatest function not found for signature";
        
        auto outputType = std::make_shared<DataType>(outputTypeId);
        ExecutionContext context;
        context.SetResultRowSize(inputs[0]->GetSize());
        std::stack<BaseVector*> args;
        
        // Push arguments in reverse order (stack order)
        for (auto it = inputs.rbegin(); it != inputs.rend(); ++it) {
            args.push(*it);
        }
        
        ASSERT_NO_THROW(function->Apply(args, outputType, result, &context))
            << "Greatest function threw an exception";
    }
};

// ==================== Integer Tests ====================

// Test: Integer greatest with 2 arguments - basic case
TEST(GreatestTest, IntGreatestTwoArgs) {
    std::cout << "=== Test: INT greatest with 2 arguments ===" << std::endl;
    
    std::vector<int32_t> arg1Values = {100, 500, 300};
    std::vector<int32_t> arg2Values = {400, 200, 600};
    std::vector<int32_t> expected = {400, 500, 600};  // Greatest values
    std::vector<bool> expectedNull = {false, false, false};
    
    BaseVector* arg1Vec = GreatestFunctionTestHelper::CreateNumericVector(arg1Values, OMNI_INT);
    BaseVector* arg2Vec = GreatestFunctionTestHelper::CreateNumericVector(arg2Values, OMNI_INT);
    
    BaseVector* resultVec = nullptr;
    GreatestFunctionTestHelper::ExecuteGreatest({arg1Vec, arg2Vec}, OMNI_INT, resultVec);
    GreatestFunctionTestHelper::ValidateNumericResult(resultVec, expected, expectedNull, arg1Values.size());
    
    delete arg1Vec;
    delete arg2Vec;
    delete resultVec;
}

// Test: Integer greatest with 3 arguments
TEST(GreatestTest, IntGreatestThreeArgs) {
    std::cout << "=== Test: INT greatest with 3 arguments ===" << std::endl;
    
    std::vector<int32_t> arg1Values = {100, 500, 300};
    std::vector<int32_t> arg2Values = {400, 200, 600};
    std::vector<int32_t> arg3Values = {250, 350, 450};
    std::vector<int32_t> expected = {400, 500, 600};  // Greatest values
    std::vector<bool> expectedNull = {false, false, false};
    
    BaseVector* arg1Vec = GreatestFunctionTestHelper::CreateNumericVector(arg1Values, OMNI_INT);
    BaseVector* arg2Vec = GreatestFunctionTestHelper::CreateNumericVector(arg2Values, OMNI_INT);
    BaseVector* arg3Vec = GreatestFunctionTestHelper::CreateNumericVector(arg3Values, OMNI_INT);
    
    BaseVector* resultVec = nullptr;
    GreatestFunctionTestHelper::ExecuteGreatest({arg1Vec, arg2Vec, arg3Vec}, OMNI_INT, resultVec);
    GreatestFunctionTestHelper::ValidateNumericResult(resultVec, expected, expectedNull, arg1Values.size());
    
    delete arg1Vec;
    delete arg2Vec;
    delete arg3Vec;
    delete resultVec;
}

// Test: Integer greatest with NULL in first argument
TEST(GreatestTest, IntGreatestWithNullFirst) {
    std::cout << "=== Test: INT greatest with NULL in first argument ===" << std::endl;
    
    std::vector<int32_t> arg1Values = {100, 500, 300};
    std::vector<int32_t> arg2Values = {400, 200, 600};
    std::vector<int32_t> expected = {400, 500, 600};  // NULL is ignored
    std::vector<bool> expectedNull = {false, false, false};
    
    BaseVector* arg1Vec = GreatestFunctionTestHelper::CreateNumericVector(arg1Values, OMNI_INT);
    BaseVector* arg2Vec = GreatestFunctionTestHelper::CreateNumericVector(arg2Values, OMNI_INT);
    
    // Set first value of arg1 to NULL
    arg1Vec->SetNull(0);
    
    BaseVector* resultVec = nullptr;
    GreatestFunctionTestHelper::ExecuteGreatest({arg1Vec, arg2Vec}, OMNI_INT, resultVec);
    GreatestFunctionTestHelper::ValidateNumericResult(resultVec, expected, expectedNull, arg1Values.size());
    
    delete arg1Vec;
    delete arg2Vec;
    delete resultVec;
}

// Test: Integer greatest with all NULL
TEST(GreatestTest, IntGreatestAllNull) {
    std::cout << "=== Test: INT greatest with all NULL ===" << std::endl;
    
    std::vector<int32_t> arg1Values = {100, 200, 300};
    std::vector<int32_t> arg2Values = {400, 500, 600};
    std::vector<int32_t> expected = {0, 0, 0};  // Values don't matter
    std::vector<bool> expectedNull = {true, true, true};
    
    BaseVector* arg1Vec = GreatestFunctionTestHelper::CreateNumericVector(arg1Values, OMNI_INT);
    BaseVector* arg2Vec = GreatestFunctionTestHelper::CreateNumericVector(arg2Values, OMNI_INT);
    
    // Set all values to NULL
    for (int i = 0; i < 3; ++i) {
        arg1Vec->SetNull(i);
        arg2Vec->SetNull(i);
    }
    
    BaseVector* resultVec = nullptr;
    GreatestFunctionTestHelper::ExecuteGreatest({arg1Vec, arg2Vec}, OMNI_INT, resultVec);
    GreatestFunctionTestHelper::ValidateNumericResult(resultVec, expected, expectedNull, arg1Values.size());
    
    delete arg1Vec;
    delete arg2Vec;
    delete resultVec;
}

// ==================== Long Tests ====================

TEST(GreatestTest, LongGreatest) {
    std::cout << "=== Test: LONG greatest ===" << std::endl;
    
    std::vector<int64_t> arg1Values = {1000000000000LL, 2000000000000LL, 3000000000000LL};
    std::vector<int64_t> arg2Values = {4000000000000LL, 1500000000000LL, 2500000000000LL};
    std::vector<int64_t> expected = {4000000000000LL, 2000000000000LL, 3000000000000LL};
    std::vector<bool> expectedNull = {false, false, false};
    
    BaseVector* arg1Vec = GreatestFunctionTestHelper::CreateNumericVector(arg1Values, OMNI_LONG);
    BaseVector* arg2Vec = GreatestFunctionTestHelper::CreateNumericVector(arg2Values, OMNI_LONG);
    
    BaseVector* resultVec = nullptr;
    GreatestFunctionTestHelper::ExecuteGreatest({arg1Vec, arg2Vec}, OMNI_LONG, resultVec);
    GreatestFunctionTestHelper::ValidateNumericResult(resultVec, expected, expectedNull, arg1Values.size());
    
    delete arg1Vec;
    delete arg2Vec;
    delete resultVec;
}

// ==================== Float/Double Tests ====================

TEST(GreatestTest, DoubleGreatest) {
    std::cout << "=== Test: DOUBLE greatest ===" << std::endl;
    
    std::vector<double> arg1Values = {100.5, 200.7, 300.9};
    std::vector<double> arg2Values = {400.1, 150.3, 250.5};
    std::vector<double> expected = {400.1, 200.7, 300.9};
    std::vector<bool> expectedNull = {false, false, false};
    
    BaseVector* arg1Vec = GreatestFunctionTestHelper::CreateNumericVector(arg1Values, OMNI_DOUBLE);
    BaseVector* arg2Vec = GreatestFunctionTestHelper::CreateNumericVector(arg2Values, OMNI_DOUBLE);
    
    BaseVector* resultVec = nullptr;
    GreatestFunctionTestHelper::ExecuteGreatest({arg1Vec, arg2Vec}, OMNI_DOUBLE, resultVec);
    GreatestFunctionTestHelper::ValidateNumericResult(resultVec, expected, expectedNull, arg1Values.size());
    
    delete arg1Vec;
    delete arg2Vec;
    delete resultVec;
}

TEST(GreatestTest, FloatGreatest) {
    std::cout << "=== Test: FLOAT greatest ===" << std::endl;
    
    std::vector<float> arg1Values = {100.5f, 200.7f, 300.9f};
    std::vector<float> arg2Values = {400.1f, 150.3f, 250.5f};
    std::vector<float> expected = {400.1f, 200.7f, 300.9f};
    std::vector<bool> expectedNull = {false, false, false};
    
    BaseVector* arg1Vec = GreatestFunctionTestHelper::CreateNumericVector(arg1Values, OMNI_FLOAT);
    BaseVector* arg2Vec = GreatestFunctionTestHelper::CreateNumericVector(arg2Values, OMNI_FLOAT);
    
    BaseVector* resultVec = nullptr;
    GreatestFunctionTestHelper::ExecuteGreatest({arg1Vec, arg2Vec}, OMNI_FLOAT, resultVec);
    GreatestFunctionTestHelper::ValidateNumericResult(resultVec, expected, expectedNull, arg1Values.size());
    
    delete arg1Vec;
    delete arg2Vec;
    delete resultVec;
}

// Test: NaN handling (Spark SQL semantics - NaN is greater than everything)
TEST(GreatestTest, DoubleGreatestWithNaN) {
    std::cout << "=== Test: DOUBLE greatest with NaN ===" << std::endl;
    
    constexpr double inf = std::numeric_limits<double>::infinity();
    constexpr double nan = std::numeric_limits<double>::quiet_NaN();
    
    std::vector<double> arg1Values = {inf, nan, -inf, 0.0};
    std::vector<double> arg2Values = {nan, -inf, inf, nan};
    std::vector<double> arg3Values = {-inf, 0.0, 0.0, inf};
    // NaN > anything, so results should have NaN where NaN is present
    std::vector<double> expected = {nan, nan, inf, nan};
    std::vector<bool> expectedNull = {false, false, false, false};
    
    BaseVector* arg1Vec = GreatestFunctionTestHelper::CreateNumericVector(arg1Values, OMNI_DOUBLE);
    BaseVector* arg2Vec = GreatestFunctionTestHelper::CreateNumericVector(arg2Values, OMNI_DOUBLE);
    BaseVector* arg3Vec = GreatestFunctionTestHelper::CreateNumericVector(arg3Values, OMNI_DOUBLE);
    
    BaseVector* resultVec = nullptr;
    GreatestFunctionTestHelper::ExecuteGreatest({arg1Vec, arg2Vec, arg3Vec}, OMNI_DOUBLE, resultVec);
    GreatestFunctionTestHelper::ValidateNumericResult(resultVec, expected, expectedNull, arg1Values.size());
    
    delete arg1Vec;
    delete arg2Vec;
    delete arg3Vec;
    delete resultVec;
}

// Test: Infinity handling
TEST(GreatestTest, DoubleGreatestWithInfinity) {
    std::cout << "=== Test: DOUBLE greatest with Infinity ===" << std::endl;
    
    constexpr double inf = std::numeric_limits<double>::infinity();
    
    std::vector<double> arg1Values = {0.0, -inf, inf};
    std::vector<double> arg2Values = {-inf, inf, 0.0};
    std::vector<double> arg3Values = {inf, 0.0, -inf};
    std::vector<double> expected = {inf, inf, inf};
    std::vector<bool> expectedNull = {false, false, false};
    
    BaseVector* arg1Vec = GreatestFunctionTestHelper::CreateNumericVector(arg1Values, OMNI_DOUBLE);
    BaseVector* arg2Vec = GreatestFunctionTestHelper::CreateNumericVector(arg2Values, OMNI_DOUBLE);
    BaseVector* arg3Vec = GreatestFunctionTestHelper::CreateNumericVector(arg3Values, OMNI_DOUBLE);
    
    BaseVector* resultVec = nullptr;
    GreatestFunctionTestHelper::ExecuteGreatest({arg1Vec, arg2Vec, arg3Vec}, OMNI_DOUBLE, resultVec);
    GreatestFunctionTestHelper::ValidateNumericResult(resultVec, expected, expectedNull, arg1Values.size());
    
    delete arg1Vec;
    delete arg2Vec;
    delete arg3Vec;
    delete resultVec;
}

// ==================== Boolean Tests ====================

TEST(GreatestTest, BooleanGreatest) {
    std::cout << "=== Test: BOOLEAN greatest ===" << std::endl;
    
    std::vector<bool> arg1Values = {true, false, false};
    std::vector<bool> arg2Values = {false, true, false};
    std::vector<bool> arg3Values = {false, false, true};
    // true > false
    std::vector<bool> expected = {true, true, true};
    std::vector<bool> expectedNull = {false, false, false};
    
    BaseVector* arg1Vec = GreatestFunctionTestHelper::CreateNumericVector(arg1Values, OMNI_BOOLEAN);
    BaseVector* arg2Vec = GreatestFunctionTestHelper::CreateNumericVector(arg2Values, OMNI_BOOLEAN);
    BaseVector* arg3Vec = GreatestFunctionTestHelper::CreateNumericVector(arg3Values, OMNI_BOOLEAN);
    
    BaseVector* resultVec = nullptr;
    GreatestFunctionTestHelper::ExecuteGreatest({arg1Vec, arg2Vec, arg3Vec}, OMNI_BOOLEAN, resultVec);
    GreatestFunctionTestHelper::ValidateBooleanResult(resultVec, expected, expectedNull, arg1Values.size());
    
    delete arg1Vec;
    delete arg2Vec;
    delete arg3Vec;
    delete resultVec;
}

TEST(GreatestTest, BooleanGreatestAllFalse) {
    std::cout << "=== Test: BOOLEAN greatest all false ===" << std::endl;
    
    std::vector<bool> arg1Values = {false, false, false};
    std::vector<bool> arg2Values = {false, false, false};
    std::vector<bool> expected = {false, false, false};
    std::vector<bool> expectedNull = {false, false, false};
    
    BaseVector* arg1Vec = GreatestFunctionTestHelper::CreateNumericVector(arg1Values, OMNI_BOOLEAN);
    BaseVector* arg2Vec = GreatestFunctionTestHelper::CreateNumericVector(arg2Values, OMNI_BOOLEAN);
    
    BaseVector* resultVec = nullptr;
    GreatestFunctionTestHelper::ExecuteGreatest({arg1Vec, arg2Vec}, OMNI_BOOLEAN, resultVec);
    GreatestFunctionTestHelper::ValidateBooleanResult(resultVec, expected, expectedNull, arg1Values.size());
    
    delete arg1Vec;
    delete arg2Vec;
    delete resultVec;
}

// ==================== String Tests ====================

TEST(GreatestTest, StringGreatest) {
    std::cout << "=== Test: STRING greatest ===" << std::endl;
    
    std::vector<std::string> arg1Values = {"abc", "xyz", "hello"};
    std::vector<std::string> arg2Values = {"def", "uvw", "world"};
    std::vector<std::string> expected = {"def", "xyz", "world"};  // Lexicographically greater
    std::vector<bool> expectedNull = {false, false, false};
    
    BaseVector* arg1Vec = GreatestFunctionTestHelper::CreateStringVector(arg1Values);
    BaseVector* arg2Vec = GreatestFunctionTestHelper::CreateStringVector(arg2Values);
    
    BaseVector* resultVec = nullptr;
    GreatestFunctionTestHelper::ExecuteGreatest({arg1Vec, arg2Vec}, OMNI_VARCHAR, resultVec);
    GreatestFunctionTestHelper::ValidateStringResult(resultVec, expected, expectedNull, arg1Values.size());
    
    delete arg1Vec;
    delete arg2Vec;
    delete resultVec;
}

TEST(GreatestTest, StringGreatestWithNull) {
    std::cout << "=== Test: STRING greatest with NULL ===" << std::endl;
    
    std::vector<std::string> arg1Values = {"abc", "xyz", "hello"};
    std::vector<std::string> arg2Values = {"def", "uvw", "world"};
    std::vector<std::string> expected = {"def", "xyz", "world"};  // NULL ignored
    std::vector<bool> expectedNull = {false, false, false};
    
    BaseVector* arg1Vec = GreatestFunctionTestHelper::CreateStringVector(arg1Values);
    BaseVector* arg2Vec = GreatestFunctionTestHelper::CreateStringVector(arg2Values);
    
    // Set first value of arg1 to NULL
    arg1Vec->SetNull(0);
    
    BaseVector* resultVec = nullptr;
    GreatestFunctionTestHelper::ExecuteGreatest({arg1Vec, arg2Vec}, OMNI_VARCHAR, resultVec);
    GreatestFunctionTestHelper::ValidateStringResult(resultVec, expected, expectedNull, arg1Values.size());
    
    delete arg1Vec;
    delete arg2Vec;
    delete resultVec;
}

TEST(GreatestTest, StringGreatestVaryingLengths) {
    std::cout << "=== Test: STRING greatest with varying lengths ===" << std::endl;
    
    std::vector<std::string> arg1Values = {"b", "abcde", "abcdefg"};
    std::vector<std::string> arg2Values = {"abcde", "abcdefg", "b"};
    // Lexicographic comparison: "b" > "abcde" > "abcdefg"
    std::vector<std::string> expected = {"b", "abcdefg", "b"};
    std::vector<bool> expectedNull = {false, false, false};
    
    BaseVector* arg1Vec = GreatestFunctionTestHelper::CreateStringVector(arg1Values);
    BaseVector* arg2Vec = GreatestFunctionTestHelper::CreateStringVector(arg2Values);
    
    BaseVector* resultVec = nullptr;
    GreatestFunctionTestHelper::ExecuteGreatest({arg1Vec, arg2Vec}, OMNI_VARCHAR, resultVec);
    GreatestFunctionTestHelper::ValidateStringResult(resultVec, expected, expectedNull, arg1Values.size());
    
    delete arg1Vec;
    delete arg2Vec;
    delete resultVec;
}

// ==================== Byte Tests ====================

TEST(GreatestTest, ByteGreatest) {
    std::cout << "=== Test: BYTE greatest ===" << std::endl;
    
    std::vector<int8_t> arg1Values = {10, 50, 30};
    std::vector<int8_t> arg2Values = {40, 20, 60};
    std::vector<int8_t> expected = {40, 50, 60};
    std::vector<bool> expectedNull = {false, false, false};
    
    BaseVector* arg1Vec = GreatestFunctionTestHelper::CreateNumericVector(arg1Values, OMNI_BYTE);
    BaseVector* arg2Vec = GreatestFunctionTestHelper::CreateNumericVector(arg2Values, OMNI_BYTE);
    
    BaseVector* resultVec = nullptr;
    GreatestFunctionTestHelper::ExecuteGreatest({arg1Vec, arg2Vec}, OMNI_BYTE, resultVec);
    GreatestFunctionTestHelper::ValidateNumericResult(resultVec, expected, expectedNull, arg1Values.size());
    
    delete arg1Vec;
    delete arg2Vec;
    delete resultVec;
}

// ==================== Short Tests ====================

TEST(GreatestTest, ShortGreatest) {
    std::cout << "=== Test: SHORT greatest ===" << std::endl;
    
    std::vector<int16_t> arg1Values = {1000, 5000, 3000};
    std::vector<int16_t> arg2Values = {4000, 2000, 6000};
    std::vector<int16_t> expected = {4000, 5000, 6000};
    std::vector<bool> expectedNull = {false, false, false};
    
    BaseVector* arg1Vec = GreatestFunctionTestHelper::CreateNumericVector(arg1Values, OMNI_SHORT);
    BaseVector* arg2Vec = GreatestFunctionTestHelper::CreateNumericVector(arg2Values, OMNI_SHORT);
    
    BaseVector* resultVec = nullptr;
    GreatestFunctionTestHelper::ExecuteGreatest({arg1Vec, arg2Vec}, OMNI_SHORT, resultVec);
    GreatestFunctionTestHelper::ValidateNumericResult(resultVec, expected, expectedNull, arg1Values.size());
    
    delete arg1Vec;
    delete arg2Vec;
    delete resultVec;
}

// ==================== Date Tests ====================

TEST(GreatestTest, DateGreatest) {
    std::cout << "=== Test: DATE greatest ===" << std::endl;
    
    // Date is stored as int32_t (days since epoch)
    std::vector<int32_t> arg1Values = {100, 1000, 10000};
    std::vector<int32_t> arg2Values = {500, 500, 5000};
    std::vector<int32_t> expected = {500, 1000, 10000};
    std::vector<bool> expectedNull = {false, false, false};
    
    BaseVector* arg1Vec = GreatestFunctionTestHelper::CreateNumericVector(arg1Values, OMNI_DATE32);
    BaseVector* arg2Vec = GreatestFunctionTestHelper::CreateNumericVector(arg2Values, OMNI_DATE32);
    
    BaseVector* resultVec = nullptr;
    GreatestFunctionTestHelper::ExecuteGreatest({arg1Vec, arg2Vec}, OMNI_DATE32, resultVec);
    GreatestFunctionTestHelper::ValidateNumericResult(resultVec, expected, expectedNull, arg1Values.size());
    
    delete arg1Vec;
    delete arg2Vec;
    delete resultVec;
}

// ==================== Timestamp Tests ====================

TEST(GreatestTest, TimestampGreatest) {
    std::cout << "=== Test: TIMESTAMP greatest ===" << std::endl;
    
    // Timestamp is stored as int64_t (microseconds since epoch)
    std::vector<int64_t> arg1Values = {1569000000025LL, 4859000000482LL, 581000001651LL};
    std::vector<int64_t> arg2Values = {2000000000000LL, 3000000000000LL, 1000000000000LL};
    std::vector<int64_t> expected = {2000000000000LL, 4859000000482LL, 1000000000000LL};
    std::vector<bool> expectedNull = {false, false, false};
    
    BaseVector* arg1Vec = GreatestFunctionTestHelper::CreateNumericVector(arg1Values, OMNI_TIMESTAMP);
    BaseVector* arg2Vec = GreatestFunctionTestHelper::CreateNumericVector(arg2Values, OMNI_TIMESTAMP);
    
    BaseVector* resultVec = nullptr;
    GreatestFunctionTestHelper::ExecuteGreatest({arg1Vec, arg2Vec}, OMNI_TIMESTAMP, resultVec);
    GreatestFunctionTestHelper::ValidateNumericResult(resultVec, expected, expectedNull, arg1Values.size());
    
    delete arg1Vec;
    delete arg2Vec;
    delete resultVec;
}

// ==================== Decimal64 Tests ====================

TEST(GreatestTest, Decimal64Greatest) {
    std::cout << "=== Test: DECIMAL64 greatest ===" << std::endl;
    
    // Decimal64 is stored as int64_t with a scale
    std::vector<int64_t> arg1Values = {10050LL, 20070LL, 30090LL};  // 100.50, 200.70, 300.90 with scale 2
    std::vector<int64_t> arg2Values = {40010LL, 15030LL, 25050LL};  // 400.10, 150.30, 250.50 with scale 2
    std::vector<int64_t> expected = {40010LL, 20070LL, 30090LL};
    std::vector<bool> expectedNull = {false, false, false};
    
    BaseVector* arg1Vec = GreatestFunctionTestHelper::CreateNumericVector(arg1Values, OMNI_DECIMAL64);
    BaseVector* arg2Vec = GreatestFunctionTestHelper::CreateNumericVector(arg2Values, OMNI_DECIMAL64);
    
    BaseVector* resultVec = nullptr;
    GreatestFunctionTestHelper::ExecuteGreatest({arg1Vec, arg2Vec}, OMNI_DECIMAL64, resultVec);
    GreatestFunctionTestHelper::ValidateNumericResult(resultVec, expected, expectedNull, arg1Values.size());
    
    delete arg1Vec;
    delete arg2Vec;
    delete resultVec;
}

// ==================== Edge Cases ====================

// Test: Mixed NULL pattern
TEST(GreatestTest, MixedNullPattern) {
    std::cout << "=== Test: Mixed NULL pattern ===" << std::endl;
    
    std::vector<int32_t> arg1Values = {100, 200, 300};
    std::vector<int32_t> arg2Values = {400, 500, 600};
    std::vector<int32_t> arg3Values = {250, 350, 450};
    // Row 0: arg1 is NULL, should get max(400, 250) = 400
    // Row 1: arg2 is NULL, should get max(200, 350) = 350
    // Row 2: arg3 is NULL, should get max(300, 600) = 600
    std::vector<int32_t> expected = {400, 350, 600};
    std::vector<bool> expectedNull = {false, false, false};
    
    BaseVector* arg1Vec = GreatestFunctionTestHelper::CreateNumericVector(arg1Values, OMNI_INT);
    BaseVector* arg2Vec = GreatestFunctionTestHelper::CreateNumericVector(arg2Values, OMNI_INT);
    BaseVector* arg3Vec = GreatestFunctionTestHelper::CreateNumericVector(arg3Values, OMNI_INT);
    
    arg1Vec->SetNull(0);
    arg2Vec->SetNull(1);
    arg3Vec->SetNull(2);
    
    BaseVector* resultVec = nullptr;
    GreatestFunctionTestHelper::ExecuteGreatest({arg1Vec, arg2Vec, arg3Vec}, OMNI_INT, resultVec);
    GreatestFunctionTestHelper::ValidateNumericResult(resultVec, expected, expectedNull, arg1Values.size());
    
    delete arg1Vec;
    delete arg2Vec;
    delete arg3Vec;
    delete resultVec;
}

// Test: Negative numbers
TEST(GreatestTest, NegativeNumbers) {
    std::cout << "=== Test: Negative numbers ===" << std::endl;
    
    std::vector<int32_t> arg1Values = {-100, -500, -300};
    std::vector<int32_t> arg2Values = {-400, -200, -600};
    std::vector<int32_t> expected = {-100, -200, -300};  // Greater negative (closer to 0)
    std::vector<bool> expectedNull = {false, false, false};
    
    BaseVector* arg1Vec = GreatestFunctionTestHelper::CreateNumericVector(arg1Values, OMNI_INT);
    BaseVector* arg2Vec = GreatestFunctionTestHelper::CreateNumericVector(arg2Values, OMNI_INT);
    
    BaseVector* resultVec = nullptr;
    GreatestFunctionTestHelper::ExecuteGreatest({arg1Vec, arg2Vec}, OMNI_INT, resultVec);
    GreatestFunctionTestHelper::ValidateNumericResult(resultVec, expected, expectedNull, arg1Values.size());
    
    delete arg1Vec;
    delete arg2Vec;
    delete resultVec;
}

// Test: Same values
TEST(GreatestTest, SameValues) {
    std::cout << "=== Test: Same values ===" << std::endl;
    
    std::vector<int32_t> arg1Values = {100, 200, 300};
    std::vector<int32_t> arg2Values = {100, 200, 300};
    std::vector<int32_t> expected = {100, 200, 300};
    std::vector<bool> expectedNull = {false, false, false};
    
    BaseVector* arg1Vec = GreatestFunctionTestHelper::CreateNumericVector(arg1Values, OMNI_INT);
    BaseVector* arg2Vec = GreatestFunctionTestHelper::CreateNumericVector(arg2Values, OMNI_INT);
    
    BaseVector* resultVec = nullptr;
    GreatestFunctionTestHelper::ExecuteGreatest({arg1Vec, arg2Vec}, OMNI_INT, resultVec);
    GreatestFunctionTestHelper::ValidateNumericResult(resultVec, expected, expectedNull, arg1Values.size());
    
    delete arg1Vec;
    delete arg2Vec;
    delete resultVec;
}
