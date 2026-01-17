/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
* Description: If function unit tests
*/

#include <gtest/gtest.h>
#include <iostream>
#include <string>
#include <vector>
#include <limits>
#include <cmath>

#include "test/util/test_util.h"
#include "vectorization/registration/Register.h"
#include "vectorization/functions/If.h"
#include "vectorization/registration/SimpleFunctionRegistry.h"
#include "vectorization/VectorFunction.h"
#include "codegen/func_signature.h"
#include "vector/vector_helper.h"
#include "vector/vector.h"
#include <gtest/gtest.h>

using namespace omniruntime;
using namespace omniruntime::vec;
using namespace omniruntime::vectorization;
using namespace omniruntime::mem;
using namespace omniruntime::op;
using namespace omniruntime::type;
using namespace omniruntime::codegen;
using namespace omniruntime::TestUtil;

// Initialize function registration before running tests
class IfTestEnvironment : public ::testing::Environment {
public:
    void SetUp() override {
        RegisterFunctions::RegisterAllFunctions("");
    }
};

::testing::Environment* const if_test_env = ::testing::AddGlobalTestEnvironment(new IfTestEnvironment);

class IfFunctionTestHelper {
public:
    template<typename T>
    static void ValidateNumericResult(BaseVector* result, const std::vector<T>& expected, int rowSize) {
        auto* resultVec = dynamic_cast<Vector<T>*>(result);
        ASSERT_NE(resultVec, nullptr) << "Result vector type mismatch";
        
        for (int i = 0; i < rowSize; ++i) {
            if (result->IsNull(i)) {
                std::cout << "Row " << i << ": NULL" << std::endl;
                continue;
            }
            T actualValue = resultVec->GetValue(i);
            T expectedValue = expected[i];
            std::cout << "Row " << i << ": Expected=" << expectedValue << ", Actual=" << actualValue << std::endl;
            
            if constexpr (std::is_floating_point_v<T>) {
                EXPECT_NEAR(actualValue, expectedValue, 1e-6) 
                    << "Row " << i << " value mismatch";
            } else {
                EXPECT_EQ(actualValue, expectedValue) 
                    << "Row " << i << " value mismatch";
            }
        }
    }
    
    static void ValidateStringResult(BaseVector* result, const std::vector<std::string>& expected, int rowSize) {
        auto* resultVec = dynamic_cast<Vector<LargeStringContainer<std::string_view>>*>(result);
        ASSERT_NE(resultVec, nullptr) << "Result vector is not string type";
        
        for (int i = 0; i < rowSize; ++i) {
            if (result->IsNull(i)) {
                std::cout << "Row " << i << ": NULL" << std::endl;
                continue;
            }
            std::string_view actualValue = resultVec->GetValue(i);
            std::string expectedValue = expected[i];
            std::cout << "Row " << i << ": Expected=\"" << expectedValue 
                    << "\", Actual=\"" << actualValue << "\"" << std::endl;
            EXPECT_EQ(actualValue, expectedValue) << "Row " << i << " value mismatch";
        }
    }
    
    static void ValidateBooleanResult(BaseVector* result, const std::vector<bool>& expected, int rowSize) {
        auto* resultVec = dynamic_cast<Vector<bool>*>(result);
        ASSERT_NE(resultVec, nullptr) << "Result vector is not boolean type";
        
        for (int i = 0; i < rowSize; ++i) {
            if (result->IsNull(i)) {
                std::cout << "Row " << i << ": NULL" << std::endl;
                continue;
            }
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
    
    static BaseVector* CreateBooleanVector(const std::vector<bool>& values) {
        BaseVector* vec = VectorHelper::CreateFlatVector(OMNI_BOOLEAN, values.size());
        auto* typedVec = static_cast<Vector<bool>*>(vec);
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
    
    static void ExecuteIf(BaseVector* condVec, BaseVector* trueVec, BaseVector* falseVec, 
                          DataTypeId outputTypeId, BaseVector*& result) {
        auto signature = std::make_shared<FunctionSignature>("if", 
            std::vector<DataTypeId>{OMNI_BOOLEAN, outputTypeId, outputTypeId}, outputTypeId);
        auto function = VectorFunction::Find(signature);
        ASSERT_NE(function, nullptr) << "If function not found for signature";
        
        auto outputType = std::make_shared<DataType>(outputTypeId);
        ExecutionContext context;
        context.SetResultRowSize(condVec->GetSize());
        std::stack<BaseVector*> args;
        
        // Push arguments in reverse order (stack order: false, true, condition)
        args.push(falseVec);
        args.push(trueVec);
        args.push(condVec);
        
        ASSERT_NO_THROW(function->Apply(args, outputType, result, &context))
            << "If function threw an exception";
    }
};

// Test: Integer if with true condition
TEST(IfTest, IntIfTrue) {
    std::cout << "=== Test: INT if with true condition ===" << std::endl;
    
    std::vector<bool> condValues = {true, true, true};
    std::vector<int32_t> trueValues = {100, 200, 300};
    std::vector<int32_t> falseValues = {400, 500, 600};
    std::vector<int32_t> expected = {100, 200, 300};  // Should return true values
    
    BaseVector* condVec = IfFunctionTestHelper::CreateBooleanVector(condValues);
    BaseVector* trueVec = IfFunctionTestHelper::CreateNumericVector(trueValues, OMNI_INT);
    BaseVector* falseVec = IfFunctionTestHelper::CreateNumericVector(falseValues, OMNI_INT);
    
    BaseVector* resultVec = nullptr;
    IfFunctionTestHelper::ExecuteIf(condVec, trueVec, falseVec, OMNI_INT, resultVec);
    IfFunctionTestHelper::ValidateNumericResult(resultVec, expected, condValues.size());
    
    delete condVec;
    delete trueVec;
    delete falseVec;
    delete resultVec;
}

// Test: Integer if with false condition
TEST(IfTest, IntIfFalse) {
    std::cout << "=== Test: INT if with false condition ===" << std::endl;
    
    std::vector<bool> condValues = {false, false, false};
    std::vector<int32_t> trueValues = {100, 200, 300};
    std::vector<int32_t> falseValues = {400, 500, 600};
    std::vector<int32_t> expected = {400, 500, 600};  // Should return false values
    
    BaseVector* condVec = IfFunctionTestHelper::CreateBooleanVector(condValues);
    BaseVector* trueVec = IfFunctionTestHelper::CreateNumericVector(trueValues, OMNI_INT);
    BaseVector* falseVec = IfFunctionTestHelper::CreateNumericVector(falseValues, OMNI_INT);
    
    BaseVector* resultVec = nullptr;
    IfFunctionTestHelper::ExecuteIf(condVec, trueVec, falseVec, OMNI_INT, resultVec);
    IfFunctionTestHelper::ValidateNumericResult(resultVec, expected, condValues.size());
    
    delete condVec;
    delete trueVec;
    delete falseVec;
    delete resultVec;
}

// Test: Integer if with mixed conditions
TEST(IfTest, IntIfMixed) {
    std::cout << "=== Test: INT if with mixed conditions ===" << std::endl;
    
    std::vector<bool> condValues = {true, false, true};
    std::vector<int32_t> trueValues = {100, 200, 300};
    std::vector<int32_t> falseValues = {400, 500, 600};
    std::vector<int32_t> expected = {100, 500, 300};  // Mixed results
    
    BaseVector* condVec = IfFunctionTestHelper::CreateBooleanVector(condValues);
    BaseVector* trueVec = IfFunctionTestHelper::CreateNumericVector(trueValues, OMNI_INT);
    BaseVector* falseVec = IfFunctionTestHelper::CreateNumericVector(falseValues, OMNI_INT);
    
    BaseVector* resultVec = nullptr;
    IfFunctionTestHelper::ExecuteIf(condVec, trueVec, falseVec, OMNI_INT, resultVec);
    IfFunctionTestHelper::ValidateNumericResult(resultVec, expected, condValues.size());
    
    delete condVec;
    delete trueVec;
    delete falseVec;
    delete resultVec;
}

// Test: Integer if with NULL condition
TEST(IfTest, IntIfNullCondition) {
    std::cout << "=== Test: INT if with NULL condition ===" << std::endl;
    
    std::vector<bool> condValues = {true, false, true};
    std::vector<int32_t> trueValues = {100, 200, 300};
    std::vector<int32_t> falseValues = {400, 500, 600};
    
    BaseVector* condVec = IfFunctionTestHelper::CreateBooleanVector(condValues);
    BaseVector* trueVec = IfFunctionTestHelper::CreateNumericVector(trueValues, OMNI_INT);
    BaseVector* falseVec = IfFunctionTestHelper::CreateNumericVector(falseValues, OMNI_INT);
    
    // Set middle condition to NULL
    condVec->SetNull(1);
    
    BaseVector* resultVec = nullptr;
    IfFunctionTestHelper::ExecuteIf(condVec, trueVec, falseVec, OMNI_INT, resultVec);
    
    // When condition is NULL, result should be NULL
    EXPECT_FALSE(resultVec->IsNull(0)) << "Row 0 should not be NULL";
    EXPECT_TRUE(resultVec->IsNull(1)) << "Row 1 should be NULL (condition is NULL)";
    EXPECT_FALSE(resultVec->IsNull(2)) << "Row 2 should not be NULL";
    
    delete condVec;
    delete trueVec;
    delete falseVec;
    delete resultVec;
}

// Test: Integer if with NULL true value
TEST(IfTest, IntIfNullTrueValue) {
    std::cout << "=== Test: INT if with NULL true value ===" << std::endl;
    
    std::vector<bool> condValues = {true, true, false};
    std::vector<int32_t> trueValues = {100, 200, 300};
    std::vector<int32_t> falseValues = {400, 500, 600};
    
    BaseVector* condVec = IfFunctionTestHelper::CreateBooleanVector(condValues);
    BaseVector* trueVec = IfFunctionTestHelper::CreateNumericVector(trueValues, OMNI_INT);
    BaseVector* falseVec = IfFunctionTestHelper::CreateNumericVector(falseValues, OMNI_INT);
    
    // Set first true value to NULL
    trueVec->SetNull(0);
    
    BaseVector* resultVec = nullptr;
    IfFunctionTestHelper::ExecuteIf(condVec, trueVec, falseVec, OMNI_INT, resultVec);
    
    // When true value is NULL and condition is true, result should be NULL
    EXPECT_TRUE(resultVec->IsNull(0)) << "Row 0 should be NULL (true value is NULL)";
    EXPECT_FALSE(resultVec->IsNull(1)) << "Row 1 should not be NULL";
    EXPECT_FALSE(resultVec->IsNull(2)) << "Row 2 should not be NULL";
    
    delete condVec;
    delete trueVec;
    delete falseVec;
    delete resultVec;
}

// Test: Long if
TEST(IfTest, LongIf) {
    std::cout << "=== Test: LONG if ===" << std::endl;
    
    std::vector<bool> condValues = {true, false, true};
    std::vector<int64_t> trueValues = {100LL, 200LL, 300LL};
    std::vector<int64_t> falseValues = {400LL, 500LL, 600LL};
    std::vector<int64_t> expected = {100LL, 500LL, 300LL};
    
    BaseVector* condVec = IfFunctionTestHelper::CreateBooleanVector(condValues);
    BaseVector* trueVec = IfFunctionTestHelper::CreateNumericVector(trueValues, OMNI_LONG);
    BaseVector* falseVec = IfFunctionTestHelper::CreateNumericVector(falseValues, OMNI_LONG);
    
    BaseVector* resultVec = nullptr;
    IfFunctionTestHelper::ExecuteIf(condVec, trueVec, falseVec, OMNI_LONG, resultVec);
    IfFunctionTestHelper::ValidateNumericResult(resultVec, expected, condValues.size());
    
    delete condVec;
    delete trueVec;
    delete falseVec;
    delete resultVec;
}

// Test: Double if
TEST(IfTest, DoubleIf) {
    std::cout << "=== Test: DOUBLE if ===" << std::endl;
    
    std::vector<bool> condValues = {true, false, true};
    std::vector<double> trueValues = {100.5, 200.7, 300.9};
    std::vector<double> falseValues = {400.1, 500.3, 600.5};
    std::vector<double> expected = {100.5, 500.3, 300.9};
    
    BaseVector* condVec = IfFunctionTestHelper::CreateBooleanVector(condValues);
    BaseVector* trueVec = IfFunctionTestHelper::CreateNumericVector(trueValues, OMNI_DOUBLE);
    BaseVector* falseVec = IfFunctionTestHelper::CreateNumericVector(falseValues, OMNI_DOUBLE);
    
    BaseVector* resultVec = nullptr;
    IfFunctionTestHelper::ExecuteIf(condVec, trueVec, falseVec, OMNI_DOUBLE, resultVec);
    IfFunctionTestHelper::ValidateNumericResult(resultVec, expected, condValues.size());
    
    delete condVec;
    delete trueVec;
    delete falseVec;
    delete resultVec;
}

// Test: String if
TEST(IfTest, StringIf) {
    std::cout << "=== Test: STRING if ===" << std::endl;
    
    std::vector<bool> condValues = {true, false, true};
    std::vector<std::string> trueValues = {"hello", "world", "test"};
    std::vector<std::string> falseValues = {"foo", "bar", "baz"};
    std::vector<std::string> expected = {"hello", "bar", "test"};
    
    BaseVector* condVec = IfFunctionTestHelper::CreateBooleanVector(condValues);
    BaseVector* trueVec = IfFunctionTestHelper::CreateStringVector(trueValues);
    BaseVector* falseVec = IfFunctionTestHelper::CreateStringVector(falseValues);
    
    BaseVector* resultVec = nullptr;
    IfFunctionTestHelper::ExecuteIf(condVec, trueVec, falseVec, OMNI_VARCHAR, resultVec);
    IfFunctionTestHelper::ValidateStringResult(resultVec, expected, condValues.size());
    
    delete condVec;
    delete trueVec;
    delete falseVec;
    delete resultVec;
}

// Test: String if with NULL condition
TEST(IfTest, StringIfNullCondition) {
    std::cout << "=== Test: STRING if with NULL condition ===" << std::endl;
    
    std::vector<bool> condValues = {true, false, true};
    std::vector<std::string> trueValues = {"hello", "world", "test"};
    std::vector<std::string> falseValues = {"foo", "bar", "baz"};
    
    BaseVector* condVec = IfFunctionTestHelper::CreateBooleanVector(condValues);
    BaseVector* trueVec = IfFunctionTestHelper::CreateStringVector(trueValues);
    BaseVector* falseVec = IfFunctionTestHelper::CreateStringVector(falseValues);
    
    // Set middle condition to NULL
    condVec->SetNull(1);
    
    BaseVector* resultVec = nullptr;
    IfFunctionTestHelper::ExecuteIf(condVec, trueVec, falseVec, OMNI_VARCHAR, resultVec);
    
    // When condition is NULL, result should be NULL
    EXPECT_FALSE(resultVec->IsNull(0)) << "Row 0 should not be NULL";
    EXPECT_TRUE(resultVec->IsNull(1)) << "Row 1 should be NULL (condition is NULL)";
    EXPECT_FALSE(resultVec->IsNull(2)) << "Row 2 should not be NULL";
    
    delete condVec;
    delete trueVec;
    delete falseVec;
    delete resultVec;
}

// Test: Boolean if
TEST(IfTest, BooleanIf) {
    std::cout << "=== Test: BOOLEAN if ===" << std::endl;
    
    std::vector<bool> condValues = {true, false, true};
    std::vector<bool> trueValues = {true, true, false};
    std::vector<bool> falseValues = {false, false, true};
    std::vector<bool> expected = {true, false, false};
    
    BaseVector* condVec = IfFunctionTestHelper::CreateBooleanVector(condValues);
    BaseVector* trueVec = IfFunctionTestHelper::CreateBooleanVector(trueValues);
    BaseVector* falseVec = IfFunctionTestHelper::CreateBooleanVector(falseValues);
    
    BaseVector* resultVec = nullptr;
    IfFunctionTestHelper::ExecuteIf(condVec, trueVec, falseVec, OMNI_BOOLEAN, resultVec);
    IfFunctionTestHelper::ValidateBooleanResult(resultVec, expected, condValues.size());
    
    delete condVec;
    delete trueVec;
    delete falseVec;
    delete resultVec;
}

// Test: Boolean if with NULL condition
TEST(IfTest, BooleanIfNullCondition) {
    std::cout << "=== Test: BOOLEAN if with NULL condition ===" << std::endl;
    
    std::vector<bool> condValues = {true, false, true};
    std::vector<bool> trueValues = {true, true, false};
    std::vector<bool> falseValues = {false, false, true};
    
    BaseVector* condVec = IfFunctionTestHelper::CreateBooleanVector(condValues);
    BaseVector* trueVec = IfFunctionTestHelper::CreateBooleanVector(trueValues);
    BaseVector* falseVec = IfFunctionTestHelper::CreateBooleanVector(falseValues);
    
    // Set middle condition to NULL
    condVec->SetNull(1);
    
    BaseVector* resultVec = nullptr;
    IfFunctionTestHelper::ExecuteIf(condVec, trueVec, falseVec, OMNI_BOOLEAN, resultVec);
    
    // When condition is NULL, result should be NULL
    EXPECT_FALSE(resultVec->IsNull(0)) << "Row 0 should not be NULL";
    EXPECT_TRUE(resultVec->IsNull(1)) << "Row 1 should be NULL (condition is NULL)";
    EXPECT_FALSE(resultVec->IsNull(2)) << "Row 2 should not be NULL";
    
    delete condVec;
    delete trueVec;
    delete falseVec;
    delete resultVec;
}

// Test: Float if
TEST(IfTest, FloatIf) {
    std::cout << "=== Test: FLOAT if ===" << std::endl;
    
    std::vector<bool> condValues = {true, false, true};
    std::vector<float> trueValues = {100.5f, 200.7f, 300.9f};
    std::vector<float> falseValues = {400.1f, 500.3f, 600.5f};
    std::vector<float> expected = {100.5f, 500.3f, 300.9f};
    
    BaseVector* condVec = IfFunctionTestHelper::CreateBooleanVector(condValues);
    BaseVector* trueVec = IfFunctionTestHelper::CreateNumericVector(trueValues, OMNI_FLOAT);
    BaseVector* falseVec = IfFunctionTestHelper::CreateNumericVector(falseValues, OMNI_FLOAT);
    
    BaseVector* resultVec = nullptr;
    IfFunctionTestHelper::ExecuteIf(condVec, trueVec, falseVec, OMNI_FLOAT, resultVec);
    IfFunctionTestHelper::ValidateNumericResult(resultVec, expected, condValues.size());
    
    delete condVec;
    delete trueVec;
    delete falseVec;
    delete resultVec;
}
