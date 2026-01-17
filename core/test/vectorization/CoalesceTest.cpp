/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
* Description: Coalesce function unit tests
*/

#include <gtest/gtest.h>
#include <iostream>
#include <string>
#include <vector>
#include <limits>
#include <cmath>

#include "test/util/test_util.h"
#include "vectorization/registration/Register.h"
#include "vectorization/functions/Coalesce.h"
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
class CoalesceTestEnvironment : public ::testing::Environment {
public:
    void SetUp() override {
        RegisterFunctions::RegisterAllFunctions("");
    }
};

::testing::Environment* const coalesce_test_env = ::testing::AddGlobalTestEnvironment(new CoalesceTestEnvironment);

class CoalesceFunctionTestHelper {
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
    
    static BaseVector* CreateStringVector(const std::vector<std::string>& values) {
        BaseVector* vec = VectorHelper::CreateStringVector(values.size());
        auto* typedVec = dynamic_cast<Vector<LargeStringContainer<std::string_view>>*>(vec);
        for (size_t i = 0; i < values.size(); ++i) {
            std::string_view sv(values[i]);
            typedVec->SetValue(i, sv);
        }
        return vec;
    }
    
    static void ExecuteCoalesce(const std::vector<BaseVector*>& inputs, DataTypeId outputTypeId, BaseVector*& result) {
        std::vector<DataTypeId> inputTypeIds;
        for (auto* input : inputs) {
            inputTypeIds.push_back(input->GetTypeId());
        }
        
        auto signature = std::make_shared<FunctionSignature>("coalesce", inputTypeIds, outputTypeId);
        auto function = VectorFunction::Find(signature);
        ASSERT_NE(function, nullptr) << "Coalesce function not found for signature";
        
        auto outputType = std::make_shared<DataType>(outputTypeId);
        ExecutionContext context;
        context.SetResultRowSize(inputs[0]->GetSize());
        std::stack<BaseVector*> args;
        
        // Push arguments in reverse order (stack order)
        for (auto it = inputs.rbegin(); it != inputs.rend(); ++it) {
            args.push(*it);
        }
        
        ASSERT_NO_THROW(function->Apply(args, outputType, result, &context))
            << "Coalesce function threw an exception";
    }
};

// Test: Integer coalesce with 2 arguments
TEST(CoalesceTest, IntCoalesceTwoArgs) {
    std::cout << "=== Test: INT coalesce with 2 arguments ===" << std::endl;
    
    std::vector<int32_t> arg1Values = {100, 200, 300};
    std::vector<int32_t> arg2Values = {400, 500, 600};
    std::vector<int32_t> expected = {100, 200, 300};  // First non-null
    
    BaseVector* arg1Vec = CoalesceFunctionTestHelper::CreateNumericVector(arg1Values, OMNI_INT);
    BaseVector* arg2Vec = CoalesceFunctionTestHelper::CreateNumericVector(arg2Values, OMNI_INT);
    
    BaseVector* resultVec = nullptr;
    CoalesceFunctionTestHelper::ExecuteCoalesce({arg1Vec, arg2Vec}, OMNI_INT, resultVec);
    CoalesceFunctionTestHelper::ValidateNumericResult(resultVec, expected, arg1Values.size());
    
    delete arg1Vec;
    delete arg2Vec;
    delete resultVec;
}

// Test: Integer coalesce with NULL in first argument
TEST(CoalesceTest, IntCoalesceWithNullFirst) {
    std::cout << "=== Test: INT coalesce with NULL in first argument ===" << std::endl;
    
    std::vector<int32_t> arg1Values = {100, 200, 300};
    std::vector<int32_t> arg2Values = {400, 500, 600};
    std::vector<int32_t> expected = {400, 200, 300};  // Second when first is null
    
    BaseVector* arg1Vec = CoalesceFunctionTestHelper::CreateNumericVector(arg1Values, OMNI_INT);
    BaseVector* arg2Vec = CoalesceFunctionTestHelper::CreateNumericVector(arg2Values, OMNI_INT);
    
    // Set first value of arg1 to NULL
    arg1Vec->SetNull(0);
    
    BaseVector* resultVec = nullptr;
    CoalesceFunctionTestHelper::ExecuteCoalesce({arg1Vec, arg2Vec}, OMNI_INT, resultVec);
    CoalesceFunctionTestHelper::ValidateNumericResult(resultVec, expected, arg1Values.size());
    
    delete arg1Vec;
    delete arg2Vec;
    delete resultVec;
}

// Test: Integer coalesce with all NULL
TEST(CoalesceTest, IntCoalesceAllNull) {
    std::cout << "=== Test: INT coalesce with all NULL ===" << std::endl;
    
    std::vector<int32_t> arg1Values = {100, 200, 300};
    std::vector<int32_t> arg2Values = {400, 500, 600};
    
    BaseVector* arg1Vec = CoalesceFunctionTestHelper::CreateNumericVector(arg1Values, OMNI_INT);
    BaseVector* arg2Vec = CoalesceFunctionTestHelper::CreateNumericVector(arg2Values, OMNI_INT);
    
    // Set all values to NULL
    for (int i = 0; i < 3; ++i) {
        arg1Vec->SetNull(i);
        arg2Vec->SetNull(i);
    }
    
    BaseVector* resultVec = nullptr;
    CoalesceFunctionTestHelper::ExecuteCoalesce({arg1Vec, arg2Vec}, OMNI_INT, resultVec);
    
    // All results should be NULL
    for (int i = 0; i < 3; ++i) {
        EXPECT_TRUE(resultVec->IsNull(i)) << "Row " << i << " should be NULL";
    }
    
    delete arg1Vec;
    delete arg2Vec;
    delete resultVec;
}

// Test: Integer coalesce with 3 arguments
TEST(CoalesceTest, IntCoalesceThreeArgs) {
    std::cout << "=== Test: INT coalesce with 3 arguments ===" << std::endl;
    
    std::vector<int32_t> arg1Values = {100, 200, 300};
    std::vector<int32_t> arg2Values = {400, 500, 600};
    std::vector<int32_t> arg3Values = {700, 800, 900};
    std::vector<int32_t> expected = {100, 200, 300};  // First non-null
    
    BaseVector* arg1Vec = CoalesceFunctionTestHelper::CreateNumericVector(arg1Values, OMNI_INT);
    BaseVector* arg2Vec = CoalesceFunctionTestHelper::CreateNumericVector(arg2Values, OMNI_INT);
    BaseVector* arg3Vec = CoalesceFunctionTestHelper::CreateNumericVector(arg3Values, OMNI_INT);
    
    BaseVector* resultVec = nullptr;
    CoalesceFunctionTestHelper::ExecuteCoalesce({arg1Vec, arg2Vec, arg3Vec}, OMNI_INT, resultVec);
    CoalesceFunctionTestHelper::ValidateNumericResult(resultVec, expected, arg1Values.size());
    
    delete arg1Vec;
    delete arg2Vec;
    delete arg3Vec;
    delete resultVec;
}

// Test: Integer coalesce with 3 arguments, first two NULL
TEST(CoalesceTest, IntCoalesceThreeArgsFirstTwoNull) {
    std::cout << "=== Test: INT coalesce with 3 arguments, first two NULL ===" << std::endl;
    
    std::vector<int32_t> arg1Values = {100, 200, 300};
    std::vector<int32_t> arg2Values = {400, 500, 600};
    std::vector<int32_t> arg3Values = {700, 800, 900};
    std::vector<int32_t> expected = {700, 200, 300};  // Third when first two are null
    
    BaseVector* arg1Vec = CoalesceFunctionTestHelper::CreateNumericVector(arg1Values, OMNI_INT);
    BaseVector* arg2Vec = CoalesceFunctionTestHelper::CreateNumericVector(arg2Values, OMNI_INT);
    BaseVector* arg3Vec = CoalesceFunctionTestHelper::CreateNumericVector(arg3Values, OMNI_INT);
    
    // Set first value of arg1 and arg2 to NULL
    arg1Vec->SetNull(0);
    arg2Vec->SetNull(0);
    
    BaseVector* resultVec = nullptr;
    CoalesceFunctionTestHelper::ExecuteCoalesce({arg1Vec, arg2Vec, arg3Vec}, OMNI_INT, resultVec);
    CoalesceFunctionTestHelper::ValidateNumericResult(resultVec, expected, arg1Values.size());
    
    delete arg1Vec;
    delete arg2Vec;
    delete arg3Vec;
    delete resultVec;
}

// Test: Long coalesce
TEST(CoalesceTest, LongCoalesce) {
    std::cout << "=== Test: LONG coalesce ===" << std::endl;
    
    std::vector<int64_t> arg1Values = {100LL, 200LL, 300LL};
    std::vector<int64_t> arg2Values = {400LL, 500LL, 600LL};
    std::vector<int64_t> expected = {100LL, 200LL, 300LL};
    
    BaseVector* arg1Vec = CoalesceFunctionTestHelper::CreateNumericVector(arg1Values, OMNI_LONG);
    BaseVector* arg2Vec = CoalesceFunctionTestHelper::CreateNumericVector(arg2Values, OMNI_LONG);
    
    BaseVector* resultVec = nullptr;
    CoalesceFunctionTestHelper::ExecuteCoalesce({arg1Vec, arg2Vec}, OMNI_LONG, resultVec);
    CoalesceFunctionTestHelper::ValidateNumericResult(resultVec, expected, arg1Values.size());
    
    delete arg1Vec;
    delete arg2Vec;
    delete resultVec;
}

// Test: Double coalesce
TEST(CoalesceTest, DoubleCoalesce) {
    std::cout << "=== Test: DOUBLE coalesce ===" << std::endl;
    
    std::vector<double> arg1Values = {100.5, 200.7, 300.9};
    std::vector<double> arg2Values = {400.1, 500.3, 600.5};
    std::vector<double> expected = {100.5, 200.7, 300.9};
    
    BaseVector* arg1Vec = CoalesceFunctionTestHelper::CreateNumericVector(arg1Values, OMNI_DOUBLE);
    BaseVector* arg2Vec = CoalesceFunctionTestHelper::CreateNumericVector(arg2Values, OMNI_DOUBLE);
    
    BaseVector* resultVec = nullptr;
    CoalesceFunctionTestHelper::ExecuteCoalesce({arg1Vec, arg2Vec}, OMNI_DOUBLE, resultVec);
    CoalesceFunctionTestHelper::ValidateNumericResult(resultVec, expected, arg1Values.size());
    
    delete arg1Vec;
    delete arg2Vec;
    delete resultVec;
}

// Test: String coalesce
TEST(CoalesceTest, StringCoalesce) {
    std::cout << "=== Test: STRING coalesce ===" << std::endl;
    
    std::vector<std::string> arg1Values = {"hello", "world", "test"};
    std::vector<std::string> arg2Values = {"foo", "bar", "baz"};
    std::vector<std::string> expected = {"hello", "world", "test"};
    
    BaseVector* arg1Vec = CoalesceFunctionTestHelper::CreateStringVector(arg1Values);
    BaseVector* arg2Vec = CoalesceFunctionTestHelper::CreateStringVector(arg2Values);
    
    BaseVector* resultVec = nullptr;
    CoalesceFunctionTestHelper::ExecuteCoalesce({arg1Vec, arg2Vec}, OMNI_VARCHAR, resultVec);
    CoalesceFunctionTestHelper::ValidateStringResult(resultVec, expected, arg1Values.size());
    
    delete arg1Vec;
    delete arg2Vec;
    delete resultVec;
}

// Test: String coalesce with NULL in first argument
TEST(CoalesceTest, StringCoalesceWithNullFirst) {
    std::cout << "=== Test: STRING coalesce with NULL in first argument ===" << std::endl;
    
    std::vector<std::string> arg1Values = {"hello", "world", "test"};
    std::vector<std::string> arg2Values = {"foo", "bar", "baz"};
    std::vector<std::string> expected = {"foo", "world", "test"};
    
    BaseVector* arg1Vec = CoalesceFunctionTestHelper::CreateStringVector(arg1Values);
    BaseVector* arg2Vec = CoalesceFunctionTestHelper::CreateStringVector(arg2Values);
    
    // Set first value of arg1 to NULL
    arg1Vec->SetNull(0);
    
    BaseVector* resultVec = nullptr;
    CoalesceFunctionTestHelper::ExecuteCoalesce({arg1Vec, arg2Vec}, OMNI_VARCHAR, resultVec);
    CoalesceFunctionTestHelper::ValidateStringResult(resultVec, expected, arg1Values.size());
    
    delete arg1Vec;
    delete arg2Vec;
    delete resultVec;
}

// Test: Boolean coalesce
TEST(CoalesceTest, BooleanCoalesce) {
    std::cout << "=== Test: BOOLEAN coalesce ===" << std::endl;
    
    std::vector<bool> arg1Values = {true, false, true};
    std::vector<bool> arg2Values = {false, true, false};
    std::vector<bool> expected = {true, false, true};
    
    BaseVector* arg1Vec = CoalesceFunctionTestHelper::CreateNumericVector(arg1Values, OMNI_BOOLEAN);
    BaseVector* arg2Vec = CoalesceFunctionTestHelper::CreateNumericVector(arg2Values, OMNI_BOOLEAN);
    
    BaseVector* resultVec = nullptr;
    CoalesceFunctionTestHelper::ExecuteCoalesce({arg1Vec, arg2Vec}, OMNI_BOOLEAN, resultVec);
    CoalesceFunctionTestHelper::ValidateBooleanResult(resultVec, expected, arg1Values.size());
    
    delete arg1Vec;
    delete arg2Vec;
    delete resultVec;
}

// Test: Boolean coalesce with NULL in first argument
TEST(CoalesceTest, BooleanCoalesceWithNullFirst) {
    std::cout << "=== Test: BOOLEAN coalesce with NULL in first argument ===" << std::endl;
    
    std::vector<bool> arg1Values = {true, false, true};
    std::vector<bool> arg2Values = {false, true, false};
    std::vector<bool> expected = {false, false, true};
    
    BaseVector* arg1Vec = CoalesceFunctionTestHelper::CreateNumericVector(arg1Values, OMNI_BOOLEAN);
    BaseVector* arg2Vec = CoalesceFunctionTestHelper::CreateNumericVector(arg2Values, OMNI_BOOLEAN);
    
    // Set first value of arg1 to NULL
    arg1Vec->SetNull(0);
    
    BaseVector* resultVec = nullptr;
    CoalesceFunctionTestHelper::ExecuteCoalesce({arg1Vec, arg2Vec}, OMNI_BOOLEAN, resultVec);
    CoalesceFunctionTestHelper::ValidateBooleanResult(resultVec, expected, arg1Values.size());
    
    delete arg1Vec;
    delete arg2Vec;
    delete resultVec;
}
