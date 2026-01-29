/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
* Description: Unit tests for mathematical functions (acos, acosh, asin, asinh, atan, atan2, atanh, cos, cosh, cot, csc)
*/
 	 
#include <gtest/gtest.h>
#include <iostream>
#include <string>
#include <vector>
#include <cmath>
#include <limits>

#include "test/util/test_util.h"
#include "vectorization/registration/Register.h"
#include "vectorization/ExprEval.h"
#include "vectorization/functions/MathFunctions.h"
#include "expression/expressions.h"
#include "type/data_type.h"
#include "vector/vector_helper.h"
#include "codegen/func_registry.h"
#include "type/decimal_operations.h"

using namespace omniruntime;
using namespace omniruntime::vec;
using namespace omniruntime::vectorization;
using namespace omniruntime::op;
using namespace omniruntime::expressions;
using namespace omniruntime::TestUtil;
using namespace omniruntime::codegen;
using namespace omniruntime::type;

class MathFunctionsTest : public ::testing::Test {
protected:
    void SetUp() override {
        RegisterFunctions::Register();
    }
};

// Helper function to test unary mathematical operations
template<typename T, DataTypeId typeId, DataTypeId returnId>
void TestUnaryMathOperation(
         const std::string& functionName,
         const std::vector<T>& inputData,
         const std::vector<T>& expectedResults,
         double tolerance = 1e-6) {

     int32_t rowSize = static_cast<int32_t>(inputData.size());

     BaseVector* rawInput = VectorHelper::CreateFlatVector(typeId, rowSize);
     auto* inputVector = static_cast<Vector<T>*>(rawInput);
     for (int32_t i = 0; i < rowSize; ++i) {
         inputVector->SetValue(i, inputData[i]);
         inputVector->SetNotNull(i);
     }

     std::vector<DataTypeId> argTypes = {typeId};
     auto signature = std::make_shared<FunctionSignature>(functionName, argTypes, returnId);
     auto vectorFunction = VectorFunction::Find(signature);
     ASSERT_NE(vectorFunction, nullptr);

     ExecutionContext context;
     context.SetResultRowSize(rowSize);

     std::stack<BaseVector*> args;
     args.push(rawInput); // Apply() will delete it

     BaseVector* rawResult = nullptr;
     auto resultType = std::make_shared<DataType>(typeId);
     vectorFunction->Apply(args, resultType, rawResult, &context);
     ASSERT_NE(rawResult, nullptr);
     auto* resultVector = static_cast<Vector<typename NativeType<returnId>::type>*>(rawResult);
     ASSERT_NE(resultVector, nullptr);

     for (int32_t i = 0; i < rowSize; ++i) {
         T actual = resultVector->GetValue(i);
         T expected = expectedResults[i];

         if constexpr (std::is_floating_point_v<T>) {
             if (std::isnan(expected)) {
                 EXPECT_TRUE(std::isnan(actual))
                     << "NaN mismatch at index " << i << " for " << functionName
                     << " with input=" << inputData[i];
             } else if (std::isinf(expected)) {
                 EXPECT_TRUE(std::isinf(actual) &&
                             std::signbit(actual) == std::signbit(expected))
                     << "Infinity mismatch at index " << i << " for " << functionName
                     << " with input=" << inputData[i];
             } else {
                 EXPECT_NEAR(actual, expected, tolerance)
                     << "Value mismatch at index " << i << " for " << functionName
                     << " with input=" << inputData[i]
                     << ", expected=" << expected << ", actual=" << actual;
             }
         } else {
             EXPECT_EQ(actual, expected)
                 << "Value mismatch at index " << i << " for " << functionName
                 << " with input=" << inputData[i];
         }
     }

     delete rawResult;
}


// Helper function to test binary mathematical operations
 	 template<typename T, DataTypeId typeId>
 	 void TestBinaryMathOperation(const std::string& functionName,
 	                              const std::vector<T>& leftData,
 	                              const std::vector<T>& rightData,
 	                              const std::vector<T>& expectedResults,
 	                              double tolerance = 1e-6) {
 	     int32_t rowSize = static_cast<int32_t>(leftData.size());

 	     // Create left vector
 	     BaseVector* leftVec = VectorHelper::CreateFlatVector(typeId, rowSize);
 	     auto* leftVector = static_cast<Vector<T>*>(leftVec);
 	     for (int32_t i = 0; i < rowSize; ++i) {
 	         leftVector->SetValue(i, leftData[i]);
 	         leftVector->SetNotNull(i);
 	     }

 	     // Create right vector
 	     BaseVector* rightVec = VectorHelper::CreateFlatVector(typeId, rowSize);
 	     auto* rightVector = static_cast<Vector<T>*>(rightVec);
 	     for (int32_t i = 0; i < rowSize; ++i) {
 	         rightVector->SetValue(i, rightData[i]);
 	         rightVector->SetNotNull(i);
 	     }

 	     // Create function signature
 	     std::vector<DataTypeId> argTypes = {typeId, typeId};
 	     auto signature = std::make_shared<FunctionSignature>(functionName, argTypes, typeId);

 	     // Find vector function
 	     auto vectorFunction = VectorFunction::Find(signature);
 	     ASSERT_NE(vectorFunction, nullptr) << "Function " << functionName << " not found for type " << static_cast<int>(typeId);

 	     // Create execution context
 	     ExecutionContext context;
 	     context.SetResultRowSize(rowSize);

 	     // Prepare arguments stack
 	     std::stack<BaseVector*> args;

 	     args.push(leftVec);
 	     args.push(rightVec);

 	     // Execute function
 	     BaseVector* result = nullptr;
 	     auto resultType = std::make_shared<DataType>(typeId);
 	     vectorFunction->Apply(args, resultType, result, &context);

 	     // Verify results
 	     auto* resultVector = static_cast<Vector<T>*>(result);
 	     ASSERT_NE(resultVector, nullptr);

 	     for (int32_t i = 0; i < rowSize; ++i) {
 	         T actualResult = resultVector->GetValue(i);
 	         T expectedResult = expectedResults[i];

 	         if (std::isnan(expectedResult)) {
 	             EXPECT_TRUE(std::isnan(actualResult))
 	                 << "NaN mismatch at index " << i << " for " << functionName;
 	         } else if (std::isinf(expectedResult)) {
 	             EXPECT_TRUE(std::isinf(actualResult) && std::signbit(actualResult) == std::signbit(expectedResult))
 	                 << "Infinity mismatch at index " << i << " for " << functionName;
 	         } else {
 	             EXPECT_NEAR(actualResult, expectedResult, tolerance)
 	                 << "Value mismatch at index " << i << " for " << functionName
 	                 << " with left=" << leftData[i] << ", right=" << rightData[i]
 	                 << ", expected=" << expectedResult << ", actual=" << actualResult;
 	         }
 	     }
 	     // Cleanup
 	     delete result;
 	 }

// Test acosh function
TEST(MathFunctionsTest, AcoshDouble) {
    double tolerance = 1e-6;
    std::vector<double> inputData = {1.0, 2.0, 5.0, 10.0};
    std::vector<double> expectedResults;
    std::string functionName = "acosh";
    for (double x : inputData) {
        expectedResults.push_back(std::acosh(x));
    }
    int rowSize = 4;
    auto returnType = std::make_shared<DataType>(OMNI_DOUBLE);
    auto type = std::make_shared<DataType>(OMNI_DOUBLE);
    std::vector<Expr*> args = {new FieldExpr(0, type)};
    auto funcExpr = new FuncExpr("acosh", args, returnType);
    std::vector<DataTypeId> sigArgs = {OMNI_DOUBLE};
    double col1[4] = {1.0, 2.0, 5.0, 10.0};
    std::vector vecOfTypes = {DoubleType()};
    DataTypes inputTypes(vecOfTypes);
    VectorBatch *input = CreateVectorBatch(inputTypes, rowSize, col1);
    std::cout << "=== acosh input ===" << std::endl;
    VectorHelper::PrintVecBatch(input);

    auto context = new ExecutionContext();
    context->SetResultRowSize(rowSize);

    ExprEval e(input, context);
    e.Visit(*funcExpr);
    auto result = e.GetResult();
    VectorBatch vectorBatch(rowSize);
    vectorBatch.Append(result);
    std::cout << "=== acosh Result ===" << std::endl;
    VectorHelper::PrintVecBatch(&vectorBatch);

    auto *resultVector = dynamic_cast<Vector<double> *>(result);


    for (int32_t i = 0; i < rowSize; ++i) {
        double actualResult = resultVector->GetValue(i);
        double expectedResult = expectedResults[i];

        if (std::isnan(expectedResult)) {
            EXPECT_TRUE(std::isnan(actualResult))
                << "NaN mismatch at index " << i << " for " << functionName
                << " with input=" << inputData[i];
        } else if (std::isinf(expectedResult)) {
            EXPECT_TRUE(std::isinf(actualResult) && std::signbit(actualResult) == std::signbit(expectedResult))
                << "Infinity mismatch at index " << i << " for " << functionName
                << " with input=" << inputData[i];
        } else {
            EXPECT_NEAR(actualResult, expectedResult, tolerance)
                << "Value mismatch at index " << i << " for " << functionName
                << " with input=" << inputData[i]
                << ", expected=" << expectedResult << ", actual=" << actualResult;
        }
    }
    delete input;
    delete funcExpr;
    delete context;
}

// Test acos function
TEST(MathFunctionsTest, AcosDouble) {
    std::vector<double> inputData = {1.0, 0.5, 0.0, -0.5, -1.0};
    std::vector<double> expectedResults;
    for (double x : inputData) {
        expectedResults.push_back(std::acos(x));
    }
    TestUnaryMathOperation<double, OMNI_DOUBLE, OMNI_DOUBLE>("acos", inputData, expectedResults);
}

// Test negative function for double
TEST(MathFunctionsTest, NegativeDouble) {
    std::vector<double> inputData = {1.0, -1.0, 0.0, 3.14, -3.14, 100.5, -100.5};
    std::vector<double> expectedResults;
    for (double x : inputData) {
        expectedResults.push_back(-x);
    }
    TestUnaryMathOperation<double, OMNI_DOUBLE, OMNI_DOUBLE>("negative", inputData, expectedResults, 1e-9);
}

// Test negative function for float
TEST(MathFunctionsTest, NegativeFloat) {
    std::vector<float> inputData = {1.0f, -1.0f, 0.0f, 3.14f, -3.14f, 100.5f, -100.5f};
    std::vector<float> expectedResults;
    for (float x : inputData) {
        expectedResults.push_back(-x);
    }
    TestUnaryMathOperation<float, OMNI_FLOAT, OMNI_FLOAT>("negative", inputData, expectedResults, 1e-6);
}

// Test negative function for int32
TEST(MathFunctionsTest, NegativeInt) {
    std::vector<int32_t> inputData = {1, -1, 0, 100, -100, 2147483647, -2147483648};
    std::vector<int32_t> expectedResults;
    for (int32_t x : inputData) {
        expectedResults.push_back(-x);
    }
    TestUnaryMathOperation<int32_t, OMNI_INT, OMNI_INT>("negative", inputData, expectedResults, 0);
}

// Test negative function for int64
TEST(MathFunctionsTest, NegativeLong) {
    std::vector<int64_t> inputData = {1LL, -1LL, 0LL, 100LL, -100LL, 9223372036854775807LL, -9223372036854775807LL};
    std::vector<int64_t> expectedResults;
    for (int64_t x : inputData) {
        expectedResults.push_back(-x);
    }
    TestUnaryMathOperation<int64_t, OMNI_LONG, OMNI_LONG>("negative", inputData, expectedResults, 0);
}

// Test negative function for int16
TEST(MathFunctionsTest, NegativeShort) {
    std::vector<int16_t> inputData = {1, -1, 0, 100, -100, 32767, -32768};
    std::vector<int16_t> expectedResults;
    for (int16_t x : inputData) {
        expectedResults.push_back(-x);
    }
    TestUnaryMathOperation<int16_t, OMNI_SHORT, OMNI_SHORT>("negative", inputData, expectedResults, 0);
}

// Test negative function for int8
TEST(MathFunctionsTest, NegativeByte) {
    std::vector<int8_t> inputData = {1, -1, 0, 100, -100, 127, -128};
    std::vector<int8_t> expectedResults;
    for (int8_t x : inputData) {
        expectedResults.push_back(-x);
    }
    TestUnaryMathOperation<int8_t, OMNI_BYTE, OMNI_BYTE>("negative", inputData, expectedResults, 0);
}

// Test negative function with edge cases
TEST(MathFunctionsTest, NegativeEdgeCases) {
    // Test with zero
    std::vector<double> zeroInput = {0.0};
    std::vector<double> zeroExpected = {0.0};
    TestUnaryMathOperation<double, OMNI_DOUBLE, OMNI_DOUBLE>("negative", zeroInput, zeroExpected, 1e-9);

    // Test with very small numbers
    std::vector<double> smallInput = {1e-10, -1e-10};
    std::vector<double> smallExpected = {-1e-10, 1e-10};
    TestUnaryMathOperation<double, OMNI_DOUBLE, OMNI_DOUBLE>("negative", smallInput, smallExpected, 1e-15);

    // Test with very large numbers
    std::vector<double> largeInput = {1e10, -1e10};
    std::vector<double> largeExpected = {-1e10, 1e10};
    TestUnaryMathOperation<double, OMNI_DOUBLE, OMNI_DOUBLE>("negative", largeInput, largeExpected, 1e-9);
}

// Test negative function for OMNI_DECIMAL64
TEST(MathFunctionsTest, NegativeDecimal64) {
    std::vector<int64_t> inputData = {123LL, -456LL, 0L, 999999999999999999LL};
    std::vector<int64_t> expectedResults = {-123LL, 456LL, 0L, -999999999999999999LL};
    TestUnaryMathOperation<int64_t, OMNI_DECIMAL64, OMNI_DECIMAL64>("negative", inputData, expectedResults, 0);
}

// Test negative function for OMNI_DECIMAL128
// Decimal128's MAX_LONG_PRECISION is 38, use 99999999999999999999999999999999999999 as Max
TEST(MathFunctionsTest, NegativeDecimal128) {
    std::vector<Decimal128> inputData = {Decimal128("1234567890"), Decimal128("-1234567890"), Decimal128("0"), Decimal128("99999999999999999999999999999999999999")};
    std::vector<Decimal128> expectedResults = {Decimal128("-1234567890"), Decimal128("1234567890"), Decimal128("0"), Decimal128("-99999999999999999999999999999999999999")};
    TestUnaryMathOperation<Decimal128, OMNI_DECIMAL128, OMNI_DECIMAL128>("negative", inputData, expectedResults, 0);
}

// Test asin function
TEST(MathFunctionsTest, AsinDouble) {
    std::vector<double> inputData = {1.0, 0.5, 0.0, -0.5, -1.0};
    std::vector<double> expectedResults;
    for (double x : inputData) {
        expectedResults.push_back(std::asin(x));
    }
    TestUnaryMathOperation<double, OMNI_DOUBLE, OMNI_DOUBLE>("asin", inputData, expectedResults);
}


// Test asinh function
TEST(MathFunctionsTest, AsinhDouble) {
    std::vector<double> inputData = {1.0, 1.5, 0.0, -0.5, -1.5};
    std::vector<double> expectedResults;
    for (double x : inputData) {
        expectedResults.push_back(std::asinh(x));
    }
    TestUnaryMathOperation<double, OMNI_DOUBLE, OMNI_DOUBLE>("asinh", inputData, expectedResults);
}

// Test atan function
TEST(MathFunctionsTest, AtanDouble) {
    std::vector<double> inputData = {1.0, 1.5, 0.0, -0.5, -1.5};
    std::vector<double> expectedResults;
    for (double x : inputData) {
        expectedResults.push_back(std::atan(x));
    }
    TestUnaryMathOperation<double, OMNI_DOUBLE, OMNI_DOUBLE>("atan", inputData, expectedResults);
}

// Test atan2 function
TEST(MathFunctionsTest, Atan2Double) {
    std::vector<double> inputData_x = {1.0, 0.5, 0.0, -0.5, -1.0};
	std::vector<double> inputData_y = {2.0, 1.0, 0.0, -1, -2};
    std::vector<double> expectedResults;
	for (int i = 0; i < inputData_x.size(); i++) {
		expectedResults.push_back(std::atan2(inputData_x[i] + 0.0, inputData_y[i] + 0.0));
	}
    TestBinaryMathOperation<double, OMNI_DOUBLE>("atan2", inputData_x, inputData_y, expectedResults);
}


// Test cos function
TEST(MathFunctionsTest, CosDouble) {
    std::vector<double> inputData = {1.0, 5, 0.0, -0.5, -1.0};
    std::vector<double> expectedResults;
    for (double x : inputData) {
        expectedResults.push_back(std::cos(x));
    }
    TestUnaryMathOperation<double, OMNI_DOUBLE, OMNI_DOUBLE>("cos", inputData, expectedResults);
}


// Test cosh function
TEST(MathFunctionsTest, CoshDouble) {
    std::vector<double> inputData = {1.0, 0.5, 0.0, -0.5, -1.0};
    std::vector<double> expectedResults;
    for (double x : inputData) {
        expectedResults.push_back(std::cosh(x));
    }
    TestUnaryMathOperation<double, OMNI_DOUBLE, OMNI_DOUBLE>("cosh", inputData, expectedResults);
}

// Test cbrt function
TEST(MathFunctionsTest, CbrtDouble) {
    std::vector<double> inputData = {64, 8.1, 0.0, 132, -119};
    std::vector<double> expectedResults;
    for (double x : inputData) {
        expectedResults.push_back(std::cbrt(x));
    }
    TestUnaryMathOperation<double, OMNI_DOUBLE, OMNI_DOUBLE>("cbrt", inputData, expectedResults);
}


// Test ceil double function
TEST(MathFunctionsTest, ceilDouble) {
    std::vector<double> inputData = {1.0, 0.5, 0.0, -0.5, -1.0};
    std::vector<double> expectedResults;
    for (double x : inputData) {
        expectedResults.push_back(std::ceil(x));
    }
    TestUnaryMathOperation<double, OMNI_DOUBLE, OMNI_LONG>("ceil", inputData, expectedResults);
}


// Test ceil long function
TEST(MathFunctionsTest, CeilLong) {
    std::vector<int64_t> inputData = {std::numeric_limits<int64_t>::max(), 25, 11, 41, 1};
    std::vector<int64_t> expectedResults;
    for (int64_t x : inputData) {
        expectedResults.push_back(std::ceil(x));
    }
    TestUnaryMathOperation<int64_t, OMNI_LONG, OMNI_LONG>("ceil", inputData, expectedResults);
}