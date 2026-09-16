/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
* Description: Unit tests for floor function
*/

#include <gtest/gtest.h>
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

using namespace omniruntime;
using namespace omniruntime::vec;
using namespace omniruntime::vectorization;
using namespace omniruntime::op;
using namespace omniruntime::expressions;
using namespace omniruntime::TestUtil;
using namespace omniruntime::codegen;
using namespace omniruntime::type;

class FloorTest : public ::testing::Test {
protected:
    void SetUp() override {
        RegisterFunctions::Register();
    }
};

// Helper: test decimal unary operations (ceil/floor/abs) for DECIMAL64/DECIMAL128
template <typename ValueType, typename DecimalDataTypeT, DataTypeId DTID>
void TestDecimalUnaryOperation(
    const std::string& functionName,
    const std::vector<ValueType>& inputData,
    const std::vector<ValueType>& expectedResults,
    int32_t precision,
    int32_t scale,
    const std::vector<int32_t>& nullIndices = {})
{
    int32_t rowSize = static_cast<int32_t>(inputData.size());
    auto decType = std::make_shared<DecimalDataTypeT>(precision, scale);
    BaseVector* rawInput = VectorHelper::CreateComplexVector(decType.get(), rowSize);
    auto* inputVector = static_cast<Vector<ValueType>*>(rawInput);
    for (int32_t i = 0; i < rowSize; ++i) {
        inputVector->SetValue(i, inputData[i]);
        inputVector->SetNotNull(i);
    }
    std::vector<bool> nullFlags(rowSize, false);
    for (int32_t idx : nullIndices) {
        rawInput->SetNull(idx);
        nullFlags[idx] = true;
    }

    std::vector<DataTypeId> argTypes = {DTID};
    auto signature = std::make_shared<FunctionSignature>(functionName, argTypes, DTID);
    auto vectorFunction = VectorFunction::Find(signature);
    ASSERT_NE(vectorFunction, nullptr);

    ExecutionContext context;
    context.SetResultRowSize(rowSize);
    std::stack<BaseVector*> args;
    args.push(rawInput);

    BaseVector* rawResult = nullptr;
    auto resultType = std::make_shared<DataType>(DTID);
    vectorFunction->Apply(args, resultType, rawResult, &context);
    ASSERT_NE(rawResult, nullptr);

    auto* resultVector = static_cast<Vector<ValueType>*>(rawResult);
    ASSERT_NE(resultVector, nullptr);

    for (int32_t i = 0; i < rowSize; ++i) {
        if (nullFlags[i]) {
            EXPECT_TRUE(rawResult->IsNull(i)) << "Result should be NULL at index " << i;
        } else {
            EXPECT_FALSE(rawResult->IsNull(i)) << "Result should not be NULL at index " << i;
            ValueType actual = resultVector->GetValue(i);
            ValueType expected = expectedResults[i];
            EXPECT_EQ(actual, expected)
                << "Value mismatch at index " << i << " for " << functionName
                << "(" << inputData[i] << ")"
                << ", expected=" << expected << ", actual=" << actual;
        }
    }

    delete rawResult;
}

// Test floor function with double inputs (Flink semantics: floor(double) -> double)
TEST_F(FloorTest, FloorDouble) {
    
    int32_t rowSize = 8;
    auto returnType = std::make_shared<DataType>(OMNI_DOUBLE);
    auto inputType = std::make_shared<DataType>(OMNI_DOUBLE);
    std::vector<Expr*> args = {new FieldExpr(0, inputType)};
    auto funcExpr = new FuncExpr("floor", args, returnType);
    
    // Test values: positive decimals, negative decimals, exact integers, zero
    double col1[8] = {2.878, 1.5678, -1.5, -2.878, 0.0, 5.0, -5.0, 0.999};
    std::vector vecOfTypes = {DoubleType()};
    DataTypes inputTypes(vecOfTypes);
    VectorBatch *input = CreateVectorBatch(inputTypes, rowSize, col1);
    
    auto context = new ExecutionContext();
    context->SetResultRowSize(rowSize);
    
    ExprEval e(input, context);
    e.Visit(*funcExpr);
    auto result = e.GetResult();
    
    auto *resultVector = dynamic_cast<Vector<double> *>(result);
    ASSERT_NE(resultVector, nullptr);
    
    // Expected results: floor(2.878)=2.0, floor(1.5678)=1.0, floor(-1.5)=-2.0, floor(-2.878)=-3.0,
    // floor(0.0)=0.0, floor(5.0)=5.0, floor(-5.0)=-5.0, floor(0.999)=0.0
    std::vector<double> expectedResults = {2.0, 1.0, -2.0, -3.0, 0.0, 5.0, -5.0, 0.0};
    
    for (int32_t i = 0; i < rowSize; ++i) {
        EXPECT_FALSE(result->IsNull(i)) << "Result should not be NULL at index " << i;
        double actualResult = resultVector->GetValue(i);
        double expectedResult = expectedResults[i];
        EXPECT_EQ(actualResult, expectedResult)
            << "Value mismatch at index " << i << " for floor(" << col1[i] << ")"
            << ", expected=" << expectedResult << ", actual=" << actualResult;
    }
    
    delete result;
    delete input;
    delete funcExpr;
    delete context;
}

// Test floor function with long inputs (should return same value)
TEST_F(FloorTest, FloorLong) {
    
    int32_t rowSize = 6;
    auto returnType = std::make_shared<DataType>(OMNI_LONG);
    auto inputType = std::make_shared<DataType>(OMNI_LONG);
    std::vector<Expr*> args = {new FieldExpr(0, inputType)};
    auto funcExpr = new FuncExpr("floor", args, returnType);
    
    // Test values: positive, negative, zero, max, min
    int64_t col1[6] = {100LL, -100LL, 0LL, 
                        std::numeric_limits<int64_t>::max(), 
                        std::numeric_limits<int64_t>::min(),
                        12345678901234LL};
    std::vector vecOfTypes = {LongType()};
    DataTypes inputTypes(vecOfTypes);
    VectorBatch *input = CreateVectorBatch(inputTypes, rowSize, col1);
    
    auto context = new ExecutionContext();
    context->SetResultRowSize(rowSize);
    
    ExprEval e(input, context);
    e.Visit(*funcExpr);
    auto result = e.GetResult();
    
    auto *resultVector = dynamic_cast<Vector<int64_t> *>(result);
    ASSERT_NE(resultVector, nullptr);
    
    // For long input, floor returns the same value
    for (int32_t i = 0; i < rowSize; ++i) {
        EXPECT_FALSE(result->IsNull(i)) << "Result should not be NULL at index " << i;
        int64_t actualResult = resultVector->GetValue(i);
        int64_t expectedResult = col1[i];
        EXPECT_EQ(actualResult, expectedResult)
            << "Value mismatch at index " << i << " for floor(" << col1[i] << ")"
            << ", expected=" << expectedResult << ", actual=" << actualResult;
    }
    
    delete result;
    delete input;
    delete funcExpr;
    delete context;
}

// Test floor function with edge cases (infinity, NaN)
TEST_F(FloorTest, FloorEdgeCases) {
    
    constexpr double kInf = std::numeric_limits<double>::infinity();
    constexpr double kNan = std::numeric_limits<double>::quiet_NaN();
    
    int32_t rowSize = 4;
    auto returnType = std::make_shared<DataType>(OMNI_DOUBLE);
    auto inputType = std::make_shared<DataType>(OMNI_DOUBLE);
    std::vector<Expr*> args = {new FieldExpr(0, inputType)};
    auto funcExpr = new FuncExpr("floor", args, returnType);
    
    // Test values: infinity, -infinity, NaN, very large number
    double col1[4] = {kInf, -kInf, kNan, 1e18};
    std::vector vecOfTypes = {DoubleType()};
    DataTypes inputTypes(vecOfTypes);
    VectorBatch *input = CreateVectorBatch(inputTypes, rowSize, col1);
    
    auto context = new ExecutionContext();
    context->SetResultRowSize(rowSize);
    
    ExprEval e(input, context);
    e.Visit(*funcExpr);
    auto result = e.GetResult();
    
    auto *resultVector = dynamic_cast<Vector<double> *>(result);
    ASSERT_NE(resultVector, nullptr);
    
    // Expected: floor(inf)=inf, floor(-inf)=-inf, floor(NaN)=NaN, floor(1e18)=1e18
    EXPECT_EQ(resultVector->GetValue(0), kInf) << "floor(infinity) should be infinity";
    EXPECT_EQ(resultVector->GetValue(1), -kInf) << "floor(-infinity) should be -infinity";
    EXPECT_TRUE(std::isnan(resultVector->GetValue(2))) << "floor(NaN) should be NaN";
    EXPECT_EQ(resultVector->GetValue(3), 1e18) << "floor(1e18) should be 1e18";
    
    delete result;
    delete input;
    delete funcExpr;
    delete context;
}

// Test floor function with NULL input
TEST_F(FloorTest, FloorWithNullInput) {
    
    int32_t rowSize = 4;
    auto returnType = std::make_shared<DataType>(OMNI_DOUBLE);
    auto inputType = std::make_shared<DataType>(OMNI_DOUBLE);
    std::vector<Expr*> args = {new FieldExpr(0, inputType)};
    auto funcExpr = new FuncExpr("floor", args, returnType);
    
    double col1[4] = {2.5, 3.7, -1.2, 4.9};
    std::vector vecOfTypes = {DoubleType()};
    DataTypes inputTypes(vecOfTypes);
    VectorBatch *input = CreateVectorBatch(inputTypes, rowSize, col1);
    
    // Set first and third values to NULL
    input->Get(0)->SetNull(0);
    input->Get(0)->SetNull(2);
    
    auto context = new ExecutionContext();
    context->SetResultRowSize(rowSize);
    
    ExprEval e(input, context);
    e.Visit(*funcExpr);
    auto result = e.GetResult();
    
    auto *resultVector = dynamic_cast<Vector<double> *>(result);
    
    // First and third should be NULL
    EXPECT_TRUE(result->IsNull(0)) << "Result should be NULL when input is NULL at index 0";
    EXPECT_FALSE(result->IsNull(1)) << "Result should not be NULL at index 1";
    EXPECT_TRUE(result->IsNull(2)) << "Result should be NULL when input is NULL at index 2";
    EXPECT_FALSE(result->IsNull(3)) << "Result should not be NULL at index 3";
    
    // Check non-NULL values
    EXPECT_EQ(resultVector->GetValue(1), 3.0) << "floor(3.7) should be 3.0";
    EXPECT_EQ(resultVector->GetValue(3), 4.0) << "floor(4.9) should be 4.0";
    
    delete result;
    delete input;
    delete funcExpr;
    delete context;
}

// Test floor function using VectorFunction directly for double
TEST_F(FloorTest, FloorVectorFunctionDouble) {
    
    int32_t rowSize = 5;
    
    // Create input vector
    BaseVector* rawInput = VectorHelper::CreateFlatVector(OMNI_DOUBLE, rowSize);
    auto* inputVector = static_cast<Vector<double>*>(rawInput);
    std::vector<double> inputData = {2.5, -2.5, 0.0, 100.999, -100.001};
    for (int32_t i = 0; i < rowSize; ++i) {
        inputVector->SetValue(i, inputData[i]);
        inputVector->SetNotNull(i);
    }
    
    // Create function signature: floor(double) -> double (Flink semantics)
    std::vector<DataTypeId> argTypes = {OMNI_DOUBLE};
    auto signature = std::make_shared<FunctionSignature>("floor", argTypes, OMNI_DOUBLE);
    auto vectorFunction = VectorFunction::Find(signature);
    ASSERT_NE(vectorFunction, nullptr) << "Function floor(double) not found";
    
    ExecutionContext context;
    context.SetResultRowSize(rowSize);
    
    std::stack<BaseVector*> args;
    args.push(rawInput);
    
    BaseVector* rawResult = nullptr;
    auto resultType = std::make_shared<DataType>(OMNI_DOUBLE);
    vectorFunction->Apply(args, resultType, rawResult, &context);
    ASSERT_NE(rawResult, nullptr);
    
    auto* resultVector = static_cast<Vector<double>*>(rawResult);
    ASSERT_NE(resultVector, nullptr);
    
    std::vector<double> expectedResults = {2.0, -3.0, 0.0, 100.0, -101.0};
    
    for (int32_t i = 0; i < rowSize; ++i) {
        EXPECT_FALSE(rawResult->IsNull(i)) << "Result should not be NULL at index " << i;
        double actual = resultVector->GetValue(i);
        double expected = expectedResults[i];
        EXPECT_EQ(actual, expected)
            << "Value mismatch at index " << i << " for floor(" << inputData[i] << ")"
            << ", expected=" << expected << ", actual=" << actual;
    }
    
    // Note: rawInput is already freed by FlatVectorReader's destructor inside Apply(),
    // because GetIsField() is false for vectors created by VectorHelper::CreateFlatVector.
    // Do NOT delete rawInput here to avoid double-free (heap-use-after-free).
    delete rawResult;
}

// Test floor function using VectorFunction directly for long
TEST_F(FloorTest, FloorVectorFunctionLong) {
    
    int32_t rowSize = 4;
    
    // Create input vector
    BaseVector* rawInput = VectorHelper::CreateFlatVector(OMNI_LONG, rowSize);
    auto* inputVector = static_cast<Vector<int64_t>*>(rawInput);
    std::vector<int64_t> inputData = {100LL, -100LL, 0LL, 9223372036854775807LL};
    for (int32_t i = 0; i < rowSize; ++i) {
        inputVector->SetValue(i, inputData[i]);
        inputVector->SetNotNull(i);
    }
    
    // Create function signature: floor(long) -> long
    std::vector<DataTypeId> argTypes = {OMNI_LONG};
    auto signature = std::make_shared<FunctionSignature>("floor", argTypes, OMNI_LONG);
    auto vectorFunction = VectorFunction::Find(signature);
    ASSERT_NE(vectorFunction, nullptr) << "Function floor(long) not found";
    
    ExecutionContext context;
    context.SetResultRowSize(rowSize);
    
    std::stack<BaseVector*> args;
    args.push(rawInput);
    
    BaseVector* rawResult = nullptr;
    auto resultType = std::make_shared<DataType>(OMNI_LONG);
    vectorFunction->Apply(args, resultType, rawResult, &context);
    ASSERT_NE(rawResult, nullptr);
    
    auto* resultVector = static_cast<Vector<int64_t>*>(rawResult);
    ASSERT_NE(resultVector, nullptr);
    
    // For long input, floor returns the same value
    for (int32_t i = 0; i < rowSize; ++i) {
        EXPECT_FALSE(rawResult->IsNull(i)) << "Result should not be NULL at index " << i;
        int64_t actual = resultVector->GetValue(i);
        int64_t expected = inputData[i];
        EXPECT_EQ(actual, expected)
            << "Value mismatch at index " << i << " for floor(" << inputData[i] << ")"
            << ", expected=" << expected << ", actual=" << actual;
    }
    
    // Note: rawInput is already freed by FlatVectorReader's destructor inside Apply(),
    // because GetIsField() is false for vectors created by VectorHelper::CreateFlatVector.
    // Do NOT delete rawInput here to avoid double-free (heap-use-after-free).
    delete rawResult;
}

// Test floor function with negative decimals close to integer
TEST_F(FloorTest, FloorNegativeDecimalsCloseToInteger) {
    
    int32_t rowSize = 6;
    auto returnType = std::make_shared<DataType>(OMNI_DOUBLE);
    auto inputType = std::make_shared<DataType>(OMNI_DOUBLE);
    std::vector<Expr*> args = {new FieldExpr(0, inputType)};
    auto funcExpr = new FuncExpr("floor", args, returnType);
    
    // Test values that are very close to integers
    double col1[6] = {-0.001, -0.999, -1.001, -1.999, 0.001, 0.999};
    std::vector vecOfTypes = {DoubleType()};
    DataTypes inputTypes(vecOfTypes);
    VectorBatch *input = CreateVectorBatch(inputTypes, rowSize, col1);
    
    auto context = new ExecutionContext();
    context->SetResultRowSize(rowSize);
    
    ExprEval e(input, context);
    e.Visit(*funcExpr);
    auto result = e.GetResult();
    
    auto *resultVector = dynamic_cast<Vector<double> *>(result);
    ASSERT_NE(resultVector, nullptr);
    
    // Expected: floor(-0.001)=-1.0, floor(-0.999)=-1.0, floor(-1.001)=-2.0, 
    //          floor(-1.999)=-2.0, floor(0.001)=0.0, floor(0.999)=0.0
    std::vector<double> expectedResults = {-1.0, -1.0, -2.0, -2.0, 0.0, 0.0};
    
    for (int32_t i = 0; i < rowSize; ++i) {
        EXPECT_FALSE(result->IsNull(i)) << "Result should not be NULL at index " << i;
        double actualResult = resultVector->GetValue(i);
        double expectedResult = expectedResults[i];
        EXPECT_EQ(actualResult, expectedResult)
            << "Value mismatch at index " << i << " for floor(" << col1[i] << ")"
            << ", expected=" << expectedResult << ", actual=" << actualResult;
    }
    
    delete result;
    delete input;
    delete funcExpr;
    delete context;
}

// Test floor(double) -> double (Flink semantics)
TEST_F(FloorTest, FloorDoubleReturnDouble) {

    int32_t rowSize = 5;

    // Create input vector
    BaseVector* rawInput = VectorHelper::CreateFlatVector(OMNI_DOUBLE, rowSize);
    auto* inputVector = static_cast<Vector<double>*>(rawInput);
    std::vector<double> inputData = {2.5, -2.5, 0.0, 100.999, -100.001};
    for (int32_t i = 0; i < rowSize; ++i) {
        inputVector->SetValue(i, inputData[i]);
        inputVector->SetNotNull(i);
    }

    // Create function signature: floor(double) -> double (Flink semantics)
    std::vector<DataTypeId> argTypes = {OMNI_DOUBLE};
    auto signature = std::make_shared<FunctionSignature>("floor", argTypes, OMNI_DOUBLE);
    auto vectorFunction = VectorFunction::Find(signature);
    ASSERT_NE(vectorFunction, nullptr) << "Function floor(double)->double not found";

    ExecutionContext context;
    context.SetResultRowSize(rowSize);

    std::stack<BaseVector*> args;
    args.push(rawInput);

    BaseVector* rawResult = nullptr;
    auto resultType = std::make_shared<DataType>(OMNI_DOUBLE);
    vectorFunction->Apply(args, resultType, rawResult, &context);
    ASSERT_NE(rawResult, nullptr);

    auto* resultVector = static_cast<Vector<double>*>(rawResult);
    ASSERT_NE(resultVector, nullptr);

    std::vector<double> expectedResults = {2.0, -3.0, 0.0, 100.0, -101.0};

    for (int32_t i = 0; i < rowSize; ++i) {
        EXPECT_FALSE(rawResult->IsNull(i)) << "Result should not be NULL at index " << i;
        double actual = resultVector->GetValue(i);
        double expected = expectedResults[i];
        EXPECT_EQ(actual, expected)
            << "Value mismatch at index " << i << " for floor(" << inputData[i] << ")"
            << ", expected=" << expected << ", actual=" << actual;
    }

    delete rawResult;
}

// Test floor(DECIMAL64) -> DECIMAL64 with scale=2
TEST_F(FloorTest, FloorDec64) {
    TestDecimalUnaryOperation<int64_t, Decimal64DataType, OMNI_DECIMAL64>(
        "floor", {12345, -12345, 10000, -50, 0, 999}, {123, -124, 100, -1, 0, 9}, 18, 2);
}

// Test floor(DECIMAL64) with NULL input
TEST_F(FloorTest, FloorDec64Null) {
    TestDecimalUnaryOperation<int64_t, Decimal64DataType, OMNI_DECIMAL64>(
        "floor", {12345, -12345, 10000, -50}, {12345, -124, 10000, -1}, 18, 2, {0, 2});
}

// Test floor(DECIMAL128) -> DECIMAL128 with scale=2
TEST_F(FloorTest, FloorDec128) {
    TestDecimalUnaryOperation<Decimal128, Decimal128DataType, OMNI_DECIMAL128>(
        "floor",
        {Decimal128(12345), Decimal128(-12345), Decimal128(10000), Decimal128(-50), Decimal128(0), Decimal128(999)},
        {Decimal128(123), Decimal128(-124), Decimal128(100), Decimal128(-1), Decimal128(0), Decimal128(9)},
        38, 2);
}

// Test floor(DECIMAL128) with NULL input
TEST_F(FloorTest, FloorDec128Null) {
    TestDecimalUnaryOperation<Decimal128, Decimal128DataType, OMNI_DECIMAL128>(
        "floor",
        {Decimal128(12345), Decimal128(-12345), Decimal128(10000), Decimal128(-50)},
        {Decimal128(12345), Decimal128(-124), Decimal128(10000), Decimal128(-1)},
        38, 2, {0, 2});
}