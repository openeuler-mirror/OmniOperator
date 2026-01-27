/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
* Description: Unit tests for mathematical functions (acos, acosh, asin, asinh, atan, atan2, atanh, cos, cosh, cot, csc)
*/
 	 
#include <gtest/gtest.h>
#include <iostream>
#include <string>
#include <vector>
#include <cmath>

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

class MathFunctionsTest : public ::testing::Test {
protected:
    void SetUp() override {
        RegisterFunctions::Register();
    }
};


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