/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: bitwise function test
 */

#include <gtest/gtest.h>
#include <iostream>
#include <vector>

#include "test/util/test_util.h"
#include "vectorization/registration/Register.h"
#include "vectorization/ExprEval.h"
#include "expression/expressions.h"
#include "vectorization/registration/SimpleFunctionRegistry.h"

using namespace omniruntime;
using namespace omniruntime::vec;
using namespace omniruntime::vectorization;
using namespace omniruntime::mem;
using namespace omniruntime::op;
using namespace omniruntime::expressions;
using namespace omniruntime::TestUtil;

TEST(VectorizationTest, BitwiseOrInt) {
    int rowSize = 3;
    auto type = std::make_shared<DataType>(OMNI_INT);
    std::vector<Expr*> args = {new FieldExpr(0, type), new FieldExpr(1, type)};

    auto bitOrExpr = new FuncExpr("bitwise_or", args, type);

    std::vector<DataTypeId> sigArgs = {OMNI_INT, OMNI_INT};
    auto signature = std::make_shared<FunctionSignature>("bitwise_or", sigArgs, OMNI_INT);
    ASSERT_NE(bitOrExpr->vectorFunction, nullptr) << "bitwise_or function not found";

    std::vector vecOfTypes = {IntType(), IntType()};
    DataTypes inputTypes(vecOfTypes);
    int32_t col1[rowSize] = {0b1010, 0b1100, 0b0111};
    int32_t col2[rowSize] = {0b0110, 0b0101, 0b1010};
    VectorBatch *input = CreateVectorBatch(inputTypes, rowSize, col1, col2);
    input->Get(0)->SetNull(1);

    auto context = new ExecutionContext();
    context->SetResultRowSize(rowSize);
    ExprEval e(input, context);
    e.Visit(*bitOrExpr);
    auto result = e.GetResult();

    VectorBatch vectorBatch(rowSize);
    vectorBatch.Append(result);
    std::cout << "=== BitwiseOrInt Result ===" << std::endl;
    VectorHelper::PrintVecBatch(&vectorBatch);

    auto* resultVector = dynamic_cast<Vector<int32_t>*>(result);
    EXPECT_EQ(resultVector->GetValue(0), 0b1110);
    EXPECT_TRUE(resultVector->IsNull(1));
    EXPECT_EQ(resultVector->GetValue(2), 0b1111);

    delete input;
    delete bitOrExpr;
    delete context;
}

TEST(VectorizationTest, ShiftLeftInt32) {
    int rowSize = 5;
    auto inputType = std::make_shared<DataType>(OMNI_INT);
    auto shiftType = std::make_shared<DataType>(OMNI_INT);
    std::vector<Expr*> args = {new FieldExpr(0, inputType), new FieldExpr(1, shiftType)};

    auto shiftLeftExpr = new FuncExpr("shiftleft", args, inputType);

    std::vector<DataTypeId> sigArgs = {OMNI_INT, OMNI_INT};
    auto signature = std::make_shared<FunctionSignature>("shiftleft", sigArgs, OMNI_INT);
    ASSERT_NE(shiftLeftExpr->vectorFunction, nullptr) << "shiftleft function not found";

    int32_t col1[rowSize] = {10, 8, 5, -4, 123};
    int32_t col2[rowSize] = {2, 32, -1, 3, 0};
    std::vector vecOfTypes = {IntType(), IntType()};
    DataTypes inputTypes(vecOfTypes);
    VectorBatch *input = CreateVectorBatch(inputTypes, rowSize, col1, col2);
    input->Get(0)->SetNull(3);

    auto context = new ExecutionContext();
    context->SetResultRowSize(rowSize);
    ExprEval e(input, context);
    e.Visit(*shiftLeftExpr);
    auto result = e.GetResult();

    VectorBatch vectorBatch(rowSize);
    vectorBatch.Append(result);
    std::cout << "=== ShiftLeftInt32 Result ===" << std::endl;
    VectorHelper::PrintVecBatch(&vectorBatch);

    auto* resultVector = dynamic_cast<Vector<int32_t>*>(result);
    EXPECT_EQ(resultVector->GetValue(0), 10 << 2);
    EXPECT_EQ(resultVector->GetValue(1), 8 << (32 % 32));
    EXPECT_EQ(resultVector->GetValue(2), 5 << 31);
    EXPECT_TRUE(resultVector->IsNull(3));
    EXPECT_EQ(resultVector->GetValue(4), 123 << 0);

    delete input;
    delete shiftLeftExpr;
    delete context;
}