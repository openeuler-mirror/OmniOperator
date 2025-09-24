/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: codegen test
 */

#include <gtest/gtest.h>
#include <iostream>
#include <string>
#include <vector>

#include "test/util/test_util.h"
#include "vectorization/registration/Register.h"
#include "vectorization/ExprEval.h"
#include "expression/expressions.h"
#include "type/decimal_operations.h"
#include "vectorization/registration/SimpleFunctionRegistry.h"

using namespace omniruntime;
using namespace omniruntime::vec;
using namespace omniruntime::vectorization;
using namespace omniruntime::mem;
using namespace omniruntime::op;
using namespace omniruntime::expressions;
using namespace omniruntime::TestUtil;

TEST(VectorizationTest, SimpleFunction)
{
    int rowSize = 3;
    auto type = std::make_shared<DataType>(OMNI_INT);
    std::vector<omniruntime::type::DataTypeId> args = {OMNI_INT, OMNI_INT};
    auto addExpr = new BinaryExpr(expressions::Operator::ADD, new FieldExpr(0, type), new FieldExpr(1, type), type);
    auto signature = std::make_shared<FunctionSignature>("add", args, OMNI_INT);
    addExpr->vectorFunction = SimpleFunctionRegistry::Find(signature);

    auto addExpr2 = new BinaryExpr(expressions::Operator::ADD, addExpr, new FieldExpr(1, type), type);
    addExpr2->vectorFunction = SimpleFunctionRegistry::Find(signature);

    std::vector vecOfTypes = {IntType(), IntType()};
    DataTypes inputTypes(vecOfTypes);
    int32_t col1[rowSize] = {1, 2, 3};
    int32_t col2[rowSize] = {3, 1, -4};
    VectorBatch *input = CreateVectorBatch(inputTypes, rowSize, col1, col2);
    input->Get(0)->SetNull(1);

    auto context = new ExecutionContext();
    context->SetResultRowSize(rowSize);

    ExprEval e(input, context);
    e.Visit(*addExpr2);
    auto result = e.GetResult();
    VectorBatch vectorBatch(rowSize);
    vectorBatch.Append(result);
    VectorHelper::PrintVecBatch(&vectorBatch);

    delete input;
    delete addExpr2;
    delete context;
}
