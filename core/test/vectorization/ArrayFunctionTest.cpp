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

TEST(ArrayFunctionTest, GetArrayItemTest)
{
    int rowSize = 3;
    auto type = std::make_shared<DataType>(OMNI_INT);
    auto types = DataTypes({type});
    std::vector vecOfTypes = {IntType(), IntType()};
    DataTypes inputTypes(vecOfTypes);
    int32_t col[] = {1, 2, 3, 4, 5};
    std::vector<int32_t> offset = {0,3,4,5};
    auto input = CreateArrayVectorBatch(types, offset, rowSize, 5, col);

    auto expr = FuncExpr("get_array_item", {
        new FieldExpr(0, std::make_shared<DataType>(OMNI_ARRAY)),
        new LiteralExpr(0, type)
    }, type);

    auto context = new ExecutionContext();
    context->SetResultRowSize(rowSize);

    int intputTypes[] = {30};
    ExprEval e(input, context);
    e.VisitExpr(expr);
    auto result = e.GetResult();
    VectorBatch vectorBatch(rowSize);
    vectorBatch.Append(result);
    VectorHelper::PrintVecBatch(&vectorBatch);

    delete context;
    delete input;
}
