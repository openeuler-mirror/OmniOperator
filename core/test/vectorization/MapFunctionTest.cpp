/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: codegen test
 */

#include <gtest/gtest.h>
#include <string>
#include <vector>

#include "test/util/test_util.h"
#include "vectorization/ExprEval.h"
#include "expression/expressions.h"

using namespace omniruntime;
using namespace omniruntime::vec;
using namespace omniruntime::vectorization;
using namespace omniruntime::mem;
using namespace omniruntime::op;
using namespace omniruntime::expressions;
using namespace omniruntime::TestUtil;

TEST(MapFunctionTest, MapKeysTest)
{
    int rowSize = 3;
    auto keyType = std::make_shared<DataType>(OMNI_INT);
    auto valueType = std::make_shared<DataType>(OMNI_INT);
    auto types = DataTypes({keyType, valueType});
    int32_t key[] = {1, 2, 3, 4, 5};
    int32_t value[] = {10, 20, 30, 40, 50};
    std::vector<int32_t> offset = {0,3,4,5};

    auto input = CreateMapVectorBatch(types, offset, rowSize, 5, key, value);

    auto field = new FieldExpr(0, std::make_shared<DataType>(OMNI_MAP));
    auto expr = FuncExpr("map_keys", {field}, std::make_shared<DataType>(OMNI_ARRAY));

    auto context = new ExecutionContext();
    context->SetResultRowSize(rowSize);
    ExprEval e(input, context);
    e.VisitExpr(expr);
    auto result = e.GetResult();

    ArrayVector *arrVec = dynamic_cast<ArrayVector*>(result);
    int32_t expect[] = {1, 2, 3, 4, 5};
    auto element = dynamic_cast<Vector<int32_t>*>(arrVec->GetElementVector().get());
    for (int i = 0; i < 5; ++i) {
        ASSERT_EQ(element->GetValue(i),expect[i]);
    }
    delete result;
    delete context;
    delete input;
}

TEST(MapFunctionTest, MapValuesTest)
{
    int rowSize = 3;
    auto keyType = std::make_shared<DataType>(OMNI_INT);
    auto valueType = std::make_shared<DataType>(OMNI_INT);
    auto types = DataTypes({keyType, valueType});
    int32_t key[] = {1, 2, 3, 4, 5};
    int32_t value[] = {10, 20, 30, 40, 50};
    std::vector<int32_t> offset = {0,3,4,5};

    auto input = CreateMapVectorBatch(types, offset, rowSize, 5, key, value);

    auto field = new FieldExpr(0, std::make_shared<DataType>(OMNI_MAP));
    auto expr = FuncExpr("map_values", {field}, std::make_shared<DataType>(OMNI_ARRAY));

    auto context = new ExecutionContext();
    context->SetResultRowSize(rowSize);
    ExprEval e(input, context);
    e.VisitExpr(expr);
    auto result = e.GetResult();

    ArrayVector *arrVec = dynamic_cast<ArrayVector*>(result);
    int32_t expect[] = {10, 20, 30, 40, 50};
    auto element = dynamic_cast<Vector<int32_t>*>(arrVec->GetElementVector().get());
    for (int i = 0; i < 5; ++i) {
        ASSERT_EQ(element->GetValue(i),expect[i]);
    }
    delete result;
    delete context;
    delete input;
}