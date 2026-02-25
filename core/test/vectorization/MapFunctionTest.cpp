/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Description: codegen test
 */

#include <gtest/gtest.h>
#include <string>
#include <vector>

#include "test/util/test_util.h"
#include "vectorization/ExprEval.h"
#include "expression/expressions.h"
#include "vector/row_vector.h"

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

TEST(MapFunctionTest, TransformKeysTest)
{
    int rowSize = 3;
    auto keyType = std::make_shared<DataType>(OMNI_INT);
    auto valueType = std::make_shared<DataType>(OMNI_INT);
    auto types = DataTypes({keyType, valueType});
    int32_t key[] = {1, 2, 3, 4, 5};
    int32_t value[] = {10, 20, 30, 40, 50};
    std::vector<int32_t> offset = {0,3,4,5};

    auto input = CreateMapVectorBatch(types, offset, rowSize, 5, key, value);

    std::vector<DataTypePtr> paramTypes;
    paramTypes.push_back(IntType());
    ParamRefExpr *addLeft = new ParamRefExpr("k", IntType());
    ParamRefExpr *addRight = new ParamRefExpr("v", IntType());
    BinaryExpr *addExpr = new BinaryExpr(omniruntime::expressions::Operator::ADD, addLeft, addRight, IntType());
    std::unordered_map<std::string, int32_t> paramNameToIdxMap;
    paramNameToIdxMap.emplace("k",0);
    paramNameToIdxMap.emplace("v",1);
    auto expr = FuncExpr("transform_keys", {
            new FieldExpr(0, std::make_shared<DataType>(OMNI_MAP)),
            new LambdaExpr(addExpr, paramTypes, paramNameToIdxMap, IntType())
    }, std::make_shared<DataType>(OMNI_MAP));

    auto context = new ExecutionContext();
    context->SetResultRowSize(rowSize);
    ExprEval e(input, context);
    e.VisitExpr(expr);
    auto result = e.GetResult();

    MapVector *mapVec = dynamic_cast<MapVector*>(result);
    auto keyElement = dynamic_cast<Vector<int32_t>*>(mapVec->GetKeyVector().get());
    auto ValueElement = dynamic_cast<Vector<int32_t>*>(mapVec->GetValueVector().get());
    int32_t expectKey[] = {11, 22, 33, 44 , 55};
    int32_t expectValue[] = {10, 20, 30, 40, 50};
    for (int i = 0; i < 5; ++i) {
        ASSERT_EQ(keyElement->GetValue(i),expectKey[i]);
        ASSERT_EQ(ValueElement->GetValue(i),expectValue[i]);
    }
    delete context;
    delete input;
    delete result;
}

TEST(MapFunctionTest, TransformValuesTest)
{
    int rowSize = 3;
    auto keyType = std::make_shared<DataType>(OMNI_INT);
    auto valueType = std::make_shared<DataType>(OMNI_INT);
    auto types = DataTypes({keyType, valueType});
    int32_t key[] = {1, 2, 3, 4, 5};
    int32_t value[] = {10, 20, 30, 40, 50};
    std::vector<int32_t> offset = {0,3,4,5};

    auto input = CreateMapVectorBatch(types, offset, rowSize, 5, key, value);

    std::vector<DataTypePtr> paramTypes;
    paramTypes.push_back(IntType());
    ParamRefExpr *addLeft = new ParamRefExpr("k", IntType());
    ParamRefExpr *addRight = new ParamRefExpr("v", IntType());
    BinaryExpr *addExpr = new BinaryExpr(omniruntime::expressions::Operator::ADD, addLeft, addRight, IntType());
    std::unordered_map<std::string, int32_t> paramNameToIdxMap;
    paramNameToIdxMap.emplace("k",0);
    paramNameToIdxMap.emplace("v",1);
    auto expr = FuncExpr("transform_values", {
            new FieldExpr(0, std::make_shared<DataType>(OMNI_MAP)),
            new LambdaExpr(addExpr, paramTypes, paramNameToIdxMap, IntType())
    }, std::make_shared<DataType>(OMNI_MAP));

    auto context = new ExecutionContext();
    context->SetResultRowSize(rowSize);
    ExprEval e(input, context);
    e.VisitExpr(expr);
    auto result = e.GetResult();

    MapVector *mapVec = dynamic_cast<MapVector*>(result);
    auto keyElement = dynamic_cast<Vector<int32_t>*>(mapVec->GetKeyVector().get());
    auto ValueElement = dynamic_cast<Vector<int32_t>*>(mapVec->GetValueVector().get());
    int32_t expectKey[] = {1, 2, 3, 4 , 5};
    int32_t expectValue[] = {11, 22, 33, 44, 55};
    for (int i = 0; i < 5; ++i) {
        ASSERT_EQ(keyElement->GetValue(i),expectKey[i]);
        ASSERT_EQ(ValueElement->GetValue(i),expectValue[i]);
    }
    delete context;
    delete input;
    delete result;
}

TEST(MapFunctionTest, MapEntriesBasicTest)
{
    int rowSize = 3;
    auto keyType = std::make_shared<DataType>(OMNI_INT);
    auto valueType = std::make_shared<DataType>(OMNI_INT);
    auto types = DataTypes({keyType, valueType});
    int32_t key[] = {1, 2, 3, 4, 5};
    int32_t value[] = {10, 20, 30, 40, 50};
    std::vector<int32_t> offset = {0, 3, 4, 5};

    auto input = CreateMapVectorBatch(types, offset, rowSize, 5, key, value);

    auto field = new FieldExpr(0, std::make_shared<DataType>(OMNI_MAP));
    auto expr = FuncExpr("map_entries", {field}, std::make_shared<DataType>(OMNI_ARRAY));

    auto context = new ExecutionContext();
    context->SetResultRowSize(rowSize);
    ExprEval e(input, context);
    e.VisitExpr(expr);
    auto result = e.GetResult();

    ArrayVector *arrVec = dynamic_cast<ArrayVector *>(result);
    ASSERT_NE(arrVec, nullptr);
    ASSERT_EQ(arrVec->GetSize(), rowSize);

    auto *rowElement = dynamic_cast<RowVector *>(arrVec->GetElementVector().get());
    ASSERT_NE(rowElement, nullptr);
    ASSERT_EQ(rowElement->ChildSize(), 2);

    auto *keyElement = dynamic_cast<Vector<int32_t> *>(rowElement->ChildAt(0).get());
    auto *valElement = dynamic_cast<Vector<int32_t> *>(rowElement->ChildAt(1).get());
    ASSERT_NE(keyElement, nullptr);
    ASSERT_NE(valElement, nullptr);

    int32_t expectK[] = {1, 2, 3, 4, 5};
    int32_t expectV[] = {10, 20, 30, 40, 50};
    for (int i = 0; i < 5; ++i) {
        ASSERT_EQ(keyElement->GetValue(i), expectK[i]);
        ASSERT_EQ(valElement->GetValue(i), expectV[i]);
    }

    ASSERT_EQ(arrVec->GetOffset(0), 0);
    ASSERT_EQ(arrVec->GetOffset(1), 3);
    ASSERT_EQ(arrVec->GetOffset(2), 4);
    ASSERT_EQ(arrVec->GetOffset(3), 5);

    delete result;
    delete context;
    delete input;
}

TEST(MapFunctionTest, MapEntriesSingleEntryTest)
{
    int rowSize = 1;
    auto keyType = std::make_shared<DataType>(OMNI_INT);
    auto valueType = std::make_shared<DataType>(OMNI_INT);
    auto types = DataTypes({keyType, valueType});
    int32_t key2[] = {42};
    int32_t value2[] = {100};
    std::vector<int32_t> offset = {0, 1};

    auto input = CreateMapVectorBatch(types, offset, rowSize, 1, key2, value2);

    auto field = new FieldExpr(0, std::make_shared<DataType>(OMNI_MAP));
    auto expr = FuncExpr("map_entries", {field}, std::make_shared<DataType>(OMNI_ARRAY));

    auto context = new ExecutionContext();
    context->SetResultRowSize(rowSize);
    ExprEval e(input, context);
    e.VisitExpr(expr);
    auto result = e.GetResult();

    ArrayVector *arrVec = dynamic_cast<ArrayVector *>(result);
    ASSERT_NE(arrVec, nullptr);
    ASSERT_EQ(arrVec->GetSize(), 1);

    auto *rowElement = dynamic_cast<RowVector *>(arrVec->GetElementVector().get());
    ASSERT_NE(rowElement, nullptr);
    ASSERT_EQ(rowElement->ChildSize(), 2);

    auto *keyElement = dynamic_cast<Vector<int32_t> *>(rowElement->ChildAt(0).get());
    auto *valElement = dynamic_cast<Vector<int32_t> *>(rowElement->ChildAt(1).get());
    ASSERT_EQ(keyElement->GetValue(0), 42);
    ASSERT_EQ(valElement->GetValue(0), 100);

    ASSERT_EQ(arrVec->GetOffset(0), 0);
    ASSERT_EQ(arrVec->GetOffset(1), 1);

    delete result;
    delete context;
    delete input;
}

TEST(MapFunctionTest, MapEntriesNullRowTest)
{
    int rowSize = 3;
    auto keyType = std::make_shared<DataType>(OMNI_INT);
    auto valueType = std::make_shared<DataType>(OMNI_INT);
    auto types = DataTypes({keyType, valueType});
    int32_t key3[] = {1, 2, 3, 4, 5};
    int32_t value3[] = {10, 20, 30, 40, 50};
    std::vector<int32_t> offset = {0, 3, 3, 5};

    auto input = CreateMapVectorBatch(types, offset, rowSize, 5, key3, value3);

    auto *mapVec = dynamic_cast<MapVector *>(input->Get(0));
    mapVec->SetNull(1);

    auto field = new FieldExpr(0, std::make_shared<DataType>(OMNI_MAP));
    auto expr = FuncExpr("map_entries", {field}, std::make_shared<DataType>(OMNI_ARRAY));

    auto context = new ExecutionContext();
    context->SetResultRowSize(rowSize);
    ExprEval e(input, context);
    e.VisitExpr(expr);
    auto result = e.GetResult();

    ArrayVector *arrVec = dynamic_cast<ArrayVector *>(result);
    ASSERT_NE(arrVec, nullptr);

    ASSERT_FALSE(arrVec->IsNull(0));
    ASSERT_TRUE(arrVec->IsNull(1));
    ASSERT_FALSE(arrVec->IsNull(2));

    delete result;
    delete context;
    delete input;
}

TEST(MapFunctionTest, MapEntriesMultiRowTest)
{
    int rowSize = 4;
    auto keyType = std::make_shared<DataType>(OMNI_INT);
    auto valueType = std::make_shared<DataType>(OMNI_INT);
    auto types = DataTypes({keyType, valueType});
    int32_t key4[] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    int32_t value4[] = {100, 200, 300, 400, 500, 600, 700, 800, 900, 1000};
    std::vector<int32_t> offset = {0, 2, 5, 7, 10};

    auto input = CreateMapVectorBatch(types, offset, rowSize, 10, key4, value4);

    auto field = new FieldExpr(0, std::make_shared<DataType>(OMNI_MAP));
    auto expr = FuncExpr("map_entries", {field}, std::make_shared<DataType>(OMNI_ARRAY));

    auto context = new ExecutionContext();
    context->SetResultRowSize(rowSize);
    ExprEval e(input, context);
    e.VisitExpr(expr);
    auto result = e.GetResult();

    ArrayVector *arrVec = dynamic_cast<ArrayVector *>(result);
    ASSERT_NE(arrVec, nullptr);
    ASSERT_EQ(arrVec->GetSize(), rowSize);

    ASSERT_EQ(arrVec->GetSize(0), 2);
    ASSERT_EQ(arrVec->GetSize(1), 3);
    ASSERT_EQ(arrVec->GetSize(2), 2);
    ASSERT_EQ(arrVec->GetSize(3), 3);

    auto *rowElement = dynamic_cast<RowVector *>(arrVec->GetElementVector().get());
    auto *keyElement = dynamic_cast<Vector<int32_t> *>(rowElement->ChildAt(0).get());
    auto *valElement = dynamic_cast<Vector<int32_t> *>(rowElement->ChildAt(1).get());

    for (int i = 0; i < 10; ++i) {
        ASSERT_EQ(keyElement->GetValue(i), key4[i]);
        ASSERT_EQ(valElement->GetValue(i), value4[i]);
    }

    delete result;
    delete context;
    delete input;
}