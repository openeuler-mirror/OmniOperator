/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: Slice function test
 */

#include <gtest/gtest.h>
#include <iostream>
#include <string>
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

TEST(SliceTest, SliceBasicTest)
{
    // Test case: slice([1, 2, 3, 4, 5], 1, 3) -> [1, 2, 3]
    int rowSize = 1;
    auto arrayType = std::make_shared<DataType>(OMNI_ARRAY);
    auto intType = std::make_shared<DataType>(OMNI_INT);
    auto types = DataTypes({arrayType});
    
    int32_t col[] = {1, 2, 3, 4, 5};
    std::vector<int32_t> offset = {0, 5};
    auto input = CreateArrayVectorBatch(types, offset, rowSize, 5, col);

    auto expr = FuncExpr("slice", {
        new FieldExpr(0, arrayType),
        new LiteralExpr(1, intType),  // start = 1 (1-based)
        new LiteralExpr(3, intType)   // length = 3
    }, arrayType);

    auto context = new ExecutionContext();
    context->SetResultRowSize(rowSize);

    ExprEval e(input, context);
    e.VisitExpr(expr);
    auto result = e.GetResult();
    
    // Verify result
    auto resultArray = dynamic_cast<ArrayVector *>(result);
    EXPECT_NE(resultArray, nullptr);
    EXPECT_EQ(resultArray->GetSize(), rowSize);
    EXPECT_FALSE(resultArray->IsNull(0));
    EXPECT_EQ(resultArray->GetSize(0), 3);  // Should have 3 elements
    
    // Verify elements: [1, 2, 3]
    auto elementVector = resultArray->GetElementVector();
    auto intVector = dynamic_cast<Vector<int32_t> *>(elementVector.get());
    EXPECT_NE(intVector, nullptr);
    int32_t offset = resultArray->GetOffset(0);
    EXPECT_EQ(intVector->GetValue(offset), 1);
    EXPECT_EQ(intVector->GetValue(offset + 1), 2);
    EXPECT_EQ(intVector->GetValue(offset + 2), 3);

    delete context;
    delete input;
}

TEST(SliceTest, SliceFromMiddleTest)
{
    // Test case: slice([1, 2, 3, 4, 5], 2, 2) -> [2, 3]
    int rowSize = 1;
    auto arrayType = std::make_shared<DataType>(OMNI_ARRAY);
    auto intType = std::make_shared<DataType>(OMNI_INT);
    auto types = DataTypes({arrayType});
    
    int32_t col[] = {1, 2, 3, 4, 5};
    std::vector<int32_t> offset = {0, 5};
    auto input = CreateArrayVectorBatch(types, offset, rowSize, 5, col);

    auto expr = FuncExpr("slice", {
        new FieldExpr(0, arrayType),
        new LiteralExpr(2, intType),  // start = 2
        new LiteralExpr(2, intType)   // length = 2
    }, arrayType);

    auto context = new ExecutionContext();
    context->SetResultRowSize(rowSize);

    ExprEval e(input, context);
    e.VisitExpr(expr);
    auto result = e.GetResult();
    
    auto resultArray = dynamic_cast<ArrayVector *>(result);
    EXPECT_NE(resultArray, nullptr);
    EXPECT_EQ(resultArray->GetSize(0), 2);
    
    auto elementVector = resultArray->GetElementVector();
    auto intVector = dynamic_cast<Vector<int32_t> *>(elementVector.get());
    int32_t offset = resultArray->GetOffset(0);
    EXPECT_EQ(intVector->GetValue(offset), 2);
    EXPECT_EQ(intVector->GetValue(offset + 1), 3);

    delete context;
    delete input;
}

TEST(SliceTest, SliceNegativeIndexTest)
{
    // Test case: slice([1, 2, 3, 4, 5], -2, 2) -> [4, 5]
    int rowSize = 1;
    auto arrayType = std::make_shared<DataType>(OMNI_ARRAY);
    auto intType = std::make_shared<DataType>(OMNI_INT);
    auto types = DataTypes({arrayType});
    
    int32_t col[] = {1, 2, 3, 4, 5};
    std::vector<int32_t> offset = {0, 5};
    auto input = CreateArrayVectorBatch(types, offset, rowSize, 5, col);

    auto expr = FuncExpr("slice", {
        new FieldExpr(0, arrayType),
        new LiteralExpr(-2, intType),  // start = -2 (from end)
        new LiteralExpr(2, intType)    // length = 2
    }, arrayType);

    auto context = new ExecutionContext();
    context->SetResultRowSize(rowSize);

    ExprEval e(input, context);
    e.VisitExpr(expr);
    auto result = e.GetResult();
    
    auto resultArray = dynamic_cast<ArrayVector *>(result);
    EXPECT_NE(resultArray, nullptr);
    EXPECT_EQ(resultArray->GetSize(0), 2);
    
    auto elementVector = resultArray->GetElementVector();
    auto intVector = dynamic_cast<Vector<int32_t> *>(elementVector.get());
    int32_t offset = resultArray->GetOffset(0);
    EXPECT_EQ(intVector->GetValue(offset), 4);
    EXPECT_EQ(intVector->GetValue(offset + 1), 5);

    delete context;
    delete input;
}

TEST(SliceTest, SliceExceedsBoundsTest)
{
    // Test case: slice([1, 2, 3], 2, 5) -> [2, 3] (length exceeds array size)
    int rowSize = 1;
    auto arrayType = std::make_shared<DataType>(OMNI_ARRAY);
    auto intType = std::make_shared<DataType>(OMNI_INT);
    auto types = DataTypes({arrayType});
    
    int32_t col[] = {1, 2, 3};
    std::vector<int32_t> offset = {0, 3};
    auto input = CreateArrayVectorBatch(types, offset, rowSize, 3, col);

    auto expr = FuncExpr("slice", {
        new FieldExpr(0, arrayType),
        new LiteralExpr(2, intType),  // start = 2
        new LiteralExpr(5, intType)   // length = 5 (exceeds array size)
    }, arrayType);

    auto context = new ExecutionContext();
    context->SetResultRowSize(rowSize);

    ExprEval e(input, context);
    e.VisitExpr(expr);
    auto result = e.GetResult();
    
    auto resultArray = dynamic_cast<ArrayVector *>(result);
    EXPECT_NE(resultArray, nullptr);
    EXPECT_EQ(resultArray->GetSize(0), 2);  // Should only return available elements
    
    auto elementVector = resultArray->GetElementVector();
    auto intVector = dynamic_cast<Vector<int32_t> *>(elementVector.get());
    int32_t offset = resultArray->GetOffset(0);
    EXPECT_EQ(intVector->GetValue(offset), 2);
    EXPECT_EQ(intVector->GetValue(offset + 1), 3);

    delete context;
    delete input;
}

TEST(SliceTest, SliceMultipleRowsTest)
{
    // Test case: Multiple rows with different slices
    int rowSize = 3;
    auto arrayType = std::make_shared<DataType>(OMNI_ARRAY);
    auto intType = std::make_shared<DataType>(OMNI_INT);
    auto types = DataTypes({arrayType});
    
    // Row 0: [1, 2, 3, 4, 5]
    // Row 1: [10, 20, 30]
    // Row 2: [100, 200, 300, 400]
    int32_t col[] = {1, 2, 3, 4, 5, 10, 20, 30, 100, 200, 300, 400};
    std::vector<int32_t> offset = {0, 5, 8, 12};
    auto input = CreateArrayVectorBatch(types, offset, rowSize, 12, col);

    auto expr = FuncExpr("slice", {
        new FieldExpr(0, arrayType),
        new LiteralExpr(1, intType),  // start = 1 for all rows
        new LiteralExpr(2, intType)    // length = 2 for all rows
    }, arrayType);

    auto context = new ExecutionContext();
    context->SetResultRowSize(rowSize);

    ExprEval e(input, context);
    e.VisitExpr(expr);
    auto result = e.GetResult();
    
    auto resultArray = dynamic_cast<ArrayVector *>(result);
    EXPECT_NE(resultArray, nullptr);
    EXPECT_EQ(resultArray->GetSize(), rowSize);
    
    // Verify each row
    auto elementVector = resultArray->GetElementVector();
    auto intVector = dynamic_cast<Vector<int32_t> *>(elementVector.get());
    
    // Row 0: [1, 2]
    EXPECT_EQ(resultArray->GetSize(0), 2);
    EXPECT_EQ(intVector->GetValue(resultArray->GetOffset(0)), 1);
    EXPECT_EQ(intVector->GetValue(resultArray->GetOffset(0) + 1), 2);
    
    // Row 1: [10, 20]
    EXPECT_EQ(resultArray->GetSize(1), 2);
    EXPECT_EQ(intVector->GetValue(resultArray->GetOffset(1)), 10);
    EXPECT_EQ(intVector->GetValue(resultArray->GetOffset(1) + 1), 20);
    
    // Row 2: [100, 200]
    EXPECT_EQ(resultArray->GetSize(2), 2);
    EXPECT_EQ(intVector->GetValue(resultArray->GetOffset(2)), 100);
    EXPECT_EQ(intVector->GetValue(resultArray->GetOffset(2) + 1), 200);

    delete context;
    delete input;
}

TEST(SliceTest, SliceEmptyArrayTest)
{
    // Test case: slice([], 1, 1) -> empty array
    int rowSize = 1;
    auto arrayType = std::make_shared<DataType>(OMNI_ARRAY);
    auto intType = std::make_shared<DataType>(OMNI_INT);
    auto types = DataTypes({arrayType});
    
    int32_t col[] = {};
    std::vector<int32_t> offset = {0, 0};
    auto input = CreateArrayVectorBatch(types, offset, rowSize, 0, col);

    auto expr = FuncExpr("slice", {
        new FieldExpr(0, arrayType),
        new LiteralExpr(1, intType),
        new LiteralExpr(1, intType)
    }, arrayType);

    auto context = new ExecutionContext();
    context->SetResultRowSize(rowSize);

    ExprEval e(input, context);
    e.VisitExpr(expr);
    auto result = e.GetResult();
    
    auto resultArray = dynamic_cast<ArrayVector *>(result);
    EXPECT_NE(resultArray, nullptr);
    // Should return null or empty array for out of bounds
    EXPECT_TRUE(resultArray->IsNull(0) || resultArray->GetSize(0) == 0);

    delete context;
    delete input;
}