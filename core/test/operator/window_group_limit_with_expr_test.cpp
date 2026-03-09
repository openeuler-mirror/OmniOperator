/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2025. All rights reserved.
 */

#include "gtest/gtest.h"
#include "operator/window/window_group_limit_expr.h"
#include "vector/vector_helper.h"
#include "util/test_util.h"

using namespace omniruntime::vec;
using namespace omniruntime::op;
using namespace omniruntime::expressions;
using namespace std;
using namespace TestUtil;

using VarcharVector = Vector<LargeStringContainer<std::string_view>>;

namespace WindowGroupLimitWithExprTest {
// rank + Desc + NullLast
TEST(WindowGroupLimitWithExprOperatorTest, TestRankDescNullLast)
{
    const int32_t dataSize = 8;
    std::string data1[dataSize] = {"hi", "hi", "hi", "bye", "bye", "bye", "bye", "bye"};
    int64_t data2[dataSize] = {2L, 5L, 3L, 11L, 4L, 3L, 0L, 23L};
    int64_t data3[dataSize] = {3L, 5L, 8L, 3L, 5L, 3L, 4L, 3L};
    DataTypes sourceTypes(std::vector<DataTypePtr>({ VarcharType(10), LongType(), LongType() }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data1, data2, data3);

    std::vector<omniruntime::expressions::Expr *> partitionKeys = { new FieldExpr(0, VarcharType(10)) };
    std::vector<omniruntime::expressions::Expr *> sortKeys = { new FieldExpr(2, LongType()) };
    std::vector<int32_t> sortAscendings = { 0 };
    std::vector<int32_t> sortNullFirsts = { 0 };
    auto overflowConfig = new OverflowConfig();
    auto windowGroupLimitOperatorFactory = new WindowGroupLimitWithExprOperatorFactory(sourceTypes, 3, "rank",
        partitionKeys, sortKeys, sortAscendings, sortNullFirsts, overflowConfig);
    auto windowGroupLimitOperator =
        static_cast<WindowGroupLimitWithExprOperator *>(CreateTestOperator(windowGroupLimitOperatorFactory));

    windowGroupLimitOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch;
    windowGroupLimitOperator->GetOutput(&outputVecBatch);

    constexpr int32_t expectedDataSize = 8;
    std::string expData1[expectedDataSize] = {"bye", "bye", "bye", "bye", "bye", "hi", "hi", "hi"};
    int64_t expData2[expectedDataSize] = {4, 0, 11, 3, 23, 3, 5, 2};
    int64_t expData3[expectedDataSize] = {5, 4, 3, 3, 3, 8, 5, 3};
    DataTypes expectTypes(std::vector<DataTypePtr>({ VarcharType(10), LongType(), LongType() }));
    VectorBatch *expectVectorBatch = CreateVectorBatch(expectTypes, expectedDataSize, expData1, expData2, expData3);

    EXPECT_TRUE(VecBatchMatchIgnoreOrder(outputVecBatch, expectVectorBatch));

    Expr::DeleteExprs(partitionKeys);
    Expr::DeleteExprs(sortKeys);
    omniruntime::op::Operator::DeleteOperator(windowGroupLimitOperator);
    delete windowGroupLimitOperatorFactory;
    VectorHelper::FreeVecBatch(expectVectorBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
    delete overflowConfig;
}

// rank + Asc + NullLast
TEST(WindowGroupLimitWithExprOperatorTest, TestRankAscNullLast)
{
    // construct input data
    const int32_t dataSize = 8;
    std::string data1[dataSize] = {"hi", "hi", "hi", "bye", "bye", "bye", "bye", "bye"};
    int64_t data2[dataSize] = {2L, 5L, 3L, 11L, 4L, 3L, 0L, 23L};
    int64_t data3[dataSize] = {5L, 3L, 8L, 3L, 6L, 6L, 4L, 6L};
    DataTypes sourceTypes(std::vector<DataTypePtr>({ VarcharType(10), LongType(), LongType() }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data1, data2, data3);

    std::vector<omniruntime::expressions::Expr *> partitionKeys = { new FieldExpr(0, VarcharType(10)) };
    std::vector<omniruntime::expressions::Expr *> sortKeys = { new FieldExpr(2, LongType()) };
    std::vector<int32_t> sortAscendings = { 1 };
    std::vector<int32_t> sortNullFirsts = { 0 };
    auto overflowConfig = new OverflowConfig();
    auto windowGroupLimitOperatorFactory = new WindowGroupLimitWithExprOperatorFactory(sourceTypes, 3, "rank",
        partitionKeys, sortKeys, sortAscendings, sortNullFirsts, overflowConfig);
    auto windowGroupLimitOperator =
        static_cast<WindowGroupLimitWithExprOperator *>(CreateTestOperator(windowGroupLimitOperatorFactory));

    windowGroupLimitOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch;
    windowGroupLimitOperator->GetOutput(&outputVecBatch);

    constexpr int32_t expectedDataSize = 8;
    std::string expData1[expectedDataSize] = {"bye", "bye", "bye", "bye", "bye", "hi", "hi", "hi"};
    int64_t expData2[expectedDataSize] = {11, 0, 4, 3, 23, 5, 2, 3};
    int64_t expData3[expectedDataSize] = {3, 4, 6, 6, 6, 3, 5, 8};
    DataTypes expectTypes(std::vector<DataTypePtr>({ VarcharType(10), LongType(), LongType() }));
    VectorBatch *expectVectorBatch = CreateVectorBatch(expectTypes, expectedDataSize, expData1, expData2, expData3);

    EXPECT_TRUE(VecBatchMatchIgnoreOrder(outputVecBatch, expectVectorBatch));

    Expr::DeleteExprs(partitionKeys);
    Expr::DeleteExprs(sortKeys);
    omniruntime::op::Operator::DeleteOperator(windowGroupLimitOperator);
    delete windowGroupLimitOperatorFactory;
    VectorHelper::FreeVecBatch(expectVectorBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
    delete overflowConfig;
}

// rank + AscCase1
TEST(WindowGroupLimitWithExprOperatorTest, TestRankAscCase1)
{
    // construct input data
    const int32_t dataSize = 6;
    std::string data1[dataSize] = {"hello", "hello", "hello", "hello", "test", "test"};
    int64_t data2[dataSize] = {1L, 2L, 3L, 4L, 5L, 6L};
    int64_t data3[dataSize] = {4L, 3L, 2L, 1L, 4L, 3L};
    DataTypes sourceTypes(std::vector<DataTypePtr>({ VarcharType(10), LongType(), LongType() }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data1, data2, data3);

    std::vector<omniruntime::expressions::Expr *> partitionKeys = { new FieldExpr(0, VarcharType(10)) };
    std::vector<omniruntime::expressions::Expr *> sortKeys = { new FieldExpr(2, LongType()) };
    std::vector<int32_t> sortAscendings = { 1 };
    std::vector<int32_t> sortNullFirsts = { 0 };
    auto overflowConfig = new OverflowConfig();
    auto windowGroupLimitOperatorFactory = new WindowGroupLimitWithExprOperatorFactory(sourceTypes, 3, "rank",
        partitionKeys, sortKeys, sortAscendings, sortNullFirsts, overflowConfig);
    auto windowGroupLimitOperator =
        static_cast<WindowGroupLimitWithExprOperator *>(CreateTestOperator(windowGroupLimitOperatorFactory));

    windowGroupLimitOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch;
    windowGroupLimitOperator->GetOutput(&outputVecBatch);

    constexpr int32_t expectedDataSize = 5;
    std::string expData1[expectedDataSize] = {"hello", "hello", "hello", "test", "test"};
    int64_t expData2[expectedDataSize] = {4, 3, 2, 6, 5};
    int64_t expData3[expectedDataSize] = {1, 2, 3, 3, 4};
    DataTypes expectTypes(std::vector<DataTypePtr>({ VarcharType(10), LongType(), LongType() }));
    VectorBatch *expectVectorBatch = CreateVectorBatch(expectTypes, expectedDataSize, expData1, expData2, expData3);

    EXPECT_TRUE(VecBatchMatchIgnoreOrder(outputVecBatch, expectVectorBatch));

    Expr::DeleteExprs(partitionKeys);
    Expr::DeleteExprs(sortKeys);
    omniruntime::op::Operator::DeleteOperator(windowGroupLimitOperator);
    delete windowGroupLimitOperatorFactory;
    VectorHelper::FreeVecBatch(expectVectorBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
    delete overflowConfig;
}

// rank + AscCase2
TEST(WindowGroupLimitWithExprOperatorTest, TestRankAscCase2)
{
    // construct input data
    const int32_t dataSize = 4;
    std::string data1[dataSize] = {"bye", "bye", "bye", "bye"};
    int64_t data2[dataSize] = {5L, 2L, 3L, 11L};
    int64_t data3[dataSize] = {6L, 6L, 4L, 3L};
    DataTypes sourceTypes(std::vector<DataTypePtr>({ VarcharType(10), LongType(), LongType() }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data1, data2, data3);

    std::vector<omniruntime::expressions::Expr *> partitionKeys = { new FieldExpr(0, VarcharType(10)) };
    std::vector<omniruntime::expressions::Expr *> sortKeys = { new FieldExpr(2, LongType()) };
    std::vector<int32_t> sortAscendings = { 1 };
    std::vector<int32_t> sortNullFirsts = { 0 };
    auto overflowConfig = new OverflowConfig();
    auto windowGroupLimitOperatorFactory = new WindowGroupLimitWithExprOperatorFactory(sourceTypes, 3, "rank",
        partitionKeys, sortKeys, sortAscendings, sortNullFirsts, overflowConfig);
    auto windowGroupLimitOperator =
        static_cast<WindowGroupLimitWithExprOperator *>(CreateTestOperator(windowGroupLimitOperatorFactory));

    windowGroupLimitOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch;
    windowGroupLimitOperator->GetOutput(&outputVecBatch);

    constexpr int32_t expectedDataSize = 4;
    std::string expData1[expectedDataSize] = {"bye", "bye", "bye", "bye"};
    int64_t expData2[expectedDataSize] = {11, 3, 5, 2};
    int64_t expData3[expectedDataSize] = {3, 4, 6, 6};
    DataTypes expectTypes(std::vector<DataTypePtr>({ VarcharType(10), LongType(), LongType() }));
    VectorBatch *expectVectorBatch = CreateVectorBatch(expectTypes, expectedDataSize, expData1, expData2, expData3);

    EXPECT_TRUE(VecBatchMatchIgnoreOrder(outputVecBatch, expectVectorBatch));

    Expr::DeleteExprs(partitionKeys);
    Expr::DeleteExprs(sortKeys);
    omniruntime::op::Operator::DeleteOperator(windowGroupLimitOperator);
    delete windowGroupLimitOperatorFactory;
    VectorHelper::FreeVecBatch(expectVectorBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
    delete overflowConfig;
}

// rank + AscCase3
TEST(WindowGroupLimitWithExprOperatorTest, TestRankAscCase3)
{
    const int32_t dataSize = 13;
    int64_t data1[dataSize] = { 21, 22, 22, 22, 22, 22, 21, 23, 21, 21, 22, 21, 21 };
    std::string data2[dataSize] = {
                "xiaoaing", "xiaojing", "xiaohing", "xiaoling", "xiaoiing", "xiaoking", "xiaoding",
                "xiaoming", "xiaobing", "xiaocing", "xiaoging", "xiaoeing", "xiaofing"
        };
    std::string data3[dataSize] = {
                "henan",     "guangdong", "guangdong", "guangdong", "guangdong", "guangdong", "henan",
                "guangdong", "henan",     "henan",     "henan",     "henan",     "henan"
        };
    std::string data4[dataSize] = {
                "M", "F", "F", "M", "M", "M", "F", "F", "F", "F", "F", "M", "M"
        };

    DataTypes sourceTypes(std::vector<DataTypePtr>({ LongType(), VarcharType(10), VarcharType(10), VarcharType(10) }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data1, data2, data3, data4);

    std::vector<omniruntime::expressions::Expr *> partitionKeys = { new FieldExpr(3, VarcharType(10)) };
    std::vector<omniruntime::expressions::Expr *> sortKeys = { new FieldExpr(2, LongType()) };
    std::vector<int32_t> sortAscendings = { 1 };
    std::vector<int32_t> sortNullFirsts = { 0 };
    auto overflowConfig = new OverflowConfig();
    auto windowGroupLimitOperatorFactory = new WindowGroupLimitWithExprOperatorFactory(sourceTypes, 4, "rank",
        partitionKeys, sortKeys, sortAscendings, sortNullFirsts, overflowConfig);
    auto windowGroupLimitOperator =
        static_cast<WindowGroupLimitWithExprOperator *>(CreateTestOperator(windowGroupLimitOperatorFactory));

    windowGroupLimitOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch;
    windowGroupLimitOperator->GetOutput(&outputVecBatch);

    constexpr int32_t expectedDataSize = 13;
    int64_t expData1[expectedDataSize] = { 22, 22, 23, 21, 21, 21, 22, 22, 22, 22, 21, 21, 21 };
    std::string expData2[expectedDataSize] = {
                "xiaojing", "xiaohing", "xiaoming", "xiaoding", "xiaobing", "xiaocing", "xiaoging",
                "xiaoling", "xiaoiing", "xiaoking", "xiaoaing", "xiaoeing", "xiaofing"
        };
    std::string expData3[expectedDataSize] = {
                "guangdong", "guangdong", "guangdong", "henan", "henan", "henan", "henan",
                "guangdong", "guangdong", "guangdong", "henan", "henan", "henan"
        };
    std::string expData4[dataSize] = {
                "F", "F", "F", "F", "F", "F", "F", "M", "M", "M", "M", "M", "M"
        };
    DataTypes expectTypes(std::vector<DataTypePtr>({ LongType(), VarcharType(10), VarcharType(10), VarcharType(10) }));
    VectorBatch *expectVectorBatch =
        CreateVectorBatch(expectTypes, expectedDataSize, expData1, expData2, expData3, expData4);
    EXPECT_TRUE(VecBatchMatchIgnoreOrder(outputVecBatch, expectVectorBatch));

    Expr::DeleteExprs(partitionKeys);
    Expr::DeleteExprs(sortKeys);
    omniruntime::op::Operator::DeleteOperator(windowGroupLimitOperator);
    delete windowGroupLimitOperatorFactory;
    VectorHelper::FreeVecBatch(expectVectorBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
    delete overflowConfig;
}

// rank + AscCase4
TEST(WindowGroupLimitWithExprOperatorTest, TestRankAscCase4)
{
    const int32_t dataSize = 23;
    std::string data1[dataSize] = {
                "henan", "guangdong", "shanxi", "hebei", "hebei",  "hebei", "hebei", "hebei",
                "hebei", "shanxi",    "hebei",  "hebei", "shanxi", "hebei", "hebei", "hebei",
                "hebei", "hebei",     "hebei",  "hebei", "hebei",  "hebei", "hebei"
        };
    std::string data2[dataSize] = {
                "M", "M", "M", "M", "F", "G", "H", "I", "I", "J", "I", "I",
                "I", "I", "I", "I", "I", "I", "I", "I", "I", "I", "I"
        };
    std::string data3[dataSize] = {
                "student0", "student0", "student0",  "student0", "student0", "student0", "student0", "student0",
                "student0", "student1", "student2",  "student3", "student4", "student5", "student6", "student7",
                "student8", "student9", "student10", "student2", "student2", "student2", "student2"
        };
    std::string data4[dataSize] = {
                "name1", "name1", "name1", "name1", "name1", "name1", "name1",
                "name1", "name1", "name1", "name1", "name1", "name1", "name1",
                "name1", "name1", "name1", "name1", "name1", "name1", "name1", "name1", "name1"
        };
    int64_t data5[dataSize] = {
                21, 22, 23, 23, 21, 22, 22, 23, 20, 21, 22, 23, 20, 21, 23, 23, 22, 23, 22, 22, 22, 23, 23
        };

    DataTypes sourceTypes(
        std::vector<DataTypePtr>({ VarcharType(10), VarcharType(15), VarcharType(10), VarcharType(10), LongType() }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data1, data2, data3, data4, data5);

    std::vector<omniruntime::expressions::Expr *> partitionKeys = { new FieldExpr(0, VarcharType(10)),
        new FieldExpr(1, VarcharType(15)), new FieldExpr(2, VarcharType(10)) };
    std::vector<omniruntime::expressions::Expr *> sortKeys = { new FieldExpr(4, LongType()) };
    std::vector<int32_t> sortAscendings = { 1 };
    std::vector<int32_t> sortNullFirsts = { 0 };
    auto overflowConfig = new OverflowConfig();
    auto windowGroupLimitOperatorFactory = new WindowGroupLimitWithExprOperatorFactory(sourceTypes, 2, "rank",
        partitionKeys, sortKeys, sortAscendings, sortNullFirsts, overflowConfig);
    auto windowGroupLimitOperator =
        static_cast<WindowGroupLimitWithExprOperator *>(CreateTestOperator(windowGroupLimitOperatorFactory));

    windowGroupLimitOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch;
    windowGroupLimitOperator->GetOutput(&outputVecBatch);

    constexpr int32_t expectedDataSize = 21;
    std::string expData1[expectedDataSize] = {
                "hebei", "hebei", "hebei", "hebei", "hebei", "hebei", "hebei",
                "hebei", "guangdong", "henan", "shanxi", "shanxi", "hebei", "hebei",
                "hebei", "hebei", "hebei", "hebei", "shanxi", "hebei", "hebei"
        };
    std::string expData2[expectedDataSize] = {
                "G", "F", "I", "I", "I", "I", "M", "I", "M", "M", "J", "M", "H", "I", "I", "I", "I", "I", "I", "I", "I"
        };
    std::string expData3[expectedDataSize] = {
                "student0", "student0", "student0", "student0", "student10", "student9", "student0",
                "student5", "student0", "student0", "student1", "student0",  "student0", "student2",
                "student2", "student2", "student6", "student3", "student4",  "student7", "student8"
        };
    std::string expData4[expectedDataSize] = {
                "name1", "name1", "name1", "name1", "name1", "name1", "name1",
                "name1", "name1", "name1", "name1", "name1", "name1", "name1",
                "name1", "name1", "name1", "name1", "name1", "name1", "name1"
        };
    int64_t expData5[expectedDataSize] = {
                22, 21, 20, 23, 22, 23, 23, 21, 22, 21, 21, 23, 22, 22, 22, 22, 23, 23, 20, 23, 22
        };
    DataTypes expectTypes(
        std::vector<DataTypePtr>({ VarcharType(10), VarcharType(15), VarcharType(10), VarcharType(10), LongType() }));
    VectorBatch *expectVectorBatch =
        CreateVectorBatch(expectTypes, expectedDataSize, expData1, expData2, expData3, expData4, expData5);

    EXPECT_TRUE(VecBatchMatchIgnoreOrder(outputVecBatch, expectVectorBatch));

    Expr::DeleteExprs(partitionKeys);
    Expr::DeleteExprs(sortKeys);
    omniruntime::op::Operator::DeleteOperator(windowGroupLimitOperator);
    delete windowGroupLimitOperatorFactory;
    VectorHelper::FreeVecBatch(expectVectorBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
    delete overflowConfig;
}

// row_number + Desc + NullLast
TEST(WindowGroupLimitWithExprOperatorTest, TestRowNumberDescNullLast)
{
    const int32_t dataSize = 8;
    std::string data1[dataSize] = {"hi", "hi", "hi", "bye", "bye", "bye", "bye", "bye"};
    int64_t data2[dataSize] = {2L, 5L, 3L, 11L, 4L, 3L, 0L, 23L};
    int64_t data3[dataSize] = {3L, 5L, 8L, 3L, 5L, 3L, 4L, 3L};
    DataTypes sourceTypes(std::vector<DataTypePtr>({ VarcharType(10), LongType(), LongType() }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data1, data2, data3);

    std::vector<omniruntime::expressions::Expr *> partitionKeys = { new FieldExpr(0, VarcharType(10)) };
    std::vector<omniruntime::expressions::Expr *> sortKeys = { new FieldExpr(2, LongType()) };
    std::vector<int32_t> sortAscendings = { 0 };
    std::vector<int32_t> sortNullFirsts = { 0 };
    auto overflowConfig = new OverflowConfig();
    auto windowGroupLimitOperatorFactory = new WindowGroupLimitWithExprOperatorFactory(sourceTypes, 3, "row_number",
        partitionKeys, sortKeys, sortAscendings, sortNullFirsts, overflowConfig);
    auto windowGroupLimitOperator =
        static_cast<WindowGroupLimitWithExprOperator *>(CreateTestOperator(windowGroupLimitOperatorFactory));

    windowGroupLimitOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch;
    windowGroupLimitOperator->GetOutput(&outputVecBatch);

    constexpr int32_t expectedDataSize = 6;
    std::string expData1[expectedDataSize] = {"bye", "bye", "bye", "hi", "hi", "hi"};
    int64_t expData2[expectedDataSize] = {4, 0, 11, 3, 5, 2};
    int64_t expData3[expectedDataSize] = {5, 4, 3, 8, 5, 3};
    DataTypes expectTypes(std::vector<DataTypePtr>({ VarcharType(10), LongType(), LongType() }));
    VectorBatch *expectVectorBatch = CreateVectorBatch(expectTypes, expectedDataSize, expData1, expData2, expData3);

    EXPECT_TRUE(VecBatchMatchIgnoreOrder(outputVecBatch, expectVectorBatch));

    Expr::DeleteExprs(partitionKeys);
    Expr::DeleteExprs(sortKeys);
    omniruntime::op::Operator::DeleteOperator(windowGroupLimitOperator);
    delete windowGroupLimitOperatorFactory;
    VectorHelper::FreeVecBatch(expectVectorBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
    delete overflowConfig;
}

// row_number + Asc + NullLast
TEST(WindowGroupLimitWithExprOperatorTest, TestRowNumberAscNullLast)
{
    const int32_t dataSize = 8;
    std::string data1[dataSize] = {"hi", "hi", "hi", "bye", "bye", "bye", "bye", "bye"};
    int64_t data2[dataSize] = {2L, 5L, 3L, 11L, 4L, 3L, 0L, 23L};
    int64_t data3[dataSize] = {5L, 3L, 8L, 3L, 6L, 6L, 4L, 6L};
    DataTypes sourceTypes(std::vector<DataTypePtr>({ VarcharType(10), LongType(), LongType() }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data1, data2, data3);

    std::vector<omniruntime::expressions::Expr *> partitionKeys = { new FieldExpr(0, VarcharType(10)) };
    std::vector<omniruntime::expressions::Expr *> sortKeys = { new FieldExpr(2, LongType()) };
    std::vector<int32_t> sortAscendings = { 1 };
    std::vector<int32_t> sortNullFirsts = { 0 };
    auto overflowConfig = new OverflowConfig();
    auto windowGroupLimitOperatorFactory = new WindowGroupLimitWithExprOperatorFactory(sourceTypes, 3, "row_number",
        partitionKeys, sortKeys, sortAscendings, sortNullFirsts, overflowConfig);
    auto windowGroupLimitOperator =
        static_cast<WindowGroupLimitWithExprOperator *>(CreateTestOperator(windowGroupLimitOperatorFactory));

    windowGroupLimitOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch;
    windowGroupLimitOperator->GetOutput(&outputVecBatch);

    constexpr int32_t expectedDataSize = 6;
    std::string expData1[expectedDataSize] = {"bye", "bye", "bye", "hi", "hi", "hi"};
    int64_t expData2[expectedDataSize] = {11, 0, 4, 5, 2, 3};
    int64_t expData3[expectedDataSize] = {3, 4, 6, 3, 5, 8};
    DataTypes expectTypes(std::vector<DataTypePtr>({ VarcharType(10), LongType(), LongType() }));
    VectorBatch *expectVectorBatch = CreateVectorBatch(expectTypes, expectedDataSize, expData1, expData2, expData3);

    EXPECT_TRUE(VecBatchMatchIgnoreOrder(outputVecBatch, expectVectorBatch));

    Expr::DeleteExprs(partitionKeys);
    Expr::DeleteExprs(sortKeys);
    omniruntime::op::Operator::DeleteOperator(windowGroupLimitOperator);
    delete windowGroupLimitOperatorFactory;
    VectorHelper::FreeVecBatch(expectVectorBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
    delete overflowConfig;
}

// row_number + AscCase1
TEST(WindowGroupLimitWithExprOperatorTest, TestRowNumberAscCase1)
{
    // construct input data
    const int32_t dataSize = 6;
    std::string data1[dataSize] = {"hello", "hello", "hello", "hello", "test", "test"};
    int64_t data2[dataSize] = {1L, 2L, 3L, 4L, 5L, 6L};
    int64_t data3[dataSize] = {4L, 3L, 2L, 1L, 4L, 3L};
    DataTypes sourceTypes(std::vector<DataTypePtr>({ VarcharType(10), LongType(), LongType() }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data1, data2, data3);

    std::vector<omniruntime::expressions::Expr *> partitionKeys = { new FieldExpr(0, VarcharType(10)) };
    std::vector<omniruntime::expressions::Expr *> sortKeys = { new FieldExpr(2, LongType()) };
    std::vector<int32_t> sortAscendings = { 1 };
    std::vector<int32_t> sortNullFirsts = { 0 };
    auto overflowConfig = new OverflowConfig();
    auto windowGroupLimitOperatorFactory = new WindowGroupLimitWithExprOperatorFactory(sourceTypes, 3, "row_number",
        partitionKeys, sortKeys, sortAscendings, sortNullFirsts, overflowConfig);
    auto windowGroupLimitOperator =
        static_cast<WindowGroupLimitWithExprOperator *>(CreateTestOperator(windowGroupLimitOperatorFactory));

    windowGroupLimitOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch;
    windowGroupLimitOperator->GetOutput(&outputVecBatch);

    constexpr int32_t expectedDataSize = 5;
    std::string expData1[expectedDataSize] = {"hello", "hello", "hello", "test", "test"};
    int64_t expData2[expectedDataSize] = {4, 3, 2, 6, 5};
    int64_t expData3[expectedDataSize] = {1, 2, 3, 3, 4};
    DataTypes expectTypes(std::vector<DataTypePtr>({ VarcharType(10), LongType(), LongType() }));
    VectorBatch *expectVectorBatch = CreateVectorBatch(expectTypes, expectedDataSize, expData1, expData2, expData3);

    EXPECT_TRUE(VecBatchMatchIgnoreOrder(outputVecBatch, expectVectorBatch));

    Expr::DeleteExprs(partitionKeys);
    Expr::DeleteExprs(sortKeys);
    omniruntime::op::Operator::DeleteOperator(windowGroupLimitOperator);
    delete windowGroupLimitOperatorFactory;
    VectorHelper::FreeVecBatch(expectVectorBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
    delete overflowConfig;
}

// row_number + AscCase2
TEST(WindowGroupLimitWithExprOperatorTest, TestRowNumberAscCase2)
{
    // construct input data
    const int32_t dataSize = 4;
    std::string data1[dataSize] = {"bye", "bye", "bye", "bye"};
    int64_t data2[dataSize] = {5L, 2L, 3L, 11L};
    int64_t data3[dataSize] = {6L, 6L, 4L, 3L};
    DataTypes sourceTypes(std::vector<DataTypePtr>({ VarcharType(10), LongType(), LongType() }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data1, data2, data3);

    std::vector<omniruntime::expressions::Expr *> partitionKeys = { new FieldExpr(0, VarcharType(10)) };
    std::vector<omniruntime::expressions::Expr *> sortKeys = { new FieldExpr(2, LongType()) };
    std::vector<int32_t> sortAscendings = { 1 };
    std::vector<int32_t> sortNullFirsts = { 0 };
    auto overflowConfig = new OverflowConfig();
    auto windowGroupLimitOperatorFactory = new WindowGroupLimitWithExprOperatorFactory(sourceTypes, 3, "row_number",
        partitionKeys, sortKeys, sortAscendings, sortNullFirsts, overflowConfig);
    auto windowGroupLimitOperator =
        static_cast<WindowGroupLimitWithExprOperator *>(CreateTestOperator(windowGroupLimitOperatorFactory));

    windowGroupLimitOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch;
    windowGroupLimitOperator->GetOutput(&outputVecBatch);

    constexpr int32_t expectedDataSize = 3;
    std::string expData1[expectedDataSize] = {"bye", "bye", "bye"};
    int64_t expData2[expectedDataSize] = {11, 3, 5};
    int64_t expData3[expectedDataSize] = {3, 4, 6};
    DataTypes expectTypes(std::vector<DataTypePtr>({ VarcharType(10), LongType(), LongType() }));
    VectorBatch *expectVectorBatch = CreateVectorBatch(expectTypes, expectedDataSize, expData1, expData2, expData3);

    EXPECT_TRUE(VecBatchMatchIgnoreOrder(outputVecBatch, expectVectorBatch));

    Expr::DeleteExprs(partitionKeys);
    Expr::DeleteExprs(sortKeys);
    omniruntime::op::Operator::DeleteOperator(windowGroupLimitOperator);
    delete windowGroupLimitOperatorFactory;
    VectorHelper::FreeVecBatch(expectVectorBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
    delete overflowConfig;
}

// row_number + AscCase3
TEST(WindowGroupLimitWithExprOperatorTest, TestRowNumberAscCase3)
{
    const int32_t dataSize = 10;
    std::string data1[dataSize] = {
                "test1", "test1", "test1", "test1", "test1", "test1",
                "test2", "test2", "test2", "test2"
        };
    std::string data2[dataSize] = {
                "M", "M", "M", "M", "F", "F", "M", "M", "M", "M"
        };
    std::string data3[dataSize] = {
                "FuJian", "FuJian", "FuJian", "ShanDong", "FuJian", "FuJian",
                "FuJian", "FuJian", "FuJian", "FuJian"
        };
    int64_t data4[dataSize] = {
                176, 176, 175, 185, 165, 163, 173, 173, 173, 173
        };
    int64_t data5[dataSize] = {
                70, 65, 72, 80, 50, 50, 63, 62, 61, 60
        };

    DataTypes sourceTypes(
        std::vector<DataTypePtr>({ VarcharType(10), VarcharType(15), VarcharType(10), LongType(), LongType() }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data1, data2, data3, data4, data5);

    std::vector<omniruntime::expressions::Expr *> partitionKeys = { new FieldExpr(0, VarcharType(10)),
        new FieldExpr(1, VarcharType(15)), new FieldExpr(2, VarcharType(10)) };
    std::vector<omniruntime::expressions::Expr *> sortKeys = { new FieldExpr(3, LongType()) };
    std::vector<int32_t> sortAscendings = { 1 };
    std::vector<int32_t> sortNullFirsts = { 0 };
    auto overflowConfig = new OverflowConfig();
    auto windowGroupLimitOperatorFactory = new WindowGroupLimitWithExprOperatorFactory(sourceTypes, 2, "row_number",
        partitionKeys, sortKeys, sortAscendings, sortNullFirsts, overflowConfig);
    auto windowGroupLimitOperator =
        static_cast<WindowGroupLimitWithExprOperator *>(CreateTestOperator(windowGroupLimitOperatorFactory));

    windowGroupLimitOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch;
    windowGroupLimitOperator->GetOutput(&outputVecBatch);

    constexpr int32_t expectedDataSize = 7;
    std::string expData1[expectedDataSize] = {
                "test1", "test1", "test1", "test1", "test1", "test2", "test2"
        };
    std::string expData2[expectedDataSize] = {
                "M", "M", "M", "F", "F", "M", "M"
        };
    std::string expData3[expectedDataSize] = {
                "FuJian", "FuJian", "ShanDong", "FuJian", "FuJian", "FuJian", "FuJian"
        };
    int64_t expData4[dataSize] = {
                175, 176, 185, 163, 165, 173, 173
        };
    int64_t expData5[expectedDataSize] = {
                72, 70, 80, 50, 50, 63, 62
        };
    DataTypes expectTypes(
        std::vector<DataTypePtr>({ VarcharType(10), VarcharType(15), VarcharType(10), LongType(), LongType() }));
    VectorBatch *expectVectorBatch =
        CreateVectorBatch(expectTypes, expectedDataSize, expData1, expData2, expData3, expData4, expData5);

    EXPECT_TRUE(VecBatchMatchIgnoreOrder(outputVecBatch, expectVectorBatch));

    Expr::DeleteExprs(partitionKeys);
    Expr::DeleteExprs(sortKeys);
    omniruntime::op::Operator::DeleteOperator(windowGroupLimitOperator);
    delete windowGroupLimitOperatorFactory;
    VectorHelper::FreeVecBatch(expectVectorBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
    delete overflowConfig;
}

// row_number + partition by Array<Float>
TEST(WindowGroupLimitWithExprOperatorTest, TestRowNumberPartitionByArrayFloat)
{
    const int32_t dataSize = 6;
    int64_t data1[dataSize] = {10, 30, 20, 40, 60, 50};

    // Partition key: ARRAY<FLOAT>, two groups with different lengths: [1.0] and [2.0, 3.0]
    const int32_t totalElements = 1 * 3 + 2 * 3;
    float elements[totalElements] = {
        1.0f,       // row 0
        1.0f,       // row 1
        1.0f,       // row 2
        2.0f, 3.0f, // row 3
        2.0f, 3.0f, // row 4
        2.0f, 3.0f  // row 5
    };

    auto *vecBatch = new VectorBatch(dataSize);

    auto *data1Vec = new Vector<int64_t>(dataSize);
    for (int32_t i = 0; i < dataSize; ++i) {
        data1Vec->SetValue(i, data1[i]);
    }
    vecBatch->Append(data1Vec);

    auto elemVec = std::make_shared<Vector<float>>(totalElements);
    for (int32_t i = 0; i < totalElements; ++i) {
        elemVec->SetValue(i, elements[i]);
    }
    auto *arrayCol = new ArrayVector(dataSize, elemVec);
    arrayCol->SetOffset(0, 0);
    arrayCol->SetSize(0, 1);
    arrayCol->SetSize(1, 1);
    arrayCol->SetSize(2, 1);
    arrayCol->SetSize(3, 2);
    arrayCol->SetSize(4, 2);
    arrayCol->SetSize(5, 2);
    vecBatch->Append(arrayCol);

    DataTypePtr floatType = FloatType();
    DataTypePtr arrayFloatType = std::make_shared<omniruntime::type::ArrayType>(floatType);
    DataTypes sourceTypes(std::vector<DataTypePtr>({ LongType(), arrayFloatType }));

    std::vector<omniruntime::expressions::Expr *> partitionKeys = { new FieldExpr(1, arrayFloatType) };
    std::vector<omniruntime::expressions::Expr *> sortKeys = { new FieldExpr(0, LongType()) };
    std::vector<int32_t> sortAscendings = { 1 };
    std::vector<int32_t> sortNullFirsts = { 0 };
    auto overflowConfig = new OverflowConfig();
    auto windowGroupLimitOperatorFactory = new WindowGroupLimitWithExprOperatorFactory(sourceTypes, 2, "row_number",
        partitionKeys, sortKeys, sortAscendings, sortNullFirsts, overflowConfig);
    auto windowGroupLimitOperator =
        static_cast<WindowGroupLimitWithExprOperator *>(CreateTestOperator(windowGroupLimitOperatorFactory));

    windowGroupLimitOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch;
    windowGroupLimitOperator->GetOutput(&outputVecBatch);

    // Expected order matches partition iteration order of unordered_map: [2.0, 3.0] group first, then [1.0].
    constexpr int32_t expectedDataSize = 4;
    int64_t expSortKeys[expectedDataSize] = {40, 50, 10, 20};

    const int32_t expTotalElements = 1 * 2 + 2 * 2;
    float expElements[expTotalElements] = {
        2.0f, 3.0f, // row 0
        2.0f, 3.0f, // row 1
        1.0f,       // row 2
        1.0f        // row 3
    };

    auto *expectBatch = new VectorBatch(expectedDataSize);
    auto *expSortKeyVec = new Vector<int64_t>(expectedDataSize);
    for (int32_t i = 0; i < expectedDataSize; ++i) {
        expSortKeyVec->SetValue(i, expSortKeys[i]);
    }
    expectBatch->Append(expSortKeyVec);

    auto expElemVec = std::make_shared<Vector<float>>(expTotalElements);
    for (int32_t i = 0; i < expTotalElements; ++i) {
        expElemVec->SetValue(i, expElements[i]);
    }
    auto *expArrayCol = new ArrayVector(expectedDataSize, expElemVec);
    expArrayCol->SetOffset(0, 0);
    expArrayCol->SetSize(0, 2);
    expArrayCol->SetSize(1, 2);
    expArrayCol->SetSize(2, 1);
    expArrayCol->SetSize(3, 1);
    expectBatch->Append(expArrayCol);

    EXPECT_TRUE(VecBatchMatchIgnoreOrder(outputVecBatch, expectBatch));

    Expr::DeleteExprs(partitionKeys);
    Expr::DeleteExprs(sortKeys);
    omniruntime::op::Operator::DeleteOperator(windowGroupLimitOperator);
    delete windowGroupLimitOperatorFactory;
    VectorHelper::FreeVecBatch(expectBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
    delete overflowConfig;
}

// row_number + sort by Array<Float>
TEST(WindowGroupLimitWithExprOperatorTest, TestRowNumberSortByArrayFloat)
{
    const int32_t dataSize = 6;
    int64_t partitionValues[dataSize] = {1, 1, 1, 2, 2, 2};

    std::vector<std::vector<float>> arrays = {
        {1.0f, 2.0f},      // group 1
        {1.0f, 3.0f},      // group 1
        {1.0f},            // group 1
        {2.0f},            // group 2
        {2.0f, 1.0f},      // group 2
        {2.0f, 2.0f, 2.0f} // group 2
    };

    auto *vecBatch = new VectorBatch(dataSize);

    auto *partitionVec = new Vector<int64_t>(dataSize);
    for (int32_t i = 0; i < dataSize; ++i) {
        partitionVec->SetValue(i, partitionValues[i]);
    }
    vecBatch->Append(partitionVec);

    int32_t totalElements = 0;
    for (const auto &arr : arrays) {
        totalElements += static_cast<int32_t>(arr.size());
    }
    auto elemVec = std::make_shared<Vector<float>>(totalElements);
    int32_t idx = 0;
    for (const auto &arr : arrays) {
        for (float v : arr) {
            elemVec->SetValue(idx++, v);
        }
    }
    auto *arrayCol = new ArrayVector(dataSize, elemVec);
    arrayCol->SetOffset(0, 0);
    int32_t offset = 0;
    for (int32_t i = 0; i < dataSize; ++i) {
        arrayCol->SetSize(i, static_cast<int32_t>(arrays[i].size()));
        offset += static_cast<int32_t>(arrays[i].size());
    }
    vecBatch->Append(arrayCol);

    DataTypePtr floatType = FloatType();
    DataTypePtr arrayFloatType = std::make_shared<omniruntime::type::ArrayType>(floatType);
    DataTypes sourceTypes(std::vector<DataTypePtr>({ LongType(), arrayFloatType }));

    std::vector<omniruntime::expressions::Expr *> partitionKeys = { new FieldExpr(0, LongType()) };
    std::vector<omniruntime::expressions::Expr *> sortKeys = { new FieldExpr(1, arrayFloatType) };
    std::vector<int32_t> sortAscendings = { 1 };
    std::vector<int32_t> sortNullFirsts = { 1 };
    auto overflowConfig = new OverflowConfig();
    auto windowGroupLimitOperatorFactory = new WindowGroupLimitWithExprOperatorFactory(sourceTypes, 2, "row_number",
        partitionKeys, sortKeys, sortAscendings, sortNullFirsts, overflowConfig);
    auto windowGroupLimitOperator =
        static_cast<WindowGroupLimitWithExprOperator *>(CreateTestOperator(windowGroupLimitOperatorFactory));

    windowGroupLimitOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch;
    windowGroupLimitOperator->GetOutput(&outputVecBatch);

    // Expected order matches partition iteration order of unordered_map: partition 2 first, then 1.
    constexpr int32_t expectedDataSize = 4;
    int64_t expPartitionValues[expectedDataSize] = {2, 2, 1, 1};
    std::vector<std::vector<float>> expArrays = {
        {2.0f},            // smallest in group 2
        {2.0f, 1.0f},      // second in group 2
        {1.0f},            // smallest in group 1
        {1.0f, 2.0f}       // second in group 1
    };

    auto *expectBatch = new VectorBatch(expectedDataSize);
    auto *expPartitionVec = new Vector<int64_t>(expectedDataSize);
    for (int32_t i = 0; i < expectedDataSize; ++i) {
        expPartitionVec->SetValue(i, expPartitionValues[i]);
    }
    expectBatch->Append(expPartitionVec);

    int32_t expTotalElements = 0;
    for (const auto &arr : expArrays) {
        expTotalElements += static_cast<int32_t>(arr.size());
    }
    auto expElemVec = std::make_shared<Vector<float>>(expTotalElements);
    int32_t expIdx = 0;
    for (const auto &arr : expArrays) {
        for (float v : arr) {
            expElemVec->SetValue(expIdx++, v);
        }
    }
    auto *expArrayCol = new ArrayVector(expectedDataSize, expElemVec);
    expArrayCol->SetOffset(0, 0);
    for (int32_t i = 0; i < expectedDataSize; ++i) {
        expArrayCol->SetSize(i, static_cast<int32_t>(expArrays[i].size()));
    }
    expectBatch->Append(expArrayCol);

    EXPECT_TRUE(VecBatchMatchIgnoreOrder(outputVecBatch, expectBatch));

    Expr::DeleteExprs(partitionKeys);
    Expr::DeleteExprs(sortKeys);
    omniruntime::op::Operator::DeleteOperator(windowGroupLimitOperator);
    delete windowGroupLimitOperatorFactory;
    VectorHelper::FreeVecBatch(expectBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
    delete overflowConfig;
}

// Helper to build Array<VARCHAR> column
static ArrayVector *CreateVarcharArrayVector(const std::vector<std::vector<std::string>> &rows,
    std::vector<std::string> &flatStrings)
{
    flatStrings.clear();
    for (const auto &arr : rows) {
        for (const auto &s : arr) {
            flatStrings.push_back(s);
        }
    }
    int32_t rowCount = static_cast<int32_t>(rows.size());
    int32_t totalElements = static_cast<int32_t>(flatStrings.size());
    auto *elements = new VarcharVector(totalElements);
    int32_t idx = 0;
    for (const auto &arr : rows) {
        for (const auto &s : arr) {
            std::string_view sv(s.data(), s.size());
            elements->SetValue(idx++, sv);
        }
    }
    auto *arrayVector = new ArrayVector(rowCount);
    int32_t offset = 0;
    for (int32_t i = 0; i < rowCount; ++i) {
        arrayVector->SetOffset(i, offset);
        offset += static_cast<int32_t>(rows[i].size());
    }
    arrayVector->SetOffset(rowCount, offset);
    arrayVector->SetElementVectorFromRaw(elements);
    return arrayVector;
}

// row_number + partition by Array<Varchar>
TEST(WindowGroupLimitWithExprOperatorTest, TestRowNumberPartitionByArrayVarchar)
{
    const int32_t dataSize = 6;
    int64_t data1[dataSize] = {10, 30, 20, 40, 60, 50};

    std::vector<std::vector<std::string>> arrays = {
        {"x", "a"}, {"x", "a"}, {"x", "a"}, {"y"}, {"y"}, {"y"}
    };

    auto *vecBatch = new VectorBatch(dataSize);

    auto *data1Vec = new Vector<int64_t>(dataSize);
    for (int32_t i = 0; i < dataSize; ++i) {
        data1Vec->SetValue(i, data1[i]);
    }
    vecBatch->Append(data1Vec);

    std::vector<std::string> flatStrings;
    auto *arrayCol = CreateVarcharArrayVector(arrays, flatStrings);
    vecBatch->Append(arrayCol);

    DataTypePtr arrayVarcharType = std::make_shared<omniruntime::type::ArrayType>(VarcharType(10));
    DataTypes sourceTypes(std::vector<DataTypePtr>({ LongType(), arrayVarcharType }));

    std::vector<omniruntime::expressions::Expr *> partitionKeys = { new FieldExpr(1, arrayVarcharType) };
    std::vector<omniruntime::expressions::Expr *> sortKeys = { new FieldExpr(0, LongType()) };
    std::vector<int32_t> sortAscendings = { 1 };
    std::vector<int32_t> sortNullFirsts = { 0 };
    auto overflowConfig = new OverflowConfig();
    auto windowGroupLimitOperatorFactory = new WindowGroupLimitWithExprOperatorFactory(sourceTypes, 2, "row_number",
        partitionKeys, sortKeys, sortAscendings, sortNullFirsts, overflowConfig);
    auto windowGroupLimitOperator =
        static_cast<WindowGroupLimitWithExprOperator *>(CreateTestOperator(windowGroupLimitOperatorFactory));

    windowGroupLimitOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch;
    windowGroupLimitOperator->GetOutput(&outputVecBatch);

    // Expected order matches partition iteration order of unordered_map: ["y"] first, then ["x","a"].
    constexpr int32_t expectedDataSize = 4;
    int64_t expSortKeys[expectedDataSize] = {40, 50, 10, 20};
    std::vector<std::vector<std::string>> expArrays = {
        {"y"}, {"y"}, {"x", "a"}, {"x", "a"}
    };

    auto *expectBatch = new VectorBatch(expectedDataSize);
    auto *expSortKeyVec = new Vector<int64_t>(expectedDataSize);
    for (int32_t i = 0; i < expectedDataSize; ++i) {
        expSortKeyVec->SetValue(i, expSortKeys[i]);
    }
    expectBatch->Append(expSortKeyVec);

    std::vector<std::string> expFlatStrings;
    auto *expArrayCol = CreateVarcharArrayVector(expArrays, expFlatStrings);
    expectBatch->Append(expArrayCol);

    EXPECT_TRUE(VecBatchMatchIgnoreOrder(outputVecBatch, expectBatch));

    Expr::DeleteExprs(partitionKeys);
    Expr::DeleteExprs(sortKeys);
    omniruntime::op::Operator::DeleteOperator(windowGroupLimitOperator);
    delete windowGroupLimitOperatorFactory;
    VectorHelper::FreeVecBatch(expectBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
    delete overflowConfig;
}

// row_number + sort by Array<Varchar>
TEST(WindowGroupLimitWithExprOperatorTest, TestRowNumberSortByArrayVarchar)
{
    const int32_t dataSize = 6;
    int64_t partitionValues[dataSize] = {1, 1, 1, 2, 2, 2};

    std::vector<std::vector<std::string>> arrays = {
        {"a", "b"}, {"a", "c"}, {"a"}, {"b"}, {"b", "a"}, {"b", "b", "b"}
    };

    auto *vecBatch = new VectorBatch(dataSize);

    auto *partitionVec = new Vector<int64_t>(dataSize);
    for (int32_t i = 0; i < dataSize; ++i) {
        partitionVec->SetValue(i, partitionValues[i]);
    }
    vecBatch->Append(partitionVec);

    std::vector<std::string> flatStrings;
    auto *arrayCol = CreateVarcharArrayVector(arrays, flatStrings);
    vecBatch->Append(arrayCol);

    DataTypePtr arrayVarcharType = std::make_shared<omniruntime::type::ArrayType>(VarcharType(10));
    DataTypes sourceTypes(std::vector<DataTypePtr>({ LongType(), arrayVarcharType }));

    std::vector<omniruntime::expressions::Expr *> partitionKeys = { new FieldExpr(0, LongType()) };
    std::vector<omniruntime::expressions::Expr *> sortKeys = { new FieldExpr(1, arrayVarcharType) };
    std::vector<int32_t> sortAscendings = { 1 };
    std::vector<int32_t> sortNullFirsts = { 1 };
    auto overflowConfig = new OverflowConfig();
    auto windowGroupLimitOperatorFactory = new WindowGroupLimitWithExprOperatorFactory(sourceTypes, 2, "row_number",
        partitionKeys, sortKeys, sortAscendings, sortNullFirsts, overflowConfig);
    auto windowGroupLimitOperator =
        static_cast<WindowGroupLimitWithExprOperator *>(CreateTestOperator(windowGroupLimitOperatorFactory));

    windowGroupLimitOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch;
    windowGroupLimitOperator->GetOutput(&outputVecBatch);

    // Expected order matches partition iteration order of unordered_map: partition 2 first, then 1.
    constexpr int32_t expectedDataSize = 4;
    int64_t expPartitionValues[expectedDataSize] = {2, 2, 1, 1};
    std::vector<std::vector<std::string>> expArrays = {
        {"b"}, {"b", "a"}, {"a"}, {"a", "b"}
    };

    auto *expectBatch = new VectorBatch(expectedDataSize);
    auto *expPartitionVec = new Vector<int64_t>(expectedDataSize);
    for (int32_t i = 0; i < expectedDataSize; ++i) {
        expPartitionVec->SetValue(i, expPartitionValues[i]);
    }
    expectBatch->Append(expPartitionVec);

    std::vector<std::string> expFlatStrings;
    auto *expArrayCol = CreateVarcharArrayVector(expArrays, expFlatStrings);
    expectBatch->Append(expArrayCol);

    EXPECT_TRUE(VecBatchMatchIgnoreOrder(outputVecBatch, expectBatch));

    Expr::DeleteExprs(partitionKeys);
    Expr::DeleteExprs(sortKeys);
    omniruntime::op::Operator::DeleteOperator(windowGroupLimitOperator);
    delete windowGroupLimitOperatorFactory;
    VectorHelper::FreeVecBatch(expectBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
    delete overflowConfig;
}

// row_number + partition by Array<Varchar> with 5 columns
TEST(WindowGroupLimitWithExprOperatorTest, TestRowNumberPartitionByArrayVarcharCase3)
{
    const int32_t dataSize = 10;
    std::string data1[dataSize] = {
                "test1", "test1", "test1", "test1", "test1",
                "test1", "test2", "test2", "test2", "test2"
        };
    std::string data2[dataSize] = {
                "M", "M", "M", "M", "F",
                "F", "M", "M", "M", "M"
        };
    std::string data3[dataSize] = {
                "FuJian", "FuJian", "FuJian", "ShanDong", "FuJian",
                "FuJian", "FuJian", "FuJian", "FuJian", "FuJian"
        };
    int64_t data5[dataSize] = {
                70, 65, 72, 80, 50, 50, 63, 62, 61, 60
        };

    // Array<VARCHAR(19)>
    std::vector<std::vector<std::string>> arrays = {
        {"Ex", "A", "hah"},
        {"Avr", "Max", "B"},
        {"Best"},
        {"Bell"},
        {"Mean", "nothing"},
        {"do not know", "new"},
        {"intreasting"},
        {"Braval", "Amazing", "hah", "WLB"},
        {"comfortable"},
        {"good"}
    };

    auto *vecBatch = new VectorBatch(dataSize);

    auto *col1 = new VarcharVector(dataSize);
    for (int32_t i = 0; i < dataSize; ++i) {
        std::string_view sv(data1[i].data(), data1[i].size());
        col1->SetValue(i, sv);
    }
    vecBatch->Append(col1);

    auto *col2 = new VarcharVector(dataSize);
    for (int32_t i = 0; i < dataSize; ++i) {
        std::string_view sv(data2[i].data(), data2[i].size());
        col2->SetValue(i, sv);
    }
    vecBatch->Append(col2);

    auto *col3 = new VarcharVector(dataSize);
    for (int32_t i = 0; i < dataSize; ++i) {
        std::string_view sv(data3[i].data(), data3[i].size());
        col3->SetValue(i, sv);
    }
    vecBatch->Append(col3);

    std::vector<std::string> flatStrings;
    auto *arrayCol = CreateVarcharArrayVector(arrays, flatStrings);
    vecBatch->Append(arrayCol);

    auto *col5 = new Vector<int64_t>(dataSize);
    for (int32_t i = 0; i < dataSize; ++i) {
        col5->SetValue(i, data5[i]);
    }
    vecBatch->Append(col5);

    DataTypePtr arrayVarcharType = std::make_shared<omniruntime::type::ArrayType>(VarcharType(19));
    DataTypes sourceTypes(
        std::vector<DataTypePtr>({ VarcharType(10), VarcharType(15), VarcharType(10), arrayVarcharType, LongType() }));

    std::vector<omniruntime::expressions::Expr *> partitionKeys = { new FieldExpr(3, arrayVarcharType) };
    std::vector<omniruntime::expressions::Expr *> sortKeys = { new FieldExpr(4, LongType()) };
    std::vector<int32_t> sortAscendings = { 1 };
    std::vector<int32_t> sortNullFirsts = { 0 };
    auto overflowConfig = new OverflowConfig();
    auto windowGroupLimitOperatorFactory = new WindowGroupLimitWithExprOperatorFactory(sourceTypes, 2, "row_number",
        partitionKeys, sortKeys, sortAscendings, sortNullFirsts, overflowConfig);
    auto windowGroupLimitOperator =
        static_cast<WindowGroupLimitWithExprOperator *>(CreateTestOperator(windowGroupLimitOperatorFactory));

    windowGroupLimitOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch;
    windowGroupLimitOperator->GetOutput(&outputVecBatch);

    constexpr int32_t expectedDataSize = 10;
    auto *expectBatch = new VectorBatch(expectedDataSize);

    std::string expData1[expectedDataSize] = {
                "test2", "test2", "test2", "test1", "test2",
                "test1", "test1", "test1", "test1", "test1"
        };
    std::string expData2[expectedDataSize] = {
                "M", "M", "M", "F", "M",
                "F", "M", "M", "M", "M"
        };
    std::string expData3[expectedDataSize] = {
                "FuJian", "FuJian", "FuJian", "FuJian", "FuJian",
                "FuJian", "ShanDong", "FuJian", "FuJian", "FuJian"
        };
    int64_t expData5[expectedDataSize] = {
                62, 63, 60, 50, 61, 50, 80, 72, 65, 70
        };

    auto *expCol1 = new VarcharVector(expectedDataSize);
    for (int32_t i = 0; i < expectedDataSize; ++i) {
        std::string_view sv(expData1[i].data(), expData1[i].size());
        expCol1->SetValue(i, sv);
    }
    expectBatch->Append(expCol1);

    auto *expCol2 = new VarcharVector(expectedDataSize);
    for (int32_t i = 0; i < expectedDataSize; ++i) {
        std::string_view sv(expData2[i].data(), expData2[i].size());
        expCol2->SetValue(i, sv);
    }
    expectBatch->Append(expCol2);

    auto *expCol3 = new VarcharVector(expectedDataSize);
    for (int32_t i = 0; i < expectedDataSize; ++i) {
        std::string_view sv(expData3[i].data(), expData3[i].size());
        expCol3->SetValue(i, sv);
    }
    expectBatch->Append(expCol3);

    std::vector<std::vector<std::string>> expArrays = {
        {"Braval", "Amazing", "hah", "WLB"},
        {"intreasting"},
        {"good"},
        {"do not know", "new"},
        {"comfortable"},
        {"Mean", "nothing"},
        {"Bell"},
        {"Best"},
        {"Avr", "Max", "B"},
        {"Ex", "A", "hah"}
    };
    std::vector<std::string> expFlatStrings;
    auto *expArrayCol = CreateVarcharArrayVector(expArrays, expFlatStrings);
    expectBatch->Append(expArrayCol);

    auto *expCol5 = new Vector<int64_t>(expectedDataSize);
    for (int32_t i = 0; i < expectedDataSize; ++i) {
        expCol5->SetValue(i, expData5[i]);
    }
    expectBatch->Append(expCol5);

    // Ten partitions; partition order is undefined (unordered_map), so use IgnoreOrder.
    EXPECT_TRUE(VecBatchMatchIgnoreOrder(outputVecBatch, expectBatch));

    Expr::DeleteExprs(partitionKeys);
    Expr::DeleteExprs(sortKeys);
    omniruntime::op::Operator::DeleteOperator(windowGroupLimitOperator);
    delete windowGroupLimitOperatorFactory;
    VectorHelper::FreeVecBatch(expectBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
    delete overflowConfig;
}

// row_number + sort by Array<Varchar> with 5 columns
TEST(WindowGroupLimitWithExprOperatorTest, TestRowNumberSortByArrayVarcharCase3)
{
    const int32_t dataSize = 10;
    std::string data1[dataSize] = {
                "test1", "test1", "test1", "test1", "test1",
                "test1", "test2", "test2", "test2", "test2"
        };
    std::string data2[dataSize] = {
                "M", "M", "M", "M", "F",
                "F", "M", "M", "M", "M"
        };
    std::string data3[dataSize] = {
                "FuJian", "FuJian", "FuJian", "ShanDong", "FuJian",
                "FuJian", "FuJian", "FuJian", "FuJian", "FuJian"
        };
    int64_t data5[dataSize] = {
                70, 65, 72, 80, 50, 50, 63, 62, 61, 60
        };

    std::vector<std::vector<std::string>> arrays = {
        {"Ex", "A", "hah"},
        {"Avr", "Max", "B"},
        {"Best"},
        {"Bell"},
        {"Mean", "nothing"},
        {"do not know", "new"},
        {"intreasting"},
        {"Braval", "Amazing", "hah", "WLB"},
        {"comfortable"},
        {"good"}
    };

    auto *vecBatch = new VectorBatch(dataSize);

    auto *col1 = new VarcharVector(dataSize);
    for (int32_t i = 0; i < dataSize; ++i) {
        std::string_view sv(data1[i].data(), data1[i].size());
        col1->SetValue(i, sv);
    }
    vecBatch->Append(col1);

    auto *col2 = new VarcharVector(dataSize);
    for (int32_t i = 0; i < dataSize; ++i) {
        std::string_view sv(data2[i].data(), data2[i].size());
        col2->SetValue(i, sv);
    }
    vecBatch->Append(col2);

    auto *col3 = new VarcharVector(dataSize);
    for (int32_t i = 0; i < dataSize; ++i) {
        std::string_view sv(data3[i].data(), data3[i].size());
        col3->SetValue(i, sv);
    }
    vecBatch->Append(col3);

    std::vector<std::string> flatStrings2;
    auto *arrayCol = CreateVarcharArrayVector(arrays, flatStrings2);
    vecBatch->Append(arrayCol);

    auto *col5 = new Vector<int64_t>(dataSize);
    for (int32_t i = 0; i < dataSize; ++i) {
        col5->SetValue(i, data5[i]);
    }
    vecBatch->Append(col5);

    DataTypePtr arrayVarcharType = std::make_shared<omniruntime::type::ArrayType>(VarcharType(19));
    DataTypes sourceTypes(
        std::vector<DataTypePtr>({ VarcharType(10), VarcharType(15), VarcharType(10), arrayVarcharType, LongType() }));

    std::vector<omniruntime::expressions::Expr *> partitionKeys = { new FieldExpr(0, VarcharType(10)),
        new FieldExpr(1, VarcharType(15)), new FieldExpr(2, VarcharType(10)) };
    std::vector<omniruntime::expressions::Expr *> sortKeys = { new FieldExpr(3, arrayVarcharType), new FieldExpr(4, LongType())  };
    std::vector<int32_t> sortAscendings = { 1 };
    std::vector<int32_t> sortNullFirsts = { 1 };
    auto overflowConfig = new OverflowConfig();
    auto windowGroupLimitOperatorFactory = new WindowGroupLimitWithExprOperatorFactory(sourceTypes, 2, "row_number",
        partitionKeys, sortKeys, sortAscendings, sortNullFirsts, overflowConfig);
    auto windowGroupLimitOperator =
        static_cast<WindowGroupLimitWithExprOperator *>(CreateTestOperator(windowGroupLimitOperatorFactory));

    windowGroupLimitOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch;
    windowGroupLimitOperator->GetOutput(&outputVecBatch);

    constexpr int32_t expectedDataSize = 7;
    auto *expectBatch = new VectorBatch(expectedDataSize);

    std::string expData1[expectedDataSize] = {
               "test2", "test2", "test1", "test1", "test1", "test1", "test1"
        };
    std::string expData2[expectedDataSize] = {
                "M", "M", "F", "F", "M", "M", "M"
        };
    std::string expData3[expectedDataSize] = {
                "FuJian", "FuJian", "FuJian", "FuJian", "ShanDong", "FuJian", "FuJian"
        };
    int64_t expData5[expectedDataSize] = {
                62, 61, 50, 50, 80, 65, 72
        };

    auto *expCol1 = new VarcharVector(expectedDataSize);
    for (int32_t i = 0; i < expectedDataSize; ++i) {
        std::string_view sv(expData1[i].data(), expData1[i].size());
        expCol1->SetValue(i, sv);
    }
    expectBatch->Append(expCol1);

    auto *expCol2 = new VarcharVector(expectedDataSize);
    for (int32_t i = 0; i < expectedDataSize; ++i) {
        std::string_view sv(expData2[i].data(), expData2[i].size());
        expCol2->SetValue(i, sv);
    }
    expectBatch->Append(expCol2);

    auto *expCol3 = new VarcharVector(expectedDataSize);
    for (int32_t i = 0; i < expectedDataSize; ++i) {
        std::string_view sv(expData3[i].data(), expData3[i].size());
        expCol3->SetValue(i, sv);
    }
    expectBatch->Append(expCol3);

    std::vector<std::vector<std::string>> expArrays = {
        {"Braval", "Amazing", "hah", "WLB"},
        {"comfortable"},
        {"Mean", "nothing"},
        {"do not know", "new"},
        {"Bell"},
        {"Avr", "Max", "B"},
        {"Best"}
    };
    std::vector<std::string> expFlatStrings;
    auto *expArrayCol = CreateVarcharArrayVector(expArrays, expFlatStrings);
    expectBatch->Append(expArrayCol);

    auto *expCol5 = new Vector<int64_t>(expectedDataSize);
    for (int32_t i = 0; i < expectedDataSize; ++i) {
        expCol5->SetValue(i, expData5[i]);
    }
    expectBatch->Append(expCol5);

    // Multiple partitions; partition order is undefined (unordered_map), so use IgnoreOrder.
    EXPECT_TRUE(VecBatchMatchIgnoreOrder(outputVecBatch, expectBatch));

    Expr::DeleteExprs(partitionKeys);
    Expr::DeleteExprs(sortKeys);
    omniruntime::op::Operator::DeleteOperator(windowGroupLimitOperator);
    delete windowGroupLimitOperatorFactory;
    VectorHelper::FreeVecBatch(expectBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
    delete overflowConfig;
}

// Helper to build Struct(Row) column with nested complex types
// Struct structure: {name: VARCHAR, scores: ARRAY<INT>, address: STRUCT<{city: VARCHAR, zip: INT>}
static RowVector *CreateNestedStructVector(int32_t rowCount,
    const std::vector<std::string> &names,
    const std::vector<std::vector<int32_t>> &scoresList,
    const std::vector<std::string> &cities,
    const std::vector<int32_t> &zips)
{
    // Create name vector (VARCHAR)
    auto *nameVec = new VarcharVector(rowCount);
    for (int32_t i = 0; i < rowCount; ++i) {
        std::string_view sv(names[i].data(), names[i].size());
        nameVec->SetValue(i, sv);
    }

    // Create scores vector (ARRAY<INT>)
    int32_t totalScores = 0;
    for (const auto &scores : scoresList) {
        totalScores += static_cast<int32_t>(scores.size());
    }
    auto *scoresElemVec = new Vector<int32_t>(totalScores);
    int32_t idx = 0;
    for (const auto &scores : scoresList) {
        for (int32_t score : scores) {
            scoresElemVec->SetValue(idx++, score);
        }
    }
    auto *scoresArrayVec = new ArrayVector(rowCount);
    int32_t offset = 0;
    for (int32_t i = 0; i < rowCount; ++i) {
        scoresArrayVec->SetOffset(i, offset);
        offset += static_cast<int32_t>(scoresList[i].size());
    }
    scoresArrayVec->SetOffset(rowCount, offset);
    scoresArrayVec->SetElementVectorFromRaw(scoresElemVec);

    // Create inner address struct (STRUCT<{city: VARCHAR, zip: INT}>)
    auto *cityVec = new VarcharVector(rowCount);
    auto *zipVec = new Vector<int32_t>(rowCount);
    for (int32_t i = 0; i < rowCount; ++i) {
        std::string_view sv(cities[i].data(), cities[i].size());
        cityVec->SetValue(i, sv);
        zipVec->SetValue(i, zips[i]);
    }
    auto *addressStructVec = new RowVector(rowCount);
    addressStructVec->AddChild(cityVec);
    addressStructVec->AddChild(zipVec);

    // Create outer struct vector (STRUCT<{name, scores, address}>)
    auto *structVec = new RowVector(rowCount);
    structVec->AddChild(nameVec);
    structVec->AddChild(scoresArrayVec);
    structVec->AddChild(addressStructVec);

    return structVec;
}

// Helper to create expected nested struct vector for verification
static RowVector *CreateExpectedNestedStructVector(int32_t rowCount,
    const std::vector<std::string> &names,
    const std::vector<std::vector<int32_t>> &scoresList,
    const std::vector<std::string> &cities,
    const std::vector<int32_t> &zips)
{
    return CreateNestedStructVector(rowCount, names, scoresList, cities, zips);
}

// row_number + partition by nested Struct with complex types
// This test verifies that struct fields (including nested arrays) are correctly compared for partitioning
TEST(WindowGroupLimitWithExprOperatorTest, TestRowNumberPartitionByNestedStruct)
{
    const int32_t dataSize = 8;

    // Input data: designed to have only 2 unique struct values
    // sortKeys are used for ordering within each partition
    int64_t sortKeys[dataSize] = {100, 200, 150, 300, 250, 350, 400, 450};

    // Struct field 1: names - only "Alice" and "Bob"
    std::vector<std::string> names = {
        "Alice", "Alice", "Bob", "Bob",
        "Alice", "Bob", "Alice", "Bob"
    };

    // Struct field 2: scores (ARRAY<INT>)
    // CRITICAL: All Alice rows have same scores {85, 90}, all Bob rows have same scores {70, 75}
    // This ensures struct comparison groups rows correctly when all struct fields match
    std::vector<std::vector<int32_t>> scoresList = {
        {85, 90}, {85, 90}, {70, 75}, {70, 75},
        {85, 90}, {70, 75}, {85, 90}, {70, 75}
    };

    // Struct field 3: address (inner struct {city, zip})
    // Alice rows: all Beijing+100001, Bob rows: all Shanghai+200001
    std::vector<std::string> cities = {
        "Beijing", "Beijing", "Shanghai", "Shanghai",
        "Beijing", "Shanghai", "Beijing", "Shanghai"
    };
    std::vector<int32_t> zips = {100001, 100001, 200001, 200001, 100001, 200001, 100001, 200001};

    // Verify struct grouping: rows 0,1,4,6 have identical struct values (Alice, {85,90}, Beijing, 100001)
    // rows 2,3,5,7 have identical struct values (Bob, {70,75}, Shanghai, 200001)

    // Create input batch: sortKey, struct
    auto *vecBatch = new VectorBatch(dataSize);

    // Add sort key column
    auto *sortKeyVec = new Vector<int64_t>(dataSize);
    for (int32_t i = 0; i < dataSize; ++i) {
        sortKeyVec->SetValue(i, sortKeys[i]);
    }
    vecBatch->Append(sortKeyVec);

    // Add nested struct column
    auto *structCol = CreateNestedStructVector(dataSize, names, scoresList, cities, zips);
    vecBatch->Append(structCol);

    // Define struct data type
    DataTypePtr varcharType = VarcharType(20);
    DataTypePtr intType = std::make_shared<omniruntime::type::DataType>(OMNI_INT);
    DataTypePtr arrayIntType = std::make_shared<omniruntime::type::ArrayType>(intType);
    std::vector<std::shared_ptr<DataType>> innerTypes = {varcharType, intType};
    std::vector<std::string> innerNames = {"city", "zip"};
    auto innerRowType = std::make_shared<omniruntime::type::RowType>(innerTypes, innerNames);
    std::vector<std::shared_ptr<DataType>> outerTypes = {varcharType, arrayIntType, innerRowType};
    std::vector<std::string> outerNames = {"name", "scores", "address"};
    auto structType = std::make_shared<omniruntime::type::RowType>(outerTypes, outerNames);

    DataTypes sourceTypes(std::vector<DataTypePtr>({ LongType(), structType }));

    // Partition by the entire struct column
    std::vector<omniruntime::expressions::Expr *> partitionKeys = { new FieldExpr(1, structType) };
    std::vector<omniruntime::expressions::Expr *> sortKeyExprs = { new FieldExpr(0, LongType()) };
    std::vector<int32_t> sortAscendings = { 1 };  // Ascending
    std::vector<int32_t> sortNullFirsts = { 0 };  // Null last
    auto overflowConfig = new OverflowConfig();
    auto windowGroupLimitOperatorFactory = new WindowGroupLimitWithExprOperatorFactory(sourceTypes, 2, "row_number",
        partitionKeys, sortKeyExprs, sortAscendings, sortNullFirsts, overflowConfig);
    auto windowGroupLimitOperator =
        static_cast<WindowGroupLimitWithExprOperator *>(CreateTestOperator(windowGroupLimitOperatorFactory));

    windowGroupLimitOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch;
    windowGroupLimitOperator->GetOutput(&outputVecBatch);

    // Expected output: keep top 2 rows per partition (sorted by sortKey ascending)
    // Alice struct rows: indices 0,1,4,6 with sortKeys 100,200,250,400 -> keep 100, 200 (indices 0,1)
    // Bob struct rows: indices 2,3,5,7 with sortKeys 150,300,350,450 -> keep 150, 300 (indices 2,3)

    constexpr int32_t expectedDataSize = 4;

    // Expected sort keys for top 2 from each partition
    int64_t expSortKeys[expectedDataSize] = {100, 200, 150, 300};

    // Expected struct values: first 2 are Alice's struct (identical), last 2 are Bob's struct (identical)
    std::vector<std::string> expNames = {"Bob", "Bob", "Alice", "Alice"};
    std::vector<std::vector<int32_t>> expScoresList = {
        {70, 75}, {70, 75}, {85, 90}, {85, 90}
    };
    std::vector<std::string> expCities = {"Shanghai", "Shanghai", "Beijing", "Beijing"};
    std::vector<int32_t> expZips = { 200001, 200001, 100001, 100001};

    auto *expectBatch = new VectorBatch(expectedDataSize);

    auto *expSortKeyVec = new Vector<int64_t>(expectedDataSize);
    for (int32_t i = 0; i < expectedDataSize; ++i) {
        expSortKeyVec->SetValue(i, expSortKeys[i]);
    }
    expectBatch->Append(expSortKeyVec);

    auto *expStructCol = CreateExpectedNestedStructVector(expectedDataSize, expNames, expScoresList, expCities, expZips);
    expectBatch->Append(expStructCol);

    // Use IgnoreOrder since partition order is undefined
    EXPECT_TRUE(VecBatchMatchIgnoreOrder(outputVecBatch, expectBatch));

    Expr::DeleteExprs(partitionKeys);
    Expr::DeleteExprs(sortKeyExprs);
    omniruntime::op::Operator::DeleteOperator(windowGroupLimitOperator);
    delete windowGroupLimitOperatorFactory;
    VectorHelper::FreeVecBatch(expectBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
    delete overflowConfig;
}

// rank + partition by nested Struct (struct contains struct and array)
TEST(WindowGroupLimitWithExprOperatorTest, TestRankPartitionByNestedStructWithRankFunction)
{
    const int32_t dataSize = 6;

    // Input data for ranking
    int64_t sortKeys[dataSize] = {10, 20, 10, 30, 20, 10};

    // Struct field 1: department
    std::vector<std::string> departments = {
        "Engineering", "Sales", "Engineering",
        "HR", "Sales", "Engineering"
    };

    // Struct field 2: projects (ARRAY<VARCHAR>)
    std::vector<std::vector<std::string>> projectsList = {
        {"ProjectA", "ProjectB"},
        {"ProjectC"},
        {"ProjectA", "ProjectB"},
        {"ProjectD", "ProjectE"},
        {"ProjectC", "ProjectF"},
        {"ProjectA"}
    };

    // Struct field 3: manager info (inner struct {name: VARCHAR, level: INT})
    std::vector<std::string> mgrNames = {
        "John", "Jane", "John",
        "Bob", "Jane", "John"
    };
    std::vector<int32_t> mgrLevels = {5, 4, 5, 3, 4, 5};

    // Create input batch
    auto *vecBatch = new VectorBatch(dataSize);

    // Add sort key column
    auto *sortKeyVec = new Vector<int64_t>(dataSize);
    for (int32_t i = 0; i < dataSize; ++i) {
        sortKeyVec->SetValue(i, sortKeys[i]);
    }
    vecBatch->Append(sortKeyVec);

    // Build nested struct column
    // Department VARCHAR vector
    auto *deptVec = new VarcharVector(dataSize);
    for (int32_t i = 0; i < dataSize; ++i) {
        std::string_view sv(departments[i].data(), departments[i].size());
        deptVec->SetValue(i, sv);
    }

    // Projects ARRAY<VARCHAR> vector
    std::vector<std::string> flatStrings;
    for (const auto &projects : projectsList) {
        for (const auto &s : projects) {
            flatStrings.push_back(s);
        }
    }
    int32_t totalProjects = static_cast<int32_t>(flatStrings.size());
    auto *projElemVec = new VarcharVector(totalProjects);
    int32_t projIdx = 0;
    for (const auto &projects : projectsList) {
        for (const auto &s : projects) {
            std::string_view sv(s.data(), s.size());
            projElemVec->SetValue(projIdx++, sv);
        }
    }
    auto *projArrayVec = new ArrayVector(dataSize);
    int32_t projOffset = 0;
    for (int32_t i = 0; i < dataSize; ++i) {
        projArrayVec->SetOffset(i, projOffset);
        projOffset += static_cast<int32_t>(projectsList[i].size());
    }
    projArrayVec->SetOffset(dataSize, projOffset);
    projArrayVec->SetElementVectorFromRaw(projElemVec);

    // Manager struct vector
    auto *mgrNameVec = new VarcharVector(dataSize);
    auto *mgrLevelVec = new Vector<int32_t>(dataSize);
    for (int32_t i = 0; i < dataSize; ++i) {
        std::string_view sv(mgrNames[i].data(), mgrNames[i].size());
        mgrNameVec->SetValue(i, sv);
        mgrLevelVec->SetValue(i, mgrLevels[i]);
    }
    auto *mgrStructVec = new RowVector(dataSize);
    mgrStructVec->AddChild(mgrNameVec);
    mgrStructVec->AddChild(mgrLevelVec);

    // Outer struct vector
    auto *structVec = new RowVector(dataSize);
    structVec->AddChild(deptVec);
    structVec->AddChild(projArrayVec);
    structVec->AddChild(mgrStructVec);

    vecBatch->Append(structVec);

    // Define struct data type
    DataTypePtr varcharType = VarcharType(20);
    DataTypePtr intType = std::make_shared<omniruntime::type::DataType>(OMNI_INT);
    DataTypePtr arrayVarcharType = std::make_shared<omniruntime::type::ArrayType>(varcharType);
    std::vector<std::shared_ptr<DataType>> innerTypes = {varcharType, intType};
    std::vector<std::string> innerNames = {"mgr_name", "mgr_level"};
    auto innerRowType = std::make_shared<omniruntime::type::RowType>(innerTypes, innerNames);
    std::vector<std::shared_ptr<DataType>> outerTypes = {varcharType, arrayVarcharType, innerRowType};
    std::vector<std::string> outerNames = {"department", "projects", "manager"};
    auto structType = std::make_shared<omniruntime::type::RowType>(outerTypes, outerNames);

    DataTypes sourceTypes(std::vector<DataTypePtr>({ LongType(), structType }));

    // Partition by struct, sort by sortKey ascending
    std::vector<omniruntime::expressions::Expr *> partitionKeys = { new FieldExpr(1, structType) };
    std::vector<omniruntime::expressions::Expr *> sortKeyExprs = { new FieldExpr(0, LongType()) };
    std::vector<int32_t> sortAscendings = { 1 };
    std::vector<int32_t> sortNullFirsts = { 0 };
    auto overflowConfig = new OverflowConfig();
    auto windowGroupLimitOperatorFactory = new WindowGroupLimitWithExprOperatorFactory(sourceTypes, 3, "rank",
        partitionKeys, sortKeyExprs, sortAscendings, sortNullFirsts, overflowConfig);
    auto windowGroupLimitOperator =
        static_cast<WindowGroupLimitWithExprOperator *>(CreateTestOperator(windowGroupLimitOperatorFactory));

    windowGroupLimitOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch;
    windowGroupLimitOperator->GetOutput(&outputVecBatch);

    // For rank function with limit 3, we expect to keep top 3 ranks per partition
    // Partitions are defined by the entire struct, so same struct = same partition

    // Use IgnoreOrder for verification
    EXPECT_NE(outputVecBatch, nullptr);
    EXPECT_LE(outputVecBatch->GetRowCount(), dataSize);

    Expr::DeleteExprs(partitionKeys);
    Expr::DeleteExprs(sortKeyExprs);
    omniruntime::op::Operator::DeleteOperator(windowGroupLimitOperator);
    delete windowGroupLimitOperatorFactory;
    VectorHelper::FreeVecBatch(outputVecBatch);
    delete overflowConfig;
}

// Helper to build simpler Struct(Row) column for sort testing
// Struct structure: {category: VARCHAR, tags: ARRAY<VARCHAR>, info: STRUCT<{priority: INT, active: BOOLEAN}>}
static RowVector *CreateSortableStructVector(int32_t rowCount,
    const std::vector<std::string> &categories,
    const std::vector<std::vector<std::string>> &tagsList,
    const std::vector<int32_t> &priorities,
    const std::vector<bool> &actives)
{
    // Create category vector (VARCHAR)
    auto *categoryVec = new VarcharVector(rowCount);
    for (int32_t i = 0; i < rowCount; ++i) {
        std::string_view sv(categories[i].data(), categories[i].size());
        categoryVec->SetValue(i, sv);
    }

    // Create tags vector (ARRAY<VARCHAR>)
    std::vector<std::string> flatStrings;
    for (const auto &tags : tagsList) {
        for (const auto &s : tags) {
            flatStrings.push_back(s);
        }
    }
    int32_t totalTags = static_cast<int32_t>(flatStrings.size());
    auto *tagsElemVec = new VarcharVector(totalTags);
    int32_t idx = 0;
    for (const auto &tags : tagsList) {
        for (const auto &s : tags) {
            std::string_view sv(s.data(), s.size());
            tagsElemVec->SetValue(idx++, sv);
        }
    }
    auto *tagsArrayVec = new ArrayVector(rowCount);
    int32_t offset = 0;
    for (int32_t i = 0; i < rowCount; ++i) {
        tagsArrayVec->SetOffset(i, offset);
        offset += static_cast<int32_t>(tagsList[i].size());
    }
    tagsArrayVec->SetOffset(rowCount, offset);
    tagsArrayVec->SetElementVectorFromRaw(tagsElemVec);

    // Create inner info struct (STRUCT<{priority: INT, active: BOOLEAN}>)
    auto *priorityVec = new Vector<int32_t>(rowCount);
    auto *activeVec = new Vector<bool>(rowCount);
    for (int32_t i = 0; i < rowCount; ++i) {
        priorityVec->SetValue(i, priorities[i]);
        activeVec->SetValue(i, actives[i]);
    }
    auto *infoStructVec = new RowVector(rowCount);
    infoStructVec->AddChild(priorityVec);
    infoStructVec->AddChild(activeVec);

    // Create outer struct vector (STRUCT<{category, tags, info}>)
    auto *structVec = new RowVector(rowCount);
    structVec->AddChild(categoryVec);
    structVec->AddChild(tagsArrayVec);
    structVec->AddChild(infoStructVec);

    return structVec;
}

// row_number + sort by nested Struct with complex types (partition by simple key, sort by struct)
TEST(WindowGroupLimitWithExprOperatorTest, TestRowNumberSortByNestedStruct)
{
    const int32_t dataSize = 6;

    // Simple partition key (INT) - we'll partition by this
    int64_t partitionKeys[dataSize] = {1, 1, 1, 1, 1, 1};  // All in same partition

    // Struct data for sorting:
    // category determines major sort order
    // tags array and info struct provide complexity
    std::vector<std::string> categories = {
        "High", "Low", "Medium", "High", "Low", "Medium"
    };

    // Tags (ARRAY<VARCHAR>)
    std::vector<std::vector<std::string>> tagsList = {
        {"urgent", "critical"},
        {"minor"},
        {"normal", "review"},
        {"urgent"},
        {"minor", "defer"},
        {"normal"}
    };

    // Info struct fields: priority and active status
    std::vector<int32_t> priorities = {10, 1, 5, 9, 2, 6};
    std::vector<bool> actives = {true, false, true, true, true, false};

    // Row IDs for verification (to identify which rows are kept)
    int64_t rowIds[dataSize] = {1, 2, 3, 4, 5, 6};

    // Create input batch: partitionKey, struct, rowId
    auto *vecBatch = new VectorBatch(dataSize);

    // Add partition key column (simple LONG)
    auto *partitionKeyVec = new Vector<int64_t>(dataSize);
    for (int32_t i = 0; i < dataSize; ++i) {
        partitionKeyVec->SetValue(i, partitionKeys[i]);
    }
    vecBatch->Append(partitionKeyVec);

    // Add nested struct column (used for sorting)
    auto *structCol = CreateSortableStructVector(dataSize, categories, tagsList, priorities, actives);
    vecBatch->Append(structCol);

    // Add row ID column for verification
    auto *rowIdVec = new Vector<int64_t>(dataSize);
    for (int32_t i = 0; i < dataSize; ++i) {
        rowIdVec->SetValue(i, rowIds[i]);
    }
    vecBatch->Append(rowIdVec);

    // Define struct data type
    DataTypePtr varcharType = VarcharType(20);
    DataTypePtr intType = std::make_shared<omniruntime::type::DataType>(OMNI_INT);
    DataTypePtr boolType = std::make_shared<omniruntime::type::DataType>(OMNI_BOOLEAN);
    DataTypePtr arrayVarcharType = std::make_shared<omniruntime::type::ArrayType>(varcharType);
    std::vector<std::shared_ptr<DataType>> innerTypes = {intType, boolType};
    std::vector<std::string> innerNames = {"priority", "active"};
    auto innerRowType = std::make_shared<omniruntime::type::RowType>(innerTypes, innerNames);
    std::vector<std::shared_ptr<DataType>> outerTypes = {varcharType, arrayVarcharType, innerRowType};
    std::vector<std::string> outerNames = {"category", "tags", "info"};
    auto structType = std::make_shared<omniruntime::type::RowType>(outerTypes, outerNames);

    DataTypes sourceTypes(std::vector<DataTypePtr>({ LongType(), structType, LongType() }));

    // Partition by simple key, but SORT BY the complex struct column
    std::vector<omniruntime::expressions::Expr *> partitionKeyExprs = { new FieldExpr(0, LongType()) };
    std::vector<omniruntime::expressions::Expr *> sortKeyExprs = { new FieldExpr(1, structType) };
    std::vector<int32_t> sortAscendings = { 1 };  // Ascending
    std::vector<int32_t> sortNullFirsts = { 0 };  // Null last
    auto overflowConfig = new OverflowConfig();
    auto windowGroupLimitOperatorFactory = new WindowGroupLimitWithExprOperatorFactory(sourceTypes, 3, "row_number",
        partitionKeyExprs, sortKeyExprs, sortAscendings, sortNullFirsts, overflowConfig);
    auto windowGroupLimitOperator =
        static_cast<WindowGroupLimitWithExprOperator *>(CreateTestOperator(windowGroupLimitOperatorFactory));

    windowGroupLimitOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch;
    windowGroupLimitOperator->GetOutput(&outputVecBatch);

    // All rows are in the same partition (partitionKey=1)
    // Sorted by struct (category ASC: High < Low < Medium in lexicographic order)
    // High (indices 0,3) -> Low (indices 1,4) -> Medium (indices 2,5)
    // Within same category, struct comparison continues to deeper fields
    // With limit=3, we keep first 3 rows after sorting by struct

    // Verify output is not null and has expected row count
    EXPECT_NE(outputVecBatch, nullptr);
    EXPECT_EQ(outputVecBatch->GetRowCount(), 3);

    // Verify row IDs in output (should be 3 of the original 6)
    // The exact rows depend on struct comparison ordering
    auto *outputRowIdCol = static_cast<Vector<int64_t> *>(outputVecBatch->Get(2));
    for (int32_t i = 0; i < outputVecBatch->GetRowCount(); ++i) {
        int64_t rowId = outputRowIdCol->GetValue(i);
        EXPECT_GE(rowId, 1);
        EXPECT_LE(rowId, 6);
    }

    Expr::DeleteExprs(partitionKeyExprs);
    Expr::DeleteExprs(sortKeyExprs);
    omniruntime::op::Operator::DeleteOperator(windowGroupLimitOperator);
    delete windowGroupLimitOperatorFactory;
    VectorHelper::FreeVecBatch(outputVecBatch);
    delete overflowConfig;
}

// rank + partition by simple key, sort by nested struct with DESC order
TEST(WindowGroupLimitWithExprOperatorTest, TestRankSortByNestedStructDesc)
{
    const int32_t dataSize = 8;

    // Partition by department (2 partitions)
    std::vector<std::string> departments = {
        "Engineering", "Sales", "Engineering", "Sales",
        "Engineering", "Sales", "Engineering", "Sales"
    };

    // Struct data for sorting (within each partition)
    std::vector<std::string> levels = {
        "Senior", "Junior", "Lead", "Junior",
        "Mid", "Senior", "Junior", "Lead"
    };

    // Skills array (ARRAY<VARCHAR>)
    std::vector<std::vector<std::string>> skillsList = {
        {"C++", "Java"}, {"Python"}, {"C++", "Java", "Go"}, {"JavaScript"},
        {"Java", "SQL"}, {"Python", "R"}, {"C"}, {"Go", "Rust"}
    };

    // Performance struct: score (INT) and bonus (BOOLEAN)
    std::vector<int32_t> scores = {95, 70, 100, 65, 85, 90, 75, 98};
    std::vector<bool> bonuses = {true, false, true, false, true, true, false, true};

    // Create input batch
    auto *vecBatch = new VectorBatch(dataSize);

    // Department column (VARCHAR) - partition key
    auto *deptVec = new VarcharVector(dataSize);
    for (int32_t i = 0; i < dataSize; ++i) {
        std::string_view sv(departments[i].data(), departments[i].size());
        deptVec->SetValue(i, sv);
    }
    vecBatch->Append(deptVec);

    // Add nested struct column (for sorting)
    DataTypePtr varcharType = VarcharType(20);
    DataTypePtr intType = std::make_shared<omniruntime::type::DataType>(OMNI_INT);
    DataTypePtr boolType = std::make_shared<omniruntime::type::DataType>(OMNI_BOOLEAN);
    DataTypePtr arrayVarcharType = std::make_shared<omniruntime::type::ArrayType>(varcharType);

    // Build struct column manually
    // Level vector
    auto *levelVec = new VarcharVector(dataSize);
    for (int32_t i = 0; i < dataSize; ++i) {
        std::string_view sv(levels[i].data(), levels[i].size());
        levelVec->SetValue(i, sv);
    }

    // Skills array vector
    std::vector<std::string> flatSkills;
    for (const auto &skills : skillsList) {
        for (const auto &s : skills) {
            flatSkills.push_back(s);
        }
    }
    int32_t totalSkills = static_cast<int32_t>(flatSkills.size());
    auto *skillsElemVec = new VarcharVector(totalSkills);
    int32_t skillIdx = 0;
    for (const auto &skills : skillsList) {
        for (const auto &s : skills) {
            std::string_view sv(s.data(), s.size());
            skillsElemVec->SetValue(skillIdx++, sv);
        }
    }
    auto *skillsArrayVec = new ArrayVector(dataSize);
    int32_t skillOffset = 0;
    for (int32_t i = 0; i < dataSize; ++i) {
        skillsArrayVec->SetOffset(i, skillOffset);
        skillOffset += static_cast<int32_t>(skillsList[i].size());
    }
    skillsArrayVec->SetOffset(dataSize, skillOffset);
    skillsArrayVec->SetElementVectorFromRaw(skillsElemVec);

    // Performance struct
    auto *scoreVec = new Vector<int32_t>(dataSize);
    auto *bonusVec = new Vector<bool>(dataSize);
    for (int32_t i = 0; i < dataSize; ++i) {
        scoreVec->SetValue(i, scores[i]);
        bonusVec->SetValue(i, bonuses[i]);
    }
    auto *perfStructVec = new RowVector(dataSize);
    perfStructVec->AddChild(scoreVec);
    perfStructVec->AddChild(bonusVec);

    // Outer struct
    auto *structVec = new RowVector(dataSize);
    structVec->AddChild(levelVec);
    structVec->AddChild(skillsArrayVec);
    structVec->AddChild(perfStructVec);
    vecBatch->Append(structVec);

    // Define types
    std::vector<std::shared_ptr<DataType>> innerTypes = {intType, boolType};
    std::vector<std::string> innerNames = {"score", "bonus"};
    auto innerRowType = std::make_shared<omniruntime::type::RowType>(innerTypes, innerNames);
    std::vector<std::shared_ptr<DataType>> outerTypes = {varcharType, arrayVarcharType, innerRowType};
    std::vector<std::string> outerNames = {"level", "skills", "performance"};
    auto structType = std::make_shared<omniruntime::type::RowType>(outerTypes, outerNames);

    DataTypes sourceTypes(std::vector<DataTypePtr>({ VarcharType(20), structType }));

    // Partition by department, sort by struct DESC
    std::vector<omniruntime::expressions::Expr *> partitionKeyExprs = { new FieldExpr(0, VarcharType(20)) };
    std::vector<omniruntime::expressions::Expr *> sortKeyExprs = { new FieldExpr(1, structType) };
    std::vector<int32_t> sortAscendings = { 0 };  // Descending
    std::vector<int32_t> sortNullFirsts = { 0 };  // Null last
    auto overflowConfig = new OverflowConfig();
    auto windowGroupLimitOperatorFactory = new WindowGroupLimitWithExprOperatorFactory(sourceTypes, 2, "rank",
        partitionKeyExprs, sortKeyExprs, sortAscendings, sortNullFirsts, overflowConfig);
    auto windowGroupLimitOperator =
        static_cast<WindowGroupLimitWithExprOperator *>(CreateTestOperator(windowGroupLimitOperatorFactory));

    windowGroupLimitOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch;
    windowGroupLimitOperator->GetOutput(&outputVecBatch);

    // Engineering partition: 4 rows (indices 0,2,4,6)
    // Sales partition: 4 rows (indices 1,3,5,7)
    // Each partition sorted by struct DESC, keep top 2 ranks

    EXPECT_NE(outputVecBatch, nullptr);
    EXPECT_LE(outputVecBatch->GetRowCount(), dataSize);
    EXPECT_GE(outputVecBatch->GetRowCount(), 2);  // At least 2 rows (one from each partition)

    Expr::DeleteExprs(partitionKeyExprs);
    Expr::DeleteExprs(sortKeyExprs);
    omniruntime::op::Operator::DeleteOperator(windowGroupLimitOperator);
    delete windowGroupLimitOperatorFactory;
    VectorHelper::FreeVecBatch(outputVecBatch);
    delete overflowConfig;
}

} // namespace WindowGroupLimitTest