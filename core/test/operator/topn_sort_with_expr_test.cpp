/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

#include "gtest/gtest.h"
#include "operator/topnsort/topn_sort_expr.h"
#include "vector/vector_helper.h"
#include "util/test_util.h"

using namespace omniruntime::vec;
using namespace omniruntime::op;
using namespace omniruntime::expressions;
using namespace std;
using namespace TestUtil;

namespace TopnWithExprTest {
TEST(TopNSortWithExprOperatorTest, TestTopNSortDescNullLast)
{
    // construct input data
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
    auto topNSortOperatorFactory = new TopNSortWithExprOperatorFactory(sourceTypes, 3, false, partitionKeys, sortKeys,
        sortAscendings, sortNullFirsts, overflowConfig);
    auto topNSortOperator = static_cast<TopNSortWithExprOperator *>(CreateTestOperator(topNSortOperatorFactory));

    topNSortOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch;
    topNSortOperator->GetOutput(&outputVecBatch);
    VectorHelper::PrintVecBatch(outputVecBatch);

    constexpr int32_t expectedDataSize = 8;
    std::string expData1[expectedDataSize] = {"bye", "bye", "bye", "bye", "bye", "hi", "hi", "hi"};
    int64_t expData2[expectedDataSize] = {4, 0, 11, 3, 23, 3, 5, 2};
    int64_t expData3[expectedDataSize] = {5, 4, 3, 3, 3, 8, 5, 3};
    DataTypes expectTypes(std::vector<DataTypePtr>({ VarcharType(10), LongType(), LongType() }));
    VectorBatch *expectVectorBatch = CreateVectorBatch(expectTypes, expectedDataSize, expData1, expData2, expData3);

    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expectVectorBatch));

    Expr::DeleteExprs(partitionKeys);
    Expr::DeleteExprs(sortKeys);
    omniruntime::op::Operator::DeleteOperator(topNSortOperator);
    delete topNSortOperatorFactory;
    VectorHelper::FreeVecBatch(expectVectorBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
    delete overflowConfig;
}

TEST(TopNSortWithExprOperatorTest, TestTopNSortAscNullLast)
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
    auto topNSortOperatorFactory = new TopNSortWithExprOperatorFactory(sourceTypes, 3, false, partitionKeys, sortKeys,
        sortAscendings, sortNullFirsts, overflowConfig);
    auto topNSortOperator = static_cast<TopNSortWithExprOperator *>(CreateTestOperator(topNSortOperatorFactory));

    topNSortOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch;
    topNSortOperator->GetOutput(&outputVecBatch);
    VectorHelper::PrintVecBatch(outputVecBatch);

    constexpr int32_t expectedDataSize = 8;
    std::string expData1[expectedDataSize] = {"bye", "bye", "bye", "bye", "bye", "hi", "hi", "hi"};
    int64_t expData2[expectedDataSize] = {11, 0, 4, 3, 23, 5, 2, 3};
    int64_t expData3[expectedDataSize] = {3, 4, 6, 6, 6, 3, 5, 8};
    DataTypes expectTypes(std::vector<DataTypePtr>({ VarcharType(10), LongType(), LongType() }));
    VectorBatch *expectVectorBatch = CreateVectorBatch(expectTypes, expectedDataSize, expData1, expData2, expData3);

    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expectVectorBatch));

    Expr::DeleteExprs(partitionKeys);
    Expr::DeleteExprs(sortKeys);
    omniruntime::op::Operator::DeleteOperator(topNSortOperator);
    delete topNSortOperatorFactory;
    VectorHelper::FreeVecBatch(expectVectorBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
    delete overflowConfig;
}

TEST(TopNSortWithExprOperatorTest, TestTopNSortAscCase1)
{
    // construct input data
    const int32_t dataSize = 4;
    std::string data1[dataSize] = {"bye", "bye", "bye", "bye"};
    int64_t data2[dataSize] = {2L, 5L, 3L, 11L};
    int64_t data3[dataSize] = {6L, 6L, 3L, 4L};
    DataTypes sourceTypes(std::vector<DataTypePtr>({ VarcharType(10), LongType(), LongType() }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data1, data2, data3);

    std::vector<omniruntime::expressions::Expr *> partitionKeys = { new FieldExpr(0, VarcharType(10)) };
    std::vector<omniruntime::expressions::Expr *> sortKeys = { new FieldExpr(2, LongType()) };
    std::vector<int32_t> sortAscendings = { 1 };
    std::vector<int32_t> sortNullFirsts = { 0 };
    auto overflowConfig = new OverflowConfig();
    auto topNSortOperatorFactory = new TopNSortWithExprOperatorFactory(sourceTypes, 3, false, partitionKeys, sortKeys,
        sortAscendings, sortNullFirsts, overflowConfig);
    auto topNSortOperator = static_cast<TopNSortWithExprOperator *>(CreateTestOperator(topNSortOperatorFactory));

    topNSortOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch;
    topNSortOperator->GetOutput(&outputVecBatch);
    VectorHelper::PrintVecBatch(outputVecBatch);

    constexpr int32_t expectedDataSize = 4;
    std::string expData1[expectedDataSize] = {"bye", "bye", "bye", "bye"};
    int64_t expData2[expectedDataSize] = {3, 11, 2, 5};
    int64_t expData3[expectedDataSize] = {3, 4, 6, 6};
    DataTypes expectTypes(std::vector<DataTypePtr>({ VarcharType(10), LongType(), LongType() }));
    VectorBatch *expectVectorBatch = CreateVectorBatch(expectTypes, expectedDataSize, expData1, expData2, expData3);

    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expectVectorBatch));

    Expr::DeleteExprs(partitionKeys);
    Expr::DeleteExprs(sortKeys);
    omniruntime::op::Operator::DeleteOperator(topNSortOperator);
    delete topNSortOperatorFactory;
    VectorHelper::FreeVecBatch(expectVectorBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
    delete overflowConfig;
}

TEST(TopNSortWithExprOperatorTest, TestTopNSortAscCase2)
{
    // construct input data
    const int32_t dataSize = 4;
    std::string data1[dataSize] = {"bye", "bye", "bye", "bye"};
    int64_t data2[dataSize] = {2L, 5L, 3L, 11L};
    int64_t data3[dataSize] = {6L, 6L, 4L, 3L};
    DataTypes sourceTypes(std::vector<DataTypePtr>({ VarcharType(10), LongType(), LongType() }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data1, data2, data3);

    std::vector<omniruntime::expressions::Expr *> partitionKeys = { new FieldExpr(0, VarcharType(10)) };
    std::vector<omniruntime::expressions::Expr *> sortKeys = { new FieldExpr(2, LongType()) };
    std::vector<int32_t> sortAscendings = { 1 };
    std::vector<int32_t> sortNullFirsts = { 0 };
    auto overflowConfig = new OverflowConfig();
    auto topNSortOperatorFactory = new TopNSortWithExprOperatorFactory(sourceTypes, 3, false, partitionKeys, sortKeys,
        sortAscendings, sortNullFirsts, overflowConfig);
    auto topNSortOperator = static_cast<TopNSortWithExprOperator *>(CreateTestOperator(topNSortOperatorFactory));

    topNSortOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch;
    topNSortOperator->GetOutput(&outputVecBatch);
    VectorHelper::PrintVecBatch(outputVecBatch);

    constexpr int32_t expectedDataSize = 4;
    std::string expData1[expectedDataSize] = {"bye", "bye", "bye", "bye"};
    int64_t expData2[expectedDataSize] = {11, 3, 2, 5};
    int64_t expData3[expectedDataSize] = {3, 4, 6, 6};
    DataTypes expectTypes(std::vector<DataTypePtr>({ VarcharType(10), LongType(), LongType() }));
    VectorBatch *expectVectorBatch = CreateVectorBatch(expectTypes, expectedDataSize, expData1, expData2, expData3);

    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expectVectorBatch));

    Expr::DeleteExprs(partitionKeys);
    Expr::DeleteExprs(sortKeys);
    omniruntime::op::Operator::DeleteOperator(topNSortOperator);
    delete topNSortOperatorFactory;
    VectorHelper::FreeVecBatch(expectVectorBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
    delete overflowConfig;
}

TEST(TopNSortWithExprOperatorTest, TestTopNSortAscCase3)
{
    // construct input data
    const int32_t dataSize = 5;
    std::string data1[dataSize] = {"bye", "bye", "bye", "bye", "bye"};
    int64_t data2[dataSize] = {2L, 5L, 3L, 11L, 6L};
    int64_t data3[dataSize] = {6L, 6L, 4L, 3L, 5L};
    DataTypes sourceTypes(std::vector<DataTypePtr>({ VarcharType(10), LongType(), LongType() }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data1, data2, data3);

    std::vector<omniruntime::expressions::Expr *> partitionKeys = { new FieldExpr(0, VarcharType(10)) };
    std::vector<omniruntime::expressions::Expr *> sortKeys = { new FieldExpr(2, LongType()) };
    std::vector<int32_t> sortAscendings = { 1 };
    std::vector<int32_t> sortNullFirsts = { 0 };
    auto overflowConfig = new OverflowConfig();
    auto topNSortOperatorFactory = new TopNSortWithExprOperatorFactory(sourceTypes, 3, false, partitionKeys, sortKeys,
        sortAscendings, sortNullFirsts, overflowConfig);
    auto topNSortOperator = static_cast<TopNSortWithExprOperator *>(CreateTestOperator(topNSortOperatorFactory));

    topNSortOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch;
    topNSortOperator->GetOutput(&outputVecBatch);
    VectorHelper::PrintVecBatch(outputVecBatch);

    constexpr int32_t expectedDataSize = 3;
    std::string expData1[expectedDataSize] = {"bye", "bye", "bye"};
    int64_t expData2[expectedDataSize] = {11, 3, 6};
    int64_t expData3[expectedDataSize] = {3, 4, 5};
    DataTypes expectTypes(std::vector<DataTypePtr>({ VarcharType(10), LongType(), LongType() }));
    VectorBatch *expectVectorBatch = CreateVectorBatch(expectTypes, expectedDataSize, expData1, expData2, expData3);

    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expectVectorBatch));

    Expr::DeleteExprs(partitionKeys);
    Expr::DeleteExprs(sortKeys);
    omniruntime::op::Operator::DeleteOperator(topNSortOperator);
    delete topNSortOperatorFactory;
    VectorHelper::FreeVecBatch(expectVectorBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
    delete overflowConfig;
}

TEST(TopNSortWithExprOperatorTest, TestTopNSortAscCase4)
{
    // construct input data
    const int32_t dataSize = 5;
    std::string data1[dataSize] = {"bye", "bye", "bye", "bye", "bye"};
    int64_t data2[dataSize] = {2L, 5L, 3L, 11L, 6L};
    int64_t data3[dataSize] = {6L, 6L, 3L, 6L, 3L};
    DataTypes sourceTypes(std::vector<DataTypePtr>({ VarcharType(10), LongType(), LongType() }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data1, data2, data3);

    std::vector<omniruntime::expressions::Expr *> partitionKeys = { new FieldExpr(0, VarcharType(10)) };
    std::vector<omniruntime::expressions::Expr *> sortKeys = { new FieldExpr(2, LongType()) };
    std::vector<int32_t> sortAscendings = { 1 };
    std::vector<int32_t> sortNullFirsts = { 0 };
    auto overflowConfig = new OverflowConfig();
    auto topNSortOperatorFactory = new TopNSortWithExprOperatorFactory(sourceTypes, 3, false, partitionKeys, sortKeys,
        sortAscendings, sortNullFirsts, overflowConfig);
    auto topNSortOperator = static_cast<TopNSortWithExprOperator *>(CreateTestOperator(topNSortOperatorFactory));

    topNSortOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch;
    topNSortOperator->GetOutput(&outputVecBatch);
    VectorHelper::PrintVecBatch(outputVecBatch);

    constexpr int32_t expectedDataSize = 5;
    std::string expData1[expectedDataSize] = {"bye", "bye", "bye", "bye", "bye"};
    int64_t expData2[expectedDataSize] = {3, 6, 2, 5, 11};
    int64_t expData3[expectedDataSize] = {3, 3, 6, 6, 6};
    DataTypes expectTypes(std::vector<DataTypePtr>({ VarcharType(10), LongType(), LongType() }));
    VectorBatch *expectVectorBatch = CreateVectorBatch(expectTypes, expectedDataSize, expData1, expData2, expData3);

    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expectVectorBatch));

    Expr::DeleteExprs(partitionKeys);
    Expr::DeleteExprs(sortKeys);
    omniruntime::op::Operator::DeleteOperator(topNSortOperator);
    delete topNSortOperatorFactory;
    VectorHelper::FreeVecBatch(expectVectorBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
    delete overflowConfig;
}

TEST(TopNSortWithExprOperatorTest, TestTopNSortAscCase5)
{
    // construct input data
    const int32_t dataSize = 5;
    std::string data1[dataSize] = {"bye", "bye", "bye", "bye", "bye"};
    int64_t data2[dataSize] = {2L, 5L, 3L, 11L, 6L};
    int64_t data3[dataSize] = {6L, 3L, 5L, 6L, 4L};
    DataTypes sourceTypes(std::vector<DataTypePtr>({ VarcharType(10), LongType(), LongType() }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data1, data2, data3);

    std::vector<omniruntime::expressions::Expr *> partitionKeys = { new FieldExpr(0, VarcharType(10)) };
    std::vector<omniruntime::expressions::Expr *> sortKeys = { new FieldExpr(2, LongType()) };
    std::vector<int32_t> sortAscendings = { 1 };
    std::vector<int32_t> sortNullFirsts = { 0 };
    auto overflowConfig = new OverflowConfig();
    auto topNSortOperatorFactory = new TopNSortWithExprOperatorFactory(sourceTypes, 3, false, partitionKeys, sortKeys,
        sortAscendings, sortNullFirsts, overflowConfig);
    auto topNSortOperator = static_cast<TopNSortWithExprOperator *>(CreateTestOperator(topNSortOperatorFactory));

    topNSortOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch;
    topNSortOperator->GetOutput(&outputVecBatch);
    VectorHelper::PrintVecBatch(outputVecBatch);

    constexpr int32_t expectedDataSize = 3;
    std::string expData1[expectedDataSize] = {"bye", "bye", "bye"};
    int64_t expData2[expectedDataSize] = {5, 6, 3};
    int64_t expData3[expectedDataSize] = {3, 4, 5};
    DataTypes expectTypes(std::vector<DataTypePtr>({ VarcharType(10), LongType(), LongType() }));
    VectorBatch *expectVectorBatch = CreateVectorBatch(expectTypes, expectedDataSize, expData1, expData2, expData3);

    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expectVectorBatch));

    Expr::DeleteExprs(partitionKeys);
    Expr::DeleteExprs(sortKeys);
    omniruntime::op::Operator::DeleteOperator(topNSortOperator);
    delete topNSortOperatorFactory;
    VectorHelper::FreeVecBatch(expectVectorBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
    delete overflowConfig;
}

TEST(TopNSortWithExprOperatorTest, TestTopNSortAscCase6)
{
    // construct input data
    const int32_t dataSize = 5;
    std::string data1[dataSize] = {"bye", "bye", "bye", "bye", "bye"};
    int64_t data2[dataSize] = {2L, 5L, 3L, 11L, 6L};
    int64_t data3[dataSize] = {7L, 3L, 5L, 7L, 6L};
    DataTypes sourceTypes(std::vector<DataTypePtr>({ VarcharType(10), LongType(), LongType() }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data1, data2, data3);

    std::vector<omniruntime::expressions::Expr *> partitionKeys = { new FieldExpr(0, VarcharType(10)) };
    std::vector<omniruntime::expressions::Expr *> sortKeys = { new FieldExpr(2, LongType()) };
    std::vector<int32_t> sortAscendings = { 1 };
    std::vector<int32_t> sortNullFirsts = { 0 };
    auto overflowConfig = new OverflowConfig();
    auto topNSortOperatorFactory = new TopNSortWithExprOperatorFactory(sourceTypes, 4, false, partitionKeys, sortKeys,
        sortAscendings, sortNullFirsts, overflowConfig);
    auto topNSortOperator = static_cast<TopNSortWithExprOperator *>(CreateTestOperator(topNSortOperatorFactory));

    topNSortOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch;
    topNSortOperator->GetOutput(&outputVecBatch);
    VectorHelper::PrintVecBatch(outputVecBatch);

    constexpr int32_t expectedDataSize = 5;
    std::string expData1[expectedDataSize] = {"bye", "bye", "bye", "bye", "bye"};
    int64_t expData2[expectedDataSize] = {5, 3, 6, 2, 11};
    int64_t expData3[expectedDataSize] = {3, 5, 6, 7, 7};
    DataTypes expectTypes(std::vector<DataTypePtr>({ VarcharType(10), LongType(), LongType() }));
    VectorBatch *expectVectorBatch = CreateVectorBatch(expectTypes, expectedDataSize, expData1, expData2, expData3);

    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expectVectorBatch));

    Expr::DeleteExprs(partitionKeys);
    Expr::DeleteExprs(sortKeys);
    omniruntime::op::Operator::DeleteOperator(topNSortOperator);
    delete topNSortOperatorFactory;
    VectorHelper::FreeVecBatch(expectVectorBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
    delete overflowConfig;
}

TEST(TopNSortWithExprOperatorTest, TestTopNSortAscCase7)
{
    // construct input data
    const int32_t dataSize = 5;
    std::string data1[dataSize] = {"bye", "bye", "bye", "bye", "bye"};
    int64_t data2[dataSize] = {2L, 5L, 3L, 11L, 6L};
    int64_t data3[dataSize] = {8L, 3L, 5L, 7L, 6L};
    DataTypes sourceTypes(std::vector<DataTypePtr>({ VarcharType(10), LongType(), LongType() }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data1, data2, data3);

    std::vector<omniruntime::expressions::Expr *> partitionKeys = { new FieldExpr(0, VarcharType(10)) };
    std::vector<omniruntime::expressions::Expr *> sortKeys = { new FieldExpr(2, LongType()) };
    std::vector<int32_t> sortAscendings = { 1 };
    std::vector<int32_t> sortNullFirsts = { 0 };
    auto overflowConfig = new OverflowConfig();
    auto topNSortOperatorFactory = new TopNSortWithExprOperatorFactory(sourceTypes, 4, false, partitionKeys, sortKeys,
        sortAscendings, sortNullFirsts, overflowConfig);
    auto topNSortOperator = static_cast<TopNSortWithExprOperator *>(CreateTestOperator(topNSortOperatorFactory));

    topNSortOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch;
    topNSortOperator->GetOutput(&outputVecBatch);
    VectorHelper::PrintVecBatch(outputVecBatch);

    constexpr int32_t expectedDataSize = 4;
    std::string expData1[expectedDataSize] = {"bye", "bye", "bye", "bye"};
    int64_t expData2[expectedDataSize] = {5, 3, 6, 11};
    int64_t expData3[expectedDataSize] = {3, 5, 6, 7};
    DataTypes expectTypes(std::vector<DataTypePtr>({ VarcharType(10), LongType(), LongType() }));
    VectorBatch *expectVectorBatch = CreateVectorBatch(expectTypes, expectedDataSize, expData1, expData2, expData3);

    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expectVectorBatch));

    Expr::DeleteExprs(partitionKeys);
    Expr::DeleteExprs(sortKeys);
    omniruntime::op::Operator::DeleteOperator(topNSortOperator);
    delete topNSortOperatorFactory;
    VectorHelper::FreeVecBatch(expectVectorBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
    delete overflowConfig;
}

TEST(TopNSortWithExprOperatorTest, TestTopNSortAscCase8)
{
    // construct input data
    const int32_t dataSize = 5;
    std::string data1[dataSize] = {"bye", "bye", "bye", "bye", "bye"};
    int64_t data2[dataSize] = {2L, 5L, 3L, 11L, 6L};
    int64_t data3[dataSize] = {7L, 3L, 5L, 7L, 9L};
    DataTypes sourceTypes(std::vector<DataTypePtr>({ VarcharType(10), LongType(), LongType() }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data1, data2, data3);

    std::vector<omniruntime::expressions::Expr *> partitionKeys = { new FieldExpr(0, VarcharType(10)) };
    std::vector<omniruntime::expressions::Expr *> sortKeys = { new FieldExpr(2, LongType()) };
    std::vector<int32_t> sortAscendings = { 1 };
    std::vector<int32_t> sortNullFirsts = { 0 };
    auto overflowConfig = new OverflowConfig();
    auto topNSortOperatorFactory = new TopNSortWithExprOperatorFactory(sourceTypes, 4, false, partitionKeys, sortKeys,
        sortAscendings, sortNullFirsts, overflowConfig);
    auto topNSortOperator = static_cast<TopNSortWithExprOperator *>(CreateTestOperator(topNSortOperatorFactory));

    topNSortOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch;
    topNSortOperator->GetOutput(&outputVecBatch);
    VectorHelper::PrintVecBatch(outputVecBatch);

    constexpr int32_t expectedDataSize = 4;
    std::string expData1[expectedDataSize] = {"bye", "bye", "bye", "bye"};
    int64_t expData2[expectedDataSize] = {5, 3, 2, 11};
    int64_t expData3[expectedDataSize] = {3, 5, 7, 7};
    DataTypes expectTypes(std::vector<DataTypePtr>({ VarcharType(10), LongType(), LongType() }));
    VectorBatch *expectVectorBatch = CreateVectorBatch(expectTypes, expectedDataSize, expData1, expData2, expData3);

    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expectVectorBatch));

    Expr::DeleteExprs(partitionKeys);
    Expr::DeleteExprs(sortKeys);
    omniruntime::op::Operator::DeleteOperator(topNSortOperator);
    delete topNSortOperatorFactory;
    VectorHelper::FreeVecBatch(expectVectorBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
    delete overflowConfig;
}
}