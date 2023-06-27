/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 * @Description: sort operator test implementations
 */

#include "gtest/gtest.h"
#include "operator/sort/sort_expr.h"
#include "vector/vector_helper.h"
#include "util/test_util.h"

using namespace omniruntime::vec;
using namespace omniruntime::op;
using namespace omniruntime::expressions;
using namespace TestUtil;

namespace SortWithExprTest {
const uint64_t MAX_SPILL_BYTES = (1L << 20);

TEST(SortWithExprTest, TestSortZeroExprColumns)
{
    const int32_t dataSize = 5;
    int32_t data1[dataSize] = {4, 3, 2, 1, 0};
    int64_t data2[dataSize] = {0, 1, 2, 3, 4};
    int16_t data3[dataSize] = {0, 1, 2, 3, 4};
    DataTypes sourceTypes(std::vector<DataTypePtr>({ IntType(), LongType(), ShortType() }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data1, data2, data3);

    int outputCols[3] = {0, 1, 2};
    auto col0 = new FieldExpr(0, IntType());
    auto col1 = new FieldExpr(1, LongType());
    auto col2 = new FieldExpr(2, ShortType());
    std::vector<Expr *> sortExprs { col0, col1, col2 };
    int ascendings[3] = {true, false, false};
    int nullFirsts[3] = {true, true, true};

    auto operatorFactory = SortWithExprOperatorFactory::CreateSortWithExprOperatorFactory(sourceTypes, outputCols, 3,
        sortExprs, ascendings, nullFirsts, 3, OperatorConfig());

    auto sortOperator = static_cast<SortWithExprOperator *>(CreateTestOperator(operatorFactory));
    sortOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch = nullptr;
    sortOperator->GetOutput(&outputVecBatch);

    int32_t expectData1[dataSize] = {0, 1, 2, 3, 4};
    int64_t expectData2[dataSize] = {4, 3, 2, 1, 0};
    int16_t expectData3[dataSize] = {4, 3, 2, 1, 0};
    auto expectVecBatch = CreateVectorBatch(sourceTypes, dataSize, expectData1, expectData2, expectData3);
    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expectVecBatch));

    Expr::DeleteExprs(sortExprs);
    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    delete operatorFactory;
}

TEST(SortWithExprTest, TestSortOneExprColumns)
{
    const int32_t dataSize = 5;
    int32_t data1[dataSize] = {4, 3, 2, 1, 0};
    int64_t data2[dataSize] = {0, 1, 2, 3, 4};
    DataTypes sourceTypes(std::vector<DataTypePtr>({ IntType(), LongType() }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data1, data2);

    int outputCols[2] = {0, 1};
    auto col0 = new FieldExpr(0, IntType());
    auto addCol = new FieldExpr(0, IntType());
    auto addLiteral = new LiteralExpr(50, IntType());
    auto addExpr = new BinaryExpr(omniruntime::expressions::Operator::ADD, addCol, addLiteral, IntType());
    std::vector<Expr *> sortExprs { col0, addExpr };
    int ascendings[2] = {true, false};
    int nullFirsts[2] = {true, true};

    auto operatorFactory = SortWithExprOperatorFactory::CreateSortWithExprOperatorFactory(sourceTypes, outputCols, 2,
        sortExprs, ascendings, nullFirsts, 2, OperatorConfig());

    auto sortOperator = static_cast<SortWithExprOperator *>(CreateTestOperator(operatorFactory));
    sortOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch = nullptr;
    sortOperator->GetOutput(&outputVecBatch);

    int32_t expectData1[dataSize] = {0, 1, 2, 3, 4};
    int64_t expectData2[dataSize] = {4, 3, 2, 1, 0};
    auto expectVecBatch = CreateVectorBatch(sourceTypes, dataSize, expectData1, expectData2);
    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expectVecBatch));

    Expr::DeleteExprs(sortExprs);
    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    delete operatorFactory;
}

TEST(SortWithExprTest, TestSortTwoExprColumns)
{
    const int32_t dataSize = 5;
    int32_t data1[dataSize] = {4, 3, 2, 1, 0};
    int64_t data2[dataSize] = {0, 1, 2, 3, 4};
    DataTypes sourceTypes(std::vector<DataTypePtr>({ IntType(), LongType() }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data1, data2);

    int outputCols[2] = {0, 1};
    auto add1Col = new FieldExpr(0, IntType());
    auto add1Literal = new LiteralExpr(50, IntType());
    auto add2Col = new FieldExpr(1, LongType());
    auto add2Literal = new LiteralExpr(50, LongType());
    auto add1Expr = new BinaryExpr(omniruntime::expressions::Operator::ADD, add1Col, add1Literal, IntType());
    auto add2Expr = new BinaryExpr(omniruntime::expressions::Operator::ADD, add2Literal, add2Col, LongType());
    std::vector<Expr *> sortExprs { add1Expr, add2Expr };
    int ascendings[2] = {true, false};
    int nullFirsts[2] = {true, true};

    auto operatorFactory = SortWithExprOperatorFactory::CreateSortWithExprOperatorFactory(sourceTypes, outputCols, 2,
        sortExprs, ascendings, nullFirsts, 2, OperatorConfig());

    auto sortOperator = static_cast<SortWithExprOperator *>(CreateTestOperator(operatorFactory));
    sortOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch = nullptr;
    sortOperator->GetOutput(&outputVecBatch);

    int32_t expectData1[dataSize] = {0, 1, 2, 3, 4};
    int64_t expectData2[dataSize] = {4, 3, 2, 1, 0};
    auto expectVecBatch = CreateVectorBatch(sourceTypes, dataSize, expectData1, expectData2);
    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expectVecBatch));

    Expr::DeleteExprs(sortExprs);
    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    delete operatorFactory;
}

TEST(SortWithExprTest, TestSortTwoExprDictionaryColumns)
{
    // construct input data
    const int32_t dataSize = 6;
    // prepare data
    int32_t data0[dataSize] = {0, 1, 2, 0, 1, 2};
    int64_t data1[dataSize] = {0, 1, 2, 3, 4, 5};
    int64_t data2[dataSize] = {66, 55, 44, 33, 22, 11};
    void *datas[3] = {data0, data1, data2};
    DataTypes sourceTypes(std::vector<DataTypePtr>({ IntType(), LongType(), LongType() }));
    int32_t ids[] = {0, 1, 2, 3, 4, 5};
    auto *vecBatch = new VectorBatch(dataSize);
    for (int32_t i = 0; i < 3; i++) {
        vecBatch->Append(CreateDictionaryVector(*sourceTypes.GetType(i), dataSize, ids, dataSize, datas[i]));
    }

    int32_t outputCols[2] = {1, 2};
    auto add1Col = new FieldExpr(0, IntType());
    auto add1Literal = new LiteralExpr(50, IntType());
    auto add2Col = new FieldExpr(2, LongType());
    auto add2Literal = new LiteralExpr(50, LongType());
    auto add1Expr = new BinaryExpr(omniruntime::expressions::Operator::ADD, add1Col, add1Literal, IntType());
    auto add2Expr = new BinaryExpr(omniruntime::expressions::Operator::ADD, add2Literal, add2Col, LongType());
    std::vector<Expr *> sortExprs { add1Expr, add2Expr };
    int32_t ascendings[2] = {false, true};
    int32_t nullFirsts[2] = {true, true};

    auto operatorFactory = SortWithExprOperatorFactory::CreateSortWithExprOperatorFactory(sourceTypes, outputCols, 2,
        sortExprs, ascendings, nullFirsts, 2, OperatorConfig());
    auto sortOperator = static_cast<SortWithExprOperator *>(CreateTestOperator(operatorFactory));
    sortOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch = nullptr;
    sortOperator->GetOutput(&outputVecBatch);

    int64_t expectData1[dataSize] = {5, 2, 4, 1, 3, 0};
    int64_t expectData2[dataSize] = {11, 44, 22, 55, 33, 66};
    DataTypes expectedTypes(std::vector<DataTypePtr> { LongType(), LongType() });
    auto expectVecBatch = CreateVectorBatch(expectedTypes, dataSize, expectData1, expectData2);
    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expectVecBatch));

    Expr::DeleteExprs(sortExprs);
    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    delete operatorFactory;
}

TEST(SortWithExprTest, TestSortOneVarcharExprColumn)
{
    DataTypePtr type = VarcharType(10);
    const int32_t dataSize = 4;
    const int32_t vecCount = 1;
    std::string values[dataSize] = {"hello", "world", "omni", "runtime"};
    auto vector = CreateVarcharVector(values, dataSize);
    auto *vecBatch = new VectorBatch(dataSize);
    vecBatch->Append(vector);

    DataTypes sourceTypes(std::vector<DataTypePtr>({ type }));
    int32_t outputCols[vecCount] = {0};

    auto substrCol = new FieldExpr(0, VarcharType(200));
    auto substrIndex = new LiteralExpr(1, IntType());
    auto substrLen = new LiteralExpr(4, IntType());
    std::vector<Expr *> args { substrCol, substrIndex, substrLen };
    auto substrExpr = GetFuncExpr("substr", args, VarcharType(200));
    std::vector<Expr *> sortExprs { substrExpr };

    int32_t ascendings[vecCount] = {true};
    int32_t nullFirsts[vecCount] = {true};

    auto operatorFactory = SortWithExprOperatorFactory::CreateSortWithExprOperatorFactory(sourceTypes, outputCols,
        vecCount, sortExprs, ascendings, nullFirsts, vecCount, OperatorConfig());
    auto sortOperator = static_cast<SortWithExprOperator *>(CreateTestOperator(operatorFactory));
    sortOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch = nullptr;
    sortOperator->GetOutput(&outputVecBatch);

    std::string expectValues[dataSize] = {"hello", "omni", "runtime", "world"};
    auto expectVector = CreateVarcharVector(expectValues, dataSize);
    auto expectVecBatch = new VectorBatch(dataSize);
    expectVecBatch->Append(expectVector);
    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expectVecBatch));

    Expr::DeleteExprs(sortExprs);
    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    delete operatorFactory;
}

TEST(SortWithExprTest, TestSortTwoExprDictionaryWithNull)
{
    DataTypes sourceTypes(std::vector<DataTypePtr>({ IntType(), LongType(), LongType() }));

    // construct input data
    const int32_t dataSize = 6;
    // prepare data
    int32_t data0[dataSize] = {0, 1, 2, 0, 1, 2};
    int64_t data1[dataSize] = {0, 1, 2, 3, 4, 5};
    int64_t data2[dataSize] = {66, 55, 44, 33, 22, 11};

    auto vec0 = CreateVector(dataSize, data0);
    auto vec1 = CreateVector(dataSize, data1);
    auto vec2 = CreateVector(dataSize, data2);
    for (int i = 0; i < dataSize; i = i + 2) {
        vec0->SetNull(i);
        vec1->SetNull(i);
        vec2->SetNull(i);
    }

    int32_t ids[] = {0, 1, 2, 3, 4, 5};
    auto dictVec0 = DYNAMIC_TYPE_DISPATCH(CreateDictionary, sourceTypes.GetType(0)->GetId(), vec0, ids, 6);
    auto dictVec2 = DYNAMIC_TYPE_DISPATCH(CreateDictionary, sourceTypes.GetType(2)->GetId(), vec2, ids, 6);
    auto slicedVec0 = static_cast<Vector<DictionaryContainer<int32_t>> *>(dictVec0)->Slice(1, 5);
    auto slicedVec1 = static_cast<Vector<int64_t> *>(vec1)->Slice(1, 5);
    auto slicedVec2 = static_cast<Vector<DictionaryContainer<int64_t>> *>(dictVec2)->Slice(1, 5);

    auto vecBatch = new VectorBatch(5);
    vecBatch->Append(slicedVec0);
    vecBatch->Append(slicedVec1);
    vecBatch->Append(slicedVec2);

    int32_t outputCols[2] = {1, 2};
    auto add1Col = new FieldExpr(0, IntType());
    auto add1Literal = new LiteralExpr(50, IntType());
    auto add2Col = new FieldExpr(2, LongType());
    auto add2Literal = new LiteralExpr(50, LongType());
    auto add1Expr = new BinaryExpr(omniruntime::expressions::Operator::ADD, add1Col, add1Literal, IntType());
    auto add2Expr = new BinaryExpr(omniruntime::expressions::Operator::ADD, add2Literal, add2Col, LongType());
    std::vector<Expr *> sortExprs { add1Expr, add2Expr };

    int32_t ascendings[2] = {false, true};
    int32_t nullFirsts[2] = {true, true};

    auto operatorFactory = SortWithExprOperatorFactory::CreateSortWithExprOperatorFactory(sourceTypes, outputCols, 2,
        sortExprs, ascendings, nullFirsts, 2, OperatorConfig());
    auto sortOperator = static_cast<SortWithExprOperator *>(CreateTestOperator(operatorFactory));
    sortOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch = nullptr;
    sortOperator->GetOutput(&outputVecBatch);

    int64_t expectData1[5] = {0, 0, 5, 1, 3};
    int64_t expectData2[5] = {0, 0, 11, 55, 33};
    DataTypes expectedTypes(std::vector<DataTypePtr> { LongType(), LongType() });
    auto expectVecBatch = CreateVectorBatch(expectedTypes, 5, expectData1, expectData2);
    expectVecBatch->Get(0)->SetNull(0);
    expectVecBatch->Get(0)->SetNull(1);
    expectVecBatch->Get(1)->SetNull(0);
    expectVecBatch->Get(1)->SetNull(1);
    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expectVecBatch));

    delete vec0;
    delete vec1;
    delete vec2;
    delete dictVec0;
    delete dictVec2;

    Expr::DeleteExprs(sortExprs);
    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    delete operatorFactory;
}

TEST(SortWithExprTest, TestSortSpillWithMultiRecords)
{
    DataTypes sourceTypes(std::vector<DataTypePtr>({ IntType() }));

    const int32_t dataSize1 = 5;
    int32_t data1[dataSize1] = {4, 3, 2, 1, 0};
    auto vecBatch1 = CreateVectorBatch(sourceTypes, dataSize1, data1);

    const int32_t dataSize2 = 1;
    int32_t data2[dataSize2] = {8};
    auto vecBatch2 = CreateVectorBatch(sourceTypes, dataSize2, data2);

    const int32_t dataSize3 = 4;
    int32_t data3[dataSize3] = {7, 9, 6, 5};
    auto vecBatch3 = CreateVectorBatch(sourceTypes, dataSize3, data3);

    int outputCols[1] = {0};
    auto col0 = new FieldExpr(0, IntType());
    std::vector<Expr *> sortExprs { col0 };
    int ascendings[1] = {true};
    int nullFirsts[1] = {true};
    SparkSpillConfig spillConfig(GenerateSpillPath(), MAX_SPILL_BYTES, 4);
    OperatorConfig operatorConfig(spillConfig);

    auto operatorFactory = SortWithExprOperatorFactory::CreateSortWithExprOperatorFactory(sourceTypes, outputCols, 1,
        sortExprs, ascendings, nullFirsts, 1, operatorConfig);

    auto sortOperator = static_cast<SortWithExprOperator *>(CreateTestOperator(operatorFactory));
    sortOperator->AddInput(vecBatch1);
    sortOperator->AddInput(vecBatch2);
    sortOperator->AddInput(vecBatch3);
    VectorBatch *outputVecBatch = nullptr;
    sortOperator->GetOutput(&outputVecBatch);

    auto totalDataSize = dataSize1 + dataSize2 + dataSize3;
    int32_t expectData[] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    auto expectVecBatch = CreateVectorBatch(sourceTypes, totalDataSize, expectData);
    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expectVecBatch));

    Expr::DeleteExprs(sortExprs);
    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    delete operatorFactory;
}

TEST(SortWithExprTest, TestSortSpillWithOneRecord)
{
    DataTypes sourceTypes(std::vector<DataTypePtr>({ IntType() }));

    const int32_t dataSize = 1;
    int32_t data1[dataSize] = {3};
    auto vecBatch1 = CreateVectorBatch(sourceTypes, dataSize, data1);

    int32_t data2[dataSize] = {8};
    auto vecBatch2 = CreateVectorBatch(sourceTypes, dataSize, data2);

    int32_t data3[dataSize] = {6};
    auto vecBatch3 = CreateVectorBatch(sourceTypes, dataSize, data3);

    int outputCols[1] = {0};
    auto col0 = new FieldExpr(0, IntType());
    std::vector<Expr *> sortExprs { col0 };
    int ascendings[1] = {true};
    int nullFirsts[1] = {true};
    SparkSpillConfig spillConfig(GenerateSpillPath(), MAX_SPILL_BYTES, 1);
    OperatorConfig operatorConfig(spillConfig);

    auto operatorFactory = SortWithExprOperatorFactory::CreateSortWithExprOperatorFactory(sourceTypes, outputCols, 1,
        sortExprs, ascendings, nullFirsts, 1, operatorConfig);

    auto sortOperator = static_cast<SortWithExprOperator *>(CreateTestOperator(operatorFactory));
    sortOperator->AddInput(vecBatch1);
    sortOperator->AddInput(vecBatch2);
    sortOperator->AddInput(vecBatch3);
    VectorBatch *outputVecBatch = nullptr;
    sortOperator->GetOutput(&outputVecBatch);

    auto totalDataSize = dataSize * 3;
    int32_t expectData[] = {3, 6, 8};
    auto expectVecBatch = CreateVectorBatch(sourceTypes, totalDataSize, expectData);
    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expectVecBatch));

    Expr::DeleteExprs(sortExprs);
    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    delete operatorFactory;
}
}