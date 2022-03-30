/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: ...
 */
#include <vector>
#include "gtest/gtest.h"
#include "../../src/vector/vector_helper.h"
#include "../util/test_util.h"
#include "../../src/operator/join/sortmergejoin/sort_merge_join_expr.h"

using namespace omniruntime::op;
using namespace omniruntime::expressions;
using namespace std;

TEST(SMJ_JOIN_OPERATOR_WITH_EXPR_TESTCASE, testSmjExprOneTimeEqualCondition)
{
    // select t1.b, t2.c from t1, t2 where t1.a = t2.d
    // bufferedTbl t2: double c, int d;
    // streamedTbl t1:  int a, Long b;
    std::string blank = "";
    std::vector<DataType> streamTypeVector = { IntDataType::Instance(), LongDataType::Instance() };
    DataTypes streamedTblTypes(streamTypeVector);
    FieldExpr *col0 = new FieldExpr(0, IntType());
    std::vector<Expr *> streamedEqualKeyExprs = { col0 };
    int streamedOutputCols[1] = {1};
    StreamedTableWithExprOperatorFactory *streamedWithExprOperatorFactory =
        StreamedTableWithExprOperatorFactory::CreateStreamedTableWithExprOperatorFactory(streamedTblTypes,
        streamedEqualKeyExprs, 1, streamedOutputCols, 1, JoinType::OMNI_JOIN_TYPE_INNER, blank);
    streamedWithExprOperatorFactory->SetJitContext(nullptr);
    omniruntime::op::Operator *streamedTblWithExprOperator = CreateTestOperator(streamedWithExprOperatorFactory);

    std::vector<DataType> bufferTypesVector = { DoubleDataType::Instance(), IntDataType::Instance() };
    DataTypes bufferedTblTypes(bufferTypesVector);
    FieldExpr *col1 = new FieldExpr(1, IntType());
    std::vector<Expr *> bufferedEqualKeyExprs = { col1 };
    int bufferedOutputCols[1] = {0};
    int64_t streamedWithExprOperatorFactoryAddr = (int64_t)streamedWithExprOperatorFactory;
    BufferedTableWithExprOperatorFactory *bufferedWithExprOperatorFactory =
        BufferedTableWithExprOperatorFactory::CreateBufferedTableWithExprOperatorFactory(bufferedTblTypes,
        bufferedEqualKeyExprs, 1, bufferedOutputCols, 1, streamedWithExprOperatorFactoryAddr);
    bufferedWithExprOperatorFactory->SetJitContext(nullptr);
    omniruntime::op::Operator *bufferedTblWithExprOperator = CreateTestOperator(bufferedWithExprOperatorFactory);

    // construct data;
    const int32_t streamedTblDataSize = 6;
    int32_t streamedTblDataCol1[streamedTblDataSize] = {  0,   1,   2,   3,   4,   5};
    long streamedTblDataCol2[streamedTblDataSize] =  {6600, 5500, 4400, 3300, 2200, 1100};
    VectorBatch *streamedTblVecBatch1 =
        CreateVectorBatch(streamedTblTypes, streamedTblDataSize, streamedTblDataCol1, streamedTblDataCol2);

    int32_t addInputRetCode = -1;
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatch1);
    ASSERT_EQ(addInputRetCode, static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));


    const int32_t bufferedTblSize = 6;
    double bufferedTblDataCol1[bufferedTblSize] =  {6.6, 5.5, 4.4, 3.3, 2.2, 1.1};
    int32_t bufferedTblDataCol2[bufferedTblSize] =  {  0,   1,   2,   3,   4,   5};
    VectorBatch *bufferedTblVecBatch1 =
        CreateVectorBatch(bufferedTblTypes, bufferedTblSize, bufferedTblDataCol1, bufferedTblDataCol2);
    // need add buffered table data
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatch1);
    ASSERT_EQ(addInputRetCode, static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    // add eof flag to buffered table , need add streamed table data
    VectorBatch *bufferedTblVecBatchEof = createEmptyVectorBatch(bufferTypesVector);
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatchEof);
    ASSERT_EQ(addInputRetCode, static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_STREAM_TBL_DATA));

    // add eof flag to streamed table
    VectorBatch *streamedTblVecBatchEof = createEmptyVectorBatch(streamTypeVector);
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchEof);
    ASSERT_EQ(addInputRetCode, static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_FETCH_JOIN_DATA));

    std::vector<omniruntime::vec::VectorBatch *> result;
    streamedTblWithExprOperator->GetOutput(result);

    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchEof);
    ASSERT_EQ(addInputRetCode, static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NO_RESULT));

    // check the join result
    int32_t index = 0;
    for (auto i = 0; i < result.size(); i++) {
        ASSERT_EQ(result[i]->GetVectorCount(), 2);
        ASSERT_EQ(result[i]->GetVector(0)->GetTypeId(), OMNI_LONG);
        ASSERT_EQ(result[i]->GetVector(1)->GetTypeId(), OMNI_DOUBLE);
        for (auto j = 0; j < result[i]->GetRowCount(); j++) {
            long longValue = (static_cast<LongVector *>(result[i]->GetVector(0)))->GetValue(j);
            ASSERT_EQ(longValue, streamedTblDataCol2[index]);

            double doubleValue = (static_cast<DoubleVector *>(result[i]->GetVector(1)))->GetValue(j);
            ASSERT_EQ(doubleValue, bufferedTblDataCol1[index]);
            index++;
        }
        VectorHelper::FreeVecBatch(result[i]);
    }

    delete bufferedTblWithExprOperator;
    delete streamedTblWithExprOperator;
    DeleteOperatorFactory(bufferedWithExprOperatorFactory);
    DeleteOperatorFactory(streamedWithExprOperatorFactory);
}

TEST(SMJ_JOIN_OPERATOR_WITH_EXPR_TESTCASE, testSmj2EqualConditionMultiBatchInput)
{
    using namespace omniruntime::expressions;
    // select t1.b, t2.c from t1, t2 where t1.c11 + 1 = t2.c21 - 1 and t1.c13 = t2.c23 and t1.c12 > t2.c22
    // bufferedTbl t2:  int c21, Long c22, varchar c23;
    // streamedTbl t1:  int c11, Long c12, varchar c13;
    std::string blank = "";
    std::vector<DataType> streamTypeVector = { IntDataType(), LongDataType() };
    DataTypes streamedTblTypes(streamTypeVector);
    FieldExpr *col0 = new FieldExpr(0, IntType());
    std::vector<Expr *> streamedEqualKeyExprs = { col0 };

    int streamedOutputCols[1] = {1};
    StreamedTableWithExprOperatorFactory *streamedWithExprOperatorFactory =
        StreamedTableWithExprOperatorFactory::CreateStreamedTableWithExprOperatorFactory(streamedTblTypes,
        streamedEqualKeyExprs, 1, streamedOutputCols, 1, JoinType::OMNI_JOIN_TYPE_INNER, blank);
    streamedWithExprOperatorFactory->SetJitContext(nullptr);
    omniruntime::op::Operator *streamedTblWithExprOperator = CreateTestOperator(streamedWithExprOperatorFactory);

    std::vector<DataType> bufferTypesVector = { DoubleDataType::Instance(), IntDataType::Instance() };
    DataTypes bufferedTblTypes(bufferTypesVector);
    FieldExpr *col1 = new FieldExpr(1, IntType());
    std::vector<Expr *> bufferedEqualKeyExprs = { col1 };
    int bufferedOutputCols[1] = {0};
    int64_t streamedWithExprOperatorFactoryAddr = (int64_t)streamedWithExprOperatorFactory;
    BufferedTableWithExprOperatorFactory *bufferedWithExprOperatorFactory =
        BufferedTableWithExprOperatorFactory::CreateBufferedTableWithExprOperatorFactory(bufferedTblTypes,
        bufferedEqualKeyExprs, 1, bufferedOutputCols, 1, streamedWithExprOperatorFactoryAddr);
    bufferedWithExprOperatorFactory->SetJitContext(nullptr);
    omniruntime::op::Operator *bufferedTblWithExprOperator = CreateTestOperator(bufferedWithExprOperatorFactory);

    // construct data;
    const int32_t streamedTblDataSize = 6;
    int32_t streamedTblDataCol1[streamedTblDataSize] = {  0,   1,   2,   3,   4,   5};
    long streamedTblDataCol2[streamedTblDataSize] =  {6600, 5500, 4400, 3300, 2200, 1100};
    VectorBatch *streamedTblVecBatch1 =
        CreateVectorBatch(streamedTblTypes, streamedTblDataSize, streamedTblDataCol1, streamedTblDataCol2);

    auto addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatch1);
    ASSERT_EQ(addInputRetCode, static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    const int32_t bufferedTblSize = 6;
    double bufferedTblDataCol1[bufferedTblSize] =  {6.6, 5.5, 4.4, 3.3, 2.2, 1.1};
    int32_t bufferedTblDataCol2[bufferedTblSize] =  {  0,   1,   2,   3,   4,   5};
    VectorBatch *bufferedTblVecBatch1 =
        CreateVectorBatch(bufferedTblTypes, bufferedTblSize, bufferedTblDataCol1, bufferedTblDataCol2);
    // need add buffered table data
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatch1);
    ASSERT_EQ(addInputRetCode, static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    // add eof flag to buffered table , need add streamed table data
    VectorBatch *bufferedTblVecBatchEof = createEmptyVectorBatch(bufferTypesVector);
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatchEof);
    ASSERT_EQ(addInputRetCode, static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_STREAM_TBL_DATA));

    // add eof flag to streamed table
    VectorBatch *streamedTblVecBatchEof = createEmptyVectorBatch(streamTypeVector);
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchEof);
    ASSERT_EQ(addInputRetCode, static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_FETCH_JOIN_DATA));

    std::vector<omniruntime::vec::VectorBatch *> result;
    streamedTblWithExprOperator->GetOutput(result);

    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchEof);
    ASSERT_EQ(addInputRetCode, static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NO_RESULT));

    // check the join result
    int32_t index = 0;
    for (auto i = 0; i < result.size(); i++) {
        ASSERT_EQ(result[i]->GetVectorCount(), 2);
        ASSERT_EQ(result[i]->GetVector(0)->GetTypeId(), OMNI_LONG);
        ASSERT_EQ(result[i]->GetVector(1)->GetTypeId(), OMNI_DOUBLE);
        for (auto j = 0; j < result[i]->GetRowCount(); j++) {
            long longValue = (static_cast<LongVector *>(result[i]->GetVector(0)))->GetValue(j);
            ASSERT_EQ(longValue, streamedTblDataCol2[index]);

            double doubleValue = (static_cast<DoubleVector *>(result[i]->GetVector(1)))->GetValue(j);
            ASSERT_EQ(doubleValue, bufferedTblDataCol1[index]);
            index++;
        }
        VectorHelper::FreeVecBatch(result[i]);
    }

    delete bufferedTblWithExprOperator;
    delete streamedTblWithExprOperator;
    DeleteOperatorFactory(bufferedWithExprOperatorFactory);
    DeleteOperatorFactory(streamedWithExprOperatorFactory);
}