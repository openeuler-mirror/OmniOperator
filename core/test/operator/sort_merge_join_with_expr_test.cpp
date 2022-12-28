/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: ...
 */
#include <vector>
#include "gtest/gtest.h"
#include "vector/vector_helper.h"
#include "operator/join/sortmergejoin/sort_merge_join_expr.h"
#include "util/test_util.h"

using namespace omniruntime::op;
using namespace omniruntime::expressions;
using namespace std;
using namespace TestUtil;

namespace SortMergeJoinWithExprTest {
TEST(SMJ_JOIN_OPERATOR_WITH_EXPR_TESTCASE, testSmjInnerJoinExprGreaterThanCondition)
{
    // select t1.b, t2.c from t1 full join t2 where t1.a = t2.d and t1.a > 2;
    // bufferedTbl t2: double c, int d;
    // streamedTbl t1:  int a, Long b;
    string filterJsonStr = "{\"exprType\":\"BINARY\","
        "\"returnType\":4,"
        "\"operator\":\"GREATER_THAN\","
        "\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":0},"
        "\"right\":{\"exprType\":\"LITERAL\",\"dataType\":1, \"isNull\":false, \"value\":2}}";
    std::vector<DataTypePtr> streamTypeVector = { IntType(), LongType() };
    DataTypes streamedTblTypes(streamTypeVector);
    FieldExpr *col0 = new FieldExpr(0, IntType());
    std::vector<Expr *> streamedEqualKeyExprs = { col0 };
    int streamedOutputCols[1] = {1};
    auto overflowConfig = new OverflowConfig();
    StreamedTableWithExprOperatorFactory *streamedWithExprOperatorFactory =
        StreamedTableWithExprOperatorFactory::CreateStreamedTableWithExprOperatorFactory(streamedTblTypes,
        streamedEqualKeyExprs, 1, streamedOutputCols, 1, JoinType::OMNI_JOIN_TYPE_INNER, filterJsonStr, overflowConfig);
    omniruntime::op::Operator *streamedTblWithExprOperator = CreateTestOperator(streamedWithExprOperatorFactory);

    std::vector<DataTypePtr> bufferTypesVector = { DoubleDataType::Instance(), IntType() };
    DataTypes bufferedTblTypes(bufferTypesVector);
    FieldExpr *col1 = new FieldExpr(1, IntType());
    std::vector<Expr *> bufferedEqualKeyExprs = { col1 };
    int bufferedOutputCols[1] = {0};
    int64_t streamedWithExprOperatorFactoryAddr = reinterpret_cast<int64_t>(streamedWithExprOperatorFactory);
    BufferedTableWithExprOperatorFactory *bufferedWithExprOperatorFactory =
        BufferedTableWithExprOperatorFactory::CreateBufferedTableWithExprOperatorFactory(bufferedTblTypes,
        bufferedEqualKeyExprs, 1, bufferedOutputCols, 1, streamedWithExprOperatorFactoryAddr, overflowConfig);
    omniruntime::op::Operator *bufferedTblWithExprOperator = CreateTestOperator(bufferedWithExprOperatorFactory);

    // construct data
    const int32_t streamedTblDataSize = 6;
    int32_t streamedTblDataCol1[streamedTblDataSize] = {  0,   1,   2,   3,   4,   5};
    long streamedTblDataCol2[streamedTblDataSize] =  {6600, 5500, 4400, 3300, 2200, 1100};
    VectorBatch *streamedTblVecBatch1 =
        CreateVectorBatch(streamedTblTypes, streamedTblDataSize, streamedTblDataCol1, streamedTblDataCol2);

    int32_t addInputRetCode = -1;
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatch1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    const int32_t bufferedTblSize = 6;
    double bufferedTblDataCol1[bufferedTblSize] =  {6.6, 5.5, 4.4, 3.3, 2.2, 1.1};
    int32_t bufferedTblDataCol2[bufferedTblSize] =  {  0,   1,   2,   3,   4,   5};
    VectorBatch *bufferedTblVecBatch1 =
        CreateVectorBatch(bufferedTblTypes, bufferedTblSize, bufferedTblDataCol1, bufferedTblDataCol2);
    // need add buffered table data
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatch1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    // add eof flag to buffered table , need add streamed table data
    VectorBatch *bufferedTblVecBatchEof = CreateEmptyVectorBatch(bufferTypesVector);
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatchEof);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_STREAM_TBL_DATA));

    // add eof flag to streamed table
    VectorBatch *streamedTblVecBatchEof = CreateEmptyVectorBatch(streamTypeVector);
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchEof);
    ASSERT_EQ(DecodeFetchFlag(addInputRetCode), static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_FETCH_JOIN_DATA));

    std::vector<omniruntime::vec::VectorBatch *> result;
    streamedTblWithExprOperator->GetOutput(result);

    auto streamedTblVecBatchEof1 = CreateEmptyVectorBatch(streamTypeVector);
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchEof1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode), static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_SCAN_FINISH));

    // check the join result
    long resultCol1[] =  { 3300, 2200, 1100};
    double resultCol2[] =  { 3.3, 2.2, 1.1};
    int32_t index = 0;
    for (uint32_t i = 0; i < result.size(); i++) {
        ASSERT_EQ(result[i]->GetVectorCount(), 2);
        ASSERT_EQ(result[i]->GetVector(0)->GetTypeId(), OMNI_LONG);
        ASSERT_EQ(result[i]->GetVector(1)->GetTypeId(), OMNI_DOUBLE);
        for (auto j = 0; j < result[i]->GetRowCount(); j++) {
            long longValue = (static_cast<LongVector *>(result[i]->GetVector(0)))->GetValue(j);
            ASSERT_EQ(longValue, resultCol1[index]);

            double doubleValue = (static_cast<DoubleVector *>(result[i]->GetVector(1)))->GetValue(j);
            ASSERT_EQ(doubleValue, resultCol2[index]);
            index++;
        }
        VectorHelper::FreeVecBatch(result[i]);
    }

    Expr::DeleteExprs(streamedEqualKeyExprs);
    Expr::DeleteExprs(bufferedEqualKeyExprs);
    omniruntime::op::Operator::DeleteOperator(bufferedTblWithExprOperator);
    omniruntime::op::Operator::DeleteOperator(streamedTblWithExprOperator);
    DeleteOperatorFactory(bufferedWithExprOperatorFactory);
    DeleteOperatorFactory(streamedWithExprOperatorFactory);
    delete overflowConfig;
}

TEST(SMJ_JOIN_OPERATOR_WITH_EXPR_TESTCASE, testSmjInnerJoinExprEqualCondition)
{
    using namespace omniruntime::expressions;
    // select t1.b, t2.c from t1 full join t2 where t1.a = t2.d and t1.a = 4;
    // bufferedTbl t2: double c, int d;
    // streamedTbl t1:  int a, Long b;
    string filterJsonStr = "{\"exprType\":\"BINARY\","
        "\"returnType\":4,"
        "\"operator\":\"EQUAL\","
        "\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":0},"
        "\"right\":{\"exprType\":\"LITERAL\",\"dataType\":1, \"isNull\":false, \"value\":4}}";
    std::vector<DataTypePtr> streamTypeVector = { IntType(), LongType() };
    DataTypes streamedTblTypes(streamTypeVector);
    FieldExpr *col0 = new FieldExpr(0, IntType());
    std::vector<Expr *> streamedEqualKeyExprs = { col0 };

    int streamedOutputCols[1] = {1};
    auto overflowConfig = new OverflowConfig();
    StreamedTableWithExprOperatorFactory *streamedWithExprOperatorFactory =
        StreamedTableWithExprOperatorFactory::CreateStreamedTableWithExprOperatorFactory(streamedTblTypes,
        streamedEqualKeyExprs, 1, streamedOutputCols, 1, JoinType::OMNI_JOIN_TYPE_INNER, filterJsonStr, overflowConfig);
    omniruntime::op::Operator *streamedTblWithExprOperator = CreateTestOperator(streamedWithExprOperatorFactory);

    std::vector<DataTypePtr> bufferTypesVector = { DoubleDataType::Instance(), IntType() };
    DataTypes bufferedTblTypes(bufferTypesVector);
    FieldExpr *col1 = new FieldExpr(1, IntType());
    std::vector<Expr *> bufferedEqualKeyExprs = { col1 };
    int bufferedOutputCols[1] = {0};
    int64_t streamedWithExprOperatorFactoryAddr = reinterpret_cast<int64_t>(streamedWithExprOperatorFactory);
    BufferedTableWithExprOperatorFactory *bufferedWithExprOperatorFactory =
        BufferedTableWithExprOperatorFactory::CreateBufferedTableWithExprOperatorFactory(bufferedTblTypes,
        bufferedEqualKeyExprs, 1, bufferedOutputCols, 1, streamedWithExprOperatorFactoryAddr, overflowConfig);
    omniruntime::op::Operator *bufferedTblWithExprOperator = CreateTestOperator(bufferedWithExprOperatorFactory);

    // construct data
    const int32_t streamedTblDataSize = 6;
    int32_t streamedTblDataCol1[streamedTblDataSize] = {  0,   1,   2,   3,   4,   5};
    long streamedTblDataCol2[streamedTblDataSize] =  {6600, 5500, 4400, 3300, 2200, 1100};
    VectorBatch *streamedTblVecBatch1 =
        CreateVectorBatch(streamedTblTypes, streamedTblDataSize, streamedTblDataCol1, streamedTblDataCol2);

    auto addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatch1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    const int32_t bufferedTblSize = 6;
    double bufferedTblDataCol1[bufferedTblSize] =  {6.6, 5.5, 4.4, 3.3, 2.2, 1.1};
    int32_t bufferedTblDataCol2[bufferedTblSize] =  {  0,   1,   2,   3,   4,   5};
    VectorBatch *bufferedTblVecBatch1 =
        CreateVectorBatch(bufferedTblTypes, bufferedTblSize, bufferedTblDataCol1, bufferedTblDataCol2);
    // need add buffered table data
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatch1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    // add eof flag to buffered table , need add streamed table data
    VectorBatch *bufferedTblVecBatchEof = CreateEmptyVectorBatch(bufferTypesVector);
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatchEof);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_STREAM_TBL_DATA));

    // add eof flag to streamed table
    VectorBatch *streamedTblVecBatchEof = CreateEmptyVectorBatch(streamTypeVector);
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchEof);
    ASSERT_EQ(DecodeFetchFlag(addInputRetCode), static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_FETCH_JOIN_DATA));

    std::vector<omniruntime::vec::VectorBatch *> result;
    streamedTblWithExprOperator->GetOutput(result);

    auto streamedTblVecBatchEof1 = CreateEmptyVectorBatch(streamTypeVector);
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchEof1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode), static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_SCAN_FINISH));

    // check the join result
    long resultCol1[] = {2200};
    double resultCol2[] = {2.2};
    int32_t index = 0;
    for (uint32_t i = 0; i < result.size(); i++) {
        ASSERT_EQ(result[i]->GetVectorCount(), 2);
        ASSERT_EQ(result[i]->GetVector(0)->GetTypeId(), OMNI_LONG);
        ASSERT_EQ(result[i]->GetVector(1)->GetTypeId(), OMNI_DOUBLE);
        for (auto j = 0; j < result[i]->GetRowCount(); j++) {
            long longValue = (static_cast<LongVector *>(result[i]->GetVector(0)))->GetValue(j);
            ASSERT_EQ(longValue, resultCol1[index]);

            double doubleValue = (static_cast<DoubleVector *>(result[i]->GetVector(1)))->GetValue(j);
            ASSERT_EQ(doubleValue, resultCol2[index]);
            index++;
        }
        VectorHelper::FreeVecBatch(result[i]);
    }

    Expr::DeleteExprs(streamedEqualKeyExprs);
    Expr::DeleteExprs(bufferedEqualKeyExprs);
    omniruntime::op::Operator::DeleteOperator(bufferedTblWithExprOperator);
    omniruntime::op::Operator::DeleteOperator(streamedTblWithExprOperator);
    DeleteOperatorFactory(bufferedWithExprOperatorFactory);
    DeleteOperatorFactory(streamedWithExprOperatorFactory);
    delete overflowConfig;
}

TEST(SMJ_JOIN_OPERATOR_WITH_EXPR_TESTCASE, testSmjLeftJoinstreamedGreaterThenBuffered)
{
    // select t1.b, t2.c from t1 left join t2 where t1.a = t2.d
    // bufferedTbl t2: double c, int d;
    // streamedTbl t1:  int a, Long b;
    std::string blank = "";
    std::vector<DataTypePtr> streamTypeVector = { IntType(), LongType() };
    DataTypes streamedTblTypes(streamTypeVector);
    FieldExpr *col0 = new FieldExpr(0, IntType());
    std::vector<Expr *> streamedEqualKeyExprs = { col0 };
    auto overflowConfig = new OverflowConfig();
    int streamedOutputCols[1] = {1};
    StreamedTableWithExprOperatorFactory *streamedWithExprOperatorFactory =
        StreamedTableWithExprOperatorFactory::CreateStreamedTableWithExprOperatorFactory(streamedTblTypes,
        streamedEqualKeyExprs, 1, streamedOutputCols, 1, JoinType::OMNI_JOIN_TYPE_LEFT, blank, overflowConfig);
    omniruntime::op::Operator *streamedTblWithExprOperator = CreateTestOperator(streamedWithExprOperatorFactory);

    std::vector<DataTypePtr> bufferTypesVector = { DoubleDataType::Instance(), IntType() };
    DataTypes bufferedTblTypes(bufferTypesVector);
    FieldExpr *col1 = new FieldExpr(1, IntType());
    std::vector<Expr *> bufferedEqualKeyExprs = { col1 };
    int bufferedOutputCols[1] = {0};
    int64_t streamedWithExprOperatorFactoryAddr = reinterpret_cast<int64_t>(streamedWithExprOperatorFactory);
    BufferedTableWithExprOperatorFactory *bufferedWithExprOperatorFactory =
        BufferedTableWithExprOperatorFactory::CreateBufferedTableWithExprOperatorFactory(bufferedTblTypes,
        bufferedEqualKeyExprs, 1, bufferedOutputCols, 1, streamedWithExprOperatorFactoryAddr, overflowConfig);
    omniruntime::op::Operator *bufferedTblWithExprOperator = CreateTestOperator(bufferedWithExprOperatorFactory);

    // construct data
    const int32_t streamedTblDataSize = 6;
    int32_t streamedTblDataCol1[streamedTblDataSize] = {1,   2,   3,   4,   5,   8};
    long streamedTblDataCol2[streamedTblDataSize] =  {1100, 2200, 3300, 4400, 5500, 8800};
    VectorBatch *streamedTblVecBatch1 =
        CreateVectorBatch(streamedTblTypes, streamedTblDataSize, streamedTblDataCol1, streamedTblDataCol2);

    int32_t addInputRetCode = -1;
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatch1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    const int32_t bufferedTblSize = 6;
    double bufferedTblDataCol1[bufferedTblSize] =  {1.1, 2.2, 3.3, 4.4, 5.5, 6.6};
    int32_t bufferedTblDataCol2[bufferedTblSize] =  {1,   2,   3,   4,   5,   6};
    VectorBatch *bufferedTblVecBatch1 =
        CreateVectorBatch(bufferedTblTypes, bufferedTblSize, bufferedTblDataCol1, bufferedTblDataCol2);
    // need add buffered table data
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatch1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    // add eof flag to buffered table , need add streamed table data
    VectorBatch *bufferedTblVecBatchEof = CreateEmptyVectorBatch(bufferTypesVector);
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatchEof);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_STREAM_TBL_DATA));

    // add eof flag to streamed table
    VectorBatch *streamedTblVecBatchEof = CreateEmptyVectorBatch(streamTypeVector);
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchEof);
    ASSERT_EQ(DecodeFetchFlag(addInputRetCode), static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_FETCH_JOIN_DATA));

    std::vector<omniruntime::vec::VectorBatch *> result;
    streamedTblWithExprOperator->GetOutput(result);

    auto streamedTblVecBatchEof1 = CreateEmptyVectorBatch(streamTypeVector);
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchEof1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode), static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_SCAN_FINISH));

    // check the join result
    long resultCol1[] =  {1100, 2200, 3300, 4400, 5500, 8800};
    double resultCol2[] =  {1.1, 2.2, 3.3, 4.4, 5.5, 0};
    int32_t index = 0;
    for (uint32_t i = 0; i < result.size(); i++) {
        ASSERT_EQ(result[i]->GetVectorCount(), 2);
        ASSERT_EQ(result[i]->GetVector(0)->GetTypeId(), OMNI_LONG);
        ASSERT_EQ(result[i]->GetVector(1)->GetTypeId(), OMNI_DOUBLE);
        ASSERT_EQ(result[i]->GetRowCount(), sizeof(resultCol1) / sizeof(resultCol1[0]));
        for (auto j = 0; j < result[i]->GetRowCount(); j++) {
            if (resultCol1[index] != 0) {
                long longValue = (static_cast<LongVector *>(result[i]->GetVector(0)))->GetValue(j);
                ASSERT_EQ(longValue, resultCol1[index]);
            } else {
                ASSERT_EQ((static_cast<LongVector *>(result[i]->GetVector(0)))->IsValueNull(j), true);
            }
            if (resultCol2[index] != 0) {
                double doubleValue = (static_cast<DoubleVector *>(result[i]->GetVector(1)))->GetValue(j);
                ASSERT_EQ(doubleValue, resultCol2[index]);
            } else {
                ASSERT_EQ((static_cast<DoubleVector *>(result[i]->GetVector(1)))->IsValueNull(j), true);
            }
            index++;
        }
        VectorHelper::FreeVecBatch(result[i]);
    }

    Expr::DeleteExprs(streamedEqualKeyExprs);
    Expr::DeleteExprs(bufferedEqualKeyExprs);
    omniruntime::op::Operator::DeleteOperator(bufferedTblWithExprOperator);
    omniruntime::op::Operator::DeleteOperator(streamedTblWithExprOperator);
    DeleteOperatorFactory(bufferedWithExprOperatorFactory);
    DeleteOperatorFactory(streamedWithExprOperatorFactory);
    delete overflowConfig;
}

TEST(SMJ_JOIN_OPERATOR_WITH_EXPR_TESTCASE, testSmjLeftJoinstreamedLessThenBuffered)
{
    // select t1.b, t2.c from t1 left join t2 where t1.a = t2.d
    // bufferedTbl t2: double c, int d;
    // streamedTbl t1:  int a, Long b;
    std::string blank = "";
    std::vector<DataTypePtr> streamTypeVector = { IntType(), LongType() };
    DataTypes streamedTblTypes(streamTypeVector);
    FieldExpr *col0 = new FieldExpr(0, IntType());
    std::vector<Expr *> streamedEqualKeyExprs = { col0 };
    auto overflowConfig = new OverflowConfig();
    int streamedOutputCols[1] = {1};
    StreamedTableWithExprOperatorFactory *streamedWithExprOperatorFactory =
        StreamedTableWithExprOperatorFactory::CreateStreamedTableWithExprOperatorFactory(streamedTblTypes,
        streamedEqualKeyExprs, 1, streamedOutputCols, 1, JoinType::OMNI_JOIN_TYPE_LEFT, blank, overflowConfig);
    omniruntime::op::Operator *streamedTblWithExprOperator = CreateTestOperator(streamedWithExprOperatorFactory);

    std::vector<DataTypePtr> bufferTypesVector = { DoubleDataType::Instance(), IntType() };
    DataTypes bufferedTblTypes(bufferTypesVector);
    FieldExpr *col1 = new FieldExpr(1, IntType());
    std::vector<Expr *> bufferedEqualKeyExprs = { col1 };
    int bufferedOutputCols[1] = {0};
    int64_t streamedWithExprOperatorFactoryAddr = reinterpret_cast<int64_t>(streamedWithExprOperatorFactory);
    BufferedTableWithExprOperatorFactory *bufferedWithExprOperatorFactory =
        BufferedTableWithExprOperatorFactory::CreateBufferedTableWithExprOperatorFactory(bufferedTblTypes,
        bufferedEqualKeyExprs, 1, bufferedOutputCols, 1, streamedWithExprOperatorFactoryAddr, overflowConfig);
    omniruntime::op::Operator *bufferedTblWithExprOperator = CreateTestOperator(bufferedWithExprOperatorFactory);

    // construct data
    const int32_t streamedTblDataSize = 5;
    int32_t streamedTblDataCol1[streamedTblDataSize] = {1,   2,   3,   4,   5};
    long streamedTblDataCol2[streamedTblDataSize] =  {1100, 2200, 3300, 4400, 5500};
    VectorBatch *streamedTblVecBatch1 =
        CreateVectorBatch(streamedTblTypes, streamedTblDataSize, streamedTblDataCol1, streamedTblDataCol2);

    int32_t addInputRetCode = -1;
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatch1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    const int32_t bufferedTblSize = 5;
    double bufferedTblDataCol1[bufferedTblSize] =  {1.1, 2.2, 3.3, 4.4, 8.8};
    int32_t bufferedTblDataCol2[bufferedTblSize] =  {1,   2,   3,   4,   8};
    VectorBatch *bufferedTblVecBatch1 =
        CreateVectorBatch(bufferedTblTypes, bufferedTblSize, bufferedTblDataCol1, bufferedTblDataCol2);
    // need add buffered table data
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatch1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_STREAM_TBL_DATA));

    // add eof flag to streamed table
    VectorBatch *streamedTblVecBatchEof = CreateEmptyVectorBatch(streamTypeVector);
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchEof);
    ASSERT_EQ(DecodeFetchFlag(addInputRetCode), static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_FETCH_JOIN_DATA));

    std::vector<omniruntime::vec::VectorBatch *> result;
    streamedTblWithExprOperator->GetOutput(result);

    auto streamedTblVecBatchEof1 = CreateEmptyVectorBatch(streamTypeVector);
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchEof1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode), static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_SCAN_FINISH));

    // check the join result
    long resultCol1[] =  {1100, 2200, 3300, 4400, 5500};
    double resultCol2[] =  {1.1, 2.2, 3.3, 4.4, 0};
    int32_t index = 0;
    for (uint32_t i = 0; i < result.size(); i++) {
        ASSERT_EQ(result[i]->GetVectorCount(), 2);
        ASSERT_EQ(result[i]->GetVector(0)->GetTypeId(), OMNI_LONG);
        ASSERT_EQ(result[i]->GetVector(1)->GetTypeId(), OMNI_DOUBLE);
        ASSERT_EQ(result[i]->GetRowCount(), sizeof(resultCol1) / sizeof(resultCol1[0]));
        for (auto j = 0; j < result[i]->GetRowCount(); j++) {
            if (resultCol1[index] != 0) {
                long longValue = (static_cast<LongVector *>(result[i]->GetVector(0)))->GetValue(j);
                ASSERT_EQ(longValue, resultCol1[index]);
            } else {
                ASSERT_EQ((static_cast<LongVector *>(result[i]->GetVector(0)))->IsValueNull(j), true);
            }
            if (resultCol2[index] != 0) {
                double doubleValue = (static_cast<DoubleVector *>(result[i]->GetVector(1)))->GetValue(j);
                ASSERT_EQ(doubleValue, resultCol2[index]);
            } else {
                ASSERT_EQ((static_cast<DoubleVector *>(result[i]->GetVector(1)))->IsValueNull(j), true);
            }
            index++;
        }
        VectorHelper::FreeVecBatch(result[i]);
    }

    Expr::DeleteExprs(streamedEqualKeyExprs);
    Expr::DeleteExprs(bufferedEqualKeyExprs);
    omniruntime::op::Operator::DeleteOperator(bufferedTblWithExprOperator);
    omniruntime::op::Operator::DeleteOperator(streamedTblWithExprOperator);
    DeleteOperatorFactory(bufferedWithExprOperatorFactory);
    DeleteOperatorFactory(streamedWithExprOperatorFactory);
    delete overflowConfig;
}

TEST(SMJ_JOIN_OPERATOR_WITH_EXPR_TESTCASE, testSmjLeftJoinMixGreaterLessThenBuffered)
{
    // select t1.b, t2.c from t1 left join t2 where t1.a = t2.d
    // bufferedTbl t2: double c, int d;
    // streamedTbl t1:  int a, Long b;
    std::string blank = "";
    std::vector<DataTypePtr> streamTypeVector = { IntType(), LongType() };
    DataTypes streamedTblTypes(streamTypeVector);
    FieldExpr *col0 = new FieldExpr(0, IntType());
    std::vector<Expr *> streamedEqualKeyExprs = { col0 };
    auto overflowConfig = new OverflowConfig();
    int streamedOutputCols[1] = {1};
    StreamedTableWithExprOperatorFactory *streamedWithExprOperatorFactory =
        StreamedTableWithExprOperatorFactory::CreateStreamedTableWithExprOperatorFactory(streamedTblTypes,
        streamedEqualKeyExprs, 1, streamedOutputCols, 1, JoinType::OMNI_JOIN_TYPE_LEFT, blank, overflowConfig);
    omniruntime::op::Operator *streamedTblWithExprOperator = CreateTestOperator(streamedWithExprOperatorFactory);

    std::vector<DataTypePtr> bufferTypesVector = { DoubleType(), IntType() };
    DataTypes bufferedTblTypes(bufferTypesVector);
    FieldExpr *col1 = new FieldExpr(1, IntType());
    std::vector<Expr *> bufferedEqualKeyExprs = { col1 };
    int bufferedOutputCols[1] = {0};
    int64_t streamedWithExprOperatorFactoryAddr = reinterpret_cast<int64_t>(streamedWithExprOperatorFactory);
    BufferedTableWithExprOperatorFactory *bufferedWithExprOperatorFactory =
        BufferedTableWithExprOperatorFactory::CreateBufferedTableWithExprOperatorFactory(bufferedTblTypes,
        bufferedEqualKeyExprs, 1, bufferedOutputCols, 1, streamedWithExprOperatorFactoryAddr, overflowConfig);
    omniruntime::op::Operator *bufferedTblWithExprOperator = CreateTestOperator(bufferedWithExprOperatorFactory);

    // construct data
    const int32_t streamedTblDataSize = 5;
    int32_t streamedTblDataCol1[streamedTblDataSize] = {1,   3,   3,   4,   5};
    long streamedTblDataCol2[streamedTblDataSize] =  {1100, 3300, 3300, 4400, 5500};
    VectorBatch *streamedTblVecBatch1 =
        CreateVectorBatch(streamedTblTypes, streamedTblDataSize, streamedTblDataCol1, streamedTblDataCol2);

    int32_t addInputRetCode = -1;
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatch1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    const int32_t bufferedTblSize = 5;
    double bufferedTblDataCol1[bufferedTblSize] =  {1.1, 2.2, 4.4, 4.4, 8.8};
    int32_t bufferedTblDataCol2[bufferedTblSize] =  { 1,   2,   4,   4,   8};
    VectorBatch *bufferedTblVecBatch1 =
        CreateVectorBatch(bufferedTblTypes, bufferedTblSize, bufferedTblDataCol1, bufferedTblDataCol2);
    // need add buffered table data
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatch1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_STREAM_TBL_DATA));

    // add eof flag to streamed table
    VectorBatch *streamedTblVecBatchEof = CreateEmptyVectorBatch(streamTypeVector);
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchEof);
    ASSERT_EQ(DecodeFetchFlag(addInputRetCode), static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_FETCH_JOIN_DATA));

    std::vector<omniruntime::vec::VectorBatch *> result;
    streamedTblWithExprOperator->GetOutput(result);

    auto streamedTblVecBatchEof1 = CreateEmptyVectorBatch(streamTypeVector);
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchEof1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode), static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_SCAN_FINISH));

    // check the join result
    long resultCol1[] =  {1100, 3300, 3300, 4400, 4400, 5500};
    double resultCol2[] =  {1.1, 0, 0, 4.4, 4.4, 0};
    int32_t index = 0;
    for (uint32_t i = 0; i < result.size(); i++) {
        ASSERT_EQ(result[i]->GetVectorCount(), 2);
        ASSERT_EQ(result[i]->GetVector(0)->GetTypeId(), OMNI_LONG);
        ASSERT_EQ(result[i]->GetVector(1)->GetTypeId(), OMNI_DOUBLE);
        ASSERT_EQ(result[i]->GetRowCount(), sizeof(resultCol1) / sizeof(resultCol1[0]));
        for (auto j = 0; j < result[i]->GetRowCount(); j++) {
            if (resultCol1[index] != 0) {
                long longValue = (static_cast<LongVector *>(result[i]->GetVector(0)))->GetValue(j);
                ASSERT_EQ(longValue, resultCol1[index]);
            } else {
                ASSERT_EQ((static_cast<LongVector *>(result[i]->GetVector(0)))->IsValueNull(j), true);
            }
            if (resultCol2[index] != 0) {
                double doubleValue = (static_cast<DoubleVector *>(result[i]->GetVector(1)))->GetValue(j);
                ASSERT_EQ(doubleValue, resultCol2[index]);
            } else {
                ASSERT_EQ((static_cast<DoubleVector *>(result[i]->GetVector(1)))->IsValueNull(j), true);
            }
            index++;
        }
        VectorHelper::FreeVecBatch(result[i]);
    }

    Expr::DeleteExprs(streamedEqualKeyExprs);
    Expr::DeleteExprs(bufferedEqualKeyExprs);
    omniruntime::op::Operator::DeleteOperator(bufferedTblWithExprOperator);
    omniruntime::op::Operator::DeleteOperator(streamedTblWithExprOperator);
    DeleteOperatorFactory(bufferedWithExprOperatorFactory);
    DeleteOperatorFactory(streamedWithExprOperatorFactory);
    delete overflowConfig;
}

TEST(SMJ_JOIN_OPERATOR_WITH_EXPR_TESTCASE, testSmjLeftJoinStreamedWithNullJoinKeyFirst)
{
    // select t1.b, t2.c from t1 left join t2 where t1.a = t2.d
    // bufferedTbl t2: double c, int d;
    // streamedTbl t1:  int a, Long b;
    std::string blank = "";
    std::vector<DataTypePtr> streamTypeVector = { IntType(), LongType() };
    DataTypes streamedTblTypes(streamTypeVector);
    FieldExpr *col0 = new FieldExpr(0, IntType());
    std::vector<Expr *> streamedEqualKeyExprs = { col0 };
    auto overflowConfig = new OverflowConfig();
    int streamedOutputCols[1] = {1};
    StreamedTableWithExprOperatorFactory *streamedWithExprOperatorFactory =
        StreamedTableWithExprOperatorFactory::CreateStreamedTableWithExprOperatorFactory(streamedTblTypes,
        streamedEqualKeyExprs, 1, streamedOutputCols, 1, JoinType::OMNI_JOIN_TYPE_LEFT, blank, overflowConfig);
    omniruntime::op::Operator *streamedTblWithExprOperator = CreateTestOperator(streamedWithExprOperatorFactory);

    std::vector<DataTypePtr> bufferTypesVector = { DoubleType(), IntType() };
    DataTypes bufferedTblTypes(bufferTypesVector);
    FieldExpr *col1 = new FieldExpr(1, IntType());
    std::vector<Expr *> bufferedEqualKeyExprs = { col1 };
    int bufferedOutputCols[1] = {0};
    int64_t streamedWithExprOperatorFactoryAddr = reinterpret_cast<int64_t>(streamedWithExprOperatorFactory);
    BufferedTableWithExprOperatorFactory *bufferedWithExprOperatorFactory =
        BufferedTableWithExprOperatorFactory::CreateBufferedTableWithExprOperatorFactory(bufferedTblTypes,
        bufferedEqualKeyExprs, 1, bufferedOutputCols, 1, streamedWithExprOperatorFactoryAddr, overflowConfig);
    omniruntime::op::Operator *bufferedTblWithExprOperator = CreateTestOperator(bufferedWithExprOperatorFactory);

    // construct data
    const int32_t streamedTblDataSize = 7;
    int32_t streamedTblDataCol1[streamedTblDataSize] = {1, 2, 3, 4, 5, 6, 7};
    long streamedTblDataCol2[streamedTblDataSize] =  {1100, 2200, 3300, 4400, 5500, 6600, 7700};
    VectorBatch *streamedTblVecBatch1 =
        CreateVectorBatch(streamedTblTypes, streamedTblDataSize, streamedTblDataCol1, streamedTblDataCol2);
    streamedTblVecBatch1->GetVector(0)->SetValueNull(0); // NULL, NULL, 4, 5, 6, 7
    streamedTblVecBatch1->GetVector(0)->SetValueNull(1); // NULL, NULL, 4, 5, 6, 7

    int32_t addInputRetCode = -1;
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatch1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    const int32_t bufferedTblSize = 6;
    double bufferedTblDataCol1[bufferedTblSize] =  {1.1, 2.2, 3.3, 4.4, 6.6, 8.8};
    int32_t bufferedTblDataCol2[bufferedTblSize] =  {1, 2, 3, 4, 6, 8};
    VectorBatch *bufferedTblVecBatch1 =
        CreateVectorBatch(bufferedTblTypes, bufferedTblSize, bufferedTblDataCol1, bufferedTblDataCol2);
    bufferedTblVecBatch1->GetVector(1)->SetValueNull(0); // NULL, NULL, 3, 4, 6, 8
    bufferedTblVecBatch1->GetVector(1)->SetValueNull(1); // NULL, NULL, 3, 4, 6, 8

    // need add buffered table data
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatch1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_STREAM_TBL_DATA));

    // add eof flag to streamed table
    VectorBatch *streamedTblVecBatchEof = CreateEmptyVectorBatch(streamTypeVector);
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchEof);
    ASSERT_EQ(DecodeFetchFlag(addInputRetCode), static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_FETCH_JOIN_DATA));

    std::vector<omniruntime::vec::VectorBatch *> result;
    streamedTblWithExprOperator->GetOutput(result);

    auto streamedTblVecBatchEof1 = CreateEmptyVectorBatch(streamTypeVector);
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchEof1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode), static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_SCAN_FINISH));

    // check the join result
    long resultCol1[] =  {1100, 2200, 3300, 4400, 5500, 6600, 7700};
    double resultCol2[] =  {0, 0, 3.3, 4.4, 0, 6.6, 0};
    int32_t index = 0;
    for (uint32_t i = 0; i < result.size(); i++) {
        ASSERT_EQ(result[i]->GetVectorCount(), 2);
        ASSERT_EQ(result[i]->GetVector(0)->GetTypeId(), OMNI_LONG);
        ASSERT_EQ(result[i]->GetVector(1)->GetTypeId(), OMNI_DOUBLE);
        ASSERT_EQ(result[i]->GetRowCount(), sizeof(resultCol1) / sizeof(resultCol1[0]));
        for (auto j = 0; j < result[i]->GetRowCount(); j++) {
            if (resultCol1[index] != 0) {
                long longValue = (static_cast<LongVector *>(result[i]->GetVector(0)))->GetValue(j);
                ASSERT_EQ(longValue, resultCol1[index]);
            } else {
                ASSERT_EQ((static_cast<LongVector *>(result[i]->GetVector(0)))->IsValueNull(j), true);
            }
            if (resultCol2[index] != 0) {
                double doubleValue = (static_cast<DoubleVector *>(result[i]->GetVector(1)))->GetValue(j);
                ASSERT_EQ(doubleValue, resultCol2[index]);
            } else {
                ASSERT_EQ((static_cast<DoubleVector *>(result[i]->GetVector(1)))->IsValueNull(j), true);
            }
            index++;
        }
        VectorHelper::FreeVecBatch(result[i]);
    }

    Expr::DeleteExprs(streamedEqualKeyExprs);
    Expr::DeleteExprs(bufferedEqualKeyExprs);
    omniruntime::op::Operator::DeleteOperator(bufferedTblWithExprOperator);
    omniruntime::op::Operator::DeleteOperator(streamedTblWithExprOperator);
    DeleteOperatorFactory(bufferedWithExprOperatorFactory);
    DeleteOperatorFactory(streamedWithExprOperatorFactory);
    delete overflowConfig;
}

TEST(SMJ_JOIN_OPERATOR_WITH_EXPR_TESTCASE, testSmjLeftJoinMutilColumBatch)
{
    // select t1.b, t2.c from t1 left join t2 where t1.a = t2.d
    // bufferedTbl t2: double c, int d;
    // streamedTbl t1:  int a, Long b;
    std::string blank = "";
    std::vector<DataTypePtr> streamTypeVector = { IntType(), LongType() };
    DataTypes streamedTblTypes(streamTypeVector);
    FieldExpr *col0 = new FieldExpr(0, IntType());
    std::vector<Expr *> streamedEqualKeyExprs = { col0 };
    auto overflowConfig = new OverflowConfig();
    int streamedOutputCols[1] = {1};
    StreamedTableWithExprOperatorFactory *streamedWithExprOperatorFactory =
        StreamedTableWithExprOperatorFactory::CreateStreamedTableWithExprOperatorFactory(streamedTblTypes,
        streamedEqualKeyExprs, 1, streamedOutputCols, 1, JoinType::OMNI_JOIN_TYPE_LEFT, blank, overflowConfig);
    omniruntime::op::Operator *streamedTblWithExprOperator = CreateTestOperator(streamedWithExprOperatorFactory);

    std::vector<DataTypePtr> bufferTypesVector = { DoubleType(), IntType() };
    DataTypes bufferedTblTypes(bufferTypesVector);
    FieldExpr *col1 = new FieldExpr(1, IntType());
    std::vector<Expr *> bufferedEqualKeyExprs = { col1 };
    int bufferedOutputCols[1] = {0};
    int64_t streamedWithExprOperatorFactoryAddr = reinterpret_cast<int64_t>(streamedWithExprOperatorFactory);
    BufferedTableWithExprOperatorFactory *bufferedWithExprOperatorFactory =
        BufferedTableWithExprOperatorFactory::CreateBufferedTableWithExprOperatorFactory(bufferedTblTypes,
        bufferedEqualKeyExprs, 1, bufferedOutputCols, 1, streamedWithExprOperatorFactoryAddr, overflowConfig);
    omniruntime::op::Operator *bufferedTblWithExprOperator = CreateTestOperator(bufferedWithExprOperatorFactory);

    int32_t streamedCol1Row1[1] = {1};
    long streamedCol2Row1[1] = {1100};
    int32_t streamedCol1Row2[1] = {2};
    long streamedCol2Row2[1] = {2200};
    int32_t streamedCol1Row3[1] = {3};
    long streamedCol2Row3[1] = {3300};
    int32_t streamedCol1Row4[1] = {4};
    long streamedCol2Row4[1] = {4400};
    int32_t streamedCol1Row5[1] = {5};
    long streamedCol2Row5[1] = {5500};
    int32_t streamedCol1Row6[1] = {8};
    long streamedCol2Row6[1] = {8800};
    // construct streamed data
    VectorBatch *streamedTblVecBatchRow1 = CreateVectorBatch(streamedTblTypes, 1, streamedCol1Row1, streamedCol2Row1);
    VectorBatch *streamedTblVecBatchRow2 = CreateVectorBatch(streamedTblTypes, 1, streamedCol1Row2, streamedCol2Row2);
    VectorBatch *streamedTblVecBatchRow3 = CreateVectorBatch(streamedTblTypes, 1, streamedCol1Row3, streamedCol2Row3);
    VectorBatch *streamedTblVecBatchRow4 = CreateVectorBatch(streamedTblTypes, 1, streamedCol1Row4, streamedCol2Row4);
    VectorBatch *streamedTblVecBatchRow5 = CreateVectorBatch(streamedTblTypes, 1, streamedCol1Row5, streamedCol2Row5);
    VectorBatch *streamedTblVecBatchRow6 = CreateVectorBatch(streamedTblTypes, 1, streamedCol1Row6, streamedCol2Row6);

    double bufferedCol1Row1[1] = {1.1};
    int32_t bufferedCol2Row1[1] = {1};
    double bufferedCol1Row2[1] = {2.2};
    int32_t bufferedCol2Row2[1] = {2};
    double bufferedCol1Row3[1] = {3.3};
    int32_t bufferedCol2Row3[1] = {3};
    double bufferedCol1Row4[1] = {4.4};
    int32_t bufferedCol2Row4[1] = {4};
    double bufferedCol1Row5[1] = {5.5};
    int32_t bufferedCol2Row5[1] = {5};
    double bufferedCol1Row6[1] = {6.6};
    int32_t bufferedCol2Row6[1] = {6};
    // construct buffered data
    VectorBatch *bufferedTblVecBatchRow1 = CreateVectorBatch(bufferedTblTypes, 1, bufferedCol1Row1, bufferedCol2Row1);
    VectorBatch *bufferedTblVecBatchRow2 = CreateVectorBatch(bufferedTblTypes, 1, bufferedCol1Row2, bufferedCol2Row2);
    VectorBatch *bufferedTblVecBatchRow3 = CreateVectorBatch(bufferedTblTypes, 1, bufferedCol1Row3, bufferedCol2Row3);
    VectorBatch *bufferedTblVecBatchRow4 = CreateVectorBatch(bufferedTblTypes, 1, bufferedCol1Row4, bufferedCol2Row4);
    VectorBatch *bufferedTblVecBatchRow5 = CreateVectorBatch(bufferedTblTypes, 1, bufferedCol1Row5, bufferedCol2Row5);
    VectorBatch *bufferedTblVecBatchRow6 = CreateVectorBatch(bufferedTblTypes, 1, bufferedCol1Row6, bufferedCol2Row6);

    int32_t addInputRetCode = -1;
    // join start add streamed table data
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchRow1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    // need add buffered table data
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatchRow1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    // need add buffered table data
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatchRow2);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_STREAM_TBL_DATA));

    // need add streamed table data
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchRow2);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    // need add buffered table data
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatchRow3);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_STREAM_TBL_DATA));

    // need add streamed table data
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchRow3);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    // need add buffered table data
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatchRow4);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_STREAM_TBL_DATA));

    // need add streamed table data
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchRow4);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    // need add buffered table data
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatchRow5);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_STREAM_TBL_DATA));

    // need add streamed table data
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchRow5);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    // need add buffered table data
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatchRow6);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_STREAM_TBL_DATA));

    // need add streamed table data
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchRow6);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    // add eof flag to buffered table , need add streamed table data
    VectorBatch *bufferedTblVecBatchEof = CreateEmptyVectorBatch(bufferTypesVector);
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatchEof);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_STREAM_TBL_DATA));

    // add eof flag to streamed table
    VectorBatch *streamedTblVecBatchEof = CreateEmptyVectorBatch(streamTypeVector);
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchEof);
    ASSERT_EQ(DecodeFetchFlag(addInputRetCode), static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_FETCH_JOIN_DATA));

    std::vector<omniruntime::vec::VectorBatch *> result;
    streamedTblWithExprOperator->GetOutput(result);

    // add eof flag to streamed table
    VectorBatch *streamedTblVecBatchEof1 = CreateEmptyVectorBatch(streamTypeVector);
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchEof1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode), static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_SCAN_FINISH));

    // check the join result
    long resultCol1[] =  {1100, 2200, 3300, 4400, 5500, 8800};
    double resultCol2[] =  {1.1, 2.2, 3.3, 4.4, 5.5, 0};
    int32_t index = 0;
    for (uint32_t i = 0; i < result.size(); i++) {
        ASSERT_EQ(result[i]->GetVectorCount(), 2);
        ASSERT_EQ(result[i]->GetVector(0)->GetTypeId(), OMNI_LONG);
        ASSERT_EQ(result[i]->GetVector(1)->GetTypeId(), OMNI_DOUBLE);
        ASSERT_EQ(result[i]->GetRowCount(), sizeof(resultCol1) / sizeof(resultCol1[0]));
        for (auto j = 0; j < result[i]->GetRowCount(); j++) {
            if (resultCol1[index] != 0) {
                long longValue = (static_cast<LongVector *>(result[i]->GetVector(0)))->GetValue(j);
                ASSERT_EQ(longValue, resultCol1[index]);
            } else {
                ASSERT_EQ((static_cast<LongVector *>(result[i]->GetVector(0)))->IsValueNull(j), true);
            }
            if (resultCol2[index] != 0) {
                double doubleValue = (static_cast<DoubleVector *>(result[i]->GetVector(1)))->GetValue(j);
                ASSERT_EQ(doubleValue, resultCol2[index]);
            } else {
                ASSERT_EQ((static_cast<DoubleVector *>(result[i]->GetVector(1)))->IsValueNull(j), true);
            }
            index++;
        }
        VectorHelper::FreeVecBatch(result[i]);
    }

    Expr::DeleteExprs(streamedEqualKeyExprs);
    Expr::DeleteExprs(bufferedEqualKeyExprs);
    omniruntime::op::Operator::DeleteOperator(bufferedTblWithExprOperator);
    omniruntime::op::Operator::DeleteOperator(streamedTblWithExprOperator);
    DeleteOperatorFactory(bufferedWithExprOperatorFactory);
    DeleteOperatorFactory(streamedWithExprOperatorFactory);
    delete overflowConfig;
}

TEST(SMJ_JOIN_OPERATOR_WITH_EXPR_TESTCASE, testSmjLeftJoinNullFirstWithRepeatRowsAndMutilColumBatch)
{
    // select t1.b, t2.c from t1 left join t2 where t1.a = t2.d
    // bufferedTbl t2: double c, int d;
    // streamedTbl t1:  int a, Long b;
    std::string blank = "";
    std::vector<DataTypePtr> streamTypeVector = { IntType(), LongType() };
    DataTypes streamedTblTypes(streamTypeVector);
    FieldExpr *col0 = new FieldExpr(0, IntType());
    std::vector<Expr *> streamedEqualKeyExprs = { col0 };
    auto overflowConfig = new OverflowConfig();
    int streamedOutputCols[1] = {1};
    StreamedTableWithExprOperatorFactory *streamedWithExprOperatorFactory =
        StreamedTableWithExprOperatorFactory::CreateStreamedTableWithExprOperatorFactory(streamedTblTypes,
        streamedEqualKeyExprs, 1, streamedOutputCols, 1, JoinType::OMNI_JOIN_TYPE_LEFT, blank, overflowConfig);
    omniruntime::op::Operator *streamedTblWithExprOperator = CreateTestOperator(streamedWithExprOperatorFactory);

    std::vector<DataTypePtr> bufferTypesVector = { DoubleType(), IntType() };
    DataTypes bufferedTblTypes(bufferTypesVector);
    FieldExpr *col1 = new FieldExpr(1, IntType());
    std::vector<Expr *> bufferedEqualKeyExprs = { col1 };
    int bufferedOutputCols[1] = {0};
    int64_t streamedWithExprOperatorFactoryAddr = reinterpret_cast<int64_t>(streamedWithExprOperatorFactory);
    BufferedTableWithExprOperatorFactory *bufferedWithExprOperatorFactory =
        BufferedTableWithExprOperatorFactory::CreateBufferedTableWithExprOperatorFactory(bufferedTblTypes,
        bufferedEqualKeyExprs, 1, bufferedOutputCols, 1, streamedWithExprOperatorFactoryAddr, overflowConfig);
    omniruntime::op::Operator *bufferedTblWithExprOperator = CreateTestOperator(bufferedWithExprOperatorFactory);

    int32_t streamedCol1Row1[1] = {1};
    long streamedCol2Row1[1] = {1100};
    int32_t streamedCol1Row2[1] = {2};
    long streamedCol2Row2[1] = {2200};
    int32_t streamedCol1Row3[1] = {4};
    long streamedCol2Row3[1] = {4400};
    int32_t streamedCol1Row4[1] = {5};
    long streamedCol2Row4[1] = {5500};
    int32_t streamedCol1Row5[1] = {5};
    long streamedCol2Row5[1] = {5500};
    int32_t streamedCol1Row6[1] = {6};
    long streamedCol2Row6[1] = {6600};
    // construct streamed data
    VectorBatch *streamedTblVecBatchRow1 = CreateVectorBatch(streamedTblTypes, 1, streamedCol1Row1, streamedCol2Row1);
    VectorBatch *streamedTblVecBatchRow2 = CreateVectorBatch(streamedTblTypes, 1, streamedCol1Row2, streamedCol2Row2);
    VectorBatch *streamedTblVecBatchRow3 = CreateVectorBatch(streamedTblTypes, 1, streamedCol1Row3, streamedCol2Row3);
    VectorBatch *streamedTblVecBatchRow4 = CreateVectorBatch(streamedTblTypes, 1, streamedCol1Row4, streamedCol2Row4);
    VectorBatch *streamedTblVecBatchRow5 = CreateVectorBatch(streamedTblTypes, 1, streamedCol1Row5, streamedCol2Row5);
    VectorBatch *streamedTblVecBatchRow6 = CreateVectorBatch(streamedTblTypes, 1, streamedCol1Row6, streamedCol2Row6);
    streamedTblVecBatchRow1->GetVector(0)->SetValueNull(0); // null, null, 4, 5, 5, 6
    streamedTblVecBatchRow2->GetVector(0)->SetValueNull(0); // null, null, 4, 5, 5, 6

    double bufferedCol1Row1[1] = {1.1};
    int32_t bufferedCol2Row1[1] = {1};
    double bufferedCol1Row2[1] = {2.2};
    int32_t bufferedCol2Row2[1] = {2};
    double bufferedCol1Row3[1] = {3.3};
    int32_t bufferedCol2Row3[1] = {3};
    double bufferedCol1Row4[1] = {4.4};
    int32_t bufferedCol2Row4[1] = {4};
    double bufferedCol1Row5[1] = {5.5};
    int32_t bufferedCol2Row5[1] = {5};
    double bufferedCol1Row6[1] = {5.5};
    int32_t bufferedCol2Row6[1] = {5};
    double bufferedCol1Row7[1] = {6.6};
    int32_t bufferedCol2Row7[1] = {6};
    // construct buffered data
    VectorBatch *bufferedTblVecBatchRow1 = CreateVectorBatch(bufferedTblTypes, 1, bufferedCol1Row1, bufferedCol2Row1);
    VectorBatch *bufferedTblVecBatchRow2 = CreateVectorBatch(bufferedTblTypes, 1, bufferedCol1Row2, bufferedCol2Row2);
    VectorBatch *bufferedTblVecBatchRow3 = CreateVectorBatch(bufferedTblTypes, 1, bufferedCol1Row3, bufferedCol2Row3);
    VectorBatch *bufferedTblVecBatchRow4 = CreateVectorBatch(bufferedTblTypes, 1, bufferedCol1Row4, bufferedCol2Row4);
    VectorBatch *bufferedTblVecBatchRow5 = CreateVectorBatch(bufferedTblTypes, 1, bufferedCol1Row5, bufferedCol2Row5);
    VectorBatch *bufferedTblVecBatchRow6 = CreateVectorBatch(bufferedTblTypes, 1, bufferedCol1Row6, bufferedCol2Row6);
    VectorBatch *bufferedTblVecBatchRow7 = CreateVectorBatch(bufferedTblTypes, 1, bufferedCol1Row7, bufferedCol2Row7);

    int32_t addInputRetCode = -1;
    // join start add streamed table data
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchRow1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    // need add buffered table data
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatchRow1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_STREAM_TBL_DATA));

    // need add streamed table data
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchRow2);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_STREAM_TBL_DATA));

    // need add streamed table data
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchRow3);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    // need add buffered table data
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatchRow2);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    // need add buffered table data
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatchRow3);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    // need add buffered table data
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatchRow4);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    // need add buffered table data
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatchRow5);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_STREAM_TBL_DATA));

    // need add streamed table data
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchRow4);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    // need add buffered table data
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatchRow6);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    // need add buffered table data
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatchRow7);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_STREAM_TBL_DATA));

    // need add streamed table data
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchRow5);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_STREAM_TBL_DATA));

    // need add streamed table data
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchRow6);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    // add eof flag to buffered table , need add streamed table data
    VectorBatch *bufferedTblVecBatchEof = CreateEmptyVectorBatch(bufferTypesVector);
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatchEof);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_STREAM_TBL_DATA));

    // add eof flag to streamed table
    VectorBatch *streamedTblVecBatchEof = CreateEmptyVectorBatch(streamTypeVector);
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchEof);
    ASSERT_EQ(DecodeFetchFlag(addInputRetCode), static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_FETCH_JOIN_DATA));

    std::vector<omniruntime::vec::VectorBatch *> result;
    streamedTblWithExprOperator->GetOutput(result);

    // add eof flag to streamed table
    VectorBatch *streamedTblVecBatchEof1 = CreateEmptyVectorBatch(streamTypeVector);
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchEof1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode), static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_SCAN_FINISH));

    // check the join result
    long resultCol1[] =  {1100, 2200, 4400, 5500, 5500, 5500, 5500, 6600};
    double resultCol2[] =  {0, 0, 4.4, 5.5, 5.5, 5.5, 5.5, 6.6};
    int32_t index = 0;
    for (uint32_t i = 0; i < result.size(); i++) {
        ASSERT_EQ(result[i]->GetVectorCount(), 2);
        ASSERT_EQ(result[i]->GetVector(0)->GetTypeId(), OMNI_LONG);
        ASSERT_EQ(result[i]->GetVector(1)->GetTypeId(), OMNI_DOUBLE);
        ASSERT_EQ(result[i]->GetRowCount(), sizeof(resultCol1) / sizeof(resultCol1[0]));
        for (auto j = 0; j < result[i]->GetRowCount(); j++) {
            if (resultCol1[index] != 0) {
                long longValue = (static_cast<LongVector *>(result[i]->GetVector(0)))->GetValue(j);
                ASSERT_EQ(longValue, resultCol1[index]);
            } else {
                ASSERT_EQ((static_cast<LongVector *>(result[i]->GetVector(0)))->IsValueNull(j), true);
            }
            if (resultCol2[index] != 0) {
                double doubleValue = (static_cast<DoubleVector *>(result[i]->GetVector(1)))->GetValue(j);
                ASSERT_EQ(doubleValue, resultCol2[index]);
            } else {
                ASSERT_EQ((static_cast<DoubleVector *>(result[i]->GetVector(1)))->IsValueNull(j), true);
            }
            index++;
        }
        VectorHelper::FreeVecBatch(result[i]);
    }

    Expr::DeleteExprs(streamedEqualKeyExprs);
    Expr::DeleteExprs(bufferedEqualKeyExprs);
    omniruntime::op::Operator::DeleteOperator(bufferedTblWithExprOperator);
    omniruntime::op::Operator::DeleteOperator(streamedTblWithExprOperator);
    DeleteOperatorFactory(bufferedWithExprOperatorFactory);
    DeleteOperatorFactory(streamedWithExprOperatorFactory);
    delete overflowConfig;
}

TEST(SMJ_JOIN_OPERATOR_WITH_EXPR_TESTCASE, testSmjLeftJoinRepeatRowsAndMutilColumBatch)
{
    // select t1.b, t2.c from t1 left join t2 where t1.a = t2.d
    // bufferedTbl t2: double c, int d;
    // streamedTbl t1:  int a, Long b;
    std::string blank = "";
    std::vector<DataTypePtr> streamTypeVector = { IntType(), LongType() };
    DataTypes streamedTblTypes(streamTypeVector);
    FieldExpr *col0 = new FieldExpr(0, IntType());
    std::vector<Expr *> streamedEqualKeyExprs = { col0 };
    auto overflowConfig = new OverflowConfig();
    int streamedOutputCols[1] = {1};
    StreamedTableWithExprOperatorFactory *streamedWithExprOperatorFactory =
        StreamedTableWithExprOperatorFactory::CreateStreamedTableWithExprOperatorFactory(streamedTblTypes,
        streamedEqualKeyExprs, 1, streamedOutputCols, 1, JoinType::OMNI_JOIN_TYPE_LEFT, blank, overflowConfig);
    omniruntime::op::Operator *streamedTblWithExprOperator = CreateTestOperator(streamedWithExprOperatorFactory);

    std::vector<DataTypePtr> bufferTypesVector = { DoubleType(), IntType() };
    DataTypes bufferedTblTypes(bufferTypesVector);
    FieldExpr *col1 = new FieldExpr(1, IntType());
    std::vector<Expr *> bufferedEqualKeyExprs = { col1 };
    int bufferedOutputCols[1] = {0};
    int64_t streamedWithExprOperatorFactoryAddr = reinterpret_cast<int64_t>(streamedWithExprOperatorFactory);
    BufferedTableWithExprOperatorFactory *bufferedWithExprOperatorFactory =
        BufferedTableWithExprOperatorFactory::CreateBufferedTableWithExprOperatorFactory(bufferedTblTypes,
        bufferedEqualKeyExprs, 1, bufferedOutputCols, 1, streamedWithExprOperatorFactoryAddr, overflowConfig);
    omniruntime::op::Operator *bufferedTblWithExprOperator = CreateTestOperator(bufferedWithExprOperatorFactory);

    int32_t streamedCol1Row1[1] = {1};
    long streamedCol2Row1[1] = {1100};
    int32_t streamedCol1Row2[1] = {2};
    long streamedCol2Row2[1] = {2200};
    int32_t streamedCol1Row3[1] = {4};
    long streamedCol2Row3[1] = {4400};
    int32_t streamedCol1Row4[1] = {5};
    long streamedCol2Row4[1] = {5500};
    int32_t streamedCol1Row5[1] = {5};
    long streamedCol2Row5[1] = {5500};
    int32_t streamedCol1Row6[2] = {5, 5};
    long streamedCol2Row6[2] = {5500, 5500};
    int32_t streamedCol1Row7[1] = {6};
    long streamedCol2Row7[1] = {6600};
    // construct streamed data
    VectorBatch *streamedTblVecBatchRow1 = CreateVectorBatch(streamedTblTypes, 1, streamedCol1Row1, streamedCol2Row1);
    VectorBatch *streamedTblVecBatchRow2 = CreateVectorBatch(streamedTblTypes, 1, streamedCol1Row2, streamedCol2Row2);
    VectorBatch *streamedTblVecBatchRow3 = CreateVectorBatch(streamedTblTypes, 1, streamedCol1Row3, streamedCol2Row3);
    VectorBatch *streamedTblVecBatchRow4 = CreateVectorBatch(streamedTblTypes, 1, streamedCol1Row4, streamedCol2Row4);
    VectorBatch *streamedTblVecBatchRow5 = CreateVectorBatch(streamedTblTypes, 1, streamedCol1Row5, streamedCol2Row5);
    VectorBatch *streamedTblVecBatchRow6 = CreateVectorBatch(streamedTblTypes, 2, streamedCol1Row6, streamedCol2Row6);
    VectorBatch *streamedTblVecBatchRow7 = CreateVectorBatch(streamedTblTypes, 1, streamedCol1Row7, streamedCol2Row7);

    double bufferedCol1Row1[1] = {1.1};
    int32_t bufferedCol2Row1[1] = {1};
    double bufferedCol1Row2[1] = {2.2};
    int32_t bufferedCol2Row2[1] = {2};
    double bufferedCol1Row3[1] = {3.3};
    int32_t bufferedCol2Row3[1] = {3};
    double bufferedCol1Row4[1] = {4.4};
    int32_t bufferedCol2Row4[1] = {4};
    double bufferedCol1Row5[1] = {5.5};
    int32_t bufferedCol2Row5[1] = {5};
    double bufferedCol1Row6[2] = {5.5, 5.5};
    int32_t bufferedCol2Row6[2] = {5, 5};
    double bufferedCol1Row7[1] = {5.5};
    int32_t bufferedCol2Row7[1] = {5};
    double bufferedCol1Row8[1] = {6.6};
    int32_t bufferedCol2Row8[1] = {6};
    // construct buffered data
    VectorBatch *bufferedTblVecBatchRow1 = CreateVectorBatch(bufferedTblTypes, 1, bufferedCol1Row1, bufferedCol2Row1);
    VectorBatch *bufferedTblVecBatchRow2 = CreateVectorBatch(bufferedTblTypes, 1, bufferedCol1Row2, bufferedCol2Row2);
    VectorBatch *bufferedTblVecBatchRow3 = CreateVectorBatch(bufferedTblTypes, 1, bufferedCol1Row3, bufferedCol2Row3);
    VectorBatch *bufferedTblVecBatchRow4 = CreateVectorBatch(bufferedTblTypes, 1, bufferedCol1Row4, bufferedCol2Row4);
    VectorBatch *bufferedTblVecBatchRow5 = CreateVectorBatch(bufferedTblTypes, 1, bufferedCol1Row5, bufferedCol2Row5);
    VectorBatch *bufferedTblVecBatchRow6 = CreateVectorBatch(bufferedTblTypes, 2, bufferedCol1Row6, bufferedCol2Row6);
    VectorBatch *bufferedTblVecBatchRow7 = CreateVectorBatch(bufferedTblTypes, 1, bufferedCol1Row7, bufferedCol2Row7);
    VectorBatch *bufferedTblVecBatchRow8 = CreateVectorBatch(bufferedTblTypes, 1, bufferedCol1Row8, bufferedCol2Row8);

    int32_t addInputRetCode = -1;
    // join start add streamed table data
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchRow1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    // need add buffered table data
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatchRow1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    // need add buffered table data
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatchRow2);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_STREAM_TBL_DATA));

    // need add streamed table data
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchRow2);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    // need add buffered table data
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatchRow3);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_STREAM_TBL_DATA));

    // need add streamed table data
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchRow3);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    // need add buffered table data
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatchRow4);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    // need add buffered table data
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatchRow5);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_STREAM_TBL_DATA));

    // need add streamed table data
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchRow4);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    // need add buffered table data
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatchRow6);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    // need add buffered table data
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatchRow7);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    // need add buffered table data
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatchRow8);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_STREAM_TBL_DATA));

    // need add streamed table data
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchRow5);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_STREAM_TBL_DATA));

    // need add streamed table data
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchRow6);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_STREAM_TBL_DATA));

    // need add streamed table data
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchRow7);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    // add eof flag to buffered table , need add streamed table data
    VectorBatch *bufferedTblVecBatchEof = CreateEmptyVectorBatch(bufferTypesVector);
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatchEof);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_STREAM_TBL_DATA));

    // add eof flag to streamed table
    VectorBatch *streamedTblVecBatchEof = CreateEmptyVectorBatch(streamTypeVector);
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchEof);
    ASSERT_EQ(DecodeFetchFlag(addInputRetCode), static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_FETCH_JOIN_DATA));

    std::vector<omniruntime::vec::VectorBatch *> result;
    streamedTblWithExprOperator->GetOutput(result);

    // add eof flag to streamed table
    VectorBatch *streamedTblVecBatchEof1 = CreateEmptyVectorBatch(streamTypeVector);
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchEof1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode), static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_SCAN_FINISH));

    // check the join result
    long resultCol1[] =  {1100, 2200, 4400, 5500, 5500, 5500, 5500, 5500, 5500, 5500, 5500, 5500,
                          5500, 5500, 5500, 5500, 5500, 5500, 5500, 6600};
    double resultCol2[] =  {1.1, 2.2, 4.4, 5.5, 5.5, 5.5, 5.5, 5.5, 5.5, 5.5, 5.5, 5.5, 5.5, 5.5, 5.5, 5.5,
                            5.5, 5.5, 5.5, 6.6};
    int32_t index = 0;
    for (uint32_t i = 0; i < result.size(); i++) {
        ASSERT_EQ(result[i]->GetVectorCount(), 2);
        ASSERT_EQ(result[i]->GetVector(0)->GetTypeId(), OMNI_LONG);
        ASSERT_EQ(result[i]->GetVector(1)->GetTypeId(), OMNI_DOUBLE);
        ASSERT_EQ(result[i]->GetRowCount(), sizeof(resultCol1) / sizeof(resultCol1[0]));
        for (auto j = 0; j < result[i]->GetRowCount(); j++) {
            if (resultCol1[index] != 0) {
                long longValue = (static_cast<LongVector *>(result[i]->GetVector(0)))->GetValue(j);
                ASSERT_EQ(longValue, resultCol1[index]);
            } else {
                ASSERT_EQ((static_cast<LongVector *>(result[i]->GetVector(0)))->IsValueNull(j), true);
            }
            if (resultCol2[index] != 0) {
                double doubleValue = (static_cast<DoubleVector *>(result[i]->GetVector(1)))->GetValue(j);
                ASSERT_EQ(doubleValue, resultCol2[index]);
            } else {
                ASSERT_EQ((static_cast<DoubleVector *>(result[i]->GetVector(1)))->IsValueNull(j), true);
            }
            index++;
        }
        VectorHelper::FreeVecBatch(result[i]);
    }

    Expr::DeleteExprs(streamedEqualKeyExprs);
    Expr::DeleteExprs(bufferedEqualKeyExprs);
    omniruntime::op::Operator::DeleteOperator(bufferedTblWithExprOperator);
    omniruntime::op::Operator::DeleteOperator(streamedTblWithExprOperator);
    DeleteOperatorFactory(bufferedWithExprOperatorFactory);
    DeleteOperatorFactory(streamedWithExprOperatorFactory);
    delete overflowConfig;
}

TEST(SMJ_JOIN_OPERATOR_WITH_EXPR_TESTCASE, testSmjLeftJoinStreamedWithEmptyBuffered)
{
    // select t1.b, t2.c from t1 left join t2 where t1.a = t2.d
    // bufferedTbl t2: double c, int d;
    // streamedTbl t1:  int a, Long b;
    std::string blank = "";
    std::vector<DataTypePtr> streamTypeVector = { IntType(), LongType() };
    DataTypes streamedTblTypes(streamTypeVector);
    FieldExpr *col0 = new FieldExpr(0, IntType());
    std::vector<Expr *> streamedEqualKeyExprs = { col0 };
    auto overflowConfig = new OverflowConfig();
    int streamedOutputCols[1] = {1};
    StreamedTableWithExprOperatorFactory *streamedWithExprOperatorFactory =
        StreamedTableWithExprOperatorFactory::CreateStreamedTableWithExprOperatorFactory(streamedTblTypes,
        streamedEqualKeyExprs, 1, streamedOutputCols, 1, JoinType::OMNI_JOIN_TYPE_LEFT, blank, overflowConfig);
    omniruntime::op::Operator *streamedTblWithExprOperator = CreateTestOperator(streamedWithExprOperatorFactory);

    std::vector<DataTypePtr> bufferTypesVector = { DoubleType(), IntType() };
    DataTypes bufferedTblTypes(bufferTypesVector);
    FieldExpr *col1 = new FieldExpr(1, IntType());
    std::vector<Expr *> bufferedEqualKeyExprs = { col1 };
    int bufferedOutputCols[1] = {0};
    int64_t streamedWithExprOperatorFactoryAddr = reinterpret_cast<int64_t>(streamedWithExprOperatorFactory);
    BufferedTableWithExprOperatorFactory *bufferedWithExprOperatorFactory =
        BufferedTableWithExprOperatorFactory::CreateBufferedTableWithExprOperatorFactory(bufferedTblTypes,
        bufferedEqualKeyExprs, 1, bufferedOutputCols, 1, streamedWithExprOperatorFactoryAddr, overflowConfig);
    omniruntime::op::Operator *bufferedTblWithExprOperator = CreateTestOperator(bufferedWithExprOperatorFactory);

    // construct data
    const int32_t streamedTblDataSize = 6;
    int32_t streamedTblDataCol1[streamedTblDataSize] = {1, 2, 3, 4, 5, 6};
    long streamedTblDataCol2[streamedTblDataSize] =  {1100, 2200, 3300, 4400, 5500, 6600};
    VectorBatch *streamedTblVecBatch1 =
        CreateVectorBatch(streamedTblTypes, streamedTblDataSize, streamedTblDataCol1, streamedTblDataCol2);

    int32_t addInputRetCode = -1;
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatch1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    // add eof flag to buffered table , need add streamed table data
    VectorBatch *bufferedTblVecBatchEof = CreateEmptyVectorBatch(bufferTypesVector);
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatchEof);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_STREAM_TBL_DATA));

    // add eof flag to streamed table
    VectorBatch *streamedTblVecBatchEof = CreateEmptyVectorBatch(streamTypeVector);
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchEof);
    ASSERT_EQ(DecodeFetchFlag(addInputRetCode), static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_FETCH_JOIN_DATA));

    std::vector<omniruntime::vec::VectorBatch *> result;
    streamedTblWithExprOperator->GetOutput(result);

    // add eof flag to streamed table
    VectorBatch *streamedTblVecBatchEof1 = CreateEmptyVectorBatch(streamTypeVector);
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchEof1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode), static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_SCAN_FINISH));

    // check the join result
    long resultCol1[] =  {1100, 2200, 3300, 4400, 5500, 6600};
    double resultCol2[] =  {0, 0, 0, 0, 0, 0};
    int32_t index = 0;
    for (uint32_t i = 0; i < result.size(); i++) {
        ASSERT_EQ(result[i]->GetVectorCount(), 2);
        ASSERT_EQ(result[i]->GetVector(0)->GetTypeId(), OMNI_LONG);
        ASSERT_EQ(result[i]->GetVector(1)->GetTypeId(), OMNI_DOUBLE);
        ASSERT_EQ(result[i]->GetRowCount(), sizeof(resultCol1) / sizeof(resultCol1[0]));
        for (auto j = 0; j < result[i]->GetRowCount(); j++) {
            if (resultCol1[index] != 0) {
                long longValue = (static_cast<LongVector *>(result[i]->GetVector(0)))->GetValue(j);
                ASSERT_EQ(longValue, resultCol1[index]);
            } else {
                ASSERT_EQ((static_cast<LongVector *>(result[i]->GetVector(0)))->IsValueNull(j), true);
            }
            if (resultCol2[index] != 0) {
                double doubleValue = (static_cast<DoubleVector *>(result[i]->GetVector(1)))->GetValue(j);
                ASSERT_EQ(doubleValue, resultCol2[index]);
            } else {
                ASSERT_EQ((static_cast<DoubleVector *>(result[i]->GetVector(1)))->IsValueNull(j), true);
            }
            index++;
        }
        VectorHelper::FreeVecBatch(result[i]);
    }

    Expr::DeleteExprs(streamedEqualKeyExprs);
    Expr::DeleteExprs(bufferedEqualKeyExprs);
    omniruntime::op::Operator::DeleteOperator(bufferedTblWithExprOperator);
    omniruntime::op::Operator::DeleteOperator(streamedTblWithExprOperator);
    DeleteOperatorFactory(bufferedWithExprOperatorFactory);
    DeleteOperatorFactory(streamedWithExprOperatorFactory);
    delete overflowConfig;
}

TEST(SMJ_JOIN_OPERATOR_WITH_EXPR_TESTCASE, testSmjFullOuterJoinWithNullFirst)
{
    std::string blank = "";
    std::vector<DataTypePtr> streamTypeVector = { LongType(), LongType() };
    DataTypes streamedTblTypes(streamTypeVector);
    auto streamCol0 = new FieldExpr(0, LongType());
    auto streamCol1 = new FieldExpr(1, LongType());
    std::vector<Expr *> streamedEqualKeyExprs = { streamCol0, streamCol1 };
    auto overflowConfig = new OverflowConfig();
    int32_t streamedOutputCols[2] = {0, 1};
    StreamedTableWithExprOperatorFactory *streamedWithExprOperatorFactory =
        StreamedTableWithExprOperatorFactory::CreateStreamedTableWithExprOperatorFactory(streamedTblTypes,
        streamedEqualKeyExprs, 2, streamedOutputCols, 2, JoinType::OMNI_JOIN_TYPE_FULL, blank, overflowConfig);
    omniruntime::op::Operator *streamedTblWithExprOperator = CreateTestOperator(streamedWithExprOperatorFactory);

    std::vector<DataTypePtr> bufferTypesVector = { LongType(), LongType() };
    DataTypes bufferedTblTypes(bufferTypesVector);
    auto bufferCol0 = new FieldExpr(0, LongType());
    auto bufferCol1 = new FieldExpr(1, LongType());
    std::vector<Expr *> bufferedEqualKeyExprs = { bufferCol0, bufferCol1 };
    int bufferedOutputCols[2] = {0, 1};
    int64_t streamedWithExprOperatorFactoryAddr = reinterpret_cast<int64_t>(streamedWithExprOperatorFactory);
    BufferedTableWithExprOperatorFactory *bufferedWithExprOperatorFactory =
        BufferedTableWithExprOperatorFactory::CreateBufferedTableWithExprOperatorFactory(bufferedTblTypes,
        bufferedEqualKeyExprs, 2, bufferedOutputCols, 2, streamedWithExprOperatorFactoryAddr, overflowConfig);
    omniruntime::op::Operator *bufferedTblWithExprOperator = CreateTestOperator(bufferedWithExprOperatorFactory);

    // construct data
    const int32_t streamedTblDataSize = 10;
    int64_t streamedTblDataCol1[streamedTblDataSize] = {1,3378,5439,9013,9543,12572,15591,17436,25272,30436};
    int64_t streamedTblDataCol2[streamedTblDataSize] =  {8042,8221,8261,7067,7883,8354,5861,6539,5870,6907};
    VectorBatch *streamedTblVecBatch1 =
        CreateVectorBatch(streamedTblTypes, streamedTblDataSize, streamedTblDataCol1, streamedTblDataCol2);
    streamedTblVecBatch1->GetVector(0)->SetValueNull(0);

    int32_t addInputRetCode = -1;
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatch1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    const int32_t bufferedTblSize = 10;
    int64_t bufferedTblDataCol1[bufferedTblSize] =  {1,879,7804,13206,14690,32279,36620,41764,44840,53836};
    int64_t bufferedTblDataCol2[bufferedTblSize] =  {7748,5444,5701,6737,5381,6434,8000,7231,7610,7955};
    VectorBatch *bufferedTblVecBatch1 =
        CreateVectorBatch(bufferedTblTypes, bufferedTblSize, bufferedTblDataCol1, bufferedTblDataCol2);
    bufferedTblVecBatch1->GetVector(0)->SetValueNull(0);

    // need add buffered table data
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatch1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_STREAM_TBL_DATA));

    // add eof flag to streamed table
    VectorBatch *streamedTblVecBatchEof = CreateEmptyVectorBatch(streamTypeVector);
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchEof);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    // add eof flag to buffered table , need add streamed table data
    VectorBatch *bufferedTblVecBatchEof = CreateEmptyVectorBatch(bufferTypesVector);
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatchEof);
    ASSERT_EQ(DecodeFetchFlag(addInputRetCode), static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_FETCH_JOIN_DATA));

    std::vector<omniruntime::vec::VectorBatch *> result;
    streamedTblWithExprOperator->GetOutput(result);

    auto streamedTblVecBatchEof1 = CreateEmptyVectorBatch(streamTypeVector);
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchEof1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode), static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_SCAN_FINISH));

    // check the join result
    int64_t resultCol1[] = {-1,-1,-1,3378,5439,-1,9013,9543,12572,-1,-1,15591,17436,25272,30436,-1,-1,-1,-1,-1};
    int64_t resultCol2[] = {8042,-1,-1,8221,8261,-1,7067,7883,8354,-1,-1,5861,6539,5870,6907,-1,-1,-1,-1,-1};
    int64_t resultCol3[] = {-1,-1,879,-1,-1,7804,-1,-1,-1,13206,14690,-1,-1,-1,-1,32279,36620,41764,44840,53836};
    int64_t resultCol4[] = {-1,7748,5444,-1,-1,5701,-1,-1,-1,6737,5381,-1,-1,-1,-1,6434,8000,7231,7610,7955};
    std::vector<DataTypePtr> resultTypesVec = { LongType(), LongType(), LongType(), LongType() };
    DataTypes resultTypes(resultTypesVec);
    VectorBatch *expectVecBatch = CreateVectorBatch(resultTypes, 20, resultCol1, resultCol2, resultCol3, resultCol4);
    for (int32_t colIdx = 0; colIdx < expectVecBatch->GetVectorCount(); colIdx++) {
        auto vector = static_cast<LongVector *>(expectVecBatch->GetVector(colIdx));
        for (int32_t rowIdx = 0; rowIdx < expectVecBatch->GetRowCount(); rowIdx++) {
            if (vector->GetValue(rowIdx) == -1) {
                vector->SetValueNull(rowIdx);
            }
        }
    }
    ASSERT_TRUE(VecBatchMatch(result[0], expectVecBatch));
    VectorHelper::FreeVecBatches(result);
    VectorHelper::FreeVecBatch(expectVecBatch);

    Expr::DeleteExprs(streamedEqualKeyExprs);
    Expr::DeleteExprs(bufferedEqualKeyExprs);
    omniruntime::op::Operator::DeleteOperator(bufferedTblWithExprOperator);
    omniruntime::op::Operator::DeleteOperator(streamedTblWithExprOperator);
    DeleteOperatorFactory(bufferedWithExprOperatorFactory);
    DeleteOperatorFactory(streamedWithExprOperatorFactory);
    delete overflowConfig;
}

TEST(SMJ_JOIN_OPERATOR_WITH_EXPR_TESTCASE, testSmjFullOuterJoinMissMatchBothSide)
{
    // select t1.b, t2.c from t1 full join t2 where t1.a = t2.d
    // bufferedTbl t2: double c, int d;
    // streamedTbl t1:  int a, Long b;
    std::string blank = "";
    std::vector<DataTypePtr> streamTypeVector = { IntType(), LongType() };
    DataTypes streamedTblTypes(streamTypeVector);
    FieldExpr *col0 = new FieldExpr(0, IntType());
    std::vector<Expr *> streamedEqualKeyExprs = { col0 };
    auto overflowConfig = new OverflowConfig();
    int streamedOutputCols[1] = {1};
    StreamedTableWithExprOperatorFactory *streamedWithExprOperatorFactory =
        StreamedTableWithExprOperatorFactory::CreateStreamedTableWithExprOperatorFactory(streamedTblTypes,
        streamedEqualKeyExprs, 1, streamedOutputCols, 1, JoinType::OMNI_JOIN_TYPE_FULL, blank, overflowConfig);
    omniruntime::op::Operator *streamedTblWithExprOperator = CreateTestOperator(streamedWithExprOperatorFactory);

    std::vector<DataTypePtr> bufferTypesVector = { DoubleType(), IntType() };
    DataTypes bufferedTblTypes(bufferTypesVector);
    FieldExpr *col1 = new FieldExpr(1, IntType());
    std::vector<Expr *> bufferedEqualKeyExprs = { col1 };
    int bufferedOutputCols[1] = {0};
    int64_t streamedWithExprOperatorFactoryAddr = reinterpret_cast<int64_t>(streamedWithExprOperatorFactory);
    BufferedTableWithExprOperatorFactory *bufferedWithExprOperatorFactory =
        BufferedTableWithExprOperatorFactory::CreateBufferedTableWithExprOperatorFactory(bufferedTblTypes,
        bufferedEqualKeyExprs, 1, bufferedOutputCols, 1, streamedWithExprOperatorFactoryAddr, overflowConfig);
    omniruntime::op::Operator *bufferedTblWithExprOperator = CreateTestOperator(bufferedWithExprOperatorFactory);

    // construct data
    const int32_t streamedTblDataSize = 6;
    int32_t streamedTblDataCol1[streamedTblDataSize] = {1, 2, 3, 4, 5, 7};
    long streamedTblDataCol2[streamedTblDataSize] =  {1100, 2200, 3300, 4400, 5500, 7700};
    VectorBatch *streamedTblVecBatch1 =
        CreateVectorBatch(streamedTblTypes, streamedTblDataSize, streamedTblDataCol1, streamedTblDataCol2);

    int32_t addInputRetCode = -1;
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatch1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    const int32_t bufferedTblSize = 7;
    double bufferedTblDataCol1[bufferedTblSize] =  {1.1, 2.2, 3.3, 4.4, 6.6, 8.8, 9.9};
    int32_t bufferedTblDataCol2[bufferedTblSize] =  {1, 2, 3, 4, 6, 8, 9};
    VectorBatch *bufferedTblVecBatch1 =
        CreateVectorBatch(bufferedTblTypes, bufferedTblSize, bufferedTblDataCol1, bufferedTblDataCol2);
    // need add buffered table data
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatch1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_STREAM_TBL_DATA));

    // add eof flag to streamed table
    VectorBatch *streamedTblVecBatchEof = CreateEmptyVectorBatch(streamTypeVector);
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchEof);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    // add eof flag to buffered table , need add streamed table data
    VectorBatch *bufferedTblVecBatchEof = CreateEmptyVectorBatch(bufferTypesVector);
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatchEof);
    ASSERT_EQ(DecodeFetchFlag(addInputRetCode), static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_FETCH_JOIN_DATA));

    std::vector<omniruntime::vec::VectorBatch *> result;
    streamedTblWithExprOperator->GetOutput(result);

    auto streamedTblVecBatchEof1 = CreateEmptyVectorBatch(streamTypeVector);
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchEof1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode), static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_SCAN_FINISH));

    // check the join result
    long resultCol1[] =  {1100, 2200, 3300, 4400, 5500, 0, 7700, 0, 0};
    double resultCol2[] =  {1.1, 2.2, 3.3, 4.4, 0, 6.6, 0, 8.8, 9.9};
    int32_t index = 0;
    for (uint32_t i = 0; i < result.size(); i++) {
        ASSERT_EQ(result[i]->GetVectorCount(), 2);
        ASSERT_EQ(result[i]->GetVector(0)->GetTypeId(), OMNI_LONG);
        ASSERT_EQ(result[i]->GetVector(1)->GetTypeId(), OMNI_DOUBLE);
        ASSERT_EQ(result[i]->GetRowCount(), sizeof(resultCol1) / sizeof(resultCol1[0]));
        for (auto j = 0; j < result[i]->GetRowCount(); j++) {
            if (resultCol1[index] != 0) {
                long longValue = (static_cast<LongVector *>(result[i]->GetVector(0)))->GetValue(j);
                ASSERT_EQ(longValue, resultCol1[index]);
            } else {
                ASSERT_EQ((static_cast<LongVector *>(result[i]->GetVector(0)))->IsValueNull(j), true);
            }
            if (resultCol2[index] != 0) {
                double doubleValue = (static_cast<DoubleVector *>(result[i]->GetVector(1)))->GetValue(j);
                ASSERT_EQ(doubleValue, resultCol2[index]);
            } else {
                ASSERT_EQ((static_cast<DoubleVector *>(result[i]->GetVector(1)))->IsValueNull(j), true);
            }
            index++;
        }
        VectorHelper::FreeVecBatch(result[i]);
    }

    Expr::DeleteExprs(streamedEqualKeyExprs);
    Expr::DeleteExprs(bufferedEqualKeyExprs);
    omniruntime::op::Operator::DeleteOperator(bufferedTblWithExprOperator);
    omniruntime::op::Operator::DeleteOperator(streamedTblWithExprOperator);
    DeleteOperatorFactory(bufferedWithExprOperatorFactory);
    DeleteOperatorFactory(streamedWithExprOperatorFactory);
    delete overflowConfig;
}

TEST(SMJ_JOIN_OPERATOR_WITH_EXPR_TESTCASE, testSmjFullJoinWithMutilColumBatch)
{
    // select t1.b, t2.c from t1 left join t2 where t1.a = t2.d
    // bufferedTbl t2: double c, int d;
    // streamedTbl t1:  int a, Long b;
    std::string blank = "";
    std::vector<DataTypePtr> streamTypeVector = { IntType(), LongType() };
    DataTypes streamedTblTypes(streamTypeVector);
    FieldExpr *col0 = new FieldExpr(0, IntType());
    std::vector<Expr *> streamedEqualKeyExprs = { col0 };
    auto overflowConfig = new OverflowConfig();
    int streamedOutputCols[1] = {1};
    StreamedTableWithExprOperatorFactory *streamedWithExprOperatorFactory =
        StreamedTableWithExprOperatorFactory::CreateStreamedTableWithExprOperatorFactory(streamedTblTypes,
        streamedEqualKeyExprs, 1, streamedOutputCols, 1, JoinType::OMNI_JOIN_TYPE_FULL, blank, overflowConfig);
    omniruntime::op::Operator *streamedTblWithExprOperator = CreateTestOperator(streamedWithExprOperatorFactory);

    std::vector<DataTypePtr> bufferTypesVector = { DoubleType(), IntType() };
    DataTypes bufferedTblTypes(bufferTypesVector);
    FieldExpr *col1 = new FieldExpr(1, IntType());
    std::vector<Expr *> bufferedEqualKeyExprs = { col1 };
    int bufferedOutputCols[1] = {0};
    int64_t streamedWithExprOperatorFactoryAddr = reinterpret_cast<int64_t>(streamedWithExprOperatorFactory);
    BufferedTableWithExprOperatorFactory *bufferedWithExprOperatorFactory =
        BufferedTableWithExprOperatorFactory::CreateBufferedTableWithExprOperatorFactory(bufferedTblTypes,
        bufferedEqualKeyExprs, 1, bufferedOutputCols, 1, streamedWithExprOperatorFactoryAddr, overflowConfig);
    omniruntime::op::Operator *bufferedTblWithExprOperator = CreateTestOperator(bufferedWithExprOperatorFactory);

    int32_t streamedCol1Row1[1] = {1};
    long streamedCol2Row1[1] = {1100};
    int32_t streamedCol1Row2[1] = {2};
    long streamedCol2Row2[1] = {2200};
    int32_t streamedCol1Row3[1] = {3};
    long streamedCol2Row3[1] = {3300};
    int32_t streamedCol1Row4[1] = {4};
    long streamedCol2Row4[1] = {4400};
    int32_t streamedCol1Row5[1] = {5};
    long streamedCol2Row5[1] = {5500};
    int32_t streamedCol1Row6[1] = {7};
    long streamedCol2Row6[1] = {7700};
    // construct streamed data
    VectorBatch *streamedTblVecBatchRow1 = CreateVectorBatch(streamedTblTypes, 1, streamedCol1Row1, streamedCol2Row1);
    VectorBatch *streamedTblVecBatchRow2 = CreateVectorBatch(streamedTblTypes, 1, streamedCol1Row2, streamedCol2Row2);
    VectorBatch *streamedTblVecBatchRow3 = CreateVectorBatch(streamedTblTypes, 1, streamedCol1Row3, streamedCol2Row3);
    VectorBatch *streamedTblVecBatchRow4 = CreateVectorBatch(streamedTblTypes, 1, streamedCol1Row4, streamedCol2Row4);
    VectorBatch *streamedTblVecBatchRow5 = CreateVectorBatch(streamedTblTypes, 1, streamedCol1Row5, streamedCol2Row5);
    VectorBatch *streamedTblVecBatchRow6 = CreateVectorBatch(streamedTblTypes, 1, streamedCol1Row6, streamedCol2Row6);

    double bufferedCol1Row1[1] = {1.1};
    int32_t bufferedCol2Row1[1] = {1};
    double bufferedCol1Row2[1] = {2.2};
    int32_t bufferedCol2Row2[1] = {2};
    double bufferedCol1Row3[1] = {3.3};
    int32_t bufferedCol2Row3[1] = {3};
    double bufferedCol1Row4[1] = {4.4};
    int32_t bufferedCol2Row4[1] = {4};
    double bufferedCol1Row5[1] = {6.6};
    int32_t bufferedCol2Row5[1] = {6};
    double bufferedCol1Row6[1] = {8.8};
    int32_t bufferedCol2Row6[1] = {8};
    // construct buffered data
    VectorBatch *bufferedTblVecBatchRow1 = CreateVectorBatch(bufferedTblTypes, 1, bufferedCol1Row1, bufferedCol2Row1);
    VectorBatch *bufferedTblVecBatchRow2 = CreateVectorBatch(bufferedTblTypes, 1, bufferedCol1Row2, bufferedCol2Row2);
    VectorBatch *bufferedTblVecBatchRow3 = CreateVectorBatch(bufferedTblTypes, 1, bufferedCol1Row3, bufferedCol2Row3);
    VectorBatch *bufferedTblVecBatchRow4 = CreateVectorBatch(bufferedTblTypes, 1, bufferedCol1Row4, bufferedCol2Row4);
    VectorBatch *bufferedTblVecBatchRow5 = CreateVectorBatch(bufferedTblTypes, 1, bufferedCol1Row5, bufferedCol2Row5);
    VectorBatch *bufferedTblVecBatchRow6 = CreateVectorBatch(bufferedTblTypes, 1, bufferedCol1Row6, bufferedCol2Row6);

    int32_t addInputRetCode = -1;
    // join start add streamed table data
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchRow1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    // need add buffered table data
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatchRow1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    // need add buffered table data
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatchRow2);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_STREAM_TBL_DATA));

    // need add streamed table data
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchRow2);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    // need add buffered table data
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatchRow3);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_STREAM_TBL_DATA));

    // need add streamed table data
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchRow3);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    // need add buffered table data
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatchRow4);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_STREAM_TBL_DATA));

    // need add streamed table data
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchRow4);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    // need add buffered table data
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatchRow5);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_STREAM_TBL_DATA));

    // need add streamed table data
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchRow5);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_STREAM_TBL_DATA));

    // need add streamed table data
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchRow6);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    // need add buffered table data
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatchRow6);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_STREAM_TBL_DATA));

    // add eof flag to streamed table
    VectorBatch *streamedTblVecBatchEof = CreateEmptyVectorBatch(streamTypeVector);
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchEof);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    // add eof flag to buffered table , need add streamed table data
    VectorBatch *bufferedTblVecBatchEof = CreateEmptyVectorBatch(bufferTypesVector);
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatchEof);
    ASSERT_EQ(DecodeFetchFlag(addInputRetCode), static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_FETCH_JOIN_DATA));

    std::vector<omniruntime::vec::VectorBatch *> result;
    streamedTblWithExprOperator->GetOutput(result);

    // add eof flag to streamed table
    VectorBatch *streamedTblVecBatchEof1 = CreateEmptyVectorBatch(streamTypeVector);
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchEof1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode), static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_SCAN_FINISH));

    // check the join result
    long resultCol1[] =  {1100, 2200, 3300, 4400, 5500, 0, 7700, 0};
    double resultCol2[] =  {1.1, 2.2, 3.3, 4.4, 0, 6.6, 0, 8.8};
    int32_t index = 0;
    for (uint32_t i = 0; i < result.size(); i++) {
        ASSERT_EQ(result[i]->GetVectorCount(), 2);
        ASSERT_EQ(result[i]->GetVector(0)->GetTypeId(), OMNI_LONG);
        ASSERT_EQ(result[i]->GetVector(1)->GetTypeId(), OMNI_DOUBLE);
        ASSERT_EQ(result[i]->GetRowCount(), sizeof(resultCol1) / sizeof(resultCol1[0]));
        for (auto j = 0; j < result[i]->GetRowCount(); j++) {
            if (resultCol1[index] != 0) {
                long longValue = (static_cast<LongVector *>(result[i]->GetVector(0)))->GetValue(j);
                ASSERT_EQ(longValue, resultCol1[index]);
            } else {
                ASSERT_EQ((static_cast<LongVector *>(result[i]->GetVector(0)))->IsValueNull(j), true);
            }
            if (resultCol2[index] != 0) {
                double doubleValue = (static_cast<DoubleVector *>(result[i]->GetVector(1)))->GetValue(j);
                ASSERT_EQ(doubleValue, resultCol2[index]);
            } else {
                ASSERT_EQ((static_cast<DoubleVector *>(result[i]->GetVector(1)))->IsValueNull(j), true);
            }
            index++;
        }
        VectorHelper::FreeVecBatch(result[i]);
    }

    Expr::DeleteExprs(streamedEqualKeyExprs);
    Expr::DeleteExprs(bufferedEqualKeyExprs);
    omniruntime::op::Operator::DeleteOperator(bufferedTblWithExprOperator);
    omniruntime::op::Operator::DeleteOperator(streamedTblWithExprOperator);
    DeleteOperatorFactory(bufferedWithExprOperatorFactory);
    DeleteOperatorFactory(streamedWithExprOperatorFactory);
    delete overflowConfig;
}

TEST(SMJ_JOIN_OPERATOR_WITH_EXPR_TESTCASE, testSmjFullOuterJoinMatchBothSide)
{
    // select t1.b, t2.c from t1 full join t2 where t1.a = t2.d
    // bufferedTbl t2: double c, int d;
    // streamedTbl t1:  int a, Long b;
    std::string blank = "";
    std::vector<DataTypePtr> streamTypeVector = { IntType(), LongType() };
    DataTypes streamedTblTypes(streamTypeVector);
    FieldExpr *col0 = new FieldExpr(0, IntType());
    std::vector<Expr *> streamedEqualKeyExprs = { col0 };
    auto overflowConfig = new OverflowConfig();
    int streamedOutputCols[1] = {1};
    StreamedTableWithExprOperatorFactory *streamedWithExprOperatorFactory =
        StreamedTableWithExprOperatorFactory::CreateStreamedTableWithExprOperatorFactory(streamedTblTypes,
        streamedEqualKeyExprs, 1, streamedOutputCols, 1, JoinType::OMNI_JOIN_TYPE_FULL, blank, overflowConfig);
    omniruntime::op::Operator *streamedTblWithExprOperator = CreateTestOperator(streamedWithExprOperatorFactory);

    std::vector<DataTypePtr> bufferTypesVector = { DoubleType(), IntType() };
    DataTypes bufferedTblTypes(bufferTypesVector);
    FieldExpr *col1 = new FieldExpr(1, IntType());
    std::vector<Expr *> bufferedEqualKeyExprs = { col1 };
    int bufferedOutputCols[1] = {0};
    int64_t streamedWithExprOperatorFactoryAddr = reinterpret_cast<int64_t>(streamedWithExprOperatorFactory);
    BufferedTableWithExprOperatorFactory *bufferedWithExprOperatorFactory =
        BufferedTableWithExprOperatorFactory::CreateBufferedTableWithExprOperatorFactory(bufferedTblTypes,
        bufferedEqualKeyExprs, 1, bufferedOutputCols, 1, streamedWithExprOperatorFactoryAddr, overflowConfig);
    omniruntime::op::Operator *bufferedTblWithExprOperator = CreateTestOperator(bufferedWithExprOperatorFactory);

    // construct data
    const int32_t streamedTblDataSize = 6;
    int32_t streamedTblDataCol1[streamedTblDataSize] = {1, 2, 3, 4, 5, 6};
    long streamedTblDataCol2[streamedTblDataSize] =  {1100, 2200, 3300, 4400, 5500, 6600};
    VectorBatch *streamedTblVecBatch1 =
        CreateVectorBatch(streamedTblTypes, streamedTblDataSize, streamedTblDataCol1, streamedTblDataCol2);

    int32_t addInputRetCode = -1;
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatch1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    const int32_t bufferedTblSize = 6;
    double bufferedTblDataCol1[bufferedTblSize] =  {1.1, 2.2, 3.3, 4.4, 5.5, 6.6};
    int32_t bufferedTblDataCol2[bufferedTblSize] =  {1, 2, 3, 4, 5, 6};
    VectorBatch *bufferedTblVecBatch1 =
        CreateVectorBatch(bufferedTblTypes, bufferedTblSize, bufferedTblDataCol1, bufferedTblDataCol2);
    // need add buffered table data
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatch1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    // add eof flag to buffered table , need add streamed table data
    VectorBatch *bufferedTblVecBatchEof = CreateEmptyVectorBatch(bufferTypesVector);
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatchEof);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_STREAM_TBL_DATA));

    // add eof flag to streamed table
    VectorBatch *streamedTblVecBatchEof = CreateEmptyVectorBatch(streamTypeVector);
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchEof);
    ASSERT_EQ(DecodeFetchFlag(addInputRetCode), static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_FETCH_JOIN_DATA));

    std::vector<omniruntime::vec::VectorBatch *> result;
    streamedTblWithExprOperator->GetOutput(result);

    auto streamedTblVecBatchEof1 = CreateEmptyVectorBatch(streamTypeVector);
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchEof1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode), static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_SCAN_FINISH));

    // check the join result
    long resultCol1[] =  {1100, 2200, 3300, 4400, 5500, 6600};
    double resultCol2[] =  {1.1, 2.2, 3.3, 4.4, 5.5, 6.6};
    int32_t index = 0;
    for (uint32_t i = 0; i < result.size(); i++) {
        ASSERT_EQ(result[i]->GetVectorCount(), 2);
        ASSERT_EQ(result[i]->GetVector(0)->GetTypeId(), OMNI_LONG);
        ASSERT_EQ(result[i]->GetVector(1)->GetTypeId(), OMNI_DOUBLE);
        ASSERT_EQ(result[i]->GetRowCount(), sizeof(resultCol1) / sizeof(resultCol1[0]));
        for (auto j = 0; j < result[i]->GetRowCount(); j++) {
            if (resultCol1[index] != 0) {
                long longValue = (static_cast<LongVector *>(result[i]->GetVector(0)))->GetValue(j);
                ASSERT_EQ(longValue, resultCol1[index]);
            } else {
                ASSERT_EQ((static_cast<LongVector *>(result[i]->GetVector(0)))->IsValueNull(j), true);
            }
            if (resultCol2[index] != 0) {
                double doubleValue = (static_cast<DoubleVector *>(result[i]->GetVector(1)))->GetValue(j);
                ASSERT_EQ(doubleValue, resultCol2[index]);
            } else {
                ASSERT_EQ((static_cast<DoubleVector *>(result[i]->GetVector(1)))->IsValueNull(j), true);
            }
            index++;
        }
        VectorHelper::FreeVecBatch(result[i]);
    }

    Expr::DeleteExprs(streamedEqualKeyExprs);
    Expr::DeleteExprs(bufferedEqualKeyExprs);
    omniruntime::op::Operator::DeleteOperator(bufferedTblWithExprOperator);
    omniruntime::op::Operator::DeleteOperator(streamedTblWithExprOperator);
    DeleteOperatorFactory(bufferedWithExprOperatorFactory);
    DeleteOperatorFactory(streamedWithExprOperatorFactory);
    delete overflowConfig;
}

TEST(SMJ_JOIN_OPERATOR_WITH_EXPR_TESTCASE, testSmjFullOuterJoinMissMatchWithNullJoinkKeyFirst)
{
    // select t1.b, t2.c from t1 full join t2 where t1.a = t2.d
    // bufferedTbl t2: double c, int d;
    // streamedTbl t1:  int a, Long b;
    std::string blank = "";
    std::vector<DataTypePtr> streamTypeVector = { IntType(), LongType() };
    DataTypes streamedTblTypes(streamTypeVector);
    FieldExpr *col0 = new FieldExpr(0, IntType());
    std::vector<Expr *> streamedEqualKeyExprs = { col0 };
    auto overflowConfig = new OverflowConfig();
    int streamedOutputCols[1] = {1};
    StreamedTableWithExprOperatorFactory *streamedWithExprOperatorFactory =
        StreamedTableWithExprOperatorFactory::CreateStreamedTableWithExprOperatorFactory(streamedTblTypes,
        streamedEqualKeyExprs, 1, streamedOutputCols, 1, JoinType::OMNI_JOIN_TYPE_FULL, blank, overflowConfig);
    omniruntime::op::Operator *streamedTblWithExprOperator = CreateTestOperator(streamedWithExprOperatorFactory);

    std::vector<DataTypePtr> bufferTypesVector = { DoubleType(), IntType() };
    DataTypes bufferedTblTypes(bufferTypesVector);
    FieldExpr *col1 = new FieldExpr(1, IntType());
    std::vector<Expr *> bufferedEqualKeyExprs = { col1 };
    int bufferedOutputCols[1] = {0};
    int64_t streamedWithExprOperatorFactoryAddr = reinterpret_cast<int64_t>(streamedWithExprOperatorFactory);
    BufferedTableWithExprOperatorFactory *bufferedWithExprOperatorFactory =
        BufferedTableWithExprOperatorFactory::CreateBufferedTableWithExprOperatorFactory(bufferedTblTypes,
        bufferedEqualKeyExprs, 1, bufferedOutputCols, 1, streamedWithExprOperatorFactoryAddr, overflowConfig);
    omniruntime::op::Operator *bufferedTblWithExprOperator = CreateTestOperator(bufferedWithExprOperatorFactory);

    // construct data
    const int32_t streamedTblDataSize = 6;
    int32_t streamedTblDataCol1[streamedTblDataSize] = {1, 2, 3, 4, 7, 9};
    long streamedTblDataCol2[streamedTblDataSize] =  {1100, 2200, 3300, 4400, 7700, 9900};
    VectorBatch *streamedTblVecBatch1 =
        CreateVectorBatch(streamedTblTypes, streamedTblDataSize, streamedTblDataCol1, streamedTblDataCol2);
    streamedTblVecBatch1->GetVector(0)->SetValueNull(0); // NULL, 2, 4, 5, 7, 9

    int32_t addInputRetCode = -1;
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatch1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    const int32_t bufferedTblSize = 6;
    double bufferedTblDataCol1[bufferedTblSize] =  {1.1, 2.2, 3.3, 4.4, 6.6, 8.8};
    int32_t bufferedTblDataCol2[bufferedTblSize] =  {1, 2, 3, 4, 6, 8};
    VectorBatch *bufferedTblVecBatch1 =
        CreateVectorBatch(bufferedTblTypes, bufferedTblSize, bufferedTblDataCol1, bufferedTblDataCol2);
    bufferedTblVecBatch1->GetVector(1)->SetValueNull(0); // NULL, 2, 3, 4, 6, 8

    // need add buffered table data
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatch1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    // add eof flag to buffered table , need add streamed table data
    VectorBatch *bufferedTblVecBatchEof = CreateEmptyVectorBatch(bufferTypesVector);
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatchEof);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_STREAM_TBL_DATA));

    // add eof flag to streamed table
    VectorBatch *streamedTblVecBatchEof = CreateEmptyVectorBatch(streamTypeVector);
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchEof);
    ASSERT_EQ(DecodeFetchFlag(addInputRetCode), static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_FETCH_JOIN_DATA));

    std::vector<omniruntime::vec::VectorBatch *> result;
    streamedTblWithExprOperator->GetOutput(result);

    auto streamedTblVecBatchEof1 = CreateEmptyVectorBatch(streamTypeVector);
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchEof1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode), static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_SCAN_FINISH));

    // check the join result
    long resultCol1[] =  {1100, 0, 2200, 3300, 4400, 0, 7700, 0, 9900};
    double resultCol2[] =  {0, 1.1, 2.2, 3.3, 4.4, 6.6, 0, 8.8, 0};
    int32_t index = 0;
    for (uint32_t i = 0; i < result.size(); i++) {
        ASSERT_EQ(result[i]->GetVectorCount(), 2);
        ASSERT_EQ(result[i]->GetVector(0)->GetTypeId(), OMNI_LONG);
        ASSERT_EQ(result[i]->GetVector(1)->GetTypeId(), OMNI_DOUBLE);
        ASSERT_EQ(result[i]->GetRowCount(), sizeof(resultCol1) / sizeof(resultCol1[0]));
        for (auto j = 0; j < result[i]->GetRowCount(); j++) {
            if (resultCol1[index] != 0) {
                long longValue = (static_cast<LongVector *>(result[i]->GetVector(0)))->GetValue(j);
                ASSERT_EQ(longValue, resultCol1[index]);
            } else {
                ASSERT_EQ((static_cast<LongVector *>(result[i]->GetVector(0)))->IsValueNull(j), true);
            }
            if (resultCol2[index] != 0) {
                double doubleValue = (static_cast<DoubleVector *>(result[i]->GetVector(1)))->GetValue(j);
                ASSERT_EQ(doubleValue, resultCol2[index]);
            } else {
                ASSERT_EQ((static_cast<DoubleVector *>(result[i]->GetVector(1)))->IsValueNull(j), true);
            }
            index++;
        }
        VectorHelper::FreeVecBatch(result[i]);
    }

    Expr::DeleteExprs(streamedEqualKeyExprs);
    Expr::DeleteExprs(bufferedEqualKeyExprs);
    omniruntime::op::Operator::DeleteOperator(bufferedTblWithExprOperator);
    omniruntime::op::Operator::DeleteOperator(streamedTblWithExprOperator);
    DeleteOperatorFactory(bufferedWithExprOperatorFactory);
    DeleteOperatorFactory(streamedWithExprOperatorFactory);
    delete overflowConfig;
}

TEST(SMJ_JOIN_OPERATOR_WITH_EXPR_TESTCASE, testSmjFullOuterJoinMissMatchWith2NullJoinkKeyFirst)
{
    // select t1.b, t2.c from t1 full join t2 where t1.a = t2.d
    // bufferedTbl t2: double c, int d;
    // streamedTbl t1:  int a, Long b;
    std::string blank = "";
    std::vector<DataTypePtr> streamTypeVector = { IntType(), LongType() };
    DataTypes streamedTblTypes(streamTypeVector);
    FieldExpr *col0 = new FieldExpr(0, IntType());
    std::vector<Expr *> streamedEqualKeyExprs = { col0 };
    auto overflowConfig = new OverflowConfig();
    int streamedOutputCols[1] = {1};
    StreamedTableWithExprOperatorFactory *streamedWithExprOperatorFactory =
        StreamedTableWithExprOperatorFactory::CreateStreamedTableWithExprOperatorFactory(streamedTblTypes,
        streamedEqualKeyExprs, 1, streamedOutputCols, 1, JoinType::OMNI_JOIN_TYPE_FULL, blank, overflowConfig);
    omniruntime::op::Operator *streamedTblWithExprOperator = CreateTestOperator(streamedWithExprOperatorFactory);

    std::vector<DataTypePtr> bufferTypesVector = { DoubleType(), IntType() };
    DataTypes bufferedTblTypes(bufferTypesVector);
    FieldExpr *col1 = new FieldExpr(1, IntType());
    std::vector<Expr *> bufferedEqualKeyExprs = { col1 };
    int bufferedOutputCols[1] = {0};
    int64_t streamedWithExprOperatorFactoryAddr = reinterpret_cast<int64_t>(streamedWithExprOperatorFactory);
    BufferedTableWithExprOperatorFactory *bufferedWithExprOperatorFactory =
        BufferedTableWithExprOperatorFactory::CreateBufferedTableWithExprOperatorFactory(bufferedTblTypes,
        bufferedEqualKeyExprs, 1, bufferedOutputCols, 1, streamedWithExprOperatorFactoryAddr, overflowConfig);
    omniruntime::op::Operator *bufferedTblWithExprOperator = CreateTestOperator(bufferedWithExprOperatorFactory);

    // construct data
    const int32_t streamedTblDataSize = 6;
    int32_t streamedTblDataCol1[streamedTblDataSize] = {1, 2, 3, 4, 5, 6};
    long streamedTblDataCol2[streamedTblDataSize] =  {1100, 2200, 3300, 4400, 5500, 6600};
    VectorBatch *streamedTblVecBatch1 =
        CreateVectorBatch(streamedTblTypes, streamedTblDataSize, streamedTblDataCol1, streamedTblDataCol2);
    streamedTblVecBatch1->GetVector(0)->SetValueNull(0); // NULL, NULL, 3, 4, 5, 6
    streamedTblVecBatch1->GetVector(0)->SetValueNull(1); // NULL, NULL, 3, 4, 5, 6

    int32_t addInputRetCode = -1;
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatch1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    const int32_t bufferedTblSize = 6;
    double bufferedTblDataCol1[bufferedTblSize] =  {1.1, 2.2, 3.3, 4.4, 5.5, 6.6};
    int32_t bufferedTblDataCol2[bufferedTblSize] =  {1, 2, 3, 4, 5, 6};
    VectorBatch *bufferedTblVecBatch1 =
        CreateVectorBatch(bufferedTblTypes, bufferedTblSize, bufferedTblDataCol1, bufferedTblDataCol2);
    bufferedTblVecBatch1->GetVector(1)->SetValueNull(0); // NULL, NULL, 3, 4, 5, 6
    bufferedTblVecBatch1->GetVector(1)->SetValueNull(1); // NULL, NULL, 3, 4, 5, 6

    // need add buffered table data
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatch1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    // add eof flag to buffered table , need add streamed table data
    VectorBatch *bufferedTblVecBatchEof = CreateEmptyVectorBatch(bufferTypesVector);
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatchEof);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_STREAM_TBL_DATA));

    // add eof flag to streamed table
    VectorBatch *streamedTblVecBatchEof = CreateEmptyVectorBatch(streamTypeVector);
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchEof);
    ASSERT_EQ(DecodeFetchFlag(addInputRetCode), static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_FETCH_JOIN_DATA));

    std::vector<omniruntime::vec::VectorBatch *> result;
    streamedTblWithExprOperator->GetOutput(result);

    auto streamedTblVecBatchEof1 = CreateEmptyVectorBatch(streamTypeVector);
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchEof1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode), static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_SCAN_FINISH));

    // check the join result
    long resultCol1[] =  {1100, 2200, 0, 0, 3300, 4400, 5500, 6600};
    double resultCol2[] =  {0, 0, 1.1, 2.2, 3.3, 4.4, 5.5, 6.6};
    int32_t index = 0;
    for (uint32_t i = 0; i < result.size(); i++) {
        ASSERT_EQ(result[i]->GetVectorCount(), 2);
        ASSERT_EQ(result[i]->GetVector(0)->GetTypeId(), OMNI_LONG);
        ASSERT_EQ(result[i]->GetVector(1)->GetTypeId(), OMNI_DOUBLE);
        ASSERT_EQ(result[i]->GetRowCount(), sizeof(resultCol1) / sizeof(resultCol1[0]));
        for (auto j = 0; j < result[i]->GetRowCount(); j++) {
            if (resultCol1[index] != 0) {
                long longValue = (static_cast<LongVector *>(result[i]->GetVector(0)))->GetValue(j);
                ASSERT_EQ(longValue, resultCol1[index]);
            } else {
                ASSERT_EQ((static_cast<LongVector *>(result[i]->GetVector(0)))->IsValueNull(j), true);
            }
            if (resultCol2[index] != 0) {
                double doubleValue = (static_cast<DoubleVector *>(result[i]->GetVector(1)))->GetValue(j);
                ASSERT_EQ(doubleValue, resultCol2[index]);
            } else {
                ASSERT_EQ((static_cast<DoubleVector *>(result[i]->GetVector(1)))->IsValueNull(j), true);
            }
            index++;
        }
        VectorHelper::FreeVecBatch(result[i]);
    }

    Expr::DeleteExprs(streamedEqualKeyExprs);
    Expr::DeleteExprs(bufferedEqualKeyExprs);
    omniruntime::op::Operator::DeleteOperator(bufferedTblWithExprOperator);
    omniruntime::op::Operator::DeleteOperator(streamedTblWithExprOperator);
    DeleteOperatorFactory(bufferedWithExprOperatorFactory);
    DeleteOperatorFactory(streamedWithExprOperatorFactory);
    delete overflowConfig;
}

TEST(SMJ_JOIN_OPERATOR_WITH_EXPR_TESTCASE, testSmjFullOuterJoinkKeyLast)
{
    // select t1.b, t2.c from t1 full join t2 where t1.a = t2.d
    // bufferedTbl t2: double c, int d;
    // streamedTbl t1:  int a, Long b;
    std::string blank = "";
    std::vector<DataTypePtr> streamTypeVector = { IntType(), LongType() };
    DataTypes streamedTblTypes(streamTypeVector);
    FieldExpr *col0 = new FieldExpr(0, IntType());
    std::vector<Expr *> streamedEqualKeyExprs = { col0 };
    auto overflowConfig = new OverflowConfig();
    int streamedOutputCols[1] = {1};
    StreamedTableWithExprOperatorFactory *streamedWithExprOperatorFactory =
        StreamedTableWithExprOperatorFactory::CreateStreamedTableWithExprOperatorFactory(streamedTblTypes,
        streamedEqualKeyExprs, 1, streamedOutputCols, 1, JoinType::OMNI_JOIN_TYPE_FULL, blank, overflowConfig);
    omniruntime::op::Operator *streamedTblWithExprOperator = CreateTestOperator(streamedWithExprOperatorFactory);

    std::vector<DataTypePtr> bufferTypesVector = { DoubleType(), IntType() };
    DataTypes bufferedTblTypes(bufferTypesVector);
    FieldExpr *col1 = new FieldExpr(1, IntType());
    std::vector<Expr *> bufferedEqualKeyExprs = { col1 };
    int bufferedOutputCols[1] = {0};
    int64_t streamedWithExprOperatorFactoryAddr = reinterpret_cast<int64_t>(streamedWithExprOperatorFactory);
    BufferedTableWithExprOperatorFactory *bufferedWithExprOperatorFactory =
        BufferedTableWithExprOperatorFactory::CreateBufferedTableWithExprOperatorFactory(bufferedTblTypes,
        bufferedEqualKeyExprs, 1, bufferedOutputCols, 1, streamedWithExprOperatorFactoryAddr, overflowConfig);
    omniruntime::op::Operator *bufferedTblWithExprOperator = CreateTestOperator(bufferedWithExprOperatorFactory);

    // construct data
    const int32_t streamedTblDataSize = 5;
    int32_t streamedTblDataCol1[streamedTblDataSize] = {1, 2, 3, 4, 5};
    long streamedTblDataCol2[streamedTblDataSize] =  {1100, 2200, 3300, 4400, 5500};
    VectorBatch *streamedTblVecBatch1 =
        CreateVectorBatch(streamedTblTypes, streamedTblDataSize, streamedTblDataCol1, streamedTblDataCol2);

    int32_t addInputRetCode = -1;
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatch1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    const int32_t bufferedTblSize = 5;
    double bufferedTblDataCol1[bufferedTblSize] =  {1.1, 2.2, 3.3, 4.4, 8.8};
    int32_t bufferedTblDataCol2[bufferedTblSize] =  {1, 2, 3, 4, 8};
    VectorBatch *bufferedTblVecBatch1 =
        CreateVectorBatch(bufferedTblTypes, bufferedTblSize, bufferedTblDataCol1, bufferedTblDataCol2);

    // need add buffered table data
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatch1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_STREAM_TBL_DATA));


    // add eof flag to streamed table
    VectorBatch *streamedTblVecBatchEof = CreateEmptyVectorBatch(streamTypeVector);
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchEof);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    // add eof flag to buffered table , need add streamed table data
    VectorBatch *bufferedTblVecBatchEof = CreateEmptyVectorBatch(bufferTypesVector);
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatchEof);
    ASSERT_EQ(DecodeFetchFlag(addInputRetCode), static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_FETCH_JOIN_DATA));

    std::vector<omniruntime::vec::VectorBatch *> result;
    streamedTblWithExprOperator->GetOutput(result);

    auto streamedTblVecBatchEof1 = CreateEmptyVectorBatch(streamTypeVector);
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchEof1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode), static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_SCAN_FINISH));

    // check the join result
    long resultCol1[] =  {1100, 2200, 3300, 4400, 5500, 0};
    double resultCol2[] =  {1.1, 2.2, 3.3, 4.4, 0, 8.8};
    int32_t index = 0;
    for (uint32_t i = 0; i < result.size(); i++) {
        ASSERT_EQ(result[i]->GetVectorCount(), 2);
        ASSERT_EQ(result[i]->GetVector(0)->GetTypeId(), OMNI_LONG);
        ASSERT_EQ(result[i]->GetVector(1)->GetTypeId(), OMNI_DOUBLE);
        ASSERT_EQ(result[i]->GetRowCount(), sizeof(resultCol1) / sizeof(resultCol1[0]));
        for (auto j = 0; j < result[i]->GetRowCount(); j++) {
            if (resultCol1[index] != 0) {
                long longValue = (static_cast<LongVector *>(result[i]->GetVector(0)))->GetValue(j);
                ASSERT_EQ(longValue, resultCol1[index]);
            } else {
                ASSERT_EQ((static_cast<LongVector *>(result[i]->GetVector(0)))->IsValueNull(j), true);
            }
            if (resultCol2[index] != 0) {
                double doubleValue = (static_cast<DoubleVector *>(result[i]->GetVector(1)))->GetValue(j);
                ASSERT_EQ(doubleValue, resultCol2[index]);
            } else {
                ASSERT_EQ((static_cast<DoubleVector *>(result[i]->GetVector(1)))->IsValueNull(j), true);
            }
            index++;
        }
        VectorHelper::FreeVecBatch(result[i]);
    }

    Expr::DeleteExprs(streamedEqualKeyExprs);
    Expr::DeleteExprs(bufferedEqualKeyExprs);
    omniruntime::op::Operator::DeleteOperator(bufferedTblWithExprOperator);
    omniruntime::op::Operator::DeleteOperator(streamedTblWithExprOperator);
    DeleteOperatorFactory(bufferedWithExprOperatorFactory);
    DeleteOperatorFactory(streamedWithExprOperatorFactory);
    delete overflowConfig;
}

TEST(SMJ_JOIN_OPERATOR_WITH_EXPR_TESTCASE, testSmjFullJoinNullValuesWithMutilColumBatch)
{
    // select t1.b, t2.c from t1 left join t2 where t1.a = t2.d
    // bufferedTbl t2: double c, int d;
    // streamedTbl t1:  int a, Long b;
    std::string blank = "";
    std::vector<DataTypePtr> streamTypeVector = { IntType(), LongType() };
    DataTypes streamedTblTypes(streamTypeVector);
    FieldExpr *col0 = new FieldExpr(0, IntType());
    std::vector<Expr *> streamedEqualKeyExprs = { col0 };
    auto overflowConfig = new OverflowConfig();
    int streamedOutputCols[1] = {1};
    StreamedTableWithExprOperatorFactory *streamedWithExprOperatorFactory =
        StreamedTableWithExprOperatorFactory::CreateStreamedTableWithExprOperatorFactory(streamedTblTypes,
        streamedEqualKeyExprs, 1, streamedOutputCols, 1, JoinType::OMNI_JOIN_TYPE_FULL, blank, overflowConfig);
    omniruntime::op::Operator *streamedTblWithExprOperator = CreateTestOperator(streamedWithExprOperatorFactory);

    std::vector<DataTypePtr> bufferTypesVector = { DoubleType(), IntType() };
    DataTypes bufferedTblTypes(bufferTypesVector);
    FieldExpr *col1 = new FieldExpr(1, IntType());
    std::vector<Expr *> bufferedEqualKeyExprs = { col1 };
    int bufferedOutputCols[1] = {0};
    int64_t streamedWithExprOperatorFactoryAddr = reinterpret_cast<int64_t>(streamedWithExprOperatorFactory);
    BufferedTableWithExprOperatorFactory *bufferedWithExprOperatorFactory =
        BufferedTableWithExprOperatorFactory::CreateBufferedTableWithExprOperatorFactory(bufferedTblTypes,
        bufferedEqualKeyExprs, 1, bufferedOutputCols, 1, streamedWithExprOperatorFactoryAddr, overflowConfig);
    omniruntime::op::Operator *bufferedTblWithExprOperator = CreateTestOperator(bufferedWithExprOperatorFactory);

    int32_t streamedCol1Row1[1] = {1};
    long streamedCol2Row1[1] = {1100};
    int32_t streamedCol1Row2[1] = {2};
    long streamedCol2Row2[1] = {2200};
    int32_t streamedCol1Row3[1] = {3};
    long streamedCol2Row3[1] = {3300};
    int32_t streamedCol1Row4[1] = {4};
    long streamedCol2Row4[1] = {4400};
    int32_t streamedCol1Row5[1] = {5};
    long streamedCol2Row5[1] = {5500};
    int32_t streamedCol1Row6[1] = {7};
    long streamedCol2Row6[1] = {7700};
    // construct streamed data
    VectorBatch *streamedTblVecBatchRow1 = CreateVectorBatch(streamedTblTypes, 1, streamedCol1Row1, streamedCol2Row1);
    VectorBatch *streamedTblVecBatchRow2 = CreateVectorBatch(streamedTblTypes, 1, streamedCol1Row2, streamedCol2Row2);
    VectorBatch *streamedTblVecBatchRow3 = CreateVectorBatch(streamedTblTypes, 1, streamedCol1Row3, streamedCol2Row3);
    VectorBatch *streamedTblVecBatchRow4 = CreateVectorBatch(streamedTblTypes, 1, streamedCol1Row4, streamedCol2Row4);
    VectorBatch *streamedTblVecBatchRow5 = CreateVectorBatch(streamedTblTypes, 1, streamedCol1Row5, streamedCol2Row5);
    VectorBatch *streamedTblVecBatchRow6 = CreateVectorBatch(streamedTblTypes, 1, streamedCol1Row6, streamedCol2Row6);
    streamedTblVecBatchRow1->GetVector(0)->SetValueNull(0);
    streamedTblVecBatchRow2->GetVector(0)->SetValueNull(0);
    streamedTblVecBatchRow3->GetVector(0)->SetValueNull(0);

    double bufferedCol1Row1[1] = {1.1};
    int32_t bufferedCol2Row1[1] = {1};
    double bufferedCol1Row2[1] = {2.2};
    int32_t bufferedCol2Row2[1] = {2};
    double bufferedCol1Row3[1] = {3.3};
    int32_t bufferedCol2Row3[1] = {3};
    double bufferedCol1Row4[1] = {4.4};
    int32_t bufferedCol2Row4[1] = {4};
    double bufferedCol1Row5[1] = {6.6};
    int32_t bufferedCol2Row5[1] = {6};
    double bufferedCol1Row6[1] = {8.8};
    int32_t bufferedCol2Row6[1] = {8};
    // construct buffered data
    VectorBatch *bufferedTblVecBatchRow1 = CreateVectorBatch(bufferedTblTypes, 1, bufferedCol1Row1, bufferedCol2Row1);
    VectorBatch *bufferedTblVecBatchRow2 = CreateVectorBatch(bufferedTblTypes, 1, bufferedCol1Row2, bufferedCol2Row2);
    VectorBatch *bufferedTblVecBatchRow3 = CreateVectorBatch(bufferedTblTypes, 1, bufferedCol1Row3, bufferedCol2Row3);
    VectorBatch *bufferedTblVecBatchRow4 = CreateVectorBatch(bufferedTblTypes, 1, bufferedCol1Row4, bufferedCol2Row4);
    VectorBatch *bufferedTblVecBatchRow5 = CreateVectorBatch(bufferedTblTypes, 1, bufferedCol1Row5, bufferedCol2Row5);
    VectorBatch *bufferedTblVecBatchRow6 = CreateVectorBatch(bufferedTblTypes, 1, bufferedCol1Row6, bufferedCol2Row6);
    bufferedTblVecBatchRow1->GetVector(1)->SetValueNull(0);
    bufferedTblVecBatchRow2->GetVector(1)->SetValueNull(0);
    bufferedTblVecBatchRow3->GetVector(1)->SetValueNull(0);

    int32_t addInputRetCode = -1;
    // join start add streamed table data
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchRow1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    // need add buffered table data
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatchRow1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_STREAM_TBL_DATA));

    // need add streamed table data
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchRow2);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_STREAM_TBL_DATA));

    // need add streamed table data
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchRow3);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_STREAM_TBL_DATA));

    // need add streamed table data
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchRow4);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    // need add buffered table data
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatchRow2);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    // need add buffered table data
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatchRow3);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    // need add buffered table data
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatchRow4);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    // need add buffered table data
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatchRow5);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_STREAM_TBL_DATA));

    // need add streamed table data
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchRow5);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_STREAM_TBL_DATA));

    // need add streamed table data
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchRow6);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    // need add buffered table data
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatchRow6);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_STREAM_TBL_DATA));

    // add eof flag to streamed table
    VectorBatch *streamedTblVecBatchEof = CreateEmptyVectorBatch(streamTypeVector);
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchEof);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    // add eof flag to buffered table , need add streamed table data
    VectorBatch *bufferedTblVecBatchEof = CreateEmptyVectorBatch(bufferTypesVector);
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatchEof);
    ASSERT_EQ(DecodeFetchFlag(addInputRetCode), static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_FETCH_JOIN_DATA));

    std::vector<omniruntime::vec::VectorBatch *> result;
    streamedTblWithExprOperator->GetOutput(result);

    // add eof flag to streamed table
    VectorBatch *streamedTblVecBatchEof1 = CreateEmptyVectorBatch(streamTypeVector);
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchEof1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode), static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_SCAN_FINISH));

    // check the join result
    long resultCol1[] =  {1100, 2200, 3300, 0, 0, 0, 4400, 5500, 0, 7700, 0};
    double resultCol2[] =  {0, 0, 0, 1.1, 2.2, 3.3, 4.4, 0, 6.6, 0, 8.8};
    int32_t index = 0;
    for (uint32_t i = 0; i < result.size(); i++) {
        ASSERT_EQ(result[i]->GetVectorCount(), 2);
        ASSERT_EQ(result[i]->GetVector(0)->GetTypeId(), OMNI_LONG);
        ASSERT_EQ(result[i]->GetVector(1)->GetTypeId(), OMNI_DOUBLE);
        ASSERT_EQ(result[i]->GetRowCount(), sizeof(resultCol1) / sizeof(resultCol1[0]));
        for (auto j = 0; j < result[i]->GetRowCount(); j++) {
            if (resultCol1[index] != 0) {
                long longValue = (static_cast<LongVector *>(result[i]->GetVector(0)))->GetValue(j);
                ASSERT_EQ(longValue, resultCol1[index]);
            } else {
                ASSERT_EQ((static_cast<LongVector *>(result[i]->GetVector(0)))->IsValueNull(j), true);
            }
            if (resultCol2[index] != 0) {
                double doubleValue = (static_cast<DoubleVector *>(result[i]->GetVector(1)))->GetValue(j);
                ASSERT_EQ(doubleValue, resultCol2[index]);
            } else {
                ASSERT_EQ((static_cast<DoubleVector *>(result[i]->GetVector(1)))->IsValueNull(j), true);
            }
            index++;
        }
        VectorHelper::FreeVecBatch(result[i]);
    }

    Expr::DeleteExprs(streamedEqualKeyExprs);
    Expr::DeleteExprs(bufferedEqualKeyExprs);
    omniruntime::op::Operator::DeleteOperator(bufferedTblWithExprOperator);
    omniruntime::op::Operator::DeleteOperator(streamedTblWithExprOperator);
    DeleteOperatorFactory(bufferedWithExprOperatorFactory);
    DeleteOperatorFactory(streamedWithExprOperatorFactory);
    delete overflowConfig;
}

TEST(SMJ_JOIN_OPERATOR_WITH_EXPR_TESTCASE, testSmjFullOuterJoinStreamedWithEmptyBuffered)
{
    // select t1.b, t2.c from t1 left join t2 where t1.a = t2.d
    // bufferedTbl t2: double c, int d;
    // streamedTbl t1:  int a, Long b;
    std::string blank = "";
    std::vector<DataTypePtr> streamTypeVector = { IntType(), LongType() };
    DataTypes streamedTblTypes(streamTypeVector);
    FieldExpr *col0 = new FieldExpr(0, IntType());
    std::vector<Expr *> streamedEqualKeyExprs = { col0 };
    auto overflowConfig = new OverflowConfig();
    int streamedOutputCols[1] = {1};
    StreamedTableWithExprOperatorFactory *streamedWithExprOperatorFactory =
        StreamedTableWithExprOperatorFactory::CreateStreamedTableWithExprOperatorFactory(streamedTblTypes,
        streamedEqualKeyExprs, 1, streamedOutputCols, 1, JoinType::OMNI_JOIN_TYPE_FULL, blank, overflowConfig);
    omniruntime::op::Operator *streamedTblWithExprOperator = CreateTestOperator(streamedWithExprOperatorFactory);

    std::vector<DataTypePtr> bufferTypesVector = { DoubleType(), IntType() };
    DataTypes bufferedTblTypes(bufferTypesVector);
    FieldExpr *col1 = new FieldExpr(1, IntType());
    std::vector<Expr *> bufferedEqualKeyExprs = { col1 };
    int bufferedOutputCols[1] = {0};
    int64_t streamedWithExprOperatorFactoryAddr = reinterpret_cast<int64_t>(streamedWithExprOperatorFactory);
    BufferedTableWithExprOperatorFactory *bufferedWithExprOperatorFactory =
        BufferedTableWithExprOperatorFactory::CreateBufferedTableWithExprOperatorFactory(bufferedTblTypes,
        bufferedEqualKeyExprs, 1, bufferedOutputCols, 1, streamedWithExprOperatorFactoryAddr, overflowConfig);
    omniruntime::op::Operator *bufferedTblWithExprOperator = CreateTestOperator(bufferedWithExprOperatorFactory);

    // construct data
    const int32_t streamedTblDataSize = 6;
    int32_t streamedTblDataCol1[streamedTblDataSize] = {1, 2, 3, 4, 5, 6};
    long streamedTblDataCol2[streamedTblDataSize] =  {1100, 2200, 3300, 4400, 5500, 6600};
    VectorBatch *streamedTblVecBatch1 =
        CreateVectorBatch(streamedTblTypes, streamedTblDataSize, streamedTblDataCol1, streamedTblDataCol2);

    int32_t addInputRetCode = -1;
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatch1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    // add eof flag to buffered table , need add streamed table data
    VectorBatch *bufferedTblVecBatchEof = CreateEmptyVectorBatch(bufferTypesVector);
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatchEof);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_STREAM_TBL_DATA));

    // add eof flag to streamed table
    VectorBatch *streamedTblVecBatchEof = CreateEmptyVectorBatch(streamTypeVector);
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchEof);
    ASSERT_EQ(DecodeFetchFlag(addInputRetCode), static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_FETCH_JOIN_DATA));

    std::vector<omniruntime::vec::VectorBatch *> result;
    streamedTblWithExprOperator->GetOutput(result);

    // add eof flag to streamed table
    VectorBatch *streamedTblVecBatchEof1 = CreateEmptyVectorBatch(streamTypeVector);
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchEof1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode), static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_SCAN_FINISH));

    // check the join result
    long resultCol1[] =  {1100, 2200, 3300, 4400, 5500, 6600};
    double resultCol2[] =  {0, 0, 0, 0, 0, 0};
    int32_t index = 0;
    for (uint32_t i = 0; i < result.size(); i++) {
        ASSERT_EQ(result[i]->GetVectorCount(), 2);
        ASSERT_EQ(result[i]->GetVector(0)->GetTypeId(), OMNI_LONG);
        ASSERT_EQ(result[i]->GetVector(1)->GetTypeId(), OMNI_DOUBLE);
        ASSERT_EQ(result[i]->GetRowCount(), sizeof(resultCol1) / sizeof(resultCol1[0]));
        for (auto j = 0; j < result[i]->GetRowCount(); j++) {
            if (resultCol1[index] != 0) {
                long longValue = (static_cast<LongVector *>(result[i]->GetVector(0)))->GetValue(j);
                ASSERT_EQ(longValue, resultCol1[index]);
            } else {
                ASSERT_EQ((static_cast<LongVector *>(result[i]->GetVector(0)))->IsValueNull(j), true);
            }
            if (resultCol2[index] != 0) {
                double doubleValue = (static_cast<DoubleVector *>(result[i]->GetVector(1)))->GetValue(j);
                ASSERT_EQ(doubleValue, resultCol2[index]);
            } else {
                ASSERT_EQ((static_cast<DoubleVector *>(result[i]->GetVector(1)))->IsValueNull(j), true);
            }
            index++;
        }
        VectorHelper::FreeVecBatch(result[i]);
    }

    Expr::DeleteExprs(streamedEqualKeyExprs);
    Expr::DeleteExprs(bufferedEqualKeyExprs);
    omniruntime::op::Operator::DeleteOperator(bufferedTblWithExprOperator);
    omniruntime::op::Operator::DeleteOperator(streamedTblWithExprOperator);
    DeleteOperatorFactory(bufferedWithExprOperatorFactory);
    DeleteOperatorFactory(streamedWithExprOperatorFactory);
    delete overflowConfig;
}

TEST(SMJ_JOIN_OPERATOR_WITH_EXPR_TESTCASE, testSmjFullOuterJoinEmptyStreamedWithBuffered)
{
    // select t1.b, t2.c from t1 left join t2 where t1.a = t2.d
    // bufferedTbl t2: double c, int d;
    // streamedTbl t1:  int a, Long b;
    std::string blank = "";
    std::vector<DataTypePtr> streamTypeVector = { IntType(), LongType() };
    DataTypes streamedTblTypes(streamTypeVector);
    FieldExpr *col0 = new FieldExpr(0, IntType());
    std::vector<Expr *> streamedEqualKeyExprs = { col0 };
    auto overflowConfig = new OverflowConfig();
    int streamedOutputCols[1] = {1};
    StreamedTableWithExprOperatorFactory *streamedWithExprOperatorFactory =
        StreamedTableWithExprOperatorFactory::CreateStreamedTableWithExprOperatorFactory(streamedTblTypes,
        streamedEqualKeyExprs, 1, streamedOutputCols, 1, JoinType::OMNI_JOIN_TYPE_FULL, blank, overflowConfig);
    omniruntime::op::Operator *streamedTblWithExprOperator = CreateTestOperator(streamedWithExprOperatorFactory);

    std::vector<DataTypePtr> bufferTypesVector = { DoubleType(), IntType() };
    DataTypes bufferedTblTypes(bufferTypesVector);
    FieldExpr *col1 = new FieldExpr(1, IntType());
    std::vector<Expr *> bufferedEqualKeyExprs = { col1 };
    int bufferedOutputCols[1] = {0};
    int64_t streamedWithExprOperatorFactoryAddr = reinterpret_cast<int64_t>(streamedWithExprOperatorFactory);
    BufferedTableWithExprOperatorFactory *bufferedWithExprOperatorFactory =
        BufferedTableWithExprOperatorFactory::CreateBufferedTableWithExprOperatorFactory(bufferedTblTypes,
        bufferedEqualKeyExprs, 1, bufferedOutputCols, 1, streamedWithExprOperatorFactoryAddr, overflowConfig);
    omniruntime::op::Operator *bufferedTblWithExprOperator = CreateTestOperator(bufferedWithExprOperatorFactory);

    // construct data
    const int32_t bufferedTblSize = 6;
    double bufferedTblDataCol1[bufferedTblSize] =  {1.1, 2.2, 3.3, 4.4, 5.5, 6.6};
    int32_t bufferedTblDataCol2[bufferedTblSize] =  {1, 2, 3, 4, 5, 6};
    VectorBatch *bufferedTblVecBatch1 =
        CreateVectorBatch(bufferedTblTypes, bufferedTblSize, bufferedTblDataCol1, bufferedTblDataCol2);

    int32_t addInputRetCode = -1;
    // need add streamed table data
    VectorBatch *streamedTblEmptyVecBatch = CreateEmptyVectorBatch(streamTypeVector);
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblEmptyVecBatch);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    // need add buffered table data
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatch1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    // add eof flag to buffered table , need add streamed table data
    VectorBatch *bufferedTblVecBatchEof = CreateEmptyVectorBatch(bufferTypesVector);
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatchEof);
    ASSERT_EQ(DecodeFetchFlag(addInputRetCode), static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_FETCH_JOIN_DATA));

    std::vector<omniruntime::vec::VectorBatch *> result;
    streamedTblWithExprOperator->GetOutput(result);

    // add eof flag to streamed table
    VectorBatch *streamedTblVecBatchEof1 = CreateEmptyVectorBatch(streamTypeVector);
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchEof1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode), static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_SCAN_FINISH));

    // check the join result
    long resultCol1[] =  {0, 0, 0, 0, 0, 0};
    double resultCol2[] =  {1.1, 2.2, 3.3, 4.4, 5.5, 6.6};
    int32_t index = 0;
    for (uint32_t i = 0; i < result.size(); i++) {
        ASSERT_EQ(result[i]->GetVectorCount(), 2);
        ASSERT_EQ(result[i]->GetVector(0)->GetTypeId(), OMNI_LONG);
        ASSERT_EQ(result[i]->GetVector(1)->GetTypeId(), OMNI_DOUBLE);
        ASSERT_EQ(result[i]->GetRowCount(), sizeof(resultCol1) / sizeof(resultCol1[0]));
        for (auto j = 0; j < result[i]->GetRowCount(); j++) {
            if (resultCol1[index] != 0) {
                long longValue = (static_cast<LongVector *>(result[i]->GetVector(0)))->GetValue(j);
                ASSERT_EQ(longValue, resultCol1[index]);
            } else {
                ASSERT_EQ((static_cast<LongVector *>(result[i]->GetVector(0)))->IsValueNull(j), true);
            }
            if (resultCol2[index] != 0) {
                double doubleValue = (static_cast<DoubleVector *>(result[i]->GetVector(1)))->GetValue(j);
                ASSERT_EQ(doubleValue, resultCol2[index]);
            } else {
                ASSERT_EQ((static_cast<DoubleVector *>(result[i]->GetVector(1)))->IsValueNull(j), true);
            }
            index++;
        }
        VectorHelper::FreeVecBatch(result[i]);
    }

    Expr::DeleteExprs(streamedEqualKeyExprs);
    Expr::DeleteExprs(bufferedEqualKeyExprs);
    omniruntime::op::Operator::DeleteOperator(bufferedTblWithExprOperator);
    omniruntime::op::Operator::DeleteOperator(streamedTblWithExprOperator);
    DeleteOperatorFactory(bufferedWithExprOperatorFactory);
    DeleteOperatorFactory(streamedWithExprOperatorFactory);
    delete overflowConfig;
}

TEST(SMJ_JOIN_OPERATOR_WITH_EXPR_TESTCASE, testSmjFullOuterJoinRepeatRowsAndMutilColumBatch)
{
    // select t1.b, t2.c from t1 left join t2 where t1.a = t2.d
    // bufferedTbl t2: double c, int d;
    // streamedTbl t1:  int a, Long b;
    std::string blank = "";
    std::vector<DataTypePtr> streamTypeVector = { IntType(), LongType() };
    DataTypes streamedTblTypes(streamTypeVector);
    FieldExpr *col0 = new FieldExpr(0, IntType());
    std::vector<Expr *> streamedEqualKeyExprs = { col0 };
    auto overflowConfig = new OverflowConfig();
    int streamedOutputCols[1] = {1};
    StreamedTableWithExprOperatorFactory *streamedWithExprOperatorFactory =
        StreamedTableWithExprOperatorFactory::CreateStreamedTableWithExprOperatorFactory(streamedTblTypes,
        streamedEqualKeyExprs, 1, streamedOutputCols, 1, JoinType::OMNI_JOIN_TYPE_FULL, blank, overflowConfig);
    omniruntime::op::Operator *streamedTblWithExprOperator = CreateTestOperator(streamedWithExprOperatorFactory);

    std::vector<DataTypePtr> bufferTypesVector = { DoubleType(), IntType() };
    DataTypes bufferedTblTypes(bufferTypesVector);
    FieldExpr *col1 = new FieldExpr(1, IntType());
    std::vector<Expr *> bufferedEqualKeyExprs = { col1 };
    int bufferedOutputCols[1] = {0};
    int64_t streamedWithExprOperatorFactoryAddr = reinterpret_cast<int64_t>(streamedWithExprOperatorFactory);
    BufferedTableWithExprOperatorFactory *bufferedWithExprOperatorFactory =
        BufferedTableWithExprOperatorFactory::CreateBufferedTableWithExprOperatorFactory(bufferedTblTypes,
        bufferedEqualKeyExprs, 1, bufferedOutputCols, 1, streamedWithExprOperatorFactoryAddr, overflowConfig);
    omniruntime::op::Operator *bufferedTblWithExprOperator = CreateTestOperator(bufferedWithExprOperatorFactory);

    int32_t streamedCol1Row1[1] = {1};
    long streamedCol2Row1[1] = {1100};
    int32_t streamedCol1Row2[1] = {2};
    long streamedCol2Row2[1] = {2200};
    int32_t streamedCol1Row3[1] = {4};
    long streamedCol2Row3[1] = {4400};
    int32_t streamedCol1Row4[1] = {5};
    long streamedCol2Row4[1] = {5500};
    int32_t streamedCol1Row5[1] = {5};
    long streamedCol2Row5[1] = {5500};
    int32_t streamedCol1Row6[2] = {5, 5};
    long streamedCol2Row6[2] = {5500, 5500};
    int32_t streamedCol1Row7[1] = {6};
    long streamedCol2Row7[1] = {6600};
    // construct streamed data
    VectorBatch *streamedTblVecBatchRow1 = CreateVectorBatch(streamedTblTypes, 1, streamedCol1Row1, streamedCol2Row1);
    VectorBatch *streamedTblVecBatchRow2 = CreateVectorBatch(streamedTblTypes, 1, streamedCol1Row2, streamedCol2Row2);
    VectorBatch *streamedTblVecBatchRow3 = CreateVectorBatch(streamedTblTypes, 1, streamedCol1Row3, streamedCol2Row3);
    VectorBatch *streamedTblVecBatchRow4 = CreateVectorBatch(streamedTblTypes, 1, streamedCol1Row4, streamedCol2Row4);
    VectorBatch *streamedTblVecBatchRow5 = CreateVectorBatch(streamedTblTypes, 1, streamedCol1Row5, streamedCol2Row5);
    VectorBatch *streamedTblVecBatchRow6 = CreateVectorBatch(streamedTblTypes, 2, streamedCol1Row6, streamedCol2Row6);
    VectorBatch *streamedTblVecBatchRow7 = CreateVectorBatch(streamedTblTypes, 1, streamedCol1Row7, streamedCol2Row7);

    double bufferedCol1Row1[1] = {1.1};
    int32_t bufferedCol2Row1[1] = {1};
    double bufferedCol1Row2[1] = {2.2};
    int32_t bufferedCol2Row2[1] = {2};
    double bufferedCol1Row3[1] = {3.3};
    int32_t bufferedCol2Row3[1] = {3};
    double bufferedCol1Row4[1] = {4.4};
    int32_t bufferedCol2Row4[1] = {4};
    double bufferedCol1Row5[1] = {5.5};
    int32_t bufferedCol2Row5[1] = {5};
    double bufferedCol1Row6[2] = {5.5, 5.5};
    int32_t bufferedCol2Row6[2] = {5, 5};
    double bufferedCol1Row7[1] = {5.5};
    int32_t bufferedCol2Row7[1] = {5};
    double bufferedCol1Row8[1] = {6.6};
    int32_t bufferedCol2Row8[1] = {6};
    // construct buffered data
    VectorBatch *bufferedTblVecBatchRow1 = CreateVectorBatch(bufferedTblTypes, 1, bufferedCol1Row1, bufferedCol2Row1);
    VectorBatch *bufferedTblVecBatchRow2 = CreateVectorBatch(bufferedTblTypes, 1, bufferedCol1Row2, bufferedCol2Row2);
    VectorBatch *bufferedTblVecBatchRow3 = CreateVectorBatch(bufferedTblTypes, 1, bufferedCol1Row3, bufferedCol2Row3);
    VectorBatch *bufferedTblVecBatchRow4 = CreateVectorBatch(bufferedTblTypes, 1, bufferedCol1Row4, bufferedCol2Row4);
    VectorBatch *bufferedTblVecBatchRow5 = CreateVectorBatch(bufferedTblTypes, 1, bufferedCol1Row5, bufferedCol2Row5);
    VectorBatch *bufferedTblVecBatchRow6 = CreateVectorBatch(bufferedTblTypes, 2, bufferedCol1Row6, bufferedCol2Row6);
    VectorBatch *bufferedTblVecBatchRow7 = CreateVectorBatch(bufferedTblTypes, 1, bufferedCol1Row7, bufferedCol2Row7);
    VectorBatch *bufferedTblVecBatchRow8 = CreateVectorBatch(bufferedTblTypes, 1, bufferedCol1Row8, bufferedCol2Row8);

    int32_t addInputRetCode = -1;
    // join start add streamed table data
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchRow1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    // need add buffered table data
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatchRow1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    // need add buffered table data
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatchRow2);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_STREAM_TBL_DATA));

    // need add streamed table data
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchRow2);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    // need add buffered table data
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatchRow3);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_STREAM_TBL_DATA));

    // need add streamed table data
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchRow3);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    // need add buffered table data
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatchRow4);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    // need add buffered table data
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatchRow5);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_STREAM_TBL_DATA));

    // need add streamed table data
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchRow4);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    // need add buffered table data
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatchRow6);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    // need add buffered table data
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatchRow7);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    // need add buffered table data
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatchRow8);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_STREAM_TBL_DATA));

    // need add streamed table data
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchRow5);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_STREAM_TBL_DATA));

    // need add streamed table data
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchRow6);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_STREAM_TBL_DATA));

    // need add streamed table data
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchRow7);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    // add eof flag to buffered table , need add streamed table data
    VectorBatch *bufferedTblVecBatchEof = CreateEmptyVectorBatch(bufferTypesVector);
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatchEof);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_STREAM_TBL_DATA));

    // add eof flag to streamed table
    VectorBatch *streamedTblVecBatchEof = CreateEmptyVectorBatch(streamTypeVector);
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchEof);
    ASSERT_EQ(DecodeFetchFlag(addInputRetCode), static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_FETCH_JOIN_DATA));

    std::vector<omniruntime::vec::VectorBatch *> result;
    streamedTblWithExprOperator->GetOutput(result);

    // add eof flag to streamed table
    VectorBatch *streamedTblVecBatchEof1 = CreateEmptyVectorBatch(streamTypeVector);
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchEof1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode), static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_SCAN_FINISH));

    // check the join result
    long resultCol1[] =  {1100, 2200, 0, 4400, 5500, 5500, 5500, 5500, 5500, 5500, 5500, 5500, 5500,
                          5500, 5500, 5500, 5500, 5500, 5500, 5500, 6600};
    double resultCol2[] =  {1.1, 2.2, 3.3, 4.4, 5.5, 5.5, 5.5, 5.5, 5.5, 5.5, 5.5, 5.5, 5.5, 5.5, 5.5, 5.5, 5.5,
                            5.5, 5.5, 5.5, 6.6};
    int32_t index = 0;
    for (uint32_t i = 0; i < result.size(); i++) {
        ASSERT_EQ(result[i]->GetVectorCount(), 2);
        ASSERT_EQ(result[i]->GetVector(0)->GetTypeId(), OMNI_LONG);
        ASSERT_EQ(result[i]->GetVector(1)->GetTypeId(), OMNI_DOUBLE);
        ASSERT_EQ(result[i]->GetRowCount(), sizeof(resultCol1) / sizeof(resultCol1[0]));
        for (auto j = 0; j < result[i]->GetRowCount(); j++) {
            if (resultCol1[index] != 0) {
                long longValue = (static_cast<LongVector *>(result[i]->GetVector(0)))->GetValue(j);
                ASSERT_EQ(longValue, resultCol1[index]);
            } else {
                ASSERT_EQ((static_cast<LongVector *>(result[i]->GetVector(0)))->IsValueNull(j), true);
            }
            if (resultCol2[index] != 0) {
                double doubleValue = (static_cast<DoubleVector *>(result[i]->GetVector(1)))->GetValue(j);
                ASSERT_EQ(doubleValue, resultCol2[index]);
            } else {
                ASSERT_EQ((static_cast<DoubleVector *>(result[i]->GetVector(1)))->IsValueNull(j), true);
            }
            index++;
        }
        VectorHelper::FreeVecBatch(result[i]);
    }

    Expr::DeleteExprs(streamedEqualKeyExprs);
    Expr::DeleteExprs(bufferedEqualKeyExprs);
    omniruntime::op::Operator::DeleteOperator(bufferedTblWithExprOperator);
    omniruntime::op::Operator::DeleteOperator(streamedTblWithExprOperator);
    DeleteOperatorFactory(bufferedWithExprOperatorFactory);
    DeleteOperatorFactory(streamedWithExprOperatorFactory);
    delete overflowConfig;
}

TEST(SMJ_JOIN_OPERATOR_WITH_EXPR_TESTCASE, testSmjFullOuterJoinMissMatchBothSideWithExpression)
{
    // select t1.b, t2.c from t1 full join t2 where t1.a = t2.d and t1.a > 2;
    // bufferedTbl t2: double c, int d;
    // streamedTbl t1:  int a, Long b;
    string filterJsonStr = "{\"exprType\":\"BINARY\","
        "\"returnType\":4,"
        "\"operator\":\"GREATER_THAN\","
        "\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":0},"
        "\"right\":{\"exprType\":\"LITERAL\",\"dataType\":1, \"isNull\":false, \"value\":2}}";
    std::vector<DataTypePtr> streamTypeVector = { IntType(), LongType() };
    DataTypes streamedTblTypes(streamTypeVector);
    FieldExpr *col0 = new FieldExpr(0, IntType());
    std::vector<Expr *> streamedEqualKeyExprs = { col0 };
    auto overflowConfig = new OverflowConfig();
    int streamedOutputCols[1] = {1};
    StreamedTableWithExprOperatorFactory *streamedWithExprOperatorFactory =
        StreamedTableWithExprOperatorFactory::CreateStreamedTableWithExprOperatorFactory(streamedTblTypes,
        streamedEqualKeyExprs, 1, streamedOutputCols, 1, JoinType::OMNI_JOIN_TYPE_FULL, filterJsonStr, overflowConfig);
    omniruntime::op::Operator *streamedTblWithExprOperator = CreateTestOperator(streamedWithExprOperatorFactory);

    std::vector<DataTypePtr> bufferTypesVector = { DoubleType(), IntType() };
    DataTypes bufferedTblTypes(bufferTypesVector);
    FieldExpr *col1 = new FieldExpr(1, IntType());
    std::vector<Expr *> bufferedEqualKeyExprs = { col1 };
    int bufferedOutputCols[1] = {0};
    int64_t streamedWithExprOperatorFactoryAddr = reinterpret_cast<int64_t>(streamedWithExprOperatorFactory);
    BufferedTableWithExprOperatorFactory *bufferedWithExprOperatorFactory =
        BufferedTableWithExprOperatorFactory::CreateBufferedTableWithExprOperatorFactory(bufferedTblTypes,
        bufferedEqualKeyExprs, 1, bufferedOutputCols, 1, streamedWithExprOperatorFactoryAddr, overflowConfig);
    omniruntime::op::Operator *bufferedTblWithExprOperator = CreateTestOperator(bufferedWithExprOperatorFactory);

    // construct data
    const int32_t streamedTblDataSize = 6;
    int32_t streamedTblDataCol1[streamedTblDataSize] = {1, 2, 3, 4, 5, 7};
    long streamedTblDataCol2[streamedTblDataSize] =  {1100, 2200, 3300, 4400, 5500, 7700};
    VectorBatch *streamedTblVecBatch1 =
        CreateVectorBatch(streamedTblTypes, streamedTblDataSize, streamedTblDataCol1, streamedTblDataCol2);

    int32_t addInputRetCode = -1;
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatch1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    const int32_t bufferedTblSize = 7;
    double bufferedTblDataCol1[bufferedTblSize] =  {1.1, 2.2, 3.3, 4.4, 6.6, 8.8, 9.9};
    int32_t bufferedTblDataCol2[bufferedTblSize] =  {1, 2, 3, 4, 6, 8, 9};
    VectorBatch *bufferedTblVecBatch1 =
        CreateVectorBatch(bufferedTblTypes, bufferedTblSize, bufferedTblDataCol1, bufferedTblDataCol2);
    // need add buffered table data
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatch1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_STREAM_TBL_DATA));

    // add eof flag to streamed table
    VectorBatch *streamedTblVecBatchEof = CreateEmptyVectorBatch(streamTypeVector);
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchEof);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    // add eof flag to buffered table , need add streamed table data
    VectorBatch *bufferedTblVecBatchEof = CreateEmptyVectorBatch(bufferTypesVector);
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatchEof);
    ASSERT_EQ(DecodeFetchFlag(addInputRetCode), static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_FETCH_JOIN_DATA));

    std::vector<omniruntime::vec::VectorBatch *> result;
    streamedTblWithExprOperator->GetOutput(result);

    auto streamedTblVecBatchEof1 = CreateEmptyVectorBatch(streamTypeVector);
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchEof1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode), static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_SCAN_FINISH));

    // check the join result
    long resultCol1[] =  {0, 1100, 0, 2200, 3300, 4400, 5500, 0, 7700, 0, 0};
    double resultCol2[] =  {1.1, 0, 2.2, 0, 3.3, 4.4, 0, 6.6, 0, 8.8, 9.9};
    int32_t index = 0;
    for (uint32_t i = 0; i < result.size(); i++) {
        ASSERT_EQ(result[i]->GetVectorCount(), 2);
        ASSERT_EQ(result[i]->GetVector(0)->GetTypeId(), OMNI_LONG);
        ASSERT_EQ(result[i]->GetVector(1)->GetTypeId(), OMNI_DOUBLE);
        ASSERT_EQ(result[i]->GetRowCount(), sizeof(resultCol1) / sizeof(resultCol1[0]));
        for (auto j = 0; j < result[i]->GetRowCount(); j++) {
            if (resultCol1[index] != 0) {
                long longValue = (static_cast<LongVector *>(result[i]->GetVector(0)))->GetValue(j);
                ASSERT_EQ(longValue, resultCol1[index]);
            } else {
                ASSERT_EQ((static_cast<LongVector *>(result[i]->GetVector(0)))->IsValueNull(j), true);
            }
            if (resultCol2[index] != 0) {
                double doubleValue = (static_cast<DoubleVector *>(result[i]->GetVector(1)))->GetValue(j);
                ASSERT_EQ(doubleValue, resultCol2[index]);
            } else {
                ASSERT_EQ((static_cast<DoubleVector *>(result[i]->GetVector(1)))->IsValueNull(j), true);
            }
            index++;
        }
        VectorHelper::FreeVecBatch(result[i]);
    }

    Expr::DeleteExprs(streamedEqualKeyExprs);
    Expr::DeleteExprs(bufferedEqualKeyExprs);
    omniruntime::op::Operator::DeleteOperator(bufferedTblWithExprOperator);
    omniruntime::op::Operator::DeleteOperator(streamedTblWithExprOperator);
    DeleteOperatorFactory(bufferedWithExprOperatorFactory);
    DeleteOperatorFactory(streamedWithExprOperatorFactory);
    delete overflowConfig;
}

TEST(SMJ_JOIN_OPERATOR_WITH_EXPR_TESTCASE, testSmjLeftJoinStreamedWithNullJoinKeyFirstWithExpression)
{
    // select t1.b, t2.c from t1 full join t2 where t1.a = t2.d and t1.a > 3;
    // bufferedTbl t2: double c, int d;
    // streamedTbl t1:  int a, Long b;
    string filterJsonStr = "{\"exprType\":\"BINARY\","
        "\"returnType\":4,"
        "\"operator\":\"GREATER_THAN\","
        "\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":0},"
        "\"right\":{\"exprType\":\"LITERAL\",\"dataType\":1, \"isNull\":false, \"value\":3}}";
    std::vector<DataTypePtr> streamTypeVector = { IntType(), LongType() };
    DataTypes streamedTblTypes(streamTypeVector);
    FieldExpr *col0 = new FieldExpr(0, IntType());
    std::vector<Expr *> streamedEqualKeyExprs = { col0 };
    auto overflowConfig = new OverflowConfig();
    int streamedOutputCols[1] = {1};
    StreamedTableWithExprOperatorFactory *streamedWithExprOperatorFactory =
        StreamedTableWithExprOperatorFactory::CreateStreamedTableWithExprOperatorFactory(streamedTblTypes,
        streamedEqualKeyExprs, 1, streamedOutputCols, 1, JoinType::OMNI_JOIN_TYPE_LEFT, filterJsonStr, overflowConfig);
    omniruntime::op::Operator *streamedTblWithExprOperator = CreateTestOperator(streamedWithExprOperatorFactory);

    std::vector<DataTypePtr> bufferTypesVector = { DoubleType(), IntType() };
    DataTypes bufferedTblTypes(bufferTypesVector);
    FieldExpr *col1 = new FieldExpr(1, IntType());
    std::vector<Expr *> bufferedEqualKeyExprs = { col1 };
    int bufferedOutputCols[1] = {0};
    int64_t streamedWithExprOperatorFactoryAddr = reinterpret_cast<int64_t>(streamedWithExprOperatorFactory);
    BufferedTableWithExprOperatorFactory *bufferedWithExprOperatorFactory =
        BufferedTableWithExprOperatorFactory::CreateBufferedTableWithExprOperatorFactory(bufferedTblTypes,
        bufferedEqualKeyExprs, 1, bufferedOutputCols, 1, streamedWithExprOperatorFactoryAddr, overflowConfig);
    omniruntime::op::Operator *bufferedTblWithExprOperator = CreateTestOperator(bufferedWithExprOperatorFactory);

    // construct data
    const int32_t streamedTblDataSize = 7;
    int32_t streamedTblDataCol1[streamedTblDataSize] = {1, 2, 3, 4, 5, 6, 7};
    long streamedTblDataCol2[streamedTblDataSize] =  {1100, 2200, 3300, 4400, 5500, 6600, 7700};
    VectorBatch *streamedTblVecBatch1 =
        CreateVectorBatch(streamedTblTypes, streamedTblDataSize, streamedTblDataCol1, streamedTblDataCol2);
    streamedTblVecBatch1->GetVector(0)->SetValueNull(0); // NULL, NULL, 4, 5, 6, 7
    streamedTblVecBatch1->GetVector(0)->SetValueNull(1); // NULL, NULL, 4, 5, 6, 7

    int32_t addInputRetCode = -1;
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatch1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    const int32_t bufferedTblSize = 6;
    double bufferedTblDataCol1[bufferedTblSize] =  {1.1, 2.2, 3.3, 4.4, 6.6, 8.8};
    int32_t bufferedTblDataCol2[bufferedTblSize] =  {1, 2, 3, 4, 6, 8};
    VectorBatch *bufferedTblVecBatch1 =
        CreateVectorBatch(bufferedTblTypes, bufferedTblSize, bufferedTblDataCol1, bufferedTblDataCol2);
    bufferedTblVecBatch1->GetVector(1)->SetValueNull(0); // NULL, NULL, 3, 4, 6, 8
    bufferedTblVecBatch1->GetVector(1)->SetValueNull(1); // NULL, NULL, 3, 4, 6, 8

    // need add buffered table data
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatch1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_STREAM_TBL_DATA));

    // add eof flag to streamed table
    VectorBatch *streamedTblVecBatchEof = CreateEmptyVectorBatch(streamTypeVector);
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchEof);
    ASSERT_EQ(DecodeFetchFlag(addInputRetCode), static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_FETCH_JOIN_DATA));

    std::vector<omniruntime::vec::VectorBatch *> result;
    streamedTblWithExprOperator->GetOutput(result);

    auto streamedTblVecBatchEof1 = CreateEmptyVectorBatch(streamTypeVector);
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchEof1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode), static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_SCAN_FINISH));

    // check the join result
    long resultCol1[] =  {1100, 2200, 3300, 4400, 5500, 6600, 7700};
    double resultCol2[] =  {0, 0, 0, 4.4, 0, 6.6, 0};
    int32_t index = 0;
    for (uint32_t i = 0; i < result.size(); i++) {
        ASSERT_EQ(result[i]->GetVectorCount(), 2);
        ASSERT_EQ(result[i]->GetVector(0)->GetTypeId(), OMNI_LONG);
        ASSERT_EQ(result[i]->GetVector(1)->GetTypeId(), OMNI_DOUBLE);
        ASSERT_EQ(result[i]->GetRowCount(), sizeof(resultCol1) / sizeof(resultCol1[0]));
        for (auto j = 0; j < result[i]->GetRowCount(); j++) {
            if (resultCol1[index] != 0) {
                long longValue = (static_cast<LongVector *>(result[i]->GetVector(0)))->GetValue(j);
                ASSERT_EQ(longValue, resultCol1[index]);
            } else {
                ASSERT_EQ((static_cast<LongVector *>(result[i]->GetVector(0)))->IsValueNull(j), true);
            }
            if (resultCol2[index] != 0) {
                double doubleValue = (static_cast<DoubleVector *>(result[i]->GetVector(1)))->GetValue(j);
                ASSERT_EQ(doubleValue, resultCol2[index]);
            } else {
                ASSERT_EQ((static_cast<DoubleVector *>(result[i]->GetVector(1)))->IsValueNull(j), true);
            }
            index++;
        }
        VectorHelper::FreeVecBatch(result[i]);
    }

    Expr::DeleteExprs(streamedEqualKeyExprs);
    Expr::DeleteExprs(bufferedEqualKeyExprs);
    omniruntime::op::Operator::DeleteOperator(bufferedTblWithExprOperator);
    omniruntime::op::Operator::DeleteOperator(streamedTblWithExprOperator);
    DeleteOperatorFactory(bufferedWithExprOperatorFactory);
    DeleteOperatorFactory(streamedWithExprOperatorFactory);
    delete overflowConfig;
}

TEST(SMJ_JOIN_OPERATOR_WITH_EXPR_TESTCASE, testSmjLeftSemiJoinWithFilterEmpty)
{
    // select t1.a, t1.b from t1 left semi join t2 where t1.a = t2.d
    // streamedTbl(left table) t1:  int a, long b;
    // bufferedTbl(right table) t2: double c, int d;
    std::string blank = "";
    std::vector<DataTypePtr> streamTypeVector = { IntType(), LongType() };
    DataTypes streamedTblTypes(streamTypeVector);
    auto col0 = new FieldExpr(0, IntType());
    std::vector<Expr *> streamedEqualKeyExprs = { col0 };
    auto overflowConfig = new OverflowConfig();
    int streamedOutputCols[2] = {0, 1};
    StreamedTableWithExprOperatorFactory *streamedWithExprOperatorFactory =
        StreamedTableWithExprOperatorFactory::CreateStreamedTableWithExprOperatorFactory(streamedTblTypes,
        streamedEqualKeyExprs, 1, streamedOutputCols, 2, JoinType::OMNI_JOIN_TYPE_LEFT_SEMI, blank, overflowConfig);
    omniruntime::op::Operator *streamedTblWithExprOperator = CreateTestOperator(streamedWithExprOperatorFactory);

    std::vector<DataTypePtr> bufferTypesVector = { DoubleDataType::Instance(), IntType() };
    DataTypes bufferedTblTypes(bufferTypesVector);
    auto col1 = new FieldExpr(1, IntType());
    std::vector<Expr *> bufferedEqualKeyExprs = { col1 };
    int bufferedOutputCols[0] = {};
    auto streamedWithExprOperatorFactoryAddr = reinterpret_cast<int64_t>(streamedWithExprOperatorFactory);
    BufferedTableWithExprOperatorFactory *bufferedWithExprOperatorFactory =
        BufferedTableWithExprOperatorFactory::CreateBufferedTableWithExprOperatorFactory(bufferedTblTypes,
        bufferedEqualKeyExprs, 1, bufferedOutputCols, 0, streamedWithExprOperatorFactoryAddr, overflowConfig);
    omniruntime::op::Operator *bufferedTblWithExprOperator = CreateTestOperator(bufferedWithExprOperatorFactory);

    // construct data
    const int32_t streamedTblDataSize = 6;
    int32_t streamedTblDataCol1[streamedTblDataSize] = {0, 1, 2, 2, 4, 5};
    long streamedTblDataCol2[streamedTblDataSize] =  {6600, 5500, 4400, 3300, 2200, 1100};
    VectorBatch *streamedTblVecBatch1 =
        CreateVectorBatch(streamedTblTypes, streamedTblDataSize, streamedTblDataCol1, streamedTblDataCol2);

    int32_t addInputRetCode = -1;
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatch1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    const int32_t bufferedTblSize = 6;
    double bufferedTblDataCol1[bufferedTblSize] =  {6.6, 5.5, 4.4, 3.3, 2.2, 1.1};
    int32_t bufferedTblDataCol2[bufferedTblSize] =  {0, 1, 2, 3, 4, 5};
    VectorBatch *bufferedTblVecBatch1 =
        CreateVectorBatch(bufferedTblTypes, bufferedTblSize, bufferedTblDataCol1, bufferedTblDataCol2);
    // need add buffered table data
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatch1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    // add eof flag to buffered table , need add streamed table data
    VectorBatch *bufferedTblVecBatchEof = CreateEmptyVectorBatch(bufferTypesVector);
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatchEof);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_STREAM_TBL_DATA));

    // add eof flag to streamed table
    VectorBatch *streamedTblVecBatchEof = CreateEmptyVectorBatch(streamTypeVector);
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchEof);
    ASSERT_EQ(DecodeFetchFlag(addInputRetCode), static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_FETCH_JOIN_DATA));

    std::vector<omniruntime::vec::VectorBatch *> result;
    streamedTblWithExprOperator->GetOutput(result);

    VectorBatch *streamedTblVecBatchEof1 = CreateEmptyVectorBatch(streamTypeVector);
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchEof1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode), static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_SCAN_FINISH));

    // check the join result
    int32_t expCol1[] =  {0, 1, 2, 2, 4, 5};
    long expCol2[] =  {6600, 5500, 4400, 3300, 2200, 1100};
    VectorBatch *expectVecBatch = CreateVectorBatch(streamedTblTypes, 6, expCol1, expCol2);
    ASSERT_TRUE(VecBatchMatch(result[0], expectVecBatch));
    VectorHelper::FreeVecBatch(result[0]);
    VectorHelper::FreeVecBatch(expectVecBatch);

    Expr::DeleteExprs(streamedEqualKeyExprs);
    Expr::DeleteExprs(bufferedEqualKeyExprs);
    omniruntime::op::Operator::DeleteOperator(bufferedTblWithExprOperator);
    omniruntime::op::Operator::DeleteOperator(streamedTblWithExprOperator);
    DeleteOperatorFactory(bufferedWithExprOperatorFactory);
    DeleteOperatorFactory(streamedWithExprOperatorFactory);
    delete overflowConfig;
}

TEST(SMJ_JOIN_OPERATOR_WITH_EXPR_TESTCASE, testSmjLeftSemiJoinStreamedWithRepeatRows)
{
    // select t1.a, t1.b from t1 left semi join t2 where t1.a = t2.d and t1.a > 1
    // streamedTbl(left table) t1:  int a, long b;
    // bufferedTbl(right table) t2: double c, int d;
    string filterJsonStr = "{\"exprType\":\"BINARY\","
        "\"returnType\":4,"
        "\"operator\":\"GREATER_THAN\","
        "\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":0},"
        "\"right\":{\"exprType\":\"LITERAL\",\"dataType\":1, \"isNull\":false, \"value\":1}}";
    std::vector<DataTypePtr> streamTypeVector = { IntType(), LongType() };
    DataTypes streamedTblTypes(streamTypeVector);
    auto col0 = new FieldExpr(0, IntType());
    std::vector<Expr *> streamedEqualKeyExprs = { col0 };
    auto overflowConfig = new OverflowConfig();
    int streamedOutputCols[2] = {0, 1};
    StreamedTableWithExprOperatorFactory *streamedWithExprOperatorFactory =
        StreamedTableWithExprOperatorFactory::CreateStreamedTableWithExprOperatorFactory(streamedTblTypes,
        streamedEqualKeyExprs, 1, streamedOutputCols, 2, JoinType::OMNI_JOIN_TYPE_LEFT_SEMI, filterJsonStr,
        overflowConfig);
    omniruntime::op::Operator *streamedTblWithExprOperator = CreateTestOperator(streamedWithExprOperatorFactory);

    std::vector<DataTypePtr> bufferTypesVector = { DoubleDataType::Instance(), IntType() };
    DataTypes bufferedTblTypes(bufferTypesVector);
    auto col1 = new FieldExpr(1, IntType());
    std::vector<Expr *> bufferedEqualKeyExprs = { col1 };
    int bufferedOutputCols[0] = {};
    auto streamedWithExprOperatorFactoryAddr = reinterpret_cast<int64_t>(streamedWithExprOperatorFactory);
    BufferedTableWithExprOperatorFactory *bufferedWithExprOperatorFactory =
        BufferedTableWithExprOperatorFactory::CreateBufferedTableWithExprOperatorFactory(bufferedTblTypes,
        bufferedEqualKeyExprs, 1, bufferedOutputCols, 0, streamedWithExprOperatorFactoryAddr, overflowConfig);
    omniruntime::op::Operator *bufferedTblWithExprOperator = CreateTestOperator(bufferedWithExprOperatorFactory);

    // construct data
    const int32_t streamedTblDataSize = 6;
    int32_t streamedTblDataCol1[streamedTblDataSize] = {0, 1, 2, 2, 4, 5};
    long streamedTblDataCol2[streamedTblDataSize] =  {6600, 5500, 4400, 3300, 2200, 1100};
    VectorBatch *streamedTblVecBatch1 =
        CreateVectorBatch(streamedTblTypes, streamedTblDataSize, streamedTblDataCol1, streamedTblDataCol2);

    int32_t addInputRetCode = -1;
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatch1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    const int32_t bufferedTblSize = 6;
    double bufferedTblDataCol1[bufferedTblSize] =  {6.6, 5.5, 4.4, 3.3, 2.2, 1.1};
    int32_t bufferedTblDataCol2[bufferedTblSize] =  {0, 1, 2, 3, 4, 5};
    VectorBatch *bufferedTblVecBatch1 =
        CreateVectorBatch(bufferedTblTypes, bufferedTblSize, bufferedTblDataCol1, bufferedTblDataCol2);
    // need add buffered table data
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatch1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    // add eof flag to buffered table , need add streamed table data
    VectorBatch *bufferedTblVecBatchEof = CreateEmptyVectorBatch(bufferTypesVector);
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatchEof);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_STREAM_TBL_DATA));

    // add eof flag to streamed table
    VectorBatch *streamedTblVecBatchEof = CreateEmptyVectorBatch(streamTypeVector);
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchEof);
    ASSERT_EQ(DecodeFetchFlag(addInputRetCode), static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_FETCH_JOIN_DATA));

    std::vector<omniruntime::vec::VectorBatch *> result;
    streamedTblWithExprOperator->GetOutput(result);

    VectorBatch *streamedTblVecBatchEof1 = CreateEmptyVectorBatch(streamTypeVector);
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchEof1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode), static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_SCAN_FINISH));

    // check the join result
    int32_t expCol1[] =  {2, 2, 4, 5};
    long expCol2[] =  {4400, 3300, 2200, 1100};
    VectorBatch *expectVecBatch = CreateVectorBatch(streamedTblTypes, 4, expCol1, expCol2);
    ASSERT_TRUE(VecBatchMatch(result[0], expectVecBatch));
    VectorHelper::FreeVecBatch(result[0]);
    VectorHelper::FreeVecBatch(expectVecBatch);

    Expr::DeleteExprs(streamedEqualKeyExprs);
    Expr::DeleteExprs(bufferedEqualKeyExprs);
    omniruntime::op::Operator::DeleteOperator(bufferedTblWithExprOperator);
    omniruntime::op::Operator::DeleteOperator(streamedTblWithExprOperator);
    DeleteOperatorFactory(bufferedWithExprOperatorFactory);
    DeleteOperatorFactory(streamedWithExprOperatorFactory);
    delete overflowConfig;
}

TEST(SMJ_JOIN_OPERATOR_WITH_EXPR_TESTCASE, testSmjLeftSemiJoinBufferedWithRepeatRows)
{
    // select t1.a, t1.b from t1 left semi join t2 where t1.a = t2.d and t1.a <= 4
    // streamedTbl(left table) t1:  int a, long b;
    // bufferedTbl(right table) t2: double c, int d;
    string filterJsonStr = "{\"exprType\":\"BINARY\","
        "\"returnType\":4,"
        "\"operator\":\"LESS_THAN_OR_EQUAL\","
        "\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":0},"
        "\"right\":{\"exprType\":\"LITERAL\",\"dataType\":1, \"isNull\":false, \"value\":4}}";
    std::vector<DataTypePtr> streamTypeVector = { IntType(), LongType() };
    DataTypes streamedTblTypes(streamTypeVector);
    auto col0 = new FieldExpr(0, IntType());
    std::vector<Expr *> streamedEqualKeyExprs = { col0 };
    auto overflowConfig = new OverflowConfig();
    int streamedOutputCols[2] = {0, 1};
    StreamedTableWithExprOperatorFactory *streamedWithExprOperatorFactory =
        StreamedTableWithExprOperatorFactory::CreateStreamedTableWithExprOperatorFactory(streamedTblTypes,
        streamedEqualKeyExprs, 1, streamedOutputCols, 2, JoinType::OMNI_JOIN_TYPE_LEFT_SEMI, filterJsonStr,
        overflowConfig);
    omniruntime::op::Operator *streamedTblWithExprOperator = CreateTestOperator(streamedWithExprOperatorFactory);

    std::vector<DataTypePtr> bufferTypesVector = { DoubleDataType::Instance(), IntType() };
    DataTypes bufferedTblTypes(bufferTypesVector);
    auto col1 = new FieldExpr(1, IntType());
    std::vector<Expr *> bufferedEqualKeyExprs = { col1 };
    int bufferedOutputCols[0] = {};
    auto streamedWithExprOperatorFactoryAddr = reinterpret_cast<int64_t>(streamedWithExprOperatorFactory);
    BufferedTableWithExprOperatorFactory *bufferedWithExprOperatorFactory =
        BufferedTableWithExprOperatorFactory::CreateBufferedTableWithExprOperatorFactory(bufferedTblTypes,
        bufferedEqualKeyExprs, 1, bufferedOutputCols, 0, streamedWithExprOperatorFactoryAddr, overflowConfig);
    omniruntime::op::Operator *bufferedTblWithExprOperator = CreateTestOperator(bufferedWithExprOperatorFactory);

    // construct data
    const int32_t streamedTblDataSize = 6;
    int32_t streamedTblDataCol1[streamedTblDataSize] = {0, 1, 2, 3, 4, 5};
    long streamedTblDataCol2[streamedTblDataSize] =  {6600, 5500, 4400, 3300, 2200, 1100};
    VectorBatch *streamedTblVecBatch1 =
        CreateVectorBatch(streamedTblTypes, streamedTblDataSize, streamedTblDataCol1, streamedTblDataCol2);

    int32_t addInputRetCode = -1;
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatch1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    const int32_t bufferedTblSize = 6;
    double bufferedTblDataCol1[bufferedTblSize] =  {6.6, 5.5, 4.4, 3.3, 2.2, 1.1};
    int32_t bufferedTblDataCol2[bufferedTblSize] =  {0, 1, 2, 2, 2, 5};
    VectorBatch *bufferedTblVecBatch1 =
        CreateVectorBatch(bufferedTblTypes, bufferedTblSize, bufferedTblDataCol1, bufferedTblDataCol2);
    // need add buffered table data
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatch1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    // add eof flag to buffered table , need add streamed table data
    VectorBatch *bufferedTblVecBatchEof = CreateEmptyVectorBatch(bufferTypesVector);
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatchEof);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_STREAM_TBL_DATA));

    // add eof flag to streamed table
    VectorBatch *streamedTblVecBatchEof = CreateEmptyVectorBatch(streamTypeVector);
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchEof);
    ASSERT_EQ(DecodeFetchFlag(addInputRetCode), static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_FETCH_JOIN_DATA));

    std::vector<omniruntime::vec::VectorBatch *> result;
    streamedTblWithExprOperator->GetOutput(result);

    VectorBatch *streamedTblVecBatchEof1 = CreateEmptyVectorBatch(streamTypeVector);
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchEof1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode), static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_SCAN_FINISH));

    // check the join result
    int32_t expCol1[] =  {0, 1, 2};
    long expCol2[] =  {6600, 5500, 4400};
    VectorBatch *expectVecBatch = CreateVectorBatch(streamedTblTypes, 3, expCol1, expCol2);
    ASSERT_TRUE(VecBatchMatch(result[0], expectVecBatch));
    VectorHelper::FreeVecBatch(result[0]);
    VectorHelper::FreeVecBatch(expectVecBatch);

    Expr::DeleteExprs(streamedEqualKeyExprs);
    Expr::DeleteExprs(bufferedEqualKeyExprs);
    omniruntime::op::Operator::DeleteOperator(bufferedTblWithExprOperator);
    omniruntime::op::Operator::DeleteOperator(streamedTblWithExprOperator);
    DeleteOperatorFactory(bufferedWithExprOperatorFactory);
    DeleteOperatorFactory(streamedWithExprOperatorFactory);
    delete overflowConfig;
}

TEST(SMJ_JOIN_OPERATOR_WITH_EXPR_TESTCASE, testSmjLeftSemiJoinBothNullFirst)
{
    // select t1.a, t1.b from t1 left semi join t2 where t1.a = t2.d and t1.a < 5
    // streamedTbl(left table) t1:  int a, long b;
    // bufferedTbl(right table) t2: double c, int d;
    string filterJsonStr = "{\"exprType\":\"BINARY\","
        "\"returnType\":4,"
        "\"operator\":\"LESS_THAN\","
        "\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":0},"
        "\"right\":{\"exprType\":\"LITERAL\",\"dataType\":1, \"isNull\":false, \"value\":5}}";
    std::vector<DataTypePtr> streamTypeVector = { IntType(), LongType() };
    DataTypes streamedTblTypes(streamTypeVector);
    auto col0 = new FieldExpr(0, IntType());
    std::vector<Expr *> streamedEqualKeyExprs = { col0 };
    auto overflowConfig = new OverflowConfig();
    int streamedOutputCols[2] = {0, 1};
    StreamedTableWithExprOperatorFactory *streamedWithExprOperatorFactory =
        StreamedTableWithExprOperatorFactory::CreateStreamedTableWithExprOperatorFactory(streamedTblTypes,
        streamedEqualKeyExprs, 1, streamedOutputCols, 2, JoinType::OMNI_JOIN_TYPE_LEFT_SEMI, filterJsonStr,
        overflowConfig);
    omniruntime::op::Operator *streamedTblWithExprOperator = CreateTestOperator(streamedWithExprOperatorFactory);

    std::vector<DataTypePtr> bufferTypesVector = { DoubleDataType::Instance(), IntType() };
    DataTypes bufferedTblTypes(bufferTypesVector);
    auto col1 = new FieldExpr(1, IntType());
    std::vector<Expr *> bufferedEqualKeyExprs = { col1 };
    int bufferedOutputCols[0] = {};
    auto streamedWithExprOperatorFactoryAddr = reinterpret_cast<int64_t>(streamedWithExprOperatorFactory);
    BufferedTableWithExprOperatorFactory *bufferedWithExprOperatorFactory =
        BufferedTableWithExprOperatorFactory::CreateBufferedTableWithExprOperatorFactory(bufferedTblTypes,
        bufferedEqualKeyExprs, 1, bufferedOutputCols, 0, streamedWithExprOperatorFactoryAddr, overflowConfig);
    omniruntime::op::Operator *bufferedTblWithExprOperator = CreateTestOperator(bufferedWithExprOperatorFactory);

    // construct data
    const int32_t streamedTblDataSize = 6;
    int32_t streamedTblDataCol1[streamedTblDataSize] = {0, 1, 2, 3, 4, 5};
    long streamedTblDataCol2[streamedTblDataSize] =  {6600, 5500, 4400, 3300, 2200, 1100};
    VectorBatch *streamedTblVecBatch1 =
        CreateVectorBatch(streamedTblTypes, streamedTblDataSize, streamedTblDataCol1, streamedTblDataCol2);
    streamedTblVecBatch1->GetVector(0)->SetValueNull(0); // NULL, 1, 2, 3, 4, 5
    streamedTblVecBatch1->GetVector(0)->SetValueNull(1); // NULL, NULL, 2, 3, 4, 5

    int32_t addInputRetCode = -1;
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatch1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    const int32_t bufferedTblSize = 6;
    double bufferedTblDataCol1[bufferedTblSize] =  {6.6, 5.5, 4.4, 3.3, 2.2, 1.1};
    int32_t bufferedTblDataCol2[bufferedTblSize] =  {0, 1, 2, 3, 4, 5};
    VectorBatch *bufferedTblVecBatch1 =
        CreateVectorBatch(bufferedTblTypes, bufferedTblSize, bufferedTblDataCol1, bufferedTblDataCol2);
    bufferedTblVecBatch1->GetVector(1)->SetValueNull(0); // NULL, 1, 2, 3, 4, 5

    // need add buffered table data
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatch1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    // add eof flag to buffered table , need add streamed table data
    VectorBatch *bufferedTblVecBatchEof = CreateEmptyVectorBatch(bufferTypesVector);
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatchEof);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_STREAM_TBL_DATA));

    // add eof flag to streamed table
    VectorBatch *streamedTblVecBatchEof = CreateEmptyVectorBatch(streamTypeVector);
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchEof);
    ASSERT_EQ(DecodeFetchFlag(addInputRetCode), static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_FETCH_JOIN_DATA));

    std::vector<omniruntime::vec::VectorBatch *> result;
    streamedTblWithExprOperator->GetOutput(result);

    VectorBatch *streamedTblVecBatchEof1 = CreateEmptyVectorBatch(streamTypeVector);
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchEof1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode), static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_SCAN_FINISH));

    // check the join result
    int32_t expCol1[] =  {2, 3, 4};
    long expCol2[] =  {4400, 3300, 2200};
    VectorBatch *expectVecBatch = CreateVectorBatch(streamedTblTypes, 3, expCol1, expCol2);
    ASSERT_TRUE(VecBatchMatch(result[0], expectVecBatch));
    VectorHelper::FreeVecBatch(result[0]);
    VectorHelper::FreeVecBatch(expectVecBatch);

    Expr::DeleteExprs(streamedEqualKeyExprs);
    Expr::DeleteExprs(bufferedEqualKeyExprs);
    omniruntime::op::Operator::DeleteOperator(bufferedTblWithExprOperator);
    omniruntime::op::Operator::DeleteOperator(streamedTblWithExprOperator);
    DeleteOperatorFactory(bufferedWithExprOperatorFactory);
    DeleteOperatorFactory(streamedWithExprOperatorFactory);
    delete overflowConfig;
}

TEST(SMJ_JOIN_OPERATOR_WITH_EXPR_TESTCASE, testSmjLeftSemiJoinFilterDeduplicate)
{
    // select t1.a, t1.b from t1 left semi join t2 where t1.a = t2.d and t2.c > 4
    // streamedTbl(left table) t1:  int a, long b;
    // bufferedTbl(right table) t2: double c, int d;
    string filterJsonStr = "{\"exprType\":\"BINARY\","
        "\"returnType\":4,"
        "\"operator\":\"GREATER_THAN\","
        "\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":3,\"colVal\":2},"
        "\"right\":{\"exprType\":\"LITERAL\",\"dataType\":3, \"isNull\":false, \"value\":4}}";
    std::vector<DataTypePtr> streamTypeVector = { IntType(), LongType() };
    DataTypes streamedTblTypes(streamTypeVector);
    auto col0 = new FieldExpr(0, IntType());
    std::vector<Expr *> streamedEqualKeyExprs = { col0 };
    auto overflowConfig = new OverflowConfig();
    int streamedOutputCols[2] = {0, 1};
    StreamedTableWithExprOperatorFactory *streamedWithExprOperatorFactory =
        StreamedTableWithExprOperatorFactory::CreateStreamedTableWithExprOperatorFactory(streamedTblTypes,
        streamedEqualKeyExprs, 1, streamedOutputCols, 2, JoinType::OMNI_JOIN_TYPE_LEFT_SEMI, filterJsonStr,
        overflowConfig);
    omniruntime::op::Operator *streamedTblWithExprOperator = CreateTestOperator(streamedWithExprOperatorFactory);

    std::vector<DataTypePtr> bufferTypesVector = { DoubleDataType::Instance(), IntType() };
    DataTypes bufferedTblTypes(bufferTypesVector);
    auto col1 = new FieldExpr(1, IntType());
    std::vector<Expr *> bufferedEqualKeyExprs = { col1 };
    int bufferedOutputCols[0] = {};
    auto streamedWithExprOperatorFactoryAddr = reinterpret_cast<int64_t>(streamedWithExprOperatorFactory);
    BufferedTableWithExprOperatorFactory *bufferedWithExprOperatorFactory =
        BufferedTableWithExprOperatorFactory::CreateBufferedTableWithExprOperatorFactory(bufferedTblTypes,
        bufferedEqualKeyExprs, 1, bufferedOutputCols, 0, streamedWithExprOperatorFactoryAddr, overflowConfig);
    omniruntime::op::Operator *bufferedTblWithExprOperator = CreateTestOperator(bufferedWithExprOperatorFactory);

    // construct data
    const int32_t streamedTblDataSize = 6;
    int32_t streamedTblDataCol1[streamedTblDataSize] = {0, 1, 1, 2, 3, 4};
    long streamedTblDataCol2[streamedTblDataSize] =  {6600, 5500, 4400, 3300, 2200, 1100};
    VectorBatch *streamedTblVecBatch1 =
        CreateVectorBatch(streamedTblTypes, streamedTblDataSize, streamedTblDataCol1, streamedTblDataCol2);
    streamedTblVecBatch1->GetVector(0)->SetValueNull(0); // NULL, 1, 1, 2, 3, 4

    int32_t addInputRetCode = -1;
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatch1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    const int32_t bufferedTblSize = 6;
    double bufferedTblDataCol1[bufferedTblSize] =  {6.6, 5.5, 3.3, 4.4, 3.3, 4.4};
    int32_t bufferedTblDataCol2[bufferedTblSize] =  {0, 1, 2, 2, 3, 4};
    VectorBatch *bufferedTblVecBatch1 =
        CreateVectorBatch(bufferedTblTypes, bufferedTblSize, bufferedTblDataCol1, bufferedTblDataCol2);
    bufferedTblVecBatch1->GetVector(1)->SetValueNull(0); // NULL, 1, 2, 2, 3, 4

    // need add buffered table data
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatch1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    // add eof flag to buffered table , need add streamed table data
    VectorBatch *bufferedTblVecBatchEof = CreateEmptyVectorBatch(bufferTypesVector);
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatchEof);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_STREAM_TBL_DATA));

    // add eof flag to streamed table
    VectorBatch *streamedTblVecBatchEof = CreateEmptyVectorBatch(streamTypeVector);
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchEof);
    ASSERT_EQ(DecodeFetchFlag(addInputRetCode), static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_FETCH_JOIN_DATA));

    std::vector<omniruntime::vec::VectorBatch *> result;
    streamedTblWithExprOperator->GetOutput(result);

    VectorBatch *streamedTblVecBatchEof1 = CreateEmptyVectorBatch(streamTypeVector);
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchEof1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode), static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_SCAN_FINISH));

    // check the join result
    int32_t expCol1[] =  {1, 1, 2, 4};
    long expCol2[] =  {5500, 4400, 3300, 1100};
    VectorBatch *expectVecBatch = CreateVectorBatch(streamedTblTypes, 4, expCol1, expCol2);
    ASSERT_TRUE(VecBatchMatch(result[0], expectVecBatch));
    VectorHelper::FreeVecBatch(result[0]);
    VectorHelper::FreeVecBatch(expectVecBatch);

    Expr::DeleteExprs(streamedEqualKeyExprs);
    Expr::DeleteExprs(bufferedEqualKeyExprs);
    omniruntime::op::Operator::DeleteOperator(bufferedTblWithExprOperator);
    omniruntime::op::Operator::DeleteOperator(streamedTblWithExprOperator);
    DeleteOperatorFactory(bufferedWithExprOperatorFactory);
    DeleteOperatorFactory(streamedWithExprOperatorFactory);
    delete overflowConfig;
}

TEST(SMJ_JOIN_OPERATOR_WITH_EXPR_TESTCASE, testSmjLeftSemiJoinBufferedWithDuplicateFilterResults)
{
    // select t1.a, t1.b from t1 left semi join t2 where t1.a = t2.d and t2.c > 4
    // streamedTbl(left table) t1:  int a, long b;
    // bufferedTbl(right table) t2: double c, int d;
    string filterJsonStr = "{\"exprType\":\"BINARY\","
        "\"returnType\":4,"
        "\"operator\":\"GREATER_THAN\","
        "\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":3,\"colVal\":2},"
        "\"right\":{\"exprType\":\"LITERAL\",\"dataType\":3, \"isNull\":false, \"value\":4}}";
    std::vector<DataTypePtr> streamTypeVector = { IntType(), LongType() };
    DataTypes streamedTblTypes(streamTypeVector);
    auto col0 = new FieldExpr(0, IntType());
    std::vector<Expr *> streamedEqualKeyExprs = { col0 };
    auto overflowConfig = new OverflowConfig();
    int streamedOutputCols[2] = {0, 1};
    StreamedTableWithExprOperatorFactory *streamedWithExprOperatorFactory =
        StreamedTableWithExprOperatorFactory::CreateStreamedTableWithExprOperatorFactory(streamedTblTypes,
        streamedEqualKeyExprs, 1, streamedOutputCols, 2, JoinType::OMNI_JOIN_TYPE_LEFT_SEMI, filterJsonStr,
        overflowConfig);
    omniruntime::op::Operator *streamedTblWithExprOperator = CreateTestOperator(streamedWithExprOperatorFactory);

    std::vector<DataTypePtr> bufferTypesVector = { DoubleDataType::Instance(), IntType() };
    DataTypes bufferedTblTypes(bufferTypesVector);
    auto col1 = new FieldExpr(1, IntType());
    std::vector<Expr *> bufferedEqualKeyExprs = { col1 };
    int bufferedOutputCols[0] = {};
    auto streamedWithExprOperatorFactoryAddr = reinterpret_cast<int64_t>(streamedWithExprOperatorFactory);
    BufferedTableWithExprOperatorFactory *bufferedWithExprOperatorFactory =
        BufferedTableWithExprOperatorFactory::CreateBufferedTableWithExprOperatorFactory(bufferedTblTypes,
        bufferedEqualKeyExprs, 1, bufferedOutputCols, 0, streamedWithExprOperatorFactoryAddr, overflowConfig);
    omniruntime::op::Operator *bufferedTblWithExprOperator = CreateTestOperator(bufferedWithExprOperatorFactory);

    // construct data
    const int32_t streamedTblDataSize = 6;
    int32_t streamedTblDataCol1[streamedTblDataSize] = {0, 1, 1, 2, 3, 4};
    long streamedTblDataCol2[streamedTblDataSize] =  {6600, 5500, 4400, 3300, 2200, 1100};
    VectorBatch *streamedTblVecBatch1 =
        CreateVectorBatch(streamedTblTypes, streamedTblDataSize, streamedTblDataCol1, streamedTblDataCol2);
    streamedTblVecBatch1->GetVector(0)->SetValueNull(0); // NULL, 1, 1, 2, 3, 4

    int32_t addInputRetCode = -1;
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatch1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    const int32_t bufferedTblSize = 7;
    double bufferedTblDataCol1[bufferedTblSize] =  {6.6, 5.5, 3.3, 4.4, 4.5, 3.3, 4.4};
    int32_t bufferedTblDataCol2[bufferedTblSize] =  {0, 1, 2, 2, 2, 3, 4};
    VectorBatch *bufferedTblVecBatch1 =
        CreateVectorBatch(bufferedTblTypes, bufferedTblSize, bufferedTblDataCol1, bufferedTblDataCol2);
    bufferedTblVecBatch1->GetVector(1)->SetValueNull(0); // NULL, 1, 2, 2, 2, 3, 4

    // need add buffered table data
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatch1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    // add eof flag to buffered table , need add streamed table data
    VectorBatch *bufferedTblVecBatchEof = CreateEmptyVectorBatch(bufferTypesVector);
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatchEof);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_STREAM_TBL_DATA));

    // add eof flag to streamed table
    VectorBatch *streamedTblVecBatchEof = CreateEmptyVectorBatch(streamTypeVector);
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchEof);
    ASSERT_EQ(DecodeFetchFlag(addInputRetCode), static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_FETCH_JOIN_DATA));

    std::vector<omniruntime::vec::VectorBatch *> result;
    streamedTblWithExprOperator->GetOutput(result);

    VectorBatch *streamedTblVecBatchEof1 = CreateEmptyVectorBatch(streamTypeVector);
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchEof1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode), static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_SCAN_FINISH));

    // check the join result
    int32_t expCol1[] =  {1, 1, 2, 4};
    long expCol2[] =  {5500, 4400, 3300, 1100};
    VectorBatch *expectVecBatch = CreateVectorBatch(streamedTblTypes, 4, expCol1, expCol2);
    ASSERT_TRUE(VecBatchMatch(result[0], expectVecBatch));
    VectorHelper::FreeVecBatch(result[0]);
    VectorHelper::FreeVecBatch(expectVecBatch);

    Expr::DeleteExprs(streamedEqualKeyExprs);
    Expr::DeleteExprs(bufferedEqualKeyExprs);
    omniruntime::op::Operator::DeleteOperator(bufferedTblWithExprOperator);
    omniruntime::op::Operator::DeleteOperator(streamedTblWithExprOperator);
    DeleteOperatorFactory(bufferedWithExprOperatorFactory);
    DeleteOperatorFactory(streamedWithExprOperatorFactory);
    delete overflowConfig;
}

TEST(SMJ_JOIN_OPERATOR_WITH_EXPR_TESTCASE, testSmjLeftAntiJoinStreamedWithEmptyFilterExpression)
{
    // select * from AntiTest_s left anti join AntiTest_b on AntiTest_s.CountryID = AntiTest_b.ID;
    // streamedTbl AntiTest_s: CountryID int, Units long;
    // bufferedTbl AntiTest_b: ID int, Country double;
    string filterJsonStr = "";
    std::vector<DataTypePtr> streamTypeVector = { IntType(), LongType() };
    DataTypes streamedTblTypes(streamTypeVector);
    FieldExpr *streamCol0 = new FieldExpr(0, IntType());
    std::vector<Expr *> streamedEqualKeyExprs = { streamCol0 };
    int streamedOutputCols[2] = {0, 1};
    auto overflowConfig = new OverflowConfig();
    StreamedTableWithExprOperatorFactory *streamedWithExprOperatorFactory =
        StreamedTableWithExprOperatorFactory::CreateStreamedTableWithExprOperatorFactory(streamedTblTypes,
        streamedEqualKeyExprs, 1, streamedOutputCols, 2, JoinType::OMNI_JOIN_TYPE_LEFT_ANTI, filterJsonStr,
        overflowConfig);
    omniruntime::op::Operator *streamedTblWithExprOperator = CreateTestOperator(streamedWithExprOperatorFactory);

    std::vector<DataTypePtr> bufferTypesVector = { IntType(), DoubleDataType::Instance() };
    DataTypes bufferedTblTypes(bufferTypesVector);
    FieldExpr *bufferCol0 = new FieldExpr(0, IntType());
    std::vector<Expr *> bufferedEqualKeyExprs = { bufferCol0 };
    int bufferedOutputCols[0] = {};
    int64_t streamedWithExprOperatorFactoryAddr = reinterpret_cast<int64_t>(streamedWithExprOperatorFactory);
    BufferedTableWithExprOperatorFactory *bufferedWithExprOperatorFactory =
        BufferedTableWithExprOperatorFactory::CreateBufferedTableWithExprOperatorFactory(bufferedTblTypes,
        bufferedEqualKeyExprs, 1, bufferedOutputCols, 0, streamedWithExprOperatorFactoryAddr, overflowConfig);
    omniruntime::op::Operator *bufferedTblWithExprOperator = CreateTestOperator(bufferedWithExprOperatorFactory);

    // construct data
    const int32_t streamedTblDataSize = 4;
    int32_t streamedTblDataCol1[streamedTblDataSize] = {1,   1,   2,   3};
    long streamedTblDataCol2[streamedTblDataSize] =  {40,  25,  35,  30};
    VectorBatch *streamedTblVecBatch1 =
        CreateVectorBatch(streamedTblTypes, streamedTblDataSize, streamedTblDataCol1, streamedTblDataCol2);

    int32_t addInputRetCode = -1;
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatch1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    const int32_t bufferedTblSize = 2;
    int32_t bufferedTblDataCol1[bufferedTblSize] =  {3,  4};
    double bufferedTblDataCol2[bufferedTblSize] =  {3.3, 4.4};
    VectorBatch *bufferedTblVecBatch1 =
        CreateVectorBatch(bufferedTblTypes, bufferedTblSize, bufferedTblDataCol1, bufferedTblDataCol2);
    // need add buffered table data
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatch1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_STREAM_TBL_DATA));

    // add eof flag to streamed table
    VectorBatch *streamedTblVecBatchEof = CreateEmptyVectorBatch(streamTypeVector);
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchEof);
    ASSERT_EQ(DecodeFetchFlag(addInputRetCode), static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_FETCH_JOIN_DATA));

    std::vector<omniruntime::vec::VectorBatch *> result;
    streamedTblWithExprOperator->GetOutput(result);

    auto streamedTblVecBatchEof1 = CreateEmptyVectorBatch(streamTypeVector);
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchEof1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode), static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_SCAN_FINISH));

    // check the join result
    int resultCol1[] =  {1,  1,  2};
    long resultCol2[] =  {40,  25,  35};
    VectorBatch *expectVecBatch = CreateVectorBatch(streamedTblTypes, 3, resultCol1, resultCol2);
    ASSERT_TRUE(VecBatchMatch(result[0], expectVecBatch));
    VectorHelper::FreeVecBatch(result[0]);
    VectorHelper::FreeVecBatch(expectVecBatch);

    Expr::DeleteExprs(streamedEqualKeyExprs);
    Expr::DeleteExprs(bufferedEqualKeyExprs);
    omniruntime::op::Operator::DeleteOperator(bufferedTblWithExprOperator);
    omniruntime::op::Operator::DeleteOperator(streamedTblWithExprOperator);
    DeleteOperatorFactory(bufferedWithExprOperatorFactory);
    DeleteOperatorFactory(streamedWithExprOperatorFactory);
    delete overflowConfig;
}

TEST(SMJ_JOIN_OPERATOR_WITH_EXPR_TESTCASE, testSmjLeftAntiJoinEqualCondition)
{
    // select * from AntiTest_s left anti join AntiTest_b on AntiTest_s.CountryID = AntiTest_b.ID and
    // AntiTest_s.CountryID = 4; streamedTbl AntiTest_s: CountryID int, Units long; bufferedTbl AntiTest_b: ID int,
    // Country double;
    string filterJsonStr = "{\"exprType\":\"BINARY\","
        "\"returnType\":4,"
        "\"operator\":\"EQUAL\","
        "\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":0},"
        "\"right\":{\"exprType\":\"LITERAL\",\"dataType\":1, \"isNull\":false, \"value\":4}}";
    std::vector<DataTypePtr> streamTypeVector = { IntType(), LongType() };
    DataTypes streamedTblTypes(streamTypeVector);
    FieldExpr *streamCol0 = new FieldExpr(0, IntType());
    std::vector<Expr *> streamedEqualKeyExprs = { streamCol0 };
    int streamedOutputCols[2] = {0, 1};
    auto overflowConfig = new OverflowConfig();
    StreamedTableWithExprOperatorFactory *streamedWithExprOperatorFactory =
        StreamedTableWithExprOperatorFactory::CreateStreamedTableWithExprOperatorFactory(streamedTblTypes,
        streamedEqualKeyExprs, 1, streamedOutputCols, 2, JoinType::OMNI_JOIN_TYPE_LEFT_ANTI, filterJsonStr,
        overflowConfig);
    omniruntime::op::Operator *streamedTblWithExprOperator = CreateTestOperator(streamedWithExprOperatorFactory);

    std::vector<DataTypePtr> bufferTypesVector = { IntType(), DoubleDataType::Instance() };
    DataTypes bufferedTblTypes(bufferTypesVector);
    FieldExpr *bufferCol0 = new FieldExpr(0, IntType());
    std::vector<Expr *> bufferedEqualKeyExprs = { bufferCol0 };
    int bufferedOutputCols[0] = {};
    int64_t streamedWithExprOperatorFactoryAddr = reinterpret_cast<int64_t>(streamedWithExprOperatorFactory);
    BufferedTableWithExprOperatorFactory *bufferedWithExprOperatorFactory =
        BufferedTableWithExprOperatorFactory::CreateBufferedTableWithExprOperatorFactory(bufferedTblTypes,
        bufferedEqualKeyExprs, 1, bufferedOutputCols, 0, streamedWithExprOperatorFactoryAddr, overflowConfig);
    omniruntime::op::Operator *bufferedTblWithExprOperator = CreateTestOperator(bufferedWithExprOperatorFactory);

    // construct data
    const int32_t streamedTblDataSize = 4;
    int32_t streamedTblDataCol1[streamedTblDataSize] = {1,   1,   2,   3};
    long streamedTblDataCol2[streamedTblDataSize] =  {40,  25,  35,  30};
    VectorBatch *streamedTblVecBatch1 =
        CreateVectorBatch(streamedTblTypes, streamedTblDataSize, streamedTblDataCol1, streamedTblDataCol2);

    int32_t addInputRetCode = -1;
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatch1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    const int32_t bufferedTblSize = 2;
    int32_t bufferedTblDataCol1[bufferedTblSize] =  {3,  4};
    double bufferedTblDataCol2[bufferedTblSize] =  {3.3, 4.4};
    VectorBatch *bufferedTblVecBatch1 =
        CreateVectorBatch(bufferedTblTypes, bufferedTblSize, bufferedTblDataCol1, bufferedTblDataCol2);
    // need add buffered table data
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatch1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_STREAM_TBL_DATA));

    // add eof flag to streamed table
    VectorBatch *streamedTblVecBatchEof = CreateEmptyVectorBatch(streamTypeVector);
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchEof);
    ASSERT_EQ(DecodeFetchFlag(addInputRetCode), static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_FETCH_JOIN_DATA));

    std::vector<omniruntime::vec::VectorBatch *> result;
    streamedTblWithExprOperator->GetOutput(result);

    auto streamedTblVecBatchEof1 = CreateEmptyVectorBatch(streamTypeVector);
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchEof1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode), static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_SCAN_FINISH));

    // check the join result
    int resultCol1[] =  {1,  1,  2,  3};
    long resultCol2[] =  {40,  25,  35,  30};
    VectorBatch *expectVecBatch = CreateVectorBatch(streamedTblTypes, 4, resultCol1, resultCol2);
    ASSERT_TRUE(VecBatchMatch(result[0], expectVecBatch));
    VectorHelper::FreeVecBatch(result[0]);
    VectorHelper::FreeVecBatch(expectVecBatch);

    Expr::DeleteExprs(streamedEqualKeyExprs);
    Expr::DeleteExprs(bufferedEqualKeyExprs);
    omniruntime::op::Operator::DeleteOperator(bufferedTblWithExprOperator);
    omniruntime::op::Operator::DeleteOperator(streamedTblWithExprOperator);
    DeleteOperatorFactory(bufferedWithExprOperatorFactory);
    DeleteOperatorFactory(streamedWithExprOperatorFactory);
    delete overflowConfig;
}

TEST(SMJ_JOIN_OPERATOR_WITH_EXPR_TESTCASE, testSmjLeftAntiJoinBufferDoubleJoinRowFilterOut)
{
    // select * from AntiTest_s left anti join AntiTest_b on AntiTest_s.CountryID = AntiTest_b.ID and
    // AntiTest_s.CountryID = 4; streamedTbl AntiTest_s: CountryID int, Units long; bufferedTbl AntiTest_b: ID int,
    // Country double;
    string filterJsonStr = "{\"exprType\":\"BINARY\","
        "\"returnType\":4,"
        "\"operator\":\"EQUAL\","
        "\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":0},"
        "\"right\":{\"exprType\":\"LITERAL\",\"dataType\":1, \"isNull\":false, \"value\":4}}";
    std::vector<DataTypePtr> streamTypeVector = { IntType(), LongType() };
    DataTypes streamedTblTypes(streamTypeVector);
    FieldExpr *streamCol0 = new FieldExpr(0, IntType());
    std::vector<Expr *> streamedEqualKeyExprs = { streamCol0 };
    int streamedOutputCols[2] = {0, 1};
    auto overflowConfig = new OverflowConfig();
    StreamedTableWithExprOperatorFactory *streamedWithExprOperatorFactory =
        StreamedTableWithExprOperatorFactory::CreateStreamedTableWithExprOperatorFactory(streamedTblTypes,
        streamedEqualKeyExprs, 1, streamedOutputCols, 2, JoinType::OMNI_JOIN_TYPE_LEFT_ANTI, filterJsonStr,
        overflowConfig);
    omniruntime::op::Operator *streamedTblWithExprOperator = CreateTestOperator(streamedWithExprOperatorFactory);

    std::vector<DataTypePtr> bufferTypesVector = { IntType(), DoubleDataType::Instance() };
    DataTypes bufferedTblTypes(bufferTypesVector);
    FieldExpr *bufferCol0 = new FieldExpr(0, IntType());
    std::vector<Expr *> bufferedEqualKeyExprs = { bufferCol0 };
    int bufferedOutputCols[0] = {};
    int64_t streamedWithExprOperatorFactoryAddr = reinterpret_cast<int64_t>(streamedWithExprOperatorFactory);
    BufferedTableWithExprOperatorFactory *bufferedWithExprOperatorFactory =
        BufferedTableWithExprOperatorFactory::CreateBufferedTableWithExprOperatorFactory(bufferedTblTypes,
        bufferedEqualKeyExprs, 1, bufferedOutputCols, 0, streamedWithExprOperatorFactoryAddr, overflowConfig);
    omniruntime::op::Operator *bufferedTblWithExprOperator = CreateTestOperator(bufferedWithExprOperatorFactory);

    // construct data
    const int32_t streamedTblDataSize = 4;
    int32_t streamedTblDataCol1[streamedTblDataSize] = {1,   1,   2,   3};
    long streamedTblDataCol2[streamedTblDataSize] =  {40,  25,  35,  30};
    VectorBatch *streamedTblVecBatch1 =
        CreateVectorBatch(streamedTblTypes, streamedTblDataSize, streamedTblDataCol1, streamedTblDataCol2);

    int32_t addInputRetCode = -1;
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatch1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    const int32_t bufferedTblSize = 4;
    int32_t bufferedTblDataCol1[bufferedTblSize] =  {3,  3,  3,  4};
    double bufferedTblDataCol2[bufferedTblSize] =  {3.3,  3.3,  3.4,  4.4};
    VectorBatch *bufferedTblVecBatch1 =
        CreateVectorBatch(bufferedTblTypes, bufferedTblSize, bufferedTblDataCol1, bufferedTblDataCol2);
    // need add buffered table data
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatch1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_STREAM_TBL_DATA));

    // add eof flag to streamed table
    VectorBatch *streamedTblVecBatchEof = CreateEmptyVectorBatch(streamTypeVector);
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchEof);
    ASSERT_EQ(DecodeFetchFlag(addInputRetCode), static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_FETCH_JOIN_DATA));

    std::vector<omniruntime::vec::VectorBatch *> result;
    streamedTblWithExprOperator->GetOutput(result);

    auto streamedTblVecBatchEof1 = CreateEmptyVectorBatch(streamTypeVector);
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchEof1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode), static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_SCAN_FINISH));

    // check the join result
    int resultCol1[] =  {1,  1,  2,  3};
    long resultCol2[] =  {40,  25,  35,  30};
    VectorBatch *expectVecBatch = CreateVectorBatch(streamedTblTypes, 4, resultCol1, resultCol2);
    ASSERT_TRUE(VecBatchMatch(result[0], expectVecBatch));
    VectorHelper::FreeVecBatch(result[0]);
    VectorHelper::FreeVecBatch(expectVecBatch);

    Expr::DeleteExprs(streamedEqualKeyExprs);
    Expr::DeleteExprs(bufferedEqualKeyExprs);
    omniruntime::op::Operator::DeleteOperator(bufferedTblWithExprOperator);
    omniruntime::op::Operator::DeleteOperator(streamedTblWithExprOperator);
    DeleteOperatorFactory(bufferedWithExprOperatorFactory);
    DeleteOperatorFactory(streamedWithExprOperatorFactory);
    delete overflowConfig;
}

TEST(SMJ_JOIN_OPERATOR_WITH_EXPR_TESTCASE, testSmjLeftAntiJoinWithStreamedNullFist)
{
    // select * from AntiTest_s left anti join AntiTest_b on AntiTest_s.CountryID = AntiTest_b.ID and
    // AntiTest_s.CountryID = 4; streamedTbl AntiTest_s: CountryID int, Units long; bufferedTbl AntiTest_b: ID int,
    // Country double;
    string filterJsonStr = "{\"exprType\":\"BINARY\","
        "\"returnType\":4,"
        "\"operator\":\"EQUAL\","
        "\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":0},"
        "\"right\":{\"exprType\":\"LITERAL\",\"dataType\":1, \"isNull\":false, \"value\":4}}";
    std::vector<DataTypePtr> streamTypeVector = { IntType(), LongType() };
    DataTypes streamedTblTypes(streamTypeVector);
    FieldExpr *streamCol0 = new FieldExpr(0, IntType());
    std::vector<Expr *> streamedEqualKeyExprs = { streamCol0 };
    int streamedOutputCols[2] = {0, 1};
    auto overflowConfig = new OverflowConfig();
    StreamedTableWithExprOperatorFactory *streamedWithExprOperatorFactory =
        StreamedTableWithExprOperatorFactory::CreateStreamedTableWithExprOperatorFactory(streamedTblTypes,
        streamedEqualKeyExprs, 1, streamedOutputCols, 2, JoinType::OMNI_JOIN_TYPE_LEFT_ANTI, filterJsonStr,
        overflowConfig);
    omniruntime::op::Operator *streamedTblWithExprOperator = CreateTestOperator(streamedWithExprOperatorFactory);

    std::vector<DataTypePtr> bufferTypesVector = { IntType(), DoubleDataType::Instance() };
    DataTypes bufferedTblTypes(bufferTypesVector);
    FieldExpr *bufferCol0 = new FieldExpr(0, IntType());
    std::vector<Expr *> bufferedEqualKeyExprs = { bufferCol0 };
    int bufferedOutputCols[0] = {};
    int64_t streamedWithExprOperatorFactoryAddr = reinterpret_cast<int64_t>(streamedWithExprOperatorFactory);
    BufferedTableWithExprOperatorFactory *bufferedWithExprOperatorFactory =
        BufferedTableWithExprOperatorFactory::CreateBufferedTableWithExprOperatorFactory(bufferedTblTypes,
        bufferedEqualKeyExprs, 1, bufferedOutputCols, 0, streamedWithExprOperatorFactoryAddr, overflowConfig);
    omniruntime::op::Operator *bufferedTblWithExprOperator = CreateTestOperator(bufferedWithExprOperatorFactory);

    // construct data
    const int32_t streamedTblDataSize = 4;
    int32_t streamedTblDataCol1[streamedTblDataSize] = {1,   1,   2,   3};
    long streamedTblDataCol2[streamedTblDataSize] =  {40,  25,  35,  30};
    VectorBatch *streamedTblVecBatch1 =
        CreateVectorBatch(streamedTblTypes, streamedTblDataSize, streamedTblDataCol1, streamedTblDataCol2);
    streamedTblVecBatch1->GetVector(0)->SetValueNull(0); // NULL, 1, 2, 3

    int32_t addInputRetCode = -1;
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatch1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    const int32_t bufferedTblSize = 2;
    int32_t bufferedTblDataCol1[bufferedTblSize] =  {3,  4};
    double bufferedTblDataCol2[bufferedTblSize] =  {3.3, 4.4};
    VectorBatch *bufferedTblVecBatch1 =
        CreateVectorBatch(bufferedTblTypes, bufferedTblSize, bufferedTblDataCol1, bufferedTblDataCol2);
    // need add buffered table data
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatch1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_STREAM_TBL_DATA));

    // add eof flag to streamed table
    VectorBatch *streamedTblVecBatchEof = CreateEmptyVectorBatch(streamTypeVector);
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchEof);
    ASSERT_EQ(DecodeFetchFlag(addInputRetCode), static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_FETCH_JOIN_DATA));

    std::vector<omniruntime::vec::VectorBatch *> result;
    streamedTblWithExprOperator->GetOutput(result);

    auto streamedTblVecBatchEof1 = CreateEmptyVectorBatch(streamTypeVector);
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchEof1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode), static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_SCAN_FINISH));

    // check the join result
    int resultCol1[] =  {0,  1,  2,  3};
    long resultCol2[] =  {40,  25,  35,  30};
    VectorBatch *expectVecBatch = CreateVectorBatch(streamedTblTypes, 4, resultCol1, resultCol2);
    expectVecBatch->GetVector(0)->SetValueNull(0);
    ASSERT_TRUE(VecBatchMatch(result[0], expectVecBatch));
    VectorHelper::FreeVecBatch(result[0]);
    VectorHelper::FreeVecBatch(expectVecBatch);

    Expr::DeleteExprs(streamedEqualKeyExprs);
    Expr::DeleteExprs(bufferedEqualKeyExprs);
    omniruntime::op::Operator::DeleteOperator(bufferedTblWithExprOperator);
    omniruntime::op::Operator::DeleteOperator(streamedTblWithExprOperator);
    DeleteOperatorFactory(bufferedWithExprOperatorFactory);
    DeleteOperatorFactory(streamedWithExprOperatorFactory);
    delete overflowConfig;
}

TEST(SMJ_JOIN_OPERATOR_WITH_EXPR_TESTCASE, testSmjLeftAntiJoinWithBufferedNullFist)
{
    // select * from AntiTest_s left anti join AntiTest_b on AntiTest_s.CountryID = AntiTest_b.ID and
    // AntiTest_s.CountryID = 4; streamedTbl AntiTest_s: CountryID int, Units long; bufferedTbl AntiTest_b: ID int,
    // Country double;
    string filterJsonStr = "{\"exprType\":\"BINARY\","
        "\"returnType\":4,"
        "\"operator\":\"EQUAL\","
        "\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":0},"
        "\"right\":{\"exprType\":\"LITERAL\",\"dataType\":1, \"isNull\":false, \"value\":4}}";
    std::vector<DataTypePtr> streamTypeVector = { IntType(), LongType() };
    DataTypes streamedTblTypes(streamTypeVector);
    FieldExpr *streamCol0 = new FieldExpr(0, IntType());
    std::vector<Expr *> streamedEqualKeyExprs = { streamCol0 };
    int streamedOutputCols[2] = {0, 1};
    auto overflowConfig = new OverflowConfig();
    StreamedTableWithExprOperatorFactory *streamedWithExprOperatorFactory =
        StreamedTableWithExprOperatorFactory::CreateStreamedTableWithExprOperatorFactory(streamedTblTypes,
        streamedEqualKeyExprs, 1, streamedOutputCols, 2, JoinType::OMNI_JOIN_TYPE_LEFT_ANTI, filterJsonStr,
        overflowConfig);
    omniruntime::op::Operator *streamedTblWithExprOperator = CreateTestOperator(streamedWithExprOperatorFactory);

    std::vector<DataTypePtr> bufferTypesVector = { IntType(), DoubleDataType::Instance() };
    DataTypes bufferedTblTypes(bufferTypesVector);
    FieldExpr *bufferCol0 = new FieldExpr(0, IntType());
    std::vector<Expr *> bufferedEqualKeyExprs = { bufferCol0 };
    int bufferedOutputCols[0] = {};
    int64_t streamedWithExprOperatorFactoryAddr = reinterpret_cast<int64_t>(streamedWithExprOperatorFactory);
    BufferedTableWithExprOperatorFactory *bufferedWithExprOperatorFactory =
        BufferedTableWithExprOperatorFactory::CreateBufferedTableWithExprOperatorFactory(bufferedTblTypes,
        bufferedEqualKeyExprs, 1, bufferedOutputCols, 0, streamedWithExprOperatorFactoryAddr, overflowConfig);
    omniruntime::op::Operator *bufferedTblWithExprOperator = CreateTestOperator(bufferedWithExprOperatorFactory);

    // construct data
    const int32_t streamedTblDataSize = 4;
    int32_t streamedTblDataCol1[streamedTblDataSize] = {1,   1,   2,   3};
    long streamedTblDataCol2[streamedTblDataSize] =  {40,  25,  35,  30};
    VectorBatch *streamedTblVecBatch1 =
        CreateVectorBatch(streamedTblTypes, streamedTblDataSize, streamedTblDataCol1, streamedTblDataCol2);

    int32_t addInputRetCode = -1;
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatch1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    const int32_t bufferedTblSize = 3;
    int32_t bufferedTblDataCol1[bufferedTblSize] =  {3,  3,  4};
    double bufferedTblDataCol2[bufferedTblSize] =  {3.3,  3.3, 4.4};
    VectorBatch *bufferedTblVecBatch1 =
        CreateVectorBatch(bufferedTblTypes, bufferedTblSize, bufferedTblDataCol1, bufferedTblDataCol2);
    bufferedTblVecBatch1->GetVector(0)->SetValueNull(0); // NULL,  3,  4

    // need add buffered table data
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatch1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_STREAM_TBL_DATA));

    // add eof flag to streamed table
    VectorBatch *streamedTblVecBatchEof = CreateEmptyVectorBatch(streamTypeVector);
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchEof);
    ASSERT_EQ(DecodeFetchFlag(addInputRetCode), static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_FETCH_JOIN_DATA));

    std::vector<omniruntime::vec::VectorBatch *> result;
    streamedTblWithExprOperator->GetOutput(result);

    auto streamedTblVecBatchEof1 = CreateEmptyVectorBatch(streamTypeVector);
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchEof1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode), static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_SCAN_FINISH));

    // check the join result
    int resultCol1[] =  {1,  1,  2,  3};
    long resultCol2[] =  {40,  25,  35,  30};
    VectorBatch *expectVecBatch = CreateVectorBatch(streamedTblTypes, 4, resultCol1, resultCol2);
    ASSERT_TRUE(VecBatchMatch(result[0], expectVecBatch));
    VectorHelper::FreeVecBatch(result[0]);
    VectorHelper::FreeVecBatch(expectVecBatch);

    Expr::DeleteExprs(streamedEqualKeyExprs);
    Expr::DeleteExprs(bufferedEqualKeyExprs);
    omniruntime::op::Operator::DeleteOperator(bufferedTblWithExprOperator);
    omniruntime::op::Operator::DeleteOperator(streamedTblWithExprOperator);
    DeleteOperatorFactory(bufferedWithExprOperatorFactory);
    DeleteOperatorFactory(streamedWithExprOperatorFactory);
    delete overflowConfig;
}

TEST(SMJ_JOIN_OPERATOR_WITH_EXPR_TESTCASE, testSmjLeftAntiJoinWithBothSideNullFist)
{
    // select * from AntiTest_s left anti join AntiTest_b on AntiTest_s.CountryID = AntiTest_b.ID and
    // AntiTest_s.CountryID = 4; streamedTbl AntiTest_s: CountryID int, Units long; bufferedTbl AntiTest_b: ID int,
    // Country double;
    string filterJsonStr = "{\"exprType\":\"BINARY\","
        "\"returnType\":4,"
        "\"operator\":\"EQUAL\","
        "\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":0},"
        "\"right\":{\"exprType\":\"LITERAL\",\"dataType\":1, \"isNull\":false, \"value\":4}}";
    std::vector<DataTypePtr> streamTypeVector = { IntType(), LongType() };
    DataTypes streamedTblTypes(streamTypeVector);
    FieldExpr *streamCol0 = new FieldExpr(0, IntType());
    std::vector<Expr *> streamedEqualKeyExprs = { streamCol0 };
    int streamedOutputCols[2] = {0, 1};
    auto overflowConfig = new OverflowConfig();
    StreamedTableWithExprOperatorFactory *streamedWithExprOperatorFactory =
        StreamedTableWithExprOperatorFactory::CreateStreamedTableWithExprOperatorFactory(streamedTblTypes,
        streamedEqualKeyExprs, 1, streamedOutputCols, 2, JoinType::OMNI_JOIN_TYPE_LEFT_ANTI, filterJsonStr,
        overflowConfig);
    omniruntime::op::Operator *streamedTblWithExprOperator = CreateTestOperator(streamedWithExprOperatorFactory);

    std::vector<DataTypePtr> bufferTypesVector = { IntType(), DoubleDataType::Instance() };
    DataTypes bufferedTblTypes(bufferTypesVector);
    FieldExpr *bufferCol0 = new FieldExpr(0, IntType());
    std::vector<Expr *> bufferedEqualKeyExprs = { bufferCol0 };
    int bufferedOutputCols[0] = {};
    int64_t streamedWithExprOperatorFactoryAddr = reinterpret_cast<int64_t>(streamedWithExprOperatorFactory);
    BufferedTableWithExprOperatorFactory *bufferedWithExprOperatorFactory =
        BufferedTableWithExprOperatorFactory::CreateBufferedTableWithExprOperatorFactory(bufferedTblTypes,
        bufferedEqualKeyExprs, 1, bufferedOutputCols, 0, streamedWithExprOperatorFactoryAddr, overflowConfig);
    omniruntime::op::Operator *bufferedTblWithExprOperator = CreateTestOperator(bufferedWithExprOperatorFactory);

    // construct data
    const int32_t streamedTblDataSize = 4;
    int32_t streamedTblDataCol1[streamedTblDataSize] = {1,   1,   2,   3};
    long streamedTblDataCol2[streamedTblDataSize] =  {40,  25,  35,  30};
    VectorBatch *streamedTblVecBatch1 =
        CreateVectorBatch(streamedTblTypes, streamedTblDataSize, streamedTblDataCol1, streamedTblDataCol2);
    streamedTblVecBatch1->GetVector(0)->SetValueNull(0); // NULL, 1, 2, 3

    int32_t addInputRetCode = -1;
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatch1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    const int32_t bufferedTblSize = 3;
    int32_t bufferedTblDataCol1[bufferedTblSize] =  {3,  3,  4};
    double bufferedTblDataCol2[bufferedTblSize] =  {3.3,  3.3,  4.4};
    VectorBatch *bufferedTblVecBatch1 =
        CreateVectorBatch(bufferedTblTypes, bufferedTblSize, bufferedTblDataCol1, bufferedTblDataCol2);
    bufferedTblVecBatch1->GetVector(0)->SetValueNull(0); // NULL,  3,  4

    // need add buffered table data
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatch1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_STREAM_TBL_DATA));

    // add eof flag to streamed table
    VectorBatch *streamedTblVecBatchEof = CreateEmptyVectorBatch(streamTypeVector);
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchEof);
    ASSERT_EQ(DecodeFetchFlag(addInputRetCode), static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_FETCH_JOIN_DATA));

    std::vector<omniruntime::vec::VectorBatch *> result;
    streamedTblWithExprOperator->GetOutput(result);

    auto streamedTblVecBatchEof1 = CreateEmptyVectorBatch(streamTypeVector);
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchEof1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode), static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_SCAN_FINISH));

    // check the join result
    int resultCol1[] =  {0,  1,  2,  3};
    long resultCol2[] =  {40,  25,  35,  30};
    VectorBatch *expectVecBatch = CreateVectorBatch(streamedTblTypes, 4, resultCol1, resultCol2);
    expectVecBatch->GetVector(0)->SetValueNull(0);
    ASSERT_TRUE(VecBatchMatch(result[0], expectVecBatch));
    VectorHelper::FreeVecBatch(result[0]);
    VectorHelper::FreeVecBatch(expectVecBatch);

    Expr::DeleteExprs(streamedEqualKeyExprs);
    Expr::DeleteExprs(bufferedEqualKeyExprs);
    omniruntime::op::Operator::DeleteOperator(bufferedTblWithExprOperator);
    omniruntime::op::Operator::DeleteOperator(streamedTblWithExprOperator);
    DeleteOperatorFactory(bufferedWithExprOperatorFactory);
    DeleteOperatorFactory(streamedWithExprOperatorFactory);
    delete overflowConfig;
}

TEST(SMJ_JOIN_OPERATOR_WITH_EXPR_TESTCASE, testSmjLeftAntiJoinWithMutilRowAndFilterOut)
{
    // select * from AntiTest_s left anti join AntiTest_b on AntiTest_s.CountryID = AntiTest_b.ID and
    // AntiTest_b.Country = 3.3; streamedTbl AntiTest_s: CountryID int, Units long; bufferedTbl AntiTest_b: ID int,
    // Country double;
    string filterJsonStr = "{\"exprType\":\"BINARY\","
        "\"returnType\":4,"
        "\"operator\":\"GREATER_THAN\","
        "\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":3,\"colVal\":3},"
        "\"right\":{\"exprType\":\"LITERAL\",\"dataType\":3, \"isNull\":false, \"value\":3.3}}";
    std::vector<DataTypePtr> streamTypeVector = { IntType(), LongType() };
    DataTypes streamedTblTypes(streamTypeVector);
    FieldExpr *streamCol0 = new FieldExpr(0, IntType());
    std::vector<Expr *> streamedEqualKeyExprs = { streamCol0 };
    int streamedOutputCols[2] = {0, 1};
    auto overflowConfig = new OverflowConfig();
    StreamedTableWithExprOperatorFactory *streamedWithExprOperatorFactory =
        StreamedTableWithExprOperatorFactory::CreateStreamedTableWithExprOperatorFactory(streamedTblTypes,
        streamedEqualKeyExprs, 1, streamedOutputCols, 2, JoinType::OMNI_JOIN_TYPE_LEFT_ANTI, filterJsonStr,
        overflowConfig);
    omniruntime::op::Operator *streamedTblWithExprOperator = CreateTestOperator(streamedWithExprOperatorFactory);

    std::vector<DataTypePtr> bufferTypesVector = { IntType(), DoubleDataType::Instance() };
    DataTypes bufferedTblTypes(bufferTypesVector);
    FieldExpr *bufferCol0 = new FieldExpr(0, IntType());
    std::vector<Expr *> bufferedEqualKeyExprs = { bufferCol0 };
    int bufferedOutputCols[0] = {};
    int64_t streamedWithExprOperatorFactoryAddr = reinterpret_cast<int64_t>(streamedWithExprOperatorFactory);
    BufferedTableWithExprOperatorFactory *bufferedWithExprOperatorFactory =
        BufferedTableWithExprOperatorFactory::CreateBufferedTableWithExprOperatorFactory(bufferedTblTypes,
        bufferedEqualKeyExprs, 1, bufferedOutputCols, 0, streamedWithExprOperatorFactoryAddr, overflowConfig);
    omniruntime::op::Operator *bufferedTblWithExprOperator = CreateTestOperator(bufferedWithExprOperatorFactory);

    // construct data
    const int32_t streamedTblDataSize = 4;
    int32_t streamedTblDataCol1[streamedTblDataSize] = {1,   1,   2,   3};
    long streamedTblDataCol2[streamedTblDataSize] =  {40,  25,  35,  30};
    VectorBatch *streamedTblVecBatch1 =
        CreateVectorBatch(streamedTblTypes, streamedTblDataSize, streamedTblDataCol1, streamedTblDataCol2);
    streamedTblVecBatch1->GetVector(0)->SetValueNull(0); // NULL, 1, 2, 3

    int32_t addInputRetCode = -1;
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatch1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA));

    const int32_t bufferedTblSize = 5;
    int32_t bufferedTblDataCol1[bufferedTblSize] =  {2,  3,  3,  3,  4};
    double bufferedTblDataCol2[bufferedTblSize] =  {2.2,  3.4,  3.3,  3.3,  4.5};
    VectorBatch *bufferedTblVecBatch1 =
        CreateVectorBatch(bufferedTblTypes, bufferedTblSize, bufferedTblDataCol1, bufferedTblDataCol2);

    // need add buffered table data
    addInputRetCode = bufferedTblWithExprOperator->AddInput(bufferedTblVecBatch1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode),
        static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_STREAM_TBL_DATA));

    // add eof flag to streamed table
    VectorBatch *streamedTblVecBatchEof = CreateEmptyVectorBatch(streamTypeVector);
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchEof);
    ASSERT_EQ(DecodeFetchFlag(addInputRetCode), static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_FETCH_JOIN_DATA));

    std::vector<omniruntime::vec::VectorBatch *> result;
    streamedTblWithExprOperator->GetOutput(result);

    auto streamedTblVecBatchEof1 = CreateEmptyVectorBatch(streamTypeVector);
    addInputRetCode = streamedTblWithExprOperator->AddInput(streamedTblVecBatchEof1);
    ASSERT_EQ(DecodeAddFlag(addInputRetCode), static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_SCAN_FINISH));

    // check the join result
    int resultCol1[] =  {0,  1,  2,  3};
    long resultCol2[] =  {40,  25,  35,  30};
    VectorBatch *expectVecBatch = CreateVectorBatch(streamedTblTypes, 4, resultCol1, resultCol2);
    expectVecBatch->GetVector(0)->SetValueNull(0);
    ASSERT_TRUE(VecBatchMatch(result[0], expectVecBatch));
    VectorHelper::FreeVecBatch(result[0]);
    VectorHelper::FreeVecBatch(expectVecBatch);

    Expr::DeleteExprs(streamedEqualKeyExprs);
    Expr::DeleteExprs(bufferedEqualKeyExprs);
    omniruntime::op::Operator::DeleteOperator(bufferedTblWithExprOperator);
    omniruntime::op::Operator::DeleteOperator(streamedTblWithExprOperator);
    DeleteOperatorFactory(bufferedWithExprOperatorFactory);
    DeleteOperatorFactory(streamedWithExprOperatorFactory);
    delete overflowConfig;
}
}