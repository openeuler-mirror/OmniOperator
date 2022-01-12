/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: ...
 */
#include <vector>
#include "gtest/gtest.h"
#include "../../../src/vector/vector_helper.h"
#include "../../util/test_util.h"
#include "../../../src/operator/pages_index.h"
#include "../../../src/operator/join/sortmergejoin/dynamic_pages_index.h"
#include "../../../src/operator/join/sortmergejoin/sort_merge_join_resultBuilder.h"
#include "../../../src/operator/join/sortmergejoin/sort_merge_join.h"

using namespace omniruntime::op;
using namespace std;

TEST(DYNAMICPAGESINDEX_TESTCASE, testMultiAddVecBatches)
{
    std::vector<VecType> types = { IntVecType::Instance(), DoubleVecType::Instance() };
    VecTypes sourceTypes(types);
    DynamicPagesIndex *dynamicPagesIndex = new DynamicPagesIndex(sourceTypes);
    ASSERT_EQ(dynamicPagesIndex->GetPositionCount(), 0);

    // construct data;
    const int32_t DATA_SIZE1 = 6;
    // table1
    int32_t data1[DATA_SIZE1] = {0, 1, 2, 0, 1, 2};
    double data2[DATA_SIZE1] = {6.6, 5.5, 4.4, 3.3, 2.2, 1.1};
    // table2
    const int32_t DATA_SIZE2 = 7;
    int32_t data3[DATA_SIZE2] = {10, 11, 12, 10, 11, 12, 15};
    double data4[DATA_SIZE2] = {16.6, 15.5, 14.4, 13.3, 12.2, 11.1, 11.3};

    VectorBatch *vecBatch1 = CreateVectorBatch(sourceTypes, DATA_SIZE1, data1, data2);
    VectorBatch *vecBatch2 = CreateVectorBatch(sourceTypes, DATA_SIZE2, data3, data4);

    std::vector<VectorBatch *> vector1;
    vector1.push_back(vecBatch1);
    dynamicPagesIndex->AddVecBatches(vector1);
    ASSERT_EQ(dynamicPagesIndex->GetPositionCount(), DATA_SIZE1);
    ASSERT_EQ(dynamicPagesIndex->IsDataFinish(), false);

    std::vector<VectorBatch *> vector2;
    vector2.push_back(vecBatch2);
    dynamicPagesIndex->AddVecBatches(vector2);
    ASSERT_EQ(dynamicPagesIndex->GetPositionCount(), DATA_SIZE1 + DATA_SIZE2);
    ASSERT_EQ(dynamicPagesIndex->IsDataFinish(), false);

    std::vector<VectorBatch *> vector3;
    VectorBatch *emptyVectorBatch = createEmptyVectorBatch(types);
    vector3.push_back(emptyVectorBatch);

    dynamicPagesIndex->AddVecBatches(vector3);
    ASSERT_EQ(dynamicPagesIndex->GetPositionCount(), DATA_SIZE1 + DATA_SIZE2);
    ASSERT_EQ(dynamicPagesIndex->IsDataFinish(), true);

    VectorHelper::FreeVecBatch(vecBatch1);
    VectorHelper::FreeVecBatch(vecBatch2);
    VectorHelper::FreeVecBatch(emptyVectorBatch);
    delete dynamicPagesIndex;
}


TEST(DYNAMICPAGESINDEX_TESTCASE, testFreeInterface)
{
    std::vector<VecType> types = { IntVecType::Instance(), DoubleVecType::Instance() };
    VecTypes sourceTypes(types);
    DynamicPagesIndex *dynamicPagesIndex = new DynamicPagesIndex(sourceTypes);
    ASSERT_EQ(dynamicPagesIndex->GetPositionCount(), 0);

    // construct data;
    const int32_t DATA_SIZE1 = 6;
    // table1
    int32_t data1[DATA_SIZE1] = {0, 1, 2, 0, 1, 2};
    double data2[DATA_SIZE1] = {6.6, 5.5, 4.4, 3.3, 2.2, 1.1};
    // table2
    const int32_t DATA_SIZE2 = 7;
    int32_t data3[DATA_SIZE2] = {10, 11, 12, 10, 11, 12, 15};
    double data4[DATA_SIZE2] = {16.6, 15.5, 14.4, 13.3, 12.2, 11.1, 11.3};

    VectorBatch *vecBatch1 = CreateVectorBatch(sourceTypes, DATA_SIZE1, data1, data2);
    VectorBatch *vecBatch2 = CreateVectorBatch(sourceTypes, DATA_SIZE2, data3, data4);

    std::vector<VectorBatch *> vector1;
    vector1.push_back(vecBatch1);
    dynamicPagesIndex->AddVecBatches(vector1);
    ASSERT_EQ(dynamicPagesIndex->GetPositionCount(), DATA_SIZE1);
    ASSERT_EQ(dynamicPagesIndex->IsDataFinish(), false);

    std::vector<VectorBatch *> vector2;
    vector2.push_back(vecBatch2);
    dynamicPagesIndex->AddVecBatches(vector2);
    ASSERT_EQ(dynamicPagesIndex->GetPositionCount(), DATA_SIZE1 + DATA_SIZE2);
    ASSERT_EQ(dynamicPagesIndex->IsDataFinish(), false);

    // use dynamicPageIndex to free vecBatch1
    dynamicPagesIndex->FreeVecBatch(0);
    ASSERT_EQ(dynamicPagesIndex->GetColumns(0, 0), nullptr);
    ASSERT_NE(dynamicPagesIndex->GetColumns(1, 0), nullptr);

    VectorHelper::FreeVecBatch(vecBatch2);
    delete dynamicPagesIndex;
}

TEST(DYNAMICPAGESINDEX_TESTCASE, testDataValue)
{
    std::vector<VecType> types = { IntVecType::Instance(), DoubleVecType::Instance() };
    VecTypes sourceTypes(types);
    DynamicPagesIndex *dynamicPagesIndex = new DynamicPagesIndex(sourceTypes);
    ASSERT_EQ(dynamicPagesIndex->GetPositionCount(), 0);

    // construct data;
    const int32_t DATA_SIZE1 = 6;
    // table1
    int32_t data1[DATA_SIZE1] = {  0,   1,   2,   3,   4,   5};
    double data2[DATA_SIZE1] =  {6.6, 5.5, 4.4, 3.3, 2.2, 1.1};
    // table2
    const int32_t DATA_SIZE2 = 7;
    int32_t data3[DATA_SIZE2] = {  6,    7,    8,    9,    10,   11,   12};
    double data4[DATA_SIZE2] =  {16.6, 15.5, 14.4, 13.3, 12.2, 11.1, 11.3};

    VectorBatch *vecBatch1 = CreateVectorBatch(sourceTypes, DATA_SIZE1, data1, data2);
    VectorBatch *vecBatch2 = CreateVectorBatch(sourceTypes, DATA_SIZE2, data3, data4);
    vecBatch2->GetVector(0)->SetValueNull(5);

    std::vector<VectorBatch *> vector1;
    vector1.push_back(vecBatch1);
    dynamicPagesIndex->AddVecBatches(vector1);
    ASSERT_EQ(dynamicPagesIndex->GetPositionCount(), DATA_SIZE1);
    ASSERT_EQ(dynamicPagesIndex->IsDataFinish(), false);
    // fetch logical row of 6
    int64_t row6ValueAddress = dynamicPagesIndex->GetValueAddresses(5);
    int32_t row6vecBatchIndex = DecodeSliceIndex(row6ValueAddress);
    int32_t row6IndexInVecBatch = DecodePosition(row6ValueAddress);
    ASSERT_EQ(row6vecBatchIndex, 0);
    ASSERT_EQ(row6IndexInVecBatch, 5);
    DoubleVector *row6DdoubleVector = static_cast<DoubleVector *>(dynamicPagesIndex->GetColumns(row6vecBatchIndex, 1));
    ASSERT_EQ(row6DdoubleVector->GetValue(5), 1.1);


    std::vector<VectorBatch *> vector2;
    vector2.push_back(vecBatch2);
    dynamicPagesIndex->AddVecBatches(vector2);
    ASSERT_EQ(dynamicPagesIndex->GetPositionCount(), DATA_SIZE1 + DATA_SIZE2);
    ASSERT_EQ(dynamicPagesIndex->IsDataFinish(), false);

    // fetch logical row of 8
    int64_t row8ValueAddress = dynamicPagesIndex->GetValueAddresses(7);
    int32_t row8vecBatchIndex = DecodeSliceIndex(row8ValueAddress);
    int32_t row8IndexInVecBatch = DecodePosition(row8ValueAddress);
    ASSERT_EQ(row8vecBatchIndex, 1);
    ASSERT_EQ(row8IndexInVecBatch, 1);
    DoubleVector *row8DoubleVector = static_cast<DoubleVector *>(dynamicPagesIndex->GetColumns(row8vecBatchIndex, 1));
    ASSERT_EQ(row8DoubleVector->GetValue(1), 15.5);

    // fetch logical row of 12
    int64_t row12ValueAddress = dynamicPagesIndex->GetValueAddresses(11);
    int32_t row12vecBatchIndex = DecodeSliceIndex(row12ValueAddress);
    int32_t row12IndexInVecBatch = DecodePosition(row12ValueAddress);
    ASSERT_EQ(row12vecBatchIndex, 1);
    ASSERT_EQ(row12IndexInVecBatch, 5);
    IntVector *row12IntVector = static_cast<IntVector *>(dynamicPagesIndex->GetColumns(row12vecBatchIndex, 0));
    ASSERT_EQ(row12IntVector->IsValueNull(4), false);
    ASSERT_EQ(row12IntVector->IsValueNull(5), true);
    ASSERT_EQ(row12IntVector->IsValueNull(6), false);

    VectorHelper::FreeVecBatch(vecBatch1);
    VectorHelper::FreeVecBatch(vecBatch2);
    delete dynamicPagesIndex;
}

TEST(SMJ_JOIN_OPERATOR_TESTCASE, testSmjOneTimeEqualCondition)
{
    // select t1.b, t2.c from t1, t2 where t1.a = t2.d
    // bufferedTbl t2: double c, int d;
    // streamedTbl t1:  int a, Long b;
    std::string blank = "";
    SortMergeJoinOperator *smjOp = new SortMergeJoinOperator(OMNI_JOIN_TYPE_INNER, blank);

    std::vector<VecType> streamTypesVector = { IntVecType::Instance(), LongVecType::Instance() };
    VecTypes streamedTblTypes(streamTypesVector);
    std::vector<int32_t> streamedKeysCols;
    streamedKeysCols.push_back(0);
    std::vector<int32_t> streamedOutputCols;
    streamedOutputCols.push_back(1);
    smjOp->ConfigStreamedTblInfo(streamedTblTypes, streamedKeysCols, streamedOutputCols);

    std::vector<VecType> bufferTypesVector = { DoubleVecType::Instance(), IntVecType::Instance() };
    VecTypes bufferedTblTypes(bufferTypesVector);
    std::vector<int32_t> bufferedKeysCols;
    bufferedKeysCols.push_back(1);
    std::vector<int32_t> bufferedOutputCols;
    bufferedOutputCols.push_back(0);
    smjOp->ConfigBufferedTblInfo(bufferedTblTypes, bufferedKeysCols, bufferedOutputCols);
    smjOp->InitScannerAndResultBuilder();

    // construct data;
    const int32_t streamedTblDataSize = 6;
    int32_t streamedTblDataCol1[streamedTblDataSize] = {  0,   1,   2,   3,   4,   5};
    long streamedTblDataCol2[streamedTblDataSize] =  {6600, 5500, 4400, 3300, 2200, 1100};
    VectorBatch *streamedTblVecBatch1 =
        CreateVectorBatch(streamedTblTypes, streamedTblDataSize, streamedTblDataCol1, streamedTblDataCol2);

    const int32_t bufferedTblSize = 6;
    double bufferedTblDataCol1[bufferedTblSize] =  {6.6, 5.5, 4.4, 3.3, 2.2, 1.1};
    int32_t bufferedTblDataCol2[bufferedTblSize] =  {  0,   1,   2,   3,   4,   5};
    VectorBatch *bufferedTblVecBatch1 =
        CreateVectorBatch(bufferedTblTypes, bufferedTblSize, bufferedTblDataCol1, bufferedTblDataCol2);

    // need add buffered table data
    int32_t addInputRetCode = smjOp->AddStreamedTableInput(streamedTblVecBatch1);
    ASSERT_EQ(addInputRetCode, SMJ_NEED_ADD_BUFFER_TBL_DATA);

    // need add buffered table data
    addInputRetCode = smjOp->AddBufferedTableInput(bufferedTblVecBatch1);
    ASSERT_EQ(addInputRetCode, SMJ_NEED_ADD_BUFFER_TBL_DATA);

    // add eof flag to buffered table , need add streamed table data
    VectorBatch *bufferedTblVecBatchEof = createEmptyVectorBatch(bufferTypesVector);
    addInputRetCode = smjOp->AddBufferedTableInput(bufferedTblVecBatchEof);
    ASSERT_EQ(addInputRetCode, SMJ_NEED_ADD_STREAM_TBL_DATA);

    // add eof flag to streamed table
    VectorBatch *streamedTblVecBatchEof = createEmptyVectorBatch(streamTypesVector);
    addInputRetCode = smjOp->AddStreamedTableInput(streamedTblVecBatchEof);
    ASSERT_EQ(addInputRetCode, SMJ_FETCH_JOIN_DATA);

    std::vector<omniruntime::vec::VectorBatch *> result;
    smjOp->GetOutput(result);

    // check the join result
    int32_t index = 0;
    for (auto i = 0; i < result.size(); i++) {
        ASSERT_EQ(result[i]->GetVectorCount(), 2);
        ASSERT_EQ(result[i]->GetVector(0)->GetTypeId(), OMNI_VEC_TYPE_LONG);
        ASSERT_EQ(result[i]->GetVector(1)->GetTypeId(), OMNI_VEC_TYPE_DOUBLE);
        for (auto j = 0; j < result[i]->GetRowCount(); j++) {
            long longValue = (static_cast<LongVector *>(result[i]->GetVector(0)))->GetValue(j);
            ASSERT_EQ(longValue, streamedTblDataCol2[index]);

            double doubleValue = (static_cast<DoubleVector *>(result[i]->GetVector(1)))->GetValue(j);
            ASSERT_EQ(doubleValue, bufferedTblDataCol1[index]);
            index++;
        }
        VectorHelper::FreeVecBatch(result[i]);
    }

    VectorHelper::FreeVecBatch(streamedTblVecBatch1);
    VectorHelper::FreeVecBatch(bufferedTblVecBatch1);
    VectorHelper::FreeVecBatch(bufferedTblVecBatchEof);
    VectorHelper::FreeVecBatch(streamedTblVecBatchEof);
    delete smjOp;
}
