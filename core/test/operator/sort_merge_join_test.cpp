/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: ...
 */

#include <vector>
#include "gtest/gtest.h"
#include "../../src/vector/vector_helper.h"
#include "../util/test_util.h"
#include "../../src/operator/pages_index.h"
#include "../../src/operator/join/sortmergejoin/dynamic_pages_index.h"
#include "../../src/operator/join/sortmergejoin/sort_merge_join_resultBuilder.h"
#include "../../src/operator/join/sortmergejoin/sort_merge_join.h"

using namespace omniruntime::op;
using namespace std;

TEST(NativeSortMergeJoinTest, TestMultiAddVecBatches) {
    std::vector<VecType> types = {IntVecType::Instance(), DoubleVecType::Instance()};
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

    dynamicPagesIndex->FreeAllRemainingVecBatch();
//    VectorHelper::FreeVecBatch(emptyVectorBatch);
    delete dynamicPagesIndex;
}

TEST(NativeSortMergeJoinTest, TestDataValue) {
    std::vector<VecType> types = {IntVecType::Instance(), DoubleVecType::Instance()};
    VecTypes sourceTypes(types);
    DynamicPagesIndex *dynamicPagesIndex = new DynamicPagesIndex(sourceTypes);
    ASSERT_EQ(dynamicPagesIndex->GetPositionCount(), 0);

    // construct data;
    const int32_t DATA_SIZE1 = 6;
    // table1
    int32_t data1[DATA_SIZE1] = {0, 1, 2, 3, 4, 5};
    double data2[DATA_SIZE1] = {6.6, 5.5, 4.4, 3.3, 2.2, 1.1};
    // table2
    const int32_t DATA_SIZE2 = 7;
    int32_t data3[DATA_SIZE2] = {6, 7, 8, 9, 10, 11, 12};
    double data4[DATA_SIZE2] = {16.6, 15.5, 14.4, 13.3, 12.2, 11.1, 11.3};

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

    dynamicPagesIndex->FreeAllRemainingVecBatch();
    delete dynamicPagesIndex;
}

TEST(NativeSortMergeJoinTest, TestSmjOneTimeEqualCondition) {
    // select t1.b, t2.c from t1, t2 where t1.a = t2.d
    // bufferedTbl t2: double c, int d;
    // streamedTbl t1:  int a, Long b;
    std::string blank = "";
    SortMergeJoinOperator *smjOp = new SortMergeJoinOperator(OMNI_JOIN_TYPE_INNER, blank);

    std::vector<VecType> streamTypesVector = {IntVecType::Instance(), LongVecType::Instance()};
    VecTypes streamedTblTypes(streamTypesVector);
    std::vector<int32_t> streamedKeysCols;
    streamedKeysCols.push_back(0);
    std::vector<int32_t> streamedOutputCols;
    streamedOutputCols.push_back(1);
    smjOp->ConfigStreamedTblInfo(streamedTblTypes, streamedKeysCols, streamedOutputCols);

    std::vector<VecType> bufferTypesVector = {DoubleVecType::Instance(), IntVecType::Instance()};
    VecTypes bufferedTblTypes(bufferTypesVector);
    std::vector<int32_t> bufferedKeysCols;
    bufferedKeysCols.push_back(1);
    std::vector<int32_t> bufferedOutputCols;
    bufferedOutputCols.push_back(0);
    smjOp->ConfigBufferedTblInfo(bufferedTblTypes, bufferedKeysCols, bufferedOutputCols);
    smjOp->InitScannerAndResultBuilder();

    // construct data;
    const int32_t streamedTblDataSize = 6;
    int32_t streamedTblDataCol1[streamedTblDataSize] = {0, 1, 2, 3, 4, 5};
    long streamedTblDataCol2[streamedTblDataSize] = {6600, 5500, 4400, 3300, 2200, 1100};
    VectorBatch *streamedTblVecBatch1 =
            CreateVectorBatch(streamedTblTypes, streamedTblDataSize, streamedTblDataCol1, streamedTblDataCol2);

    const int32_t bufferedTblSize = 6;
    double bufferedTblDataCol1[bufferedTblSize] = {6.6, 5.5, 4.4, 3.3, 2.2, 1.1};
    int32_t bufferedTblDataCol2[bufferedTblSize] = {0, 1, 2, 3, 4, 5};
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

    addInputRetCode = smjOp->AddStreamedTableInput(streamedTblVecBatchEof);
    ASSERT_EQ(addInputRetCode, SMJ_NO_RESULT);

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

    delete smjOp;
}

void ExpectVectorEqual(std::vector<int64_t> expected, std::vector<int64_t> actual) {
    EXPECT_EQ(expected.size(), actual.size());
    for (int i = 0; i < expected.size(); ++i) {
        EXPECT_EQ(expected[i], actual[i]);
    }
}

TEST(NativeSortMergeJoinTest, TestJoinScanner1) {
    VecTypes streamedTypes(std::vector<VecType>({LongVecType(), LongVecType()}));
    VecTypes streamedKeysTypes(std::vector<VecType>({LongVecType()}));
    int32_t streamedCols[] = {0};
    auto streamedPageIndex = new DynamicPagesIndex(streamedTypes);
    VecTypes bufferedTypes(std::vector<VecType>({LongVecType(), LongVecType()}));
    VecTypes bufferedKeysTypes(std::vector<VecType>({LongVecType()}));
    int32_t bufferedCols[] = {0};
    auto bufferedPageIndex = new DynamicPagesIndex(bufferedTypes);

    const int32_t DATA_SIZE = 4;
    int64_t streamData0[] = {1, 2, 3, 5};
    int64_t bufferData0[] = {1, 5, 6, 7};
    int64_t streamedData1[] = {111, 11, 1, 0};
    VectorBatch *streamedVecBatch = std::make_unique<VectorBatch>(2, DATA_SIZE).release();
    streamedVecBatch->SetVector(0, CreateVector<LongVector, int64_t>(streamData0, DATA_SIZE));
    streamedVecBatch->SetVector(1, CreateVector<LongVector, int64_t>(streamedData1, DATA_SIZE));
    auto streamed = std::vector<VectorBatch *>();
    streamed.push_back(streamedVecBatch);
    streamedPageIndex->AddVecBatches(streamed);

    int64_t bufferData1[] = {11, 22, 33, 44};
    VectorBatch *bufferedVecBatch = std::make_unique<VectorBatch>(2, DATA_SIZE).release();
    bufferedVecBatch->SetVector(0, CreateVector<LongVector, int64_t>(bufferData0, DATA_SIZE));
    bufferedVecBatch->SetVector(1, CreateVector<LongVector, int64_t>(bufferData1, DATA_SIZE));
    auto buffered = std::vector<VectorBatch *>();
    buffered.push_back(bufferedVecBatch);
    bufferedPageIndex->AddVecBatches(buffered);
    auto scan = new SortMergeJoinScanner(streamedKeysTypes, streamedCols, 1, streamedPageIndex, bufferedKeysTypes,
        bufferedCols, bufferedPageIndex, OMNI_JOIN_TYPE_INNER, false);
    scan->FindNextJoinRows();
    std::vector<int64_t> expectedStreamedAddr({0, 3});
    std::vector<int64_t> expectedBufferedAddr({0, 1});
    std::vector<bool> isMatchPre;
    std::vector<int64_t> streamedAddr;
    std::vector<int64_t> bufferedAddr;
    scan->GetMatchedValueAddresses(isMatchPre, streamedAddr, bufferedAddr);
    ExpectVectorEqual(expectedStreamedAddr, streamedAddr);
    ExpectVectorEqual(expectedBufferedAddr, bufferedAddr);

    VectorHelper::FreeVecBatch(bufferedVecBatch);
    VectorHelper::FreeVecBatch(streamedVecBatch);
    delete bufferedPageIndex;
    delete streamedPageIndex;
    delete scan;
}

TEST(NativeSortMergeJoinTest, TestJoinScanner2) {
    VecTypes streamedTypes(std::vector<VecType>({LongVecType(), LongVecType()}));
    VecTypes streamedKeysTypes(std::vector<VecType>({LongVecType()}));
    int32_t streamedCols[] = {0};
    auto streamedPageIndex = new DynamicPagesIndex(streamedTypes);
    VecTypes bufferedTypes(std::vector<VecType>({LongVecType(), LongVecType()}));
    VecTypes bufferedKeysTypes(std::vector<VecType>({LongVecType()}));
    int32_t bufferedCols[] = {0};
    auto bufferedPageIndex = new DynamicPagesIndex(bufferedTypes);

    const int32_t DATA_SIZE = 4;
    int64_t streamData0[] = {1, 5, 6, 7};
    int64_t bufferData0[] = {1, 2, 3, 5};
    int64_t streamedData1[] = {111, 11, 1, 0};
    VectorBatch *streamedVecBatch = std::make_unique<VectorBatch>(2, DATA_SIZE).release();
    streamedVecBatch->SetVector(0, CreateVector<LongVector, int64_t>(streamData0, DATA_SIZE));
    streamedVecBatch->SetVector(1, CreateVector<LongVector, int64_t>(streamedData1, DATA_SIZE));
    auto streamed = std::vector<VectorBatch *>();
    streamed.push_back(streamedVecBatch);
    streamedPageIndex->AddVecBatches(streamed);

    int64_t bufferData1[] = {11, 22, 33, 44};
    VectorBatch *bufferedVecBatch = std::make_unique<VectorBatch>(2, DATA_SIZE).release();
    bufferedVecBatch->SetVector(0, CreateVector<LongVector, int64_t>(bufferData0, DATA_SIZE));
    bufferedVecBatch->SetVector(1, CreateVector<LongVector, int64_t>(bufferData1, DATA_SIZE));
    auto buffered = std::vector<VectorBatch *>();
    buffered.push_back(bufferedVecBatch);
    bufferedPageIndex->AddVecBatches(buffered);
    auto scan = new SortMergeJoinScanner(streamedKeysTypes, streamedCols, 1, streamedPageIndex, bufferedKeysTypes,
                                         bufferedCols, bufferedPageIndex, OMNI_JOIN_TYPE_INNER, false);
    auto ret = scan->FindNextJoinRows();
    std::vector<bool> isPreMatched;
    std::vector<int64_t> streamedAddr;
    std::vector<int64_t> bufferedAddr;
    scan->GetMatchedValueAddresses(isPreMatched, streamedAddr, bufferedAddr);

    VectorBatch *eofVecBatch = std::make_unique<VectorBatch>(1, 0).release();
    bufferedPageIndex->AddVecBatches(std::vector<VectorBatch *>{eofVecBatch});
    ret = scan->FindNextJoinRows();
    std::vector<int64_t> expectedStreamedAddr({0, 1});
    std::vector<int64_t> expectedBufferedAddr({0, 3});
    scan->GetMatchedValueAddresses(isPreMatched, streamedAddr, bufferedAddr);
    ExpectVectorEqual(expectedStreamedAddr, streamedAddr);
    ExpectVectorEqual(expectedBufferedAddr, bufferedAddr);

    VectorHelper::FreeVecBatch(bufferedVecBatch);
    VectorHelper::FreeVecBatch(streamedVecBatch);
    delete eofVecBatch;
    delete bufferedPageIndex;
    delete streamedPageIndex;
    delete scan;
}

TEST(NativeSortMergeJoinTest, TestJoinScanner3) {
    VecTypes streamedTypes(std::vector<VecType>({LongVecType(), LongVecType()}));
    VecTypes streamedKeysTypes(std::vector<VecType>({LongVecType()}));
    int32_t streamedCols[] = {0};
    auto streamedPageIndex = new DynamicPagesIndex(streamedTypes);
    VecTypes bufferedTypes(std::vector<VecType>({LongVecType(), LongVecType()}));
    VecTypes bufferedKeysTypes(std::vector<VecType>({LongVecType()}));
    int32_t bufferedCols[] = {0};
    auto bufferedPageIndex = new DynamicPagesIndex(bufferedTypes);

    const int32_t DATA_SIZE = 4;
    int64_t streamData0[] = {1, 2, 3, 5};
    int64_t bufferData0[] = {0, 3, 5, 6};
    int64_t streamedData1[] = {111, 11, 1, 0};
    VectorBatch *streamedVecBatch = std::make_unique<VectorBatch>(2, DATA_SIZE).release();
    streamedVecBatch->SetVector(0, CreateVector<LongVector, int64_t>(streamData0, DATA_SIZE));
    streamedVecBatch->SetVector(1, CreateVector<LongVector, int64_t>(streamedData1, DATA_SIZE));
    auto streamed = std::vector<VectorBatch *>();
    streamed.push_back(streamedVecBatch);
    streamedPageIndex->AddVecBatches(streamed);

    int64_t bufferData1[] = {11, 22, 33, 44};
    VectorBatch *bufferedVecBatch = std::make_unique<VectorBatch>(2, DATA_SIZE).release();
    bufferedVecBatch->SetVector(0, CreateVector<LongVector, int64_t>(bufferData0, DATA_SIZE));
    bufferedVecBatch->SetVector(1, CreateVector<LongVector, int64_t>(bufferData1, DATA_SIZE));
    auto buffered = std::vector<VectorBatch *>();
    buffered.push_back(bufferedVecBatch);
    bufferedPageIndex->AddVecBatches(buffered);
    auto scan = new SortMergeJoinScanner(streamedKeysTypes, streamedCols, 1, streamedPageIndex, bufferedKeysTypes,
        bufferedCols, bufferedPageIndex, OMNI_JOIN_TYPE_INNER, false);
    scan->FindNextJoinRows();
    std::vector<int64_t> expectedStreamedAddr({2, 3});
    std::vector<int64_t> expectedBufferedAddr({1, 2});
    std::vector<bool> isPreMatched;
    std::vector<int64_t> streamedAddr;
    std::vector<int64_t> bufferedAddr;
    scan->GetMatchedValueAddresses(isPreMatched, streamedAddr, bufferedAddr);
    ExpectVectorEqual(expectedStreamedAddr, streamedAddr);
    ExpectVectorEqual(expectedBufferedAddr, bufferedAddr);

    VectorHelper::FreeVecBatch(bufferedVecBatch);
    VectorHelper::FreeVecBatch(streamedVecBatch);
    delete bufferedPageIndex;
    delete streamedPageIndex;
    delete scan;
}

TEST(NativeSortMergeJoinTest, TestJoinScanner4) {
    VecTypes streamedTypes(std::vector<VecType>({LongVecType(), LongVecType()}));
    VecTypes streamedKeysTypes(std::vector<VecType>({LongVecType()}));
    int32_t streamedCols[] = {0};
    auto streamedPageIndex = new DynamicPagesIndex(streamedTypes);
    VecTypes bufferedTypes(std::vector<VecType>({LongVecType(), LongVecType()}));
    VecTypes bufferedKeysTypes(std::vector<VecType>({LongVecType()}));
    int32_t bufferedCols[] = {0};
    auto bufferedPageIndex = new DynamicPagesIndex(bufferedTypes);

    const int32_t DATA_SIZE = 4;
    int64_t streamData0[] = {0, 3, 5, 6};
    int64_t bufferData0[] = {1, 2, 3, 5};
    int64_t streamedData1[] = {111, 11, 1, 0};
    VectorBatch *streamedVecBatch = std::make_unique<VectorBatch>(2, DATA_SIZE).release();
    streamedVecBatch->SetVector(0, CreateVector<LongVector, int64_t>(streamData0, DATA_SIZE));
    streamedVecBatch->SetVector(1, CreateVector<LongVector, int64_t>(streamedData1, DATA_SIZE));
    auto streamed = std::vector<VectorBatch *>();
    streamed.push_back(streamedVecBatch);
    streamedPageIndex->AddVecBatches(streamed);

    int64_t bufferData1[] = {11, 22, 33, 44};
    VectorBatch *bufferedVecBatch = std::make_unique<VectorBatch>(2, DATA_SIZE).release();
    bufferedVecBatch->SetVector(0, CreateVector<LongVector, int64_t>(bufferData0, DATA_SIZE));
    bufferedVecBatch->SetVector(1, CreateVector<LongVector, int64_t>(bufferData1, DATA_SIZE));
    auto buffered = std::vector<VectorBatch *>();
    buffered.push_back(bufferedVecBatch);
    bufferedPageIndex->AddVecBatches(buffered);
    auto scan = new SortMergeJoinScanner(streamedKeysTypes, streamedCols, 1, streamedPageIndex, bufferedKeysTypes,
        bufferedCols, bufferedPageIndex, OMNI_JOIN_TYPE_INNER, false);
    scan->FindNextJoinRows();
    std::vector<int64_t> expectedStreamedAddr({1, 2});
    std::vector<int64_t> expectedBufferedAddr({2, 3});
    std::vector<bool> isPreMatched;
    std::vector<int64_t> streamedAddr;
    std::vector<int64_t> bufferedAddr;
    scan->GetMatchedValueAddresses(isPreMatched, streamedAddr, bufferedAddr);
    VectorBatch *eofVecBatch = std::make_unique<VectorBatch>(2, 0).release();
    bufferedPageIndex->AddVecBatches(std::vector<VectorBatch *>{eofVecBatch});
    scan->FindNextJoinRows();
    scan->GetMatchedValueAddresses(isPreMatched, streamedAddr, bufferedAddr);
    ExpectVectorEqual(expectedStreamedAddr, streamedAddr);
    ExpectVectorEqual(expectedBufferedAddr, bufferedAddr);

    VectorHelper::FreeVecBatch(bufferedVecBatch);
    VectorHelper::FreeVecBatch(streamedVecBatch);
    delete eofVecBatch;
    delete bufferedPageIndex;
    delete streamedPageIndex;
    delete scan;
}

TEST(NativeSortMergeJoinTest, TestJoinScanner5) {
    VecTypes streamedTypes(std::vector<VecType>({LongVecType(), LongVecType()}));
    VecTypes streamedKeysTypes(std::vector<VecType>({LongVecType()}));
    int32_t streamedCols[] = {0};
    auto streamedPageIndex = new DynamicPagesIndex(streamedTypes);
    VecTypes bufferedTypes(std::vector<VecType>({LongVecType(), LongVecType()}));
    VecTypes bufferedKeysTypes(std::vector<VecType>({LongVecType()}));
    int32_t bufferedCols[] = {0};
    auto bufferedPageIndex = new DynamicPagesIndex(bufferedTypes);

    const int32_t DATA_SIZE = 4;
    int64_t streamData0[] = {1, 2, 3, 5};
    int64_t bufferData0[] = {0, 5, 6, 7};
    int64_t streamedData1[] = {111, 11, 1, 0};
    VectorBatch *streamedVecBatch = std::make_unique<VectorBatch>(2, DATA_SIZE).release();
    streamedVecBatch->SetVector(0, CreateVector<LongVector, int64_t>(streamData0, DATA_SIZE));
    streamedVecBatch->SetVector(1, CreateVector<LongVector, int64_t>(streamedData1, DATA_SIZE));
    auto streamed = std::vector<VectorBatch *>();
    streamed.push_back(streamedVecBatch);
    streamedPageIndex->AddVecBatches(streamed);

    int64_t bufferData1[] = {11, 22, 33, 44};
    VectorBatch *bufferedVecBatch = std::make_unique<VectorBatch>(2, DATA_SIZE).release();
    bufferedVecBatch->SetVector(0, CreateVector<LongVector, int64_t>(bufferData0, DATA_SIZE));
    bufferedVecBatch->SetVector(1, CreateVector<LongVector, int64_t>(bufferData1, DATA_SIZE));
    auto buffered = std::vector<VectorBatch *>();
    buffered.push_back(bufferedVecBatch);
    bufferedPageIndex->AddVecBatches(buffered);
    auto scan = new SortMergeJoinScanner(streamedKeysTypes, streamedCols, 1, streamedPageIndex, bufferedKeysTypes,
        bufferedCols, bufferedPageIndex, OMNI_JOIN_TYPE_INNER, false);
    scan->FindNextJoinRows();
    std::vector<int64_t> expectedStreamedAddr({3});
    std::vector<int64_t> expectedBufferedAddr({1});
    std::vector<bool> isPreMatched;
    std::vector<int64_t> streamedAddr;
    std::vector<int64_t> bufferedAddr;
    scan->GetMatchedValueAddresses(isPreMatched, streamedAddr, bufferedAddr);
    ExpectVectorEqual(expectedStreamedAddr, streamedAddr);
    ExpectVectorEqual(expectedBufferedAddr, bufferedAddr);

    VectorHelper::FreeVecBatch(bufferedVecBatch);
    VectorHelper::FreeVecBatch(streamedVecBatch);
    delete bufferedPageIndex;
    delete streamedPageIndex;
    delete scan;
}

TEST(NativeSortMergeJoinTest, TestJoinScanner6) {
    VecTypes streamedTypes(std::vector<VecType>({LongVecType(), LongVecType()}));
    VecTypes streamedKeysTypes(std::vector<VecType>({LongVecType()}));
    int32_t streamedCols[] = {0};
    auto streamedPageIndex = new DynamicPagesIndex(streamedTypes);
    VecTypes bufferedTypes(std::vector<VecType>({LongVecType(), LongVecType()}));
    VecTypes bufferedKeysTypes(std::vector<VecType>({LongVecType()}));
    int32_t bufferedCols[] = {0};
    auto bufferedPageIndex = new DynamicPagesIndex(bufferedTypes);

    const int32_t DATA_SIZE = 4;
    int64_t streamData0[] = {0, 5, 6, 7};
    int64_t bufferData0[] = {1, 2, 3, 5};
    int64_t streamedData1[] = {111, 11, 1, 0};
    VectorBatch *streamedVecBatch = std::make_unique<VectorBatch>(2, DATA_SIZE).release();
    streamedVecBatch->SetVector(0, CreateVector<LongVector, int64_t>(streamData0, DATA_SIZE));
    streamedVecBatch->SetVector(1, CreateVector<LongVector, int64_t>(streamedData1, DATA_SIZE));
    auto streamed = std::vector<VectorBatch *>();
    streamed.push_back(streamedVecBatch);
    streamedPageIndex->AddVecBatches(streamed);

    int64_t bufferData1[] = {11, 22, 33, 44};
    VectorBatch *bufferedVecBatch = std::make_unique<VectorBatch>(2, DATA_SIZE).release();
    bufferedVecBatch->SetVector(0, CreateVector<LongVector, int64_t>(bufferData0, DATA_SIZE));
    bufferedVecBatch->SetVector(1, CreateVector<LongVector, int64_t>(bufferData1, DATA_SIZE));
    auto buffered = std::vector<VectorBatch *>();
    buffered.push_back(bufferedVecBatch);
    bufferedPageIndex->AddVecBatches(buffered);
    auto scan = new SortMergeJoinScanner(streamedKeysTypes, streamedCols, 1, streamedPageIndex, bufferedKeysTypes,
        bufferedCols, bufferedPageIndex, OMNI_JOIN_TYPE_INNER, false);
    scan->FindNextJoinRows();
    std::vector<int64_t> expectedStreamedAddr({1});
    std::vector<int64_t> expectedBufferedAddr({3});
    std::vector<bool> isPreMatched;
    std::vector<int64_t> streamedAddr;
    std::vector<int64_t> bufferedAddr;
    scan->GetMatchedValueAddresses(isPreMatched, streamedAddr, bufferedAddr);
    VectorBatch *eofVecBatch = std::make_unique<VectorBatch>(2, 0).release();
    bufferedPageIndex->AddVecBatches(std::vector<VectorBatch *>{eofVecBatch});
    scan->FindNextJoinRows();
    scan->GetMatchedValueAddresses(isPreMatched, streamedAddr, bufferedAddr);
    ExpectVectorEqual(expectedStreamedAddr, streamedAddr);
    ExpectVectorEqual(expectedBufferedAddr, bufferedAddr);

    VectorHelper::FreeVecBatch(bufferedVecBatch);
    VectorHelper::FreeVecBatch(streamedVecBatch);
    delete eofVecBatch;
    delete bufferedPageIndex;
    delete streamedPageIndex;
    delete scan;
}

TEST(NativeSortMergeJoinTest, TestRepeatBufferedTableKeys1) {
    VecTypes streamedTypes(std::vector<VecType>({LongVecType(), LongVecType()}));
    VecTypes streamedKeysTypes(std::vector<VecType>({LongVecType()}));
    int32_t streamedCols[] = {0};
    auto streamedPageIndex = new DynamicPagesIndex(streamedTypes);
    VecTypes bufferedTypes(std::vector<VecType>({LongVecType(), LongVecType()}));
    VecTypes bufferedKeysTypes(std::vector<VecType>({LongVecType()}));
    int32_t bufferedCols[] = {0};
    auto bufferedPageIndex = new DynamicPagesIndex(bufferedTypes);

    const int32_t DATA_SIZE = 4;
    int64_t streamData0[] = {-1, 0, 2, 7};
    int64_t bufferData0[] = {0, 1, 2, 2};
    int64_t streamedData1[] = {111, 11, 1, 0};
    VectorBatch *streamedVecBatch = std::make_unique<VectorBatch>(2, DATA_SIZE).release();
    streamedVecBatch->SetVector(0, CreateVector<LongVector, int64_t>(streamData0, DATA_SIZE));
    streamedVecBatch->SetVector(1, CreateVector<LongVector, int64_t>(streamedData1, DATA_SIZE));
    auto streamed = std::vector<VectorBatch *>();
    streamed.push_back(streamedVecBatch);
    streamedPageIndex->AddVecBatches(streamed);

    int64_t bufferData1[] = {11, 22, 33, 44};
    VectorBatch *bufferedVecBatch = std::make_unique<VectorBatch>(2, DATA_SIZE).release();
    bufferedVecBatch->SetVector(0, CreateVector<LongVector, int64_t>(bufferData0, DATA_SIZE));
    bufferedVecBatch->SetVector(1, CreateVector<LongVector, int64_t>(bufferData1, DATA_SIZE));
    auto buffered = std::vector<VectorBatch *>();
    buffered.push_back(bufferedVecBatch);
    bufferedPageIndex->AddVecBatches(buffered);
    auto scan = new SortMergeJoinScanner(streamedKeysTypes, streamedCols, 1, streamedPageIndex, bufferedKeysTypes,
        bufferedCols, bufferedPageIndex, OMNI_JOIN_TYPE_INNER, false);
    scan->FindNextJoinRows();
    std::vector<int64_t> expectedStreamedAddr({1, 2, 2});
    std::vector<int64_t> expectedBufferedAddr({0, 2, 3});
    std::vector<bool> isPreMatched;
    std::vector<int64_t> streamedAddr;
    std::vector<int64_t> bufferedAddr;
    scan->GetMatchedValueAddresses(isPreMatched, streamedAddr, bufferedAddr);
    VectorBatch *eofVecBatch = std::make_unique<VectorBatch>(2, 0).release();
    bufferedPageIndex->AddVecBatches(std::vector<VectorBatch *>{eofVecBatch});
    scan->FindNextJoinRows();
    scan->GetMatchedValueAddresses(isPreMatched, streamedAddr, bufferedAddr);
    ExpectVectorEqual(expectedStreamedAddr, streamedAddr);
    ExpectVectorEqual(expectedBufferedAddr, bufferedAddr);

    VectorHelper::FreeVecBatch(bufferedVecBatch);
    VectorHelper::FreeVecBatch(streamedVecBatch);
    delete eofVecBatch;
    delete bufferedPageIndex;
    delete streamedPageIndex;
    delete scan;
}

TEST(NativeSortMergeJoinTest, TestRepeatBufferedTableKeys2) {
    VecTypes streamedTypes(std::vector<VecType>({LongVecType(), LongVecType()}));
    VecTypes streamedKeysTypes(std::vector<VecType>({LongVecType()}));
    int32_t streamedCols[] = {0};
    auto streamedPageIndex = new DynamicPagesIndex(streamedTypes);
    VecTypes bufferedTypes(std::vector<VecType>({LongVecType(), LongVecType()}));
    VecTypes bufferedKeysTypes(std::vector<VecType>({LongVecType()}));
    int32_t bufferedCols[] = {0};
    auto bufferedPageIndex = new DynamicPagesIndex(bufferedTypes);

    const int32_t DATA_SIZE = 4;
    int64_t streamData0[] = {-1, 0, 2, 7};
    int64_t bufferData0[] = {1, 2, 2, 5};
    int64_t streamedData1[] = {111, 11, 1, 0};
    VectorBatch *streamedVecBatch = std::make_unique<VectorBatch>(2, DATA_SIZE).release();
    streamedVecBatch->SetVector(0, CreateVector<LongVector, int64_t>(streamData0, DATA_SIZE));
    streamedVecBatch->SetVector(1, CreateVector<LongVector, int64_t>(streamedData1, DATA_SIZE));
    auto streamed = std::vector<VectorBatch *>();
    streamed.push_back(streamedVecBatch);
    streamedPageIndex->AddVecBatches(streamed);

    int64_t bufferData1[] = {11, 22, 33, 44};
    VectorBatch *bufferedVecBatch = std::make_unique<VectorBatch>(2, DATA_SIZE).release();
    bufferedVecBatch->SetVector(0, CreateVector<LongVector, int64_t>(bufferData0, DATA_SIZE));
    bufferedVecBatch->SetVector(1, CreateVector<LongVector, int64_t>(bufferData1, DATA_SIZE));
    auto buffered = std::vector<VectorBatch *>();
    buffered.push_back(bufferedVecBatch);
    bufferedPageIndex->AddVecBatches(buffered);
    auto scan = new SortMergeJoinScanner(streamedKeysTypes, streamedCols, 1, streamedPageIndex, bufferedKeysTypes,
        bufferedCols, bufferedPageIndex, OMNI_JOIN_TYPE_INNER, false);
    scan->FindNextJoinRows();
    std::vector<int64_t> expectedStreamedAddr({2, 2});
    std::vector<int64_t> expectedBufferedAddr({1, 2});
    std::vector<bool> isPreMatched;
    std::vector<int64_t> streamedAddr;
    std::vector<int64_t> bufferedAddr;
    scan->GetMatchedValueAddresses(isPreMatched, streamedAddr, bufferedAddr);
    ExpectVectorEqual(expectedStreamedAddr, streamedAddr);
    ExpectVectorEqual(expectedBufferedAddr, bufferedAddr);

    VectorHelper::FreeVecBatch(bufferedVecBatch);
    VectorHelper::FreeVecBatch(streamedVecBatch);
    delete bufferedPageIndex;
    delete streamedPageIndex;
    delete scan;
}

TEST(NativeSortMergeJoinTest, TestRepeatStreamedTableKeys1) {
    VecTypes streamedTypes(std::vector<VecType>({LongVecType(), LongVecType()}));
    VecTypes streamedKeysTypes(std::vector<VecType>({LongVecType()}));
    int32_t streamedCols[] = {0};
    auto streamedPageIndex = new DynamicPagesIndex(streamedTypes);
    VecTypes bufferedTypes(std::vector<VecType>({LongVecType(), LongVecType()}));
    VecTypes bufferedKeysTypes(std::vector<VecType>({LongVecType()}));
    int32_t bufferedCols[] = {0};
    auto bufferedPageIndex = new DynamicPagesIndex(bufferedTypes);

    const int32_t DATA_SIZE = 4;
    int64_t streamData0[] = {0, 1, 2, 2};
    int64_t bufferData0[] = {-1, 0, 2, 7};
    int64_t streamedData1[] = {111, 11, 1, 0};
    VectorBatch *streamedVecBatch = std::make_unique<VectorBatch>(2, DATA_SIZE).release();
    streamedVecBatch->SetVector(0, CreateVector<LongVector, int64_t>(streamData0, DATA_SIZE));
    streamedVecBatch->SetVector(1, CreateVector<LongVector, int64_t>(streamedData1, DATA_SIZE));
    auto streamed = std::vector<VectorBatch *>();
    streamed.push_back(streamedVecBatch);
    streamedPageIndex->AddVecBatches(streamed);

    int64_t bufferData1[] = {11, 22, 33, 44};
    VectorBatch *bufferedVecBatch = std::make_unique<VectorBatch>(2, DATA_SIZE).release();
    bufferedVecBatch->SetVector(0, CreateVector<LongVector, int64_t>(bufferData0, DATA_SIZE));
    bufferedVecBatch->SetVector(1, CreateVector<LongVector, int64_t>(bufferData1, DATA_SIZE));
    auto buffered = std::vector<VectorBatch *>();
    buffered.push_back(bufferedVecBatch);
    bufferedPageIndex->AddVecBatches(buffered);
    auto scan = new SortMergeJoinScanner(streamedKeysTypes, streamedCols, 1, streamedPageIndex, bufferedKeysTypes,
        bufferedCols, bufferedPageIndex, OMNI_JOIN_TYPE_INNER, false);
    scan->FindNextJoinRows();
    std::vector<int64_t> expectedStreamedAddr({0, 2, 3});
    std::vector<int64_t> expectedBufferedAddr({1, 2, 2});
    std::vector<bool> isPreMatched;
    std::vector<int64_t> streamedAddr;
    std::vector<int64_t> bufferedAddr;
    scan->GetMatchedValueAddresses(isPreMatched, streamedAddr, bufferedAddr);
    ExpectVectorEqual(expectedStreamedAddr, streamedAddr);
    ExpectVectorEqual(expectedBufferedAddr, bufferedAddr);

    VectorHelper::FreeVecBatch(bufferedVecBatch);
    VectorHelper::FreeVecBatch(streamedVecBatch);
    delete bufferedPageIndex;
    delete streamedPageIndex;
    delete scan;
}

TEST(NativeSortMergeJoinTest, TestRepeatStreamedTableKeys2) {
    VecTypes streamedTypes(std::vector<VecType>({LongVecType(), LongVecType()}));
    VecTypes streamedKeysTypes(std::vector<VecType>({LongVecType()}));
    int32_t streamedCols[] = {0};
    auto streamedPageIndex = new DynamicPagesIndex(streamedTypes);
    VecTypes bufferedTypes(std::vector<VecType>({LongVecType(), LongVecType()}));
    VecTypes bufferedKeysTypes(std::vector<VecType>({LongVecType()}));
    int32_t bufferedCols[] = {0};
    auto bufferedPageIndex = new DynamicPagesIndex(bufferedTypes);

    const int32_t DATA_SIZE = 4;
    int64_t streamData0[] = {1, 2, 2, 5};
    int64_t bufferData0[] = {-1, 0, 2, 7};
    int64_t streamedData1[] = {111, 11, 1, 0};
    VectorBatch *streamedVecBatch = std::make_unique<VectorBatch>(2, DATA_SIZE).release();
    streamedVecBatch->SetVector(0, CreateVector<LongVector, int64_t>(streamData0, DATA_SIZE));
    streamedVecBatch->SetVector(1, CreateVector<LongVector, int64_t>(streamedData1, DATA_SIZE));
    auto streamed = std::vector<VectorBatch *>();
    streamed.push_back(streamedVecBatch);
    streamedPageIndex->AddVecBatches(streamed);

    int64_t bufferData1[] = {11, 22, 33, 44};
    VectorBatch *bufferedVecBatch = std::make_unique<VectorBatch>(2, DATA_SIZE).release();
    bufferedVecBatch->SetVector(0, CreateVector<LongVector, int64_t>(bufferData0, DATA_SIZE));
    bufferedVecBatch->SetVector(1, CreateVector<LongVector, int64_t>(bufferData1, DATA_SIZE));
    auto buffered = std::vector<VectorBatch *>();
    buffered.push_back(bufferedVecBatch);
    bufferedPageIndex->AddVecBatches(buffered);
    auto scan = new SortMergeJoinScanner(streamedKeysTypes, streamedCols, 1, streamedPageIndex, bufferedKeysTypes,
        bufferedCols, bufferedPageIndex, OMNI_JOIN_TYPE_INNER, false);
    scan->FindNextJoinRows();
    std::vector<int64_t> expectedStreamedAddr({1, 2});
    std::vector<int64_t> expectedBufferedAddr({2, 2});
    std::vector<bool> isPreMatched;
    std::vector<int64_t> streamedAddr;
    std::vector<int64_t> bufferedAddr;
    scan->GetMatchedValueAddresses(isPreMatched, streamedAddr, bufferedAddr);
    ExpectVectorEqual(expectedStreamedAddr, streamedAddr);
    ExpectVectorEqual(expectedBufferedAddr, bufferedAddr);

    VectorHelper::FreeVecBatch(bufferedVecBatch);
    VectorHelper::FreeVecBatch(streamedVecBatch);
    delete bufferedPageIndex;
    delete streamedPageIndex;
    delete scan;
}

TEST(NativeSortMergeJoinTest, TestMultipleTableKeys) {
    VecTypes streamedTypes(std::vector<VecType>({LongVecType(), LongVecType(), LongVecType(), LongVecType()}));
    VecTypes streamedKeysTypes(std::vector<VecType>({LongVecType(), LongVecType()}));
    int32_t streamedCols[] = {1, 2};
    auto streamedPageIndex = new DynamicPagesIndex(streamedTypes);
    VecTypes bufferedTypes(std::vector<VecType>({LongVecType(), LongVecType(), LongVecType(), LongVecType()}));
    VecTypes bufferedKeysTypes(std::vector<VecType>({LongVecType(), LongVecType()}));
    int32_t bufferedCols[] = {0, 1};
    auto bufferedPageIndex = new DynamicPagesIndex(bufferedTypes);

    const int32_t DATA_SIZE = 4;
    int64_t streamData0[] = {1, 2, 3, 4};
    int64_t streamData1[] = {-1, 0, 2, 7};
    int64_t streamData2[] = {-1, 0, 2, 7};
    int64_t streamData3[] = {11, 22, 33, 44};

    VectorBatch *streamedVecBatch = std::make_unique<VectorBatch>(4, DATA_SIZE).release();
    streamedVecBatch->SetVector(0, CreateVector<LongVector, int64_t>(streamData0, DATA_SIZE));
    streamedVecBatch->SetVector(1, CreateVector<LongVector, int64_t>(streamData1, DATA_SIZE));
    streamedVecBatch->SetVector(2, CreateVector<LongVector, int64_t>(streamData2, DATA_SIZE));
    streamedVecBatch->SetVector(3, CreateVector<LongVector, int64_t>(streamData3, DATA_SIZE));
    auto streamed = std::vector<VectorBatch *>();
    streamed.push_back(streamedVecBatch);
    streamedPageIndex->AddVecBatches(streamed);
    int64_t bufferData0[] = {0, 1, 2, 2};
    int64_t bufferData1[] = {0, 1, 1, 2};
    int64_t bufferData2[] = {9, 8, 7, 6};
    int64_t bufferData3[] = {111, 11, 1, 0};
    VectorBatch *bufferedVecBatch = std::make_unique<VectorBatch>(4, DATA_SIZE).release();
    bufferedVecBatch->SetVector(0, CreateVector<LongVector, int64_t>(bufferData0, DATA_SIZE));
    bufferedVecBatch->SetVector(1, CreateVector<LongVector, int64_t>(bufferData1, DATA_SIZE));
    bufferedVecBatch->SetVector(2, CreateVector<LongVector, int64_t>(bufferData2, DATA_SIZE));
    bufferedVecBatch->SetVector(3, CreateVector<LongVector, int64_t>(bufferData3, DATA_SIZE));
    auto buffered = std::vector<VectorBatch *>();
    buffered.push_back(bufferedVecBatch);
    bufferedPageIndex->AddVecBatches(buffered);
    auto scan = new SortMergeJoinScanner(streamedTypes, streamedCols, streamedKeysTypes.GetSize(),
        streamedPageIndex, bufferedTypes, bufferedCols, bufferedPageIndex, OMNI_JOIN_TYPE_INNER, false);
    scan->FindNextJoinRows();
    std::vector<int64_t> expectedStreamedAddr({1, 2});
    std::vector<int64_t> expectedBufferedAddr({0, 3});
    std::vector<bool> isPreMatched;
    std::vector<int64_t> streamedAddr;
    std::vector<int64_t> bufferedAddr;
    scan->GetMatchedValueAddresses(isPreMatched, streamedAddr, bufferedAddr);
    VectorBatch *eofVecBatch = std::make_unique<VectorBatch>(4, 0).release();
    bufferedPageIndex->AddVecBatches(std::vector<VectorBatch *>{eofVecBatch});
    scan->FindNextJoinRows();
    scan->GetMatchedValueAddresses(isPreMatched, streamedAddr, bufferedAddr);
    ExpectVectorEqual(expectedStreamedAddr, streamedAddr);
    ExpectVectorEqual(expectedBufferedAddr, bufferedAddr);

    VectorHelper::FreeVecBatch(bufferedVecBatch);
    VectorHelper::FreeVecBatch(streamedVecBatch);
    delete eofVecBatch;
    delete bufferedPageIndex;
    delete streamedPageIndex;
    delete scan;
}

TEST(NativeSortMergeJoinTest, TestNullKeys) {
    VecTypes streamedTypes(std::vector<VecType>({LongVecType(), LongVecType(), LongVecType(), LongVecType()}));
    int32_t streamedCols[] = {1, 2};
    auto streamedPageIndex = new DynamicPagesIndex(streamedTypes);
    VecTypes bufferedTypes(std::vector<VecType>({LongVecType(), LongVecType(), LongVecType()}));
    int32_t bufferedCols[] = {0, 1};
    auto bufferedPageIndex = new DynamicPagesIndex(bufferedTypes);

    const int32_t DATA_SIZE = 4;
    int64_t streamData0[] = {1, 2, 3, 4};
    int64_t streamData1[] = {-1, 0, 2, 7};
    int64_t streamData2[] = {-1, 0, 2, 7};
    int64_t streamData3[] = {11, 22, 33, 44};

    VectorBatch *streamedVecBatch = std::make_unique<VectorBatch>(4, DATA_SIZE).release();
    streamedVecBatch->SetVector(0, CreateVector<LongVector, int64_t>(streamData0, DATA_SIZE));
    streamedVecBatch->SetVector(1, CreateVector<LongVector, int64_t>(streamData1, DATA_SIZE));
    streamedVecBatch->SetVector(2, CreateVector<LongVector, int64_t>(streamData2, DATA_SIZE));
    streamedVecBatch->SetVector(3, CreateVector<LongVector, int64_t>(streamData3, DATA_SIZE));
    auto streamed = std::vector<VectorBatch *>();
    streamed.push_back(streamedVecBatch);
    streamedPageIndex->AddVecBatches(streamed);
    int64_t bufferData0[] = {0, 1, 2, 2};
    int64_t bufferData1[] = {0, 1, 2, 2};
    int64_t bufferData2[] = {9, 8, 7, 6};
    int64_t bufferData3[] = {111, 11, 1, 0};
    VectorBatch *bufferedVecBatch = std::make_unique<VectorBatch>(4, DATA_SIZE).release();
    auto bufferVector0 = CreateVector<LongVector, int64_t>(bufferData0, DATA_SIZE);
    auto bufferVector1 = CreateVector<LongVector, int64_t>(bufferData1, DATA_SIZE);
    bufferVector1->SetValueNull(2);
    bufferedVecBatch->SetVector(0, bufferVector0);
    bufferedVecBatch->SetVector(1, bufferVector1);
    bufferedVecBatch->SetVector(2, CreateVector<LongVector, int64_t>(bufferData2, DATA_SIZE));
    bufferedVecBatch->SetVector(3, CreateVector<LongVector, int64_t>(bufferData3, DATA_SIZE));
    auto buffered = std::vector<VectorBatch *>();
    buffered.push_back(bufferedVecBatch);
    bufferedPageIndex->AddVecBatches(buffered);
    auto scan = new SortMergeJoinScanner(streamedTypes, streamedCols, 2, streamedPageIndex, bufferedTypes, bufferedCols,
                                         bufferedPageIndex, OMNI_JOIN_TYPE_INNER, false);
    scan->FindNextJoinRows();
    std::vector<int64_t> expectedStreamedAddr({1, 2});
    std::vector<int64_t> expectedBufferedAddr({0, 3});
    std::vector<bool> isPreMatched;
    std::vector<int64_t> streamedAddr;
    std::vector<int64_t> bufferedAddr;
    scan->GetMatchedValueAddresses(isPreMatched, streamedAddr, bufferedAddr);
    VectorBatch *eofVecBatch = std::make_unique<VectorBatch>(4, 0).release();
    bufferedPageIndex->AddVecBatches(std::vector<VectorBatch *>{eofVecBatch});
    scan->FindNextJoinRows();
    scan->GetMatchedValueAddresses(isPreMatched, streamedAddr, bufferedAddr);
    ExpectVectorEqual(expectedStreamedAddr, streamedAddr);
    ExpectVectorEqual(expectedBufferedAddr, bufferedAddr);

    VectorHelper::FreeVecBatch(bufferedVecBatch);
    VectorHelper::FreeVecBatch(streamedVecBatch);
    delete eofVecBatch;
    delete bufferedPageIndex;
    delete streamedPageIndex;
    delete scan;
}

TEST(NativeSortMergeJoinTest, TestDateTypes) {
    VecTypes streamedTypes(
            std::vector<VecType>({LongVecType(), DoubleVecType(), VarcharVecType(5), BooleanVecType()}));
    VecTypes streamedKeysTypes(std::vector<VecType>({DoubleVecType(), VarcharVecType(5), BooleanVecType()}));
    int32_t streamedCols[] = {1, 2, 3};
    auto streamedPageIndex = new DynamicPagesIndex(streamedTypes);
    VecTypes bufferedTypes(std::vector<VecType>({DoubleVecType(), VarcharVecType(5), BooleanVecType()}));
    VecTypes bufferedKeysTypes(std::vector<VecType>({DoubleVecType(), VarcharVecType(5), BooleanVecType()}));
    int32_t bufferedCols[] = {0, 1, 2};
    auto bufferedPageIndex = new DynamicPagesIndex(bufferedTypes);

    const int32_t DATA_SIZE = 4;
    int64_t streamData0[] = {1, 2, 3, 4};
    double streamData1[] = {-1.2, 0.2, 2.2, 7.2};
    std::string streamData2[] = {"ab", "cd", "ef", "gh"};
    bool streamData3[] = {false, false, true, true};

    VectorBatch *streamedVecBatch = std::make_unique<VectorBatch>(4, DATA_SIZE).release();
    streamedVecBatch->SetVector(0, CreateVector<LongVector, int64_t>(streamData0, DATA_SIZE));
    streamedVecBatch->SetVector(1, CreateVector<DoubleVector, double>(streamData1, DATA_SIZE));
    streamedVecBatch->SetVector(2, CreateVarcharVector(VarcharVecType(5), streamData2, DATA_SIZE));
    streamedVecBatch->SetVector(3, CreateVector<BooleanVector, bool>(streamData3, DATA_SIZE));
    auto streamed = std::vector<VectorBatch *>();
    streamed.push_back(streamedVecBatch);
    streamedPageIndex->AddVecBatches(streamed);

    double bufferData0[] = {-1.3, 0.2, 2.2, 7.2};
    std::string bufferData1[] = {"ab", "di", "ef", "gh"};
    bool bufferData2[] = {false, false, false, true};
    VectorBatch *bufferedVecBatch = std::make_unique<VectorBatch>(3, DATA_SIZE).release();
    bufferedVecBatch->SetVector(0, CreateVector<DoubleVector, double>(bufferData0, DATA_SIZE));
    bufferedVecBatch->SetVector(1, CreateVarcharVector(VarcharVecType(5), bufferData1, DATA_SIZE));
    bufferedVecBatch->SetVector(2, CreateVector<BooleanVector, bool>(bufferData2, DATA_SIZE));
    auto buffered = std::vector<VectorBatch *>();
    buffered.push_back(bufferedVecBatch);
    bufferedPageIndex->AddVecBatches(buffered);
    auto scan = new SortMergeJoinScanner(streamedTypes, streamedCols, streamedKeysTypes.GetSize(),
        streamedPageIndex, bufferedTypes, bufferedCols, bufferedPageIndex,
        OMNI_JOIN_TYPE_INNER, false);
    scan->FindNextJoinRows();
    std::vector<int64_t> expectedStreamedAddr({3});
    std::vector<int64_t> expectedBufferedAddr({3});
    std::vector<bool> isPreMatched;
    std::vector<int64_t> streamedAddr;
    std::vector<int64_t> bufferedAddr;
    scan->GetMatchedValueAddresses(isPreMatched, streamedAddr, bufferedAddr);
    VectorBatch *eofVecBatch = std::make_unique<VectorBatch>(4, 0).release();
    bufferedPageIndex->AddVecBatches(std::vector<VectorBatch *>{eofVecBatch});
    scan->FindNextJoinRows();
    scan->GetMatchedValueAddresses(isPreMatched, streamedAddr, bufferedAddr);
    ExpectVectorEqual(expectedStreamedAddr, streamedAddr);
    ExpectVectorEqual(expectedBufferedAddr, bufferedAddr);

    VectorHelper::FreeVecBatch(bufferedVecBatch);
    VectorHelper::FreeVecBatch(streamedVecBatch);
    delete eofVecBatch;
    delete bufferedPageIndex;
    delete streamedPageIndex;
    delete scan;
}

TEST(NativeSortMergeJoinTest, TestMultipleVecBatch) {
    VecTypes streamedTypes(std::vector<VecType>({LongVecType()}));
    VecTypes streamedKeysTypes(std::vector<VecType>({LongVecType()}));
    int32_t streamedCols[] = {0};
    auto streamedPageIndex = new DynamicPagesIndex(streamedTypes);
    VecTypes bufferedTypes(std::vector<VecType>({LongVecType()}));
    VecTypes bufferedKeysTypes(std::vector<VecType>({LongVecType()}));
    int32_t bufferedCols[] = {0};
    auto bufferedPageIndex = new DynamicPagesIndex(bufferedTypes);

    long streamData0[] = {1, 2, 3, 4, 4, 5, 6, 7, 10, 13, 13, 15, 18, 26};
    int streamedSize0 = 14;
    VectorBatch *streamedVecBatch = std::make_unique<VectorBatch>(1, streamedSize0).release();
    streamedVecBatch->SetVector(0, CreateVector<LongVector, int64_t>(streamData0, streamedSize0));
    auto streamed = std::vector<VectorBatch *>();
    streamed.push_back(streamedVecBatch);
    streamedPageIndex->AddVecBatches(streamed);

    long bufferData0[] = {2, 4, 4, 4, 4, 5, 6, 10};
    int bufferSize0 = 8;
    VectorBatch *bufferedVecBatch = std::make_unique<VectorBatch>(1, bufferSize0).release();
    bufferedVecBatch->SetVector(0, CreateVector<LongVector, int64_t>(bufferData0, bufferSize0));
    auto buffered = std::vector<VectorBatch *>();
    buffered.push_back(bufferedVecBatch);
    bufferedPageIndex->AddVecBatches(buffered);
    auto scan = new SortMergeJoinScanner(streamedKeysTypes, streamedCols, streamedKeysTypes.GetSize(),
        streamedPageIndex, bufferedKeysTypes, bufferedCols, bufferedPageIndex,
        OMNI_JOIN_TYPE_INNER, false);
    scan->FindNextJoinRows();
    std::vector<bool> isPreMatched;
    std::vector<int64_t> streamedAddr;
    std::vector<int64_t> bufferedAddr;
    scan->GetMatchedValueAddresses(isPreMatched, streamedAddr, bufferedAddr);

    long bufferData1[] = {10, 13, 13, 17, 17, 18, 18, 19};
    int bufferSize1 = 8;
    VectorBatch *bufferedVecBatch1 = std::make_unique<VectorBatch>(1, bufferSize1).release();
    bufferedVecBatch1->SetVector(0, CreateVector<LongVector, int64_t>(bufferData1, bufferSize1));
    auto buffered1 = std::vector<VectorBatch *>();
    buffered1.push_back(bufferedVecBatch1);
    bufferedPageIndex->AddVecBatches(buffered1);
    scan->FindNextJoinRows();
    scan->GetMatchedValueAddresses(isPreMatched, streamedAddr, bufferedAddr);

    long bufferData2[] = {20, 21, 23, 24, 25, 25, 25, 25};
    int bufferSize2 = 8;
    VectorBatch *bufferedVecBatch2 = std::make_unique<VectorBatch>(1, bufferSize2).release();
    bufferedVecBatch2->SetVector(0, CreateVector<LongVector, int64_t>(bufferData2, bufferSize2));
    auto buffered2 = std::vector<VectorBatch *>();
    buffered2.push_back(bufferedVecBatch2);
    bufferedPageIndex->AddVecBatches(buffered2);
    scan->FindNextJoinRows();
    scan->GetMatchedValueAddresses(isPreMatched, streamedAddr, bufferedAddr);

    long bufferData3[] = {30, 31, 42, 43, 44, 45, 46, 47};
    int bufferSize3 = 8;
    VectorBatch *bufferedVecBatch3 = std::make_unique<VectorBatch>(1, bufferSize3).release();
    bufferedVecBatch3->SetVector(0, CreateVector<LongVector, int64_t>(bufferData3, bufferSize3));
    auto buffered3 = std::vector<VectorBatch *>();
    buffered3.push_back(bufferedVecBatch3);
    bufferedPageIndex->AddVecBatches(buffered3);
    scan->FindNextJoinRows();
    scan->GetMatchedValueAddresses(isPreMatched, streamedAddr, bufferedAddr);

    long streamData1[] = {28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41};
    int streamedSize1 = 14;
    VectorBatch *streamedVecBatch1 = std::make_unique<VectorBatch>(1, streamedSize1).release();
    streamedVecBatch1->SetVector(0, CreateVector<LongVector, int64_t>(streamData1, streamedSize1));
    auto streamed1 = std::vector<VectorBatch *>();
    streamed1.push_back(streamedVecBatch1);
    streamedPageIndex->AddVecBatches(streamed1);
    scan->FindNextJoinRows();
    scan->GetMatchedValueAddresses(isPreMatched, streamedAddr, bufferedAddr);

    long streamData2[] = {43, 43, 43, 43, 43, 43, 43, 43, 43, 43, 43, 43, 43, 47};
    int streamedSize2 = 14;
    VectorBatch *streamedVecBatch2 = std::make_unique<VectorBatch>(1, streamedSize2).release();
    streamedVecBatch2->SetVector(0, CreateVector<LongVector, int64_t>(streamData2, streamedSize2));
    auto streamed2 = std::vector<VectorBatch *>();
    streamed2.push_back(streamedVecBatch2);
    streamedPageIndex->AddVecBatches(streamed2);
    scan->FindNextJoinRows();
    scan->GetMatchedValueAddresses(isPreMatched, streamedAddr, bufferedAddr);

    VectorBatch *eofVecBatch = std::make_unique<VectorBatch>(1, 0).release();
    bufferedPageIndex->AddVecBatches(std::vector<VectorBatch *>{eofVecBatch});
    scan->FindNextJoinRows();
    scan->GetMatchedValueAddresses(isPreMatched, streamedAddr, bufferedAddr);

    streamedPageIndex->AddVecBatches(std::vector<VectorBatch *>{eofVecBatch});
    scan->FindNextJoinRows();
    scan->GetMatchedValueAddresses(isPreMatched, streamedAddr, bufferedAddr);

    std::vector<int64_t> expectedStreamedAddr(
            {1, 3, 3, 3, 3, 4, 4, 4, 4,
             5, 6, 8, 8, 9, 9, 10, 10, 12,
             12, 4294967298, 4294967299, 8589934592, 8589934593, 8589934594, 8589934595, 8589934596, 8589934597,
             8589934598, 8589934599, 8589934600, 8589934601, 8589934602, 8589934603, 8589934604, 8589934605});
    std::vector<int64_t> expectedBufferedAddr(
            {0, 1, 2, 3, 4, 1, 2,
             3, 4, 5, 6, 7, 4294967296, 4294967297,
             4294967298, 4294967297, 4294967298, 4294967301, 4294967302, 12884901888, 12884901889,
             12884901891, 12884901891, 12884901891, 12884901891, 12884901891, 12884901891, 12884901891,
             12884901891, 12884901891, 12884901891, 12884901891, 12884901891, 12884901891, 12884901895});
    ExpectVectorEqual(expectedStreamedAddr, streamedAddr);
    ExpectVectorEqual(expectedBufferedAddr, bufferedAddr);

    VectorHelper::FreeVecBatch(bufferedVecBatch);
    VectorHelper::FreeVecBatch(streamedVecBatch);
    VectorHelper::FreeVecBatch(bufferedVecBatch1);
    VectorHelper::FreeVecBatch(streamedVecBatch1);
    VectorHelper::FreeVecBatch(bufferedVecBatch2);
    VectorHelper::FreeVecBatch(streamedVecBatch2);
    VectorHelper::FreeVecBatch(bufferedVecBatch3);
    delete eofVecBatch;
    delete bufferedPageIndex;
    delete streamedPageIndex;
    delete scan;
}

TEST(NativeSortMergeJoinTest, TestReturnCode) {
    VecTypes streamedTypes(std::vector<VecType>({LongVecType(), LongVecType()}));
    VecTypes streamedKeysTypes(std::vector<VecType>({LongVecType()}));
    int32_t streamedCols[] = {0};
    auto streamedPageIndex = new DynamicPagesIndex(streamedTypes);
    VecTypes bufferedTypes(std::vector<VecType>({LongVecType(), LongVecType()}));
    VecTypes bufferedKeysTypes(std::vector<VecType>({LongVecType()}));
    int32_t bufferedCols[] = {0};
    auto bufferedPageIndex = new DynamicPagesIndex(bufferedTypes);

    const int32_t DATA_SIZE = 4;
    int64_t streamData0[] = {1, 2, 3, 4};
    int64_t bufferData0[] = {1, 2, 3, 4};
    int64_t streamedData1[] = {111, 11, 1, 0};
    VectorBatch *streamedVecBatch = std::make_unique<VectorBatch>(2, DATA_SIZE).release();
    streamedVecBatch->SetVector(0, CreateVector<LongVector, int64_t>(streamData0, DATA_SIZE));
    streamedVecBatch->SetVector(1, CreateVector<LongVector, int64_t>(streamedData1, DATA_SIZE));
    auto streamed = std::vector<VectorBatch *>();
    streamed.push_back(streamedVecBatch);
    streamedPageIndex->AddVecBatches(streamed);

    int64_t streamData2[] = {4, 6, 7};
    VectorBatch *streamedVecBatch2 = std::make_unique<VectorBatch>(2, DATA_SIZE).release();
    streamedVecBatch2->SetVector(0, CreateVector<LongVector, int64_t>(streamData2, DATA_SIZE));
    streamedVecBatch2->SetVector(1, CreateVector<LongVector, int64_t>(streamedData1, DATA_SIZE));

    int64_t bufferData1[] = {11, 22, 33, 44};
    VectorBatch *bufferedVecBatch = std::make_unique<VectorBatch>(2, DATA_SIZE).release();
    bufferedVecBatch->SetVector(0, CreateVector<LongVector, int64_t>(bufferData0, DATA_SIZE));
    bufferedVecBatch->SetVector(1, CreateVector<LongVector, int64_t>(bufferData1, DATA_SIZE));
    auto buffered = std::vector<VectorBatch *>();
    buffered.push_back(bufferedVecBatch);
    bufferedPageIndex->AddVecBatches(buffered);

    auto scan = new SortMergeJoinScanner(streamedKeysTypes, streamedCols, 1, streamedPageIndex, bufferedKeysTypes,
        bufferedCols, bufferedPageIndex, OMNI_JOIN_TYPE_INNER, false);
    auto ret = scan->FindNextJoinRows();
    ASSERT_EQ(DecodeStreamedTblResult(ret), 0);
    ASSERT_EQ(DecodeBufferedTblResult(ret), 1);
    ASSERT_EQ(DecodeJoinResult(ret), 1);
    std::vector<bool> isPreMatched;
    std::vector<int64_t> streamedAddr;
    std::vector<int64_t> bufferedAddr;
    scan->GetMatchedValueAddresses(isPreMatched, streamedAddr, bufferedAddr);

    VectorBatch *eofVecBatch = std::make_unique<VectorBatch>(2, 0).release();
    bufferedPageIndex->AddVecBatches(std::vector<VectorBatch *>{eofVecBatch});
    ret = scan->FindNextJoinRows();
    ASSERT_EQ(DecodeStreamedTblResult(ret), 1);
    ASSERT_EQ(DecodeBufferedTblResult(ret), 0);
    ASSERT_EQ(DecodeJoinResult(ret), 1);
    streamedPageIndex->AddVecBatches(std::vector<VectorBatch *>{streamedVecBatch2});
    ret = scan->FindNextJoinRows();
    ASSERT_EQ(DecodeStreamedTblResult(ret), 0);
    ASSERT_EQ(DecodeBufferedTblResult(ret), 1);
    ASSERT_EQ(DecodeJoinResult(ret), 1);

    bufferedPageIndex->AddVecBatches(std::vector<VectorBatch *>{eofVecBatch});
    ret = scan->FindNextJoinRows();
    ASSERT_EQ(DecodeStreamedTblResult(ret), 1);
    ASSERT_EQ(DecodeBufferedTblResult(ret), 2);
    ASSERT_EQ(DecodeJoinResult(ret), 0);

    streamedPageIndex->AddVecBatches(std::vector<VectorBatch *>{eofVecBatch});
    ret = scan->FindNextJoinRows();
    ASSERT_EQ(DecodeStreamedTblResult(ret), 2);
    ASSERT_EQ(DecodeBufferedTblResult(ret), 2);
    ASSERT_EQ(DecodeJoinResult(ret), 1);

    VectorHelper::FreeVecBatch(bufferedVecBatch);
    VectorHelper::FreeVecBatch(streamedVecBatch);
    VectorHelper::FreeVecBatch(streamedVecBatch2);
    delete eofVecBatch;
    delete bufferedPageIndex;
    delete streamedPageIndex;
    delete scan;
}

TEST(NativeSortMergeJoinTest, TestReturnCode2) {
    VecTypes streamedTypes(std::vector<VecType>({LongVecType(), LongVecType()}));
    VecTypes streamedKeysTypes(std::vector<VecType>({LongVecType()}));
    int32_t streamedCols[] = {0};
    auto streamedPageIndex = new DynamicPagesIndex(streamedTypes);
    VecTypes bufferedTypes(std::vector<VecType>({LongVecType(), LongVecType()}));
    VecTypes bufferedKeysTypes(std::vector<VecType>({LongVecType()}));
    int32_t bufferedCols[] = {0};
    auto bufferedPageIndex = new DynamicPagesIndex(bufferedTypes);

    const int32_t DATA_SIZE = 4;
    int64_t streamData0[] = {1, 2, 3};
    int64_t bufferData0[] = {1, 2, 3, 4};
    int64_t streamedData1[] = {111, 11, 1};
    VectorBatch *streamedVecBatch = std::make_unique<VectorBatch>(2, 3).release();
    streamedVecBatch->SetVector(0, CreateVector<LongVector, int64_t>(streamData0, DATA_SIZE));
    streamedVecBatch->SetVector(1, CreateVector<LongVector, int64_t>(streamedData1, DATA_SIZE));
    auto streamed = std::vector<VectorBatch *>();
    streamed.push_back(streamedVecBatch);
    streamedPageIndex->AddVecBatches(streamed);

    int64_t bufferData1[] = {11, 22, 33, 44};
    VectorBatch *bufferedVecBatch = std::make_unique<VectorBatch>(2, DATA_SIZE).release();
    bufferedVecBatch->SetVector(0, CreateVector<LongVector, int64_t>(bufferData0, DATA_SIZE));
    bufferedVecBatch->SetVector(1, CreateVector<LongVector, int64_t>(bufferData1, DATA_SIZE));
    auto buffered = std::vector<VectorBatch *>();
    buffered.push_back(bufferedVecBatch);
    bufferedPageIndex->AddVecBatches(buffered);

    int64_t bufferData2[] = {11, 22, 33, 44};
    VectorBatch *bufferedVecBatch2 = std::make_unique<VectorBatch>(2, DATA_SIZE).release();
    bufferedVecBatch2->SetVector(0, CreateVector<LongVector, int64_t>(bufferData0, DATA_SIZE));
    bufferedVecBatch2->SetVector(1, CreateVector<LongVector, int64_t>(bufferData2, DATA_SIZE));

    auto scan = new SortMergeJoinScanner(streamedKeysTypes, streamedCols, 1, streamedPageIndex, bufferedKeysTypes,
        bufferedCols, bufferedPageIndex, OMNI_JOIN_TYPE_INNER, false);
    auto ret = scan->FindNextJoinRows();
    ASSERT_EQ(DecodeStreamedTblResult(ret), 1);
    ASSERT_EQ(DecodeBufferedTblResult(ret), 0);
    ASSERT_EQ(DecodeJoinResult(ret), 1);
    std::vector<bool> isPreMatched;
    std::vector<int64_t> streamedAddr;
    std::vector<int64_t> bufferedAddr;
    scan->GetMatchedValueAddresses(isPreMatched, streamedAddr, bufferedAddr);

    VectorBatch *eofVecBatch = std::make_unique<VectorBatch>(2, 0).release();
    streamedPageIndex->AddVecBatches(std::vector<VectorBatch *>{eofVecBatch});
    ret = scan->FindNextJoinRows();
    ASSERT_EQ(DecodeStreamedTblResult(ret), 2);
    ASSERT_EQ(DecodeBufferedTblResult(ret), 1);
    ASSERT_EQ(DecodeJoinResult(ret), 0);
    bufferedPageIndex->AddVecBatches(std::vector<VectorBatch *>{bufferedVecBatch2});
    ret = scan->FindNextJoinRows();
    ASSERT_EQ(DecodeStreamedTblResult(ret), 2);
    ASSERT_EQ(DecodeBufferedTblResult(ret), 2);
    ASSERT_EQ(DecodeJoinResult(ret), 1);

    VectorHelper::FreeVecBatch(bufferedVecBatch);
    VectorHelper::FreeVecBatch(streamedVecBatch);
    VectorHelper::FreeVecBatch(bufferedVecBatch2);
    delete eofVecBatch;
    delete bufferedPageIndex;
    delete streamedPageIndex;
    delete scan;
}

TEST(NativeSortMergeJoinTest, TestJoinScanner7) {
    VecTypes streamedTypes(std::vector<VecType>({LongVecType()}));
    VecTypes streamedKeysTypes(std::vector<VecType>({LongVecType()}));
    int32_t streamedCols[] = {0};
    auto streamedPageIndex = new DynamicPagesIndex(streamedTypes);
    VecTypes bufferedTypes(std::vector<VecType>({LongVecType()}));
    VecTypes bufferedKeysTypes(std::vector<VecType>({LongVecType()}));
    int32_t bufferedCols[] = {0};
    auto bufferedPageIndex = new DynamicPagesIndex(bufferedTypes);

    const int32_t DATA_SIZE = 6;
    // stream data0
    int64_t streamData0[] = {0, 1, 2, 3, 4, 5};
    VectorBatch *streamedVecBatch = std::make_unique<VectorBatch>(1, DATA_SIZE).release();
    streamedVecBatch->SetVector(0, CreateVector<LongVector, int64_t>(streamData0, DATA_SIZE));

    // buffer data0
    int64_t bufferData0[] = {0, 1, 2, 3, 4, 5};
    VectorBatch *bufferedVecBatch = std::make_unique<VectorBatch>(1, DATA_SIZE).release();
    bufferedVecBatch->SetVector(0, CreateVector<LongVector, int64_t>(bufferData0, DATA_SIZE));

    // buffer data1
    int64_t bufferData1[] = {5, 5, 5, 5, 5, 5};
    VectorBatch *bufferedVecBatch1 = std::make_unique<VectorBatch>(1, DATA_SIZE).release();
    bufferedVecBatch1->SetVector(0, CreateVector<LongVector, int64_t>(bufferData1, DATA_SIZE));

    // stream data1
    int64_t streamData1[] = {5, 5, 5, 5, 5, 5};
    VectorBatch *streamedVecBatch1 = std::make_unique<VectorBatch>(1, DATA_SIZE).release();
    streamedVecBatch1->SetVector(0, CreateVector<LongVector, int64_t>(streamData1, DATA_SIZE));

    // stream data2
    int64_t streamData2[] = {5, 6, 6, 7, 8, 9};
    VectorBatch *streamedVecBatch2 = std::make_unique<VectorBatch>(1, DATA_SIZE).release();
    streamedVecBatch2->SetVector(0, CreateVector<LongVector, int64_t>(streamData2, DATA_SIZE));

    // buffer data2
    int64_t bufferData2[] = {5, 6, 7, 7, 7, 7};
    VectorBatch *bufferedVecBatch2 = std::make_unique<VectorBatch>(1, DATA_SIZE).release();
    bufferedVecBatch2->SetVector(0, CreateVector<LongVector, int64_t>(bufferData2, DATA_SIZE));

    // add stream0
    streamedPageIndex->AddVecBatches(std::vector<VectorBatch *>{streamedVecBatch});

    // add buffer0
    bufferedPageIndex->AddVecBatches(std::vector<VectorBatch *>{bufferedVecBatch});

    auto scan = new SortMergeJoinScanner(streamedKeysTypes, streamedCols, 1, streamedPageIndex, bufferedKeysTypes,
                                         bufferedCols, bufferedPageIndex, OMNI_JOIN_TYPE_INNER, false);
    auto ret = scan->FindNextJoinRows();

    // get output
    std::vector<bool> isPreMatched;
    std::vector<int64_t> streamedAddr;
    std::vector<int64_t> bufferedAddr;
    scan->GetMatchedValueAddresses(isPreMatched, streamedAddr, bufferedAddr);

    // add buffer1
    bufferedPageIndex->AddVecBatches(std::vector<VectorBatch *>{bufferedVecBatch1});
    ret = scan->FindNextJoinRows();
    // get output
    scan->GetMatchedValueAddresses(isPreMatched, streamedAddr, bufferedAddr);

    // add buffer2
    bufferedPageIndex->AddVecBatches(std::vector<VectorBatch *>{bufferedVecBatch2});
    ret = scan->FindNextJoinRows();
    // get output
    scan->GetMatchedValueAddresses(isPreMatched, streamedAddr, bufferedAddr);

    // add stream1
    streamedPageIndex->AddVecBatches(std::vector<VectorBatch *>{streamedVecBatch1});
    ret = scan->FindNextJoinRows();

    // get output
    scan->GetMatchedValueAddresses(isPreMatched, streamedAddr, bufferedAddr);

    // add stream2
    streamedPageIndex->AddVecBatches(std::vector<VectorBatch *>{streamedVecBatch2});
    ret = scan->FindNextJoinRows();
    // get output
    scan->GetMatchedValueAddresses(isPreMatched, streamedAddr, bufferedAddr);

    // add buffer eof
    VectorBatch *eofVecBatch = std::make_unique<VectorBatch>(1, 0).release();
    bufferedPageIndex->AddVecBatches(std::vector<VectorBatch *>{eofVecBatch});
    ret = scan->FindNextJoinRows();
    scan->GetMatchedValueAddresses(isPreMatched, streamedAddr, bufferedAddr);

    // add buffer eof
    bufferedPageIndex->AddVecBatches(std::vector<VectorBatch *>{eofVecBatch});
    ret = scan->FindNextJoinRows();
    scan->GetMatchedValueAddresses(isPreMatched, streamedAddr, bufferedAddr);

    // add stream eof
    streamedPageIndex->AddVecBatches(std::vector<VectorBatch *>{eofVecBatch});
    ret = scan->FindNextJoinRows();
    scan->GetMatchedValueAddresses(isPreMatched, streamedAddr, bufferedAddr);

    std::vector<int64_t> expectedStreamedAddr(
            {0, 1, 2, 3, 4, 5, 5, 5, 5,
             5, 5, 5, 5, 4294967296, 4294967296, 4294967296, 4294967296, 4294967296,
             4294967296, 4294967296, 4294967296, 4294967297, 4294967297, 4294967297, 4294967297, 4294967297, 4294967297,
             4294967297, 4294967297, 4294967298, 4294967298, 4294967298, 4294967298, 4294967298, 4294967298, 4294967298,
             4294967298, 4294967299, 4294967299, 4294967299, 4294967299, 4294967299, 4294967299, 4294967299, 4294967299,
             4294967300, 4294967300, 4294967300, 4294967300, 4294967300, 4294967300, 4294967300, 4294967300, 4294967301,
             4294967301, 4294967301, 4294967301, 4294967301, 4294967301, 4294967301, 4294967301, 8589934592, 8589934592,
             8589934592, 8589934592, 8589934592, 8589934592, 8589934592, 8589934592, 8589934593, 8589934594, 8589934595,
             8589934595, 8589934595, 8589934595});
    std::vector<int64_t> expectedBufferedAddr(
            {0, 1, 2, 3, 4, 5, 4294967296, 4294967297, 4294967298,
             4294967299, 4294967300, 4294967301, 8589934592, 5, 4294967296, 4294967297, 4294967298, 4294967299,
             4294967300, 4294967301, 8589934592, 5, 4294967296, 4294967297, 4294967298, 4294967299, 4294967300,
             4294967301, 8589934592, 5, 4294967296, 4294967297, 4294967298, 4294967299, 4294967300, 4294967301,
             8589934592, 5, 4294967296, 4294967297, 4294967298, 4294967299, 4294967300, 4294967301, 8589934592,
             5, 4294967296, 4294967297, 4294967298, 4294967299, 4294967300, 4294967301, 8589934592, 5,
             4294967296, 4294967297, 4294967298, 4294967299, 4294967300, 4294967301, 8589934592, 5, 4294967296,
             4294967297, 4294967298, 4294967299, 4294967300, 4294967301, 8589934592, 8589934593, 8589934593, 8589934594,
             8589934595, 8589934596, 8589934597});
    ExpectVectorEqual(expectedStreamedAddr, streamedAddr);
    ExpectVectorEqual(expectedBufferedAddr, bufferedAddr);

    VectorHelper::FreeVecBatch(bufferedVecBatch);
    VectorHelper::FreeVecBatch(streamedVecBatch);
    VectorHelper::FreeVecBatch(bufferedVecBatch1);
    VectorHelper::FreeVecBatch(streamedVecBatch1);
    VectorHelper::FreeVecBatch(bufferedVecBatch2);
    VectorHelper::FreeVecBatch(streamedVecBatch2);
    delete eofVecBatch;
    delete bufferedPageIndex;
    delete streamedPageIndex;
    delete scan;
}

TEST(NativeSortMergeJoinTest, TestSortMergeJoinResultBuilder) {
    std::vector<VecType> leftTypes = {IntVecType::Instance(), DoubleVecType::Instance()};
    VecTypes leftSourceTypes(leftTypes);
    std::vector<VecType> rightTypes = {IntVecType::Instance(), DoubleVecType::Instance(), VarcharVecType(3)};
    VecTypes rightSourceTypes(rightTypes);

    auto *leftPagesIndex = new DynamicPagesIndex(leftSourceTypes);
    auto *rightPagesIndex = new DynamicPagesIndex(rightSourceTypes);

    const int32_t DATA_SIZE = 6;
    int32_t leftData1_1[DATA_SIZE] = {0, 1, 2, 3, 4, 5};
    Vector *leftVector1 = CreateVector<IntVector, int32_t>(leftData1_1, DATA_SIZE);
    // build dictionary vector for test
    double leftData1_2[DATA_SIZE] = {0.0, 1.1, 2.2, 3.3, 4.4, 5.5};
    Vector *dataVector1 = CreateVector<DoubleVector, double>(leftData1_2, DATA_SIZE);
    int32_t ids1[DATA_SIZE] = {0, 1, 2, 3, 4, 5};
    auto dicVector1 = new DictionaryVector(dataVector1, ids1, DATA_SIZE);
    auto *leftVecBatch1 = new VectorBatch(2, DATA_SIZE);
    leftVecBatch1->SetVector(0, leftVector1);
    leftVecBatch1->SetVector(1, dicVector1);

    int32_t leftData2_1[DATA_SIZE] = {6, 7, 8, 9, 10, 11};
    Vector *leftVector2 = CreateVector<IntVector, int32_t>(leftData2_1, DATA_SIZE);
    double leftData2_2[DATA_SIZE] = {6.6, 7.7, 8.8, 9.9, 10.1, 11.1};
    Vector *dataVector2 = CreateVector<DoubleVector, double>(leftData2_2, DATA_SIZE);
    int32_t ids2[DATA_SIZE] = {0, 1, 2, 3, 4, 5};
    auto dicVector2 = new DictionaryVector(dataVector2, ids2, DATA_SIZE);
    auto *leftVecBatch2 = new VectorBatch(2, DATA_SIZE);
    leftVecBatch2->SetVector(0, leftVector2);
    leftVecBatch2->SetVector(1, dicVector2);

    std::vector<VectorBatch *> leftBatchVector;
    leftBatchVector.push_back(leftVecBatch1);
    leftBatchVector.push_back(leftVecBatch2);
    leftPagesIndex->AddVecBatches(leftBatchVector);
    int32_t leftTableOutputCols[2] = {0, 1};
    int32_t leftTableOutputColsCount = 2;

    int32_t rightData1_1[DATA_SIZE] = {5, 4, 3, 2, 1, 0};
    double rightData1_2[DATA_SIZE] = {5.5, 4.4, 3.3, 2.2, 1.1, 0.0};
    std::string rightData1_3[DATA_SIZE] = {"555", "444", "33", "22", "1", "0"};
    int32_t rightData2_1[DATA_SIZE] = {11, 10, 9, 8, 7, 6};
    double rightData2_2[DATA_SIZE] = {11.1, 10.1, 9.9, 8.8, 7.7, 6.6};
    std::string rightData2_3[DATA_SIZE] = {"111", "101", "99", "88", "7", "6"};

    VectorBatch *rightVecBatch1 =
            CreateVectorBatch(rightSourceTypes, DATA_SIZE, rightData1_1, rightData1_2, rightData1_3);
    VectorBatch *rightVecBatch2 =
            CreateVectorBatch(rightSourceTypes, DATA_SIZE, rightData2_1, rightData2_2, rightData2_3);
    std::vector<VectorBatch *> rightBatchVector;
    rightBatchVector.push_back(rightVecBatch1);
    rightBatchVector.push_back(rightVecBatch2);
    rightPagesIndex->AddVecBatches(rightBatchVector);
    int32_t rightTableOutputCols[2] = {1, 2};
    int32_t rightTableOutputColsCount = 2;
    string filter;

    VectorAllocator *vecAllocator = VectorAllocatorFactory::GetGlobalAllocator();

    auto *resultBuilder =
            new JoinResultBuilder(leftSourceTypes, leftTableOutputCols, leftTableOutputColsCount, leftPagesIndex,
                                  rightSourceTypes, rightTableOutputCols, rightTableOutputColsCount, rightPagesIndex,
                                  filter, vecAllocator);

    std::vector<bool> isPreMatched;
    isPreMatched.insert(isPreMatched.end(), 6, false);
    std::vector<int64_t> leftAddress1 = {EncodeSyntheticAddress(0, 1), EncodeSyntheticAddress(0, 3),
        EncodeSyntheticAddress(0, 5), EncodeSyntheticAddress(1, 1),
        EncodeSyntheticAddress(1, 3), EncodeSyntheticAddress(1, 5)};
    std::vector<int64_t> rightAddress1 = {EncodeSyntheticAddress(0, 0), EncodeSyntheticAddress(0, 2),
        EncodeSyntheticAddress(0, 4), EncodeSyntheticAddress(1, 0),
        EncodeSyntheticAddress(1, 2), EncodeSyntheticAddress(1, 4)};

    ASSERT_EQ(resultBuilder->AddJoinValueAddresses(isPreMatched, leftAddress1, rightAddress1), 0);

    std::vector<omniruntime::vec::VectorBatch *> outputPages;

    resultBuilder->GetOutput(outputPages);
    resultBuilder->Finish();

    ASSERT_EQ(outputPages.size(), 1);

    int32_t expectedData1[6] = {1, 3, 5, 7, 9, 11};
    double expectedData2[6] = {1.1, 3.3, 5.5, 7.7, 9.9, 11.1};
    double expectedData3[6] = {5.5, 3.3, 1.1, 11.1, 9.9, 7.7};
    string expectedData4[6] = {"555", "33", "1", "111", "99", "7"};

    AssertVecBatchEquals(outputPages[0], 4, 6, expectedData1, expectedData2, expectedData3, expectedData4);

    delete dataVector1;
    delete dataVector2;
    delete resultBuilder;
    delete leftPagesIndex;
    delete rightPagesIndex;
    VectorHelper::FreeVecBatches(outputPages);
}

TEST(NativeSortMergeJoinTest, TestSortMergeJoinResultBuilderWithFilter) {
    std::vector<VecType> leftTypes = {IntVecType::Instance(), DoubleVecType::Instance()};
    VecTypes leftSourceTypes(leftTypes);
    std::vector<VecType> rightTypes = {IntVecType::Instance(), DoubleVecType::Instance(), VarcharVecType(3)};
    VecTypes rightSourceTypes(rightTypes);

    auto *leftPagesIndex = new DynamicPagesIndex(leftSourceTypes);
    auto *rightPagesIndex = new DynamicPagesIndex(rightSourceTypes);

    const int32_t DATA_SIZE = 6;
    int32_t leftData1_1[DATA_SIZE] = {0, 1, 2, 3, 4, 5};
    double leftData1_2[DATA_SIZE] = {0.0, 1.1, 2.2, 3.3, 4.4, 5.5};

    VectorBatch *leftVecBatch = CreateVectorBatch(leftSourceTypes, DATA_SIZE, leftData1_1, leftData1_2);
    std::vector<VectorBatch *> leftBatchVector;
    leftBatchVector.push_back(leftVecBatch);
    leftPagesIndex->AddVecBatches(leftBatchVector);
    int32_t leftTableOutputCols[2] = {0, 1};
    int32_t leftTableOutputColsCount = 2;

    int32_t rightData1_1[DATA_SIZE] = {5, 4, 3, 2, 1, 0};
    double rightData1_2[DATA_SIZE] = {5.5, 4.4, 3.3, 2.2, 1.1, 0.0};
    std::string rightData1_3[DATA_SIZE] = {"555", "444", "33", "22", "1", "0"};

    VectorBatch *rightVecBatch =
            CreateVectorBatch(rightSourceTypes, DATA_SIZE, rightData1_1, rightData1_2, rightData1_3);
    std::vector<VectorBatch *> rightBatchVector;
    rightBatchVector.push_back(rightVecBatch);
    rightPagesIndex->AddVecBatches(rightBatchVector);
    int32_t rightTableOutputCols[2] = {1, 2};
    int32_t rightTableOutputColsCount = 2;
    string filter = "$operator$GREATER_THAN:4(#0, 1:1)";
    VectorAllocator *vecAllocator = VectorAllocatorFactory::GetGlobalAllocator();

    auto *resultBuilder =
            new JoinResultBuilder(leftSourceTypes, leftTableOutputCols, leftTableOutputColsCount, leftPagesIndex,
                                  rightSourceTypes, rightTableOutputCols, rightTableOutputColsCount, rightPagesIndex,
                                  filter, vecAllocator);

    std::vector<bool> isPreMatched;
    isPreMatched.insert(isPreMatched.end(), 3, false);
    std::vector<int64_t> leftAddress1 = {EncodeSyntheticAddress(0, 1), EncodeSyntheticAddress(0, 3),
                                         EncodeSyntheticAddress(0, 5)};
    std::vector<int64_t> rightAddress1 = {EncodeSyntheticAddress(0, 0), EncodeSyntheticAddress(0, 2),
                                          EncodeSyntheticAddress(0, 4)};

    ASSERT_EQ(resultBuilder->AddJoinValueAddresses(isPreMatched, leftAddress1, rightAddress1), 0);

    std::vector<omniruntime::vec::VectorBatch *> outputPages;

    resultBuilder->GetOutput(outputPages);
    resultBuilder->Finish();

    ASSERT_EQ(outputPages.size(), 1);

    int32_t expectedData1[2] = {3, 5};
    double expectedData2[2] = {3.3, 5.5};
    double expectedData3[2] = {3.3, 1.1};
    string expectedData4[2] = {"33", "1"};

    AssertVecBatchEquals(outputPages[0], 4, 2, expectedData1, expectedData2, expectedData3, expectedData4);

    delete resultBuilder;
    delete leftPagesIndex;
    delete rightPagesIndex;
    VectorHelper::FreeVecBatches(outputPages);
}
