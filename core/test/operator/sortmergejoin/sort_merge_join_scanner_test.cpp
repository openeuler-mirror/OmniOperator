/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: lookup join operator test implementations
 */

#include "gtest/gtest.h"
#include "../../../src/operator/join/sortmergejoin/sort_merge_join_scanner.h"
#include "../../util/test_util.h"
#include "../../../src/vector/vector_helper.h"

using namespace omniruntime::op;

void ExpectVectorEqual(std::vector<int64_t> expected, std::vector<int64_t> actual)
{
    EXPECT_EQ(expected.size(), actual.size());
    for (int i = 0; i < expected.size(); ++i) {
        EXPECT_EQ(expected[i], actual[i]);
    }
}

TEST(JoinScanner, Test1)
{
    VecTypes streamedTypes(std::vector<VecType>({ LongVecType(), LongVecType() }));
    VecTypes streamedKeysTypes(std::vector<VecType>({ LongVecType() }));
    int32_t streamedCols[] = {0};
    auto streamedPageIndex = new DynamicPagesIndex(streamedTypes);
    VecTypes bufferedTypes(std::vector<VecType>({ LongVecType(), LongVecType() }));
    VecTypes bufferedKeysTypes(std::vector<VecType>({ LongVecType() }));
    int32_t bufferedCols[] = {0};
    auto bufferedPageIndex = new DynamicPagesIndex(bufferedTypes);

    const int32_t DATA_SIZE = 4;
    int64_t streamData0[] = {1, 2, 3, 5};
    int64_t bufferData0[] =  {1, 5, 6, 7};
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
    std::vector<int64_t> expectedStreamedAddr({ 0, 3 });
    std::vector<int64_t> expectedBufferedAddr({ 0, 1 });
    std::vector<int64_t> streamedAddr;
    std::vector<int64_t> bufferedAddr;
    scan->GetMatchedValueAddresses(streamedAddr, bufferedAddr);
    ExpectVectorEqual(expectedStreamedAddr, streamedAddr);
    ExpectVectorEqual(expectedBufferedAddr, bufferedAddr);

    VectorHelper::FreeVecBatch(bufferedVecBatch);
    VectorHelper::FreeVecBatch(streamedVecBatch);
    delete bufferedPageIndex;
    delete streamedPageIndex;
    delete scan;
}


TEST(JoinScanner, Test2)
{
    VecTypes streamedTypes(std::vector<VecType>({ LongVecType(), LongVecType() }));
    VecTypes streamedKeysTypes(std::vector<VecType>({ LongVecType() }));
    int32_t streamedCols[] = {0};
    auto streamedPageIndex = new DynamicPagesIndex(streamedTypes);
    VecTypes bufferedTypes(std::vector<VecType>({ LongVecType(), LongVecType() }));
    VecTypes bufferedKeysTypes(std::vector<VecType>({ LongVecType() }));
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
    std::vector<int64_t> streamedAddr;
    std::vector<int64_t> bufferedAddr;
    scan->GetMatchedValueAddresses(streamedAddr, bufferedAddr);

    VectorBatch *eofVecBatch = std::make_unique<VectorBatch>(1, 0).release();
    bufferedPageIndex->AddVecBatches(std::vector<VectorBatch *> { eofVecBatch });
    ret = scan->FindNextJoinRows();
    std::vector<int64_t> expectedStreamedAddr({ 0, 1 });
    std::vector<int64_t> expectedBufferedAddr({ 0, 3 });
    scan->GetMatchedValueAddresses(streamedAddr, bufferedAddr);
    ExpectVectorEqual(expectedStreamedAddr, streamedAddr);
    ExpectVectorEqual(expectedBufferedAddr, bufferedAddr);

    VectorHelper::FreeVecBatch(bufferedVecBatch);
    VectorHelper::FreeVecBatch(streamedVecBatch);
    delete eofVecBatch;
    delete bufferedPageIndex;
    delete streamedPageIndex;
    delete scan;
}

TEST(JoinScanner, Test3)
{
    VecTypes streamedTypes(std::vector<VecType>({ LongVecType(), LongVecType() }));
    VecTypes streamedKeysTypes(std::vector<VecType>({ LongVecType() }));
    int32_t streamedCols[] = {0};
    auto streamedPageIndex = new DynamicPagesIndex(streamedTypes);
    VecTypes bufferedTypes(std::vector<VecType>({ LongVecType(), LongVecType() }));
    VecTypes bufferedKeysTypes(std::vector<VecType>({ LongVecType() }));
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
    std::vector<int64_t> expectedStreamedAddr({ 2, 3 });
    std::vector<int64_t> expectedBufferedAddr({ 1, 2 });
    std::vector<int64_t> streamedAddr;
    std::vector<int64_t> bufferedAddr;
    scan->GetMatchedValueAddresses(streamedAddr, bufferedAddr);
    ExpectVectorEqual(expectedStreamedAddr, streamedAddr);
    ExpectVectorEqual(expectedBufferedAddr, bufferedAddr);

    VectorHelper::FreeVecBatch(bufferedVecBatch);
    VectorHelper::FreeVecBatch(streamedVecBatch);
    delete bufferedPageIndex;
    delete streamedPageIndex;
    delete scan;
}

TEST(JoinScanner, Test4)
{
    VecTypes streamedTypes(std::vector<VecType>({ LongVecType(), LongVecType() }));
    VecTypes streamedKeysTypes(std::vector<VecType>({ LongVecType() }));
    int32_t streamedCols[] = {0};
    auto streamedPageIndex = new DynamicPagesIndex(streamedTypes);
    VecTypes bufferedTypes(std::vector<VecType>({ LongVecType(), LongVecType() }));
    VecTypes bufferedKeysTypes(std::vector<VecType>({ LongVecType() }));
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
    std::vector<int64_t> expectedStreamedAddr({ 1, 2 });
    std::vector<int64_t> expectedBufferedAddr({ 2, 3 });
    std::vector<int64_t> streamedAddr;
    std::vector<int64_t> bufferedAddr;
    scan->GetMatchedValueAddresses(streamedAddr, bufferedAddr);
    VectorBatch *eofVecBatch = std::make_unique<VectorBatch>(2, 0).release();
    bufferedPageIndex->AddVecBatches(std::vector<VectorBatch *> { eofVecBatch });
    scan->FindNextJoinRows();
    scan->GetMatchedValueAddresses(streamedAddr, bufferedAddr);
    ExpectVectorEqual(expectedStreamedAddr, streamedAddr);
    ExpectVectorEqual(expectedBufferedAddr, bufferedAddr);

    VectorHelper::FreeVecBatch(bufferedVecBatch);
    VectorHelper::FreeVecBatch(streamedVecBatch);
    delete eofVecBatch;
    delete bufferedPageIndex;
    delete streamedPageIndex;
    delete scan;
}

TEST(JoinScanner, Test5)
{
    VecTypes streamedTypes(std::vector<VecType>({ LongVecType(), LongVecType() }));
    VecTypes streamedKeysTypes(std::vector<VecType>({ LongVecType() }));
    int32_t streamedCols[] = {0};
    auto streamedPageIndex = new DynamicPagesIndex(streamedTypes);
    VecTypes bufferedTypes(std::vector<VecType>({ LongVecType(), LongVecType() }));
    VecTypes bufferedKeysTypes(std::vector<VecType>({ LongVecType() }));
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
    std::vector<int64_t> expectedStreamedAddr({ 3 });
    std::vector<int64_t> expectedBufferedAddr({ 1 });
    std::vector<int64_t> streamedAddr;
    std::vector<int64_t> bufferedAddr;
    scan->GetMatchedValueAddresses(streamedAddr, bufferedAddr);
    ExpectVectorEqual(expectedStreamedAddr, streamedAddr);
    ExpectVectorEqual(expectedBufferedAddr, bufferedAddr);

    VectorHelper::FreeVecBatch(bufferedVecBatch);
    VectorHelper::FreeVecBatch(streamedVecBatch);
    delete bufferedPageIndex;
    delete streamedPageIndex;
    delete scan;
}

TEST(JoinScanner, Test6)
{
    VecTypes streamedTypes(std::vector<VecType>({ LongVecType(), LongVecType() }));
    VecTypes streamedKeysTypes(std::vector<VecType>({ LongVecType() }));
    int32_t streamedCols[] = {0};
    auto streamedPageIndex = new DynamicPagesIndex(streamedTypes);
    VecTypes bufferedTypes(std::vector<VecType>({ LongVecType(), LongVecType() }));
    VecTypes bufferedKeysTypes(std::vector<VecType>({ LongVecType() }));
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
    std::vector<int64_t> expectedStreamedAddr({ 1 });
    std::vector<int64_t> expectedBufferedAddr({ 3 });
    std::vector<int64_t> streamedAddr;
    std::vector<int64_t> bufferedAddr;
    scan->GetMatchedValueAddresses(streamedAddr, bufferedAddr);
    VectorBatch *eofVecBatch = std::make_unique<VectorBatch>(2, 0).release();
    bufferedPageIndex->AddVecBatches(std::vector<VectorBatch *> { eofVecBatch });
    scan->FindNextJoinRows();
    scan->GetMatchedValueAddresses(streamedAddr, bufferedAddr);
    ExpectVectorEqual(expectedStreamedAddr, streamedAddr);
    ExpectVectorEqual(expectedBufferedAddr, bufferedAddr);

    VectorHelper::FreeVecBatch(bufferedVecBatch);
    VectorHelper::FreeVecBatch(streamedVecBatch);
    delete eofVecBatch;
    delete bufferedPageIndex;
    delete streamedPageIndex;
    delete scan;
}

TEST(JoinScanner, TestRepeatBufferedTableKeys1)
{
    VecTypes streamedTypes(std::vector<VecType>({ LongVecType(), LongVecType() }));
    VecTypes streamedKeysTypes(std::vector<VecType>({ LongVecType() }));
    int32_t streamedCols[] = {0};
    auto streamedPageIndex = new DynamicPagesIndex(streamedTypes);
    VecTypes bufferedTypes(std::vector<VecType>({ LongVecType(), LongVecType() }));
    VecTypes bufferedKeysTypes(std::vector<VecType>({ LongVecType() }));
    int32_t bufferedCols[] = {0};
    auto bufferedPageIndex = new DynamicPagesIndex(bufferedTypes);

    const int32_t DATA_SIZE = 4;
    int64_t streamData0[] = {-1, 0, 2, 7};
    int64_t bufferData0[] =  {0, 1, 2, 2};
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
    std::vector<int64_t> expectedStreamedAddr({ 1, 2, 2 });
    std::vector<int64_t> expectedBufferedAddr({ 0, 2, 3 });
    std::vector<int64_t> streamedAddr;
    std::vector<int64_t> bufferedAddr;
    scan->GetMatchedValueAddresses(streamedAddr, bufferedAddr);
    VectorBatch *eofVecBatch = std::make_unique<VectorBatch>(2, 0).release();
    bufferedPageIndex->AddVecBatches(std::vector<VectorBatch *> { eofVecBatch });
    scan->FindNextJoinRows();
    scan->GetMatchedValueAddresses(streamedAddr, bufferedAddr);
    ExpectVectorEqual(expectedStreamedAddr, streamedAddr);
    ExpectVectorEqual(expectedBufferedAddr, bufferedAddr);

    VectorHelper::FreeVecBatch(bufferedVecBatch);
    VectorHelper::FreeVecBatch(streamedVecBatch);
    delete eofVecBatch;
    delete bufferedPageIndex;
    delete streamedPageIndex;
    delete scan;
}

TEST(JoinScanner, TestRepeatBufferedTableKeys2)
{
    VecTypes streamedTypes(std::vector<VecType>({ LongVecType(), LongVecType() }));
    VecTypes streamedKeysTypes(std::vector<VecType>({ LongVecType() }));
    int32_t streamedCols[] = {0};
    auto streamedPageIndex = new DynamicPagesIndex(streamedTypes);
    VecTypes bufferedTypes(std::vector<VecType>({ LongVecType(), LongVecType() }));
    VecTypes bufferedKeysTypes(std::vector<VecType>({ LongVecType() }));
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
    std::vector<int64_t> expectedStreamedAddr({ 2, 2 });
    std::vector<int64_t> expectedBufferedAddr({ 1, 2 });
    std::vector<int64_t> streamedAddr;
    std::vector<int64_t> bufferedAddr;
    scan->GetMatchedValueAddresses(streamedAddr, bufferedAddr);
    ExpectVectorEqual(expectedStreamedAddr, streamedAddr);
    ExpectVectorEqual(expectedBufferedAddr, bufferedAddr);

    VectorHelper::FreeVecBatch(bufferedVecBatch);
    VectorHelper::FreeVecBatch(streamedVecBatch);
    delete bufferedPageIndex;
    delete streamedPageIndex;
    delete scan;
}

TEST(JoinScanner, TestRepeatStreamedTableKeys1)
{
    VecTypes streamedTypes(std::vector<VecType>({ LongVecType(), LongVecType() }));
    VecTypes streamedKeysTypes(std::vector<VecType>({ LongVecType() }));
    int32_t streamedCols[] = {0};
    auto streamedPageIndex = new DynamicPagesIndex(streamedTypes);
    VecTypes bufferedTypes(std::vector<VecType>({ LongVecType(), LongVecType() }));
    VecTypes bufferedKeysTypes(std::vector<VecType>({ LongVecType() }));
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
    std::vector<int64_t> expectedStreamedAddr({ 0, 2, 3 });
    std::vector<int64_t> expectedBufferedAddr({ 1, 2, 2 });
    std::vector<int64_t> streamedAddr;
    std::vector<int64_t> bufferedAddr;
    scan->GetMatchedValueAddresses(streamedAddr, bufferedAddr);
    ExpectVectorEqual(expectedStreamedAddr, streamedAddr);
    ExpectVectorEqual(expectedBufferedAddr, bufferedAddr);

    VectorHelper::FreeVecBatch(bufferedVecBatch);
    VectorHelper::FreeVecBatch(streamedVecBatch);
    delete bufferedPageIndex;
    delete streamedPageIndex;
    delete scan;
}

TEST(JoinScanner, TestRepeatStreamedTableKeys2)
{
    VecTypes streamedTypes(std::vector<VecType>({ LongVecType(), LongVecType() }));
    VecTypes streamedKeysTypes(std::vector<VecType>({ LongVecType() }));
    int32_t streamedCols[] = {0};
    auto streamedPageIndex = new DynamicPagesIndex(streamedTypes);
    VecTypes bufferedTypes(std::vector<VecType>({ LongVecType(), LongVecType() }));
    VecTypes bufferedKeysTypes(std::vector<VecType>({ LongVecType() }));
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
    std::vector<int64_t> expectedStreamedAddr({ 1, 2 });
    std::vector<int64_t> expectedBufferedAddr({ 2, 2 });
    std::vector<int64_t> streamedAddr;
    std::vector<int64_t> bufferedAddr;
    scan->GetMatchedValueAddresses(streamedAddr, bufferedAddr);
    ExpectVectorEqual(expectedStreamedAddr, streamedAddr);
    ExpectVectorEqual(expectedBufferedAddr, bufferedAddr);

    VectorHelper::FreeVecBatch(bufferedVecBatch);
    VectorHelper::FreeVecBatch(streamedVecBatch);
    delete bufferedPageIndex;
    delete streamedPageIndex;
    delete scan;
}

TEST(JoinScanner, TestMultipleTableKeys)
{
    VecTypes streamedTypes(std::vector<VecType>({ LongVecType(), LongVecType(), LongVecType(), LongVecType() }));
    VecTypes streamedKeysTypes(std::vector<VecType>({ LongVecType(), LongVecType() }));
    int32_t streamedCols[] = {1, 2};
    auto streamedPageIndex = new DynamicPagesIndex(streamedTypes);
    VecTypes bufferedTypes(std::vector<VecType>({ LongVecType(), LongVecType(), LongVecType() }));
    VecTypes bufferedKeysTypes(std::vector<VecType>({ LongVecType(), LongVecType() }));
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
    int64_t bufferData0[] =  {0, 1, 2, 2};
    int64_t bufferData1[] =  {0, 1, 1, 2};
    int64_t bufferData2[] =  {9, 8, 7, 6};
    int64_t bufferData3[] = {111, 11, 1, 0};
    VectorBatch *bufferedVecBatch = std::make_unique<VectorBatch>(4, DATA_SIZE).release();
    bufferedVecBatch->SetVector(0, CreateVector<LongVector, int64_t>(bufferData0, DATA_SIZE));
    bufferedVecBatch->SetVector(1, CreateVector<LongVector, int64_t>(bufferData1, DATA_SIZE));
    bufferedVecBatch->SetVector(2, CreateVector<LongVector, int64_t>(bufferData2, DATA_SIZE));
    bufferedVecBatch->SetVector(3, CreateVector<LongVector, int64_t>(bufferData3, DATA_SIZE));
    auto buffered = std::vector<VectorBatch *>();
    buffered.push_back(bufferedVecBatch);
    bufferedPageIndex->AddVecBatches(buffered);
    auto scan = new SortMergeJoinScanner(streamedKeysTypes, streamedCols, streamedKeysTypes.GetSize(),
        streamedPageIndex, bufferedKeysTypes, bufferedCols, bufferedPageIndex, OMNI_JOIN_TYPE_INNER, false);
    scan->FindNextJoinRows();
    std::vector<int64_t> expectedStreamedAddr({ 1, 2 });
    std::vector<int64_t> expectedBufferedAddr({ 0, 3 });
    std::vector<int64_t> streamedAddr;
    std::vector<int64_t> bufferedAddr;
    scan->GetMatchedValueAddresses(streamedAddr, bufferedAddr);
    VectorBatch *eofVecBatch = std::make_unique<VectorBatch>(4, 0).release();
    bufferedPageIndex->AddVecBatches(std::vector<VectorBatch *> { eofVecBatch });
    scan->FindNextJoinRows();
    scan->GetMatchedValueAddresses(streamedAddr, bufferedAddr);
    ExpectVectorEqual(expectedStreamedAddr, streamedAddr);
    ExpectVectorEqual(expectedBufferedAddr, bufferedAddr);

    VectorHelper::FreeVecBatch(bufferedVecBatch);
    VectorHelper::FreeVecBatch(streamedVecBatch);
    delete eofVecBatch;
    delete bufferedPageIndex;
    delete streamedPageIndex;
    delete scan;
}

TEST(JoinScanner, TestNullKeys)
{
    VecTypes streamedTypes(std::vector<VecType>({ LongVecType(), LongVecType(), LongVecType(), LongVecType() }));
    VecTypes streamedKeysTypes(std::vector<VecType>({ LongVecType(), LongVecType() }));
    int32_t streamedCols[] = {1, 2};
    auto streamedPageIndex = new DynamicPagesIndex(streamedTypes);
    VecTypes bufferedTypes(std::vector<VecType>({ LongVecType(), LongVecType(), LongVecType() }));
    VecTypes bufferedKeysTypes(std::vector<VecType>({ LongVecType(), LongVecType() }));
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
    int64_t bufferData0[] =  {0, 1, 2, 2};
    int64_t bufferData1[] =  {0, 1, 2, 2};
    int64_t bufferData2[] =  {9, 8, 7, 6};
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
    auto scan = new SortMergeJoinScanner(streamedKeysTypes, streamedCols, streamedKeysTypes.GetSize(),
        streamedPageIndex, bufferedKeysTypes, bufferedCols, bufferedPageIndex, OMNI_JOIN_TYPE_INNER, false);
    scan->FindNextJoinRows();
    std::vector<int64_t> expectedStreamedAddr({ 1, 2 });
    std::vector<int64_t> expectedBufferedAddr({ 0, 3 });
    std::vector<int64_t> streamedAddr;
    std::vector<int64_t> bufferedAddr;
    scan->GetMatchedValueAddresses(streamedAddr, bufferedAddr);
    VectorBatch *eofVecBatch = std::make_unique<VectorBatch>(4, 0).release();
    bufferedPageIndex->AddVecBatches(std::vector<VectorBatch *> { eofVecBatch });
    scan->FindNextJoinRows();
    scan->GetMatchedValueAddresses(streamedAddr, bufferedAddr);
    ExpectVectorEqual(expectedStreamedAddr, streamedAddr);
    ExpectVectorEqual(expectedBufferedAddr, bufferedAddr);

    VectorHelper::FreeVecBatch(bufferedVecBatch);
    VectorHelper::FreeVecBatch(streamedVecBatch);
    delete eofVecBatch;
    delete bufferedPageIndex;
    delete streamedPageIndex;
    delete scan;
}

TEST(JoinScanner, TestDateTypes)
{
    VecTypes streamedTypes(
        std::vector<VecType>({ LongVecType(), DoubleVecType(), VarcharVecType(5), BooleanVecType() }));
    VecTypes streamedKeysTypes(std::vector<VecType>({ DoubleVecType(), VarcharVecType(5), BooleanVecType() }));
    int32_t streamedCols[] = {1, 2, 3};
    auto streamedPageIndex = new DynamicPagesIndex(streamedTypes);
    VecTypes bufferedTypes(std::vector<VecType>({ DoubleVecType(), VarcharVecType(5), BooleanVecType() }));
    VecTypes bufferedKeysTypes(std::vector<VecType>({ DoubleVecType(), VarcharVecType(5), BooleanVecType() }));
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
    auto scan = new SortMergeJoinScanner(streamedKeysTypes, streamedCols, streamedKeysTypes.GetSize(),
        streamedPageIndex, bufferedKeysTypes, bufferedCols, bufferedPageIndex, OMNI_JOIN_TYPE_INNER, false);
    scan->FindNextJoinRows();
    std::vector<int64_t> expectedStreamedAddr({ 3 });
    std::vector<int64_t> expectedBufferedAddr({ 3 });
    std::vector<int64_t> streamedAddr;
    std::vector<int64_t> bufferedAddr;
    scan->GetMatchedValueAddresses(streamedAddr, bufferedAddr);
    VectorBatch *eofVecBatch = std::make_unique<VectorBatch>(4, 0).release();
    bufferedPageIndex->AddVecBatches(std::vector<VectorBatch *> { eofVecBatch });
    scan->FindNextJoinRows();
    scan->GetMatchedValueAddresses(streamedAddr, bufferedAddr);
    ExpectVectorEqual(expectedStreamedAddr, streamedAddr);
    ExpectVectorEqual(expectedBufferedAddr, bufferedAddr);

    VectorHelper::FreeVecBatch(bufferedVecBatch);
    VectorHelper::FreeVecBatch(streamedVecBatch);
    delete eofVecBatch;
    delete bufferedPageIndex;
    delete streamedPageIndex;
    delete scan;
}

TEST(JoinScanner, TestMultipleVecBatch)
{
    VecTypes streamedTypes(std::vector<VecType>({ LongVecType() }));
    VecTypes streamedKeysTypes(std::vector<VecType>({ LongVecType() }));
    int32_t streamedCols[] = {0};
    auto streamedPageIndex = new DynamicPagesIndex(streamedTypes);
    VecTypes bufferedTypes(std::vector<VecType>({ LongVecType() }));
    VecTypes bufferedKeysTypes(std::vector<VecType>({ LongVecType() }));
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
        streamedPageIndex, bufferedKeysTypes, bufferedCols, bufferedPageIndex, OMNI_JOIN_TYPE_INNER, false);
    scan->FindNextJoinRows();
    std::vector<int64_t> streamedAddr;
    std::vector<int64_t> bufferedAddr;
    scan->GetMatchedValueAddresses(streamedAddr, bufferedAddr);

    long bufferData1[] = {10, 13, 13, 17, 17, 18, 18, 19};
    int bufferSize1 = 8;
    VectorBatch *bufferedVecBatch1 = std::make_unique<VectorBatch>(1, bufferSize1).release();
    bufferedVecBatch1->SetVector(0, CreateVector<LongVector, int64_t>(bufferData1, bufferSize1));
    auto buffered1 = std::vector<VectorBatch *>();
    buffered1.push_back(bufferedVecBatch1);
    bufferedPageIndex->AddVecBatches(buffered1);
    scan->FindNextJoinRows();
    scan->GetMatchedValueAddresses(streamedAddr, bufferedAddr);

    long bufferData2[] = {20, 21, 23, 24, 25, 25, 25, 25};
    int bufferSize2 = 8;
    VectorBatch *bufferedVecBatch2 = std::make_unique<VectorBatch>(1, bufferSize2).release();
    bufferedVecBatch2->SetVector(0, CreateVector<LongVector, int64_t>(bufferData2, bufferSize2));
    auto buffered2 = std::vector<VectorBatch *>();
    buffered2.push_back(bufferedVecBatch2);
    bufferedPageIndex->AddVecBatches(buffered2);
    scan->FindNextJoinRows();
    scan->GetMatchedValueAddresses(streamedAddr, bufferedAddr);

    long bufferData3[] = {30, 31, 42, 43, 44, 45, 46, 47};
    int bufferSize3 = 8;
    VectorBatch *bufferedVecBatch3 = std::make_unique<VectorBatch>(1, bufferSize3).release();
    bufferedVecBatch3->SetVector(0, CreateVector<LongVector, int64_t>(bufferData3, bufferSize3));
    auto buffered3 = std::vector<VectorBatch *>();
    buffered3.push_back(bufferedVecBatch3);
    bufferedPageIndex->AddVecBatches(buffered3);
    scan->FindNextJoinRows();
    scan->GetMatchedValueAddresses(streamedAddr, bufferedAddr);

    long streamData1[] = {28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41};
    int streamedSize1 = 14;
    VectorBatch *streamedVecBatch1 = std::make_unique<VectorBatch>(1, streamedSize1).release();
    streamedVecBatch1->SetVector(0, CreateVector<LongVector, int64_t>(streamData1, streamedSize1));
    auto streamed1 = std::vector<VectorBatch *>();
    streamed1.push_back(streamedVecBatch1);
    streamedPageIndex->AddVecBatches(streamed1);
    scan->FindNextJoinRows();
    scan->GetMatchedValueAddresses(streamedAddr, bufferedAddr);

    long streamData2[] = {43, 43, 43, 43, 43, 43, 43, 43, 43, 43, 43, 43, 43, 47};
    int streamedSize2 = 14;
    VectorBatch *streamedVecBatch2 = std::make_unique<VectorBatch>(1, streamedSize2).release();
    streamedVecBatch2->SetVector(0, CreateVector<LongVector, int64_t>(streamData2, streamedSize2));
    auto streamed2 = std::vector<VectorBatch *>();
    streamed2.push_back(streamedVecBatch2);
    streamedPageIndex->AddVecBatches(streamed2);
    scan->FindNextJoinRows();
    scan->GetMatchedValueAddresses(streamedAddr, bufferedAddr);

    VectorBatch *eofVecBatch = std::make_unique<VectorBatch>(1, 0).release();
    bufferedPageIndex->AddVecBatches(std::vector<VectorBatch *> { eofVecBatch });
    scan->FindNextJoinRows();
    scan->GetMatchedValueAddresses(streamedAddr, bufferedAddr);

    streamedPageIndex->AddVecBatches(std::vector<VectorBatch *> { eofVecBatch });
    scan->FindNextJoinRows();
    scan->GetMatchedValueAddresses(streamedAddr, bufferedAddr);

    std::vector<int64_t> expectedStreamedAddr(
        { 1,          3,          3,          3,          3,          4,          4,          4,          4,
        5,          6,          8,          8,          9,          9,          10,         10,         12,
        12,         4294967298, 4294967299, 8589934592, 8589934593, 8589934594, 8589934595, 8589934596, 8589934597,
        8589934598, 8589934599, 8589934600, 8589934601, 8589934602, 8589934603, 8589934604, 8589934605 });
    std::vector<int64_t> expectedBufferedAddr(
        { 0,           1,           2,           3,           4,           1,           2,
        3,           4,           5,           6,           7,           4294967296,  4294967297,
        4294967298,  4294967297,  4294967298,  4294967301,  4294967302,  12884901888, 12884901889,
        12884901891, 12884901891, 12884901891, 12884901891, 12884901891, 12884901891, 12884901891,
        12884901891, 12884901891, 12884901891, 12884901891, 12884901891, 12884901891, 12884901895 });
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

TEST(JoinScanner, TestReturnCode)
{
    VecTypes streamedTypes(std::vector<VecType>({ LongVecType(), LongVecType() }));
    VecTypes streamedKeysTypes(std::vector<VecType>({ LongVecType() }));
    int32_t streamedCols[] = {0};
    auto streamedPageIndex = new DynamicPagesIndex(streamedTypes);
    VecTypes bufferedTypes(std::vector<VecType>({ LongVecType(), LongVecType() }));
    VecTypes bufferedKeysTypes(std::vector<VecType>({ LongVecType() }));
    int32_t bufferedCols[] = {0};
    auto bufferedPageIndex = new DynamicPagesIndex(bufferedTypes);

    const int32_t DATA_SIZE = 4;
    int64_t streamData0[] = {1, 2, 3, 4};
    int64_t bufferData0[] =  {1, 2, 3, 4};
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
    std::vector<int64_t> streamedAddr;
    std::vector<int64_t> bufferedAddr;
    scan->GetMatchedValueAddresses(streamedAddr, bufferedAddr);

    VectorBatch *eofVecBatch = std::make_unique<VectorBatch>(2, 0).release();
    bufferedPageIndex->AddVecBatches(std::vector<VectorBatch *> { eofVecBatch });
    ret = scan->FindNextJoinRows();
    ASSERT_EQ(DecodeStreamedTblResult(ret), 1);
    ASSERT_EQ(DecodeBufferedTblResult(ret), 0);
    ASSERT_EQ(DecodeJoinResult(ret), 1);
    streamedPageIndex->AddVecBatches(std::vector<VectorBatch *> { streamedVecBatch2 });
    ret = scan->FindNextJoinRows();
    ASSERT_EQ(DecodeStreamedTblResult(ret), 0);
    ASSERT_EQ(DecodeBufferedTblResult(ret), 1);
    ASSERT_EQ(DecodeJoinResult(ret), 1);

    bufferedPageIndex->AddVecBatches(std::vector<VectorBatch *> { eofVecBatch });
    ret = scan->FindNextJoinRows();
    ASSERT_EQ(DecodeStreamedTblResult(ret), 1);
    ASSERT_EQ(DecodeBufferedTblResult(ret), 2);
    ASSERT_EQ(DecodeJoinResult(ret), 0);

    streamedPageIndex->AddVecBatches(std::vector<VectorBatch *> { eofVecBatch });
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

TEST(JoinScanner, TestReturnCode2)
{
    VecTypes streamedTypes(std::vector<VecType>({ LongVecType(), LongVecType() }));
    VecTypes streamedKeysTypes(std::vector<VecType>({ LongVecType() }));
    int32_t streamedCols[] = {0};
    auto streamedPageIndex = new DynamicPagesIndex(streamedTypes);
    VecTypes bufferedTypes(std::vector<VecType>({ LongVecType(), LongVecType() }));
    VecTypes bufferedKeysTypes(std::vector<VecType>({ LongVecType() }));
    int32_t bufferedCols[] = {0};
    auto bufferedPageIndex = new DynamicPagesIndex(bufferedTypes);

    const int32_t DATA_SIZE = 4;
    int64_t streamData0[] = {1, 2, 3};
    int64_t bufferData0[] =  {1, 2, 3, 4};
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
    std::vector<int64_t> streamedAddr;
    std::vector<int64_t> bufferedAddr;
    scan->GetMatchedValueAddresses(streamedAddr, bufferedAddr);

    VectorBatch *eofVecBatch = std::make_unique<VectorBatch>(2, 0).release();
    streamedPageIndex->AddVecBatches(std::vector<VectorBatch *> { eofVecBatch });
    ret = scan->FindNextJoinRows();
    ASSERT_EQ(DecodeStreamedTblResult(ret), 2);
    ASSERT_EQ(DecodeBufferedTblResult(ret), 1);
    ASSERT_EQ(DecodeJoinResult(ret), 0);
    bufferedPageIndex->AddVecBatches(std::vector<VectorBatch *> { bufferedVecBatch2 });
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

TEST(JoinScanner, Test7)
{
    VecTypes streamedTypes(std::vector<VecType>({ LongVecType() }));
    VecTypes streamedKeysTypes(std::vector<VecType>({ LongVecType() }));
    int32_t streamedCols[] = {0};
    auto streamedPageIndex = new DynamicPagesIndex(streamedTypes);
    VecTypes bufferedTypes(std::vector<VecType>({ LongVecType() }));
    VecTypes bufferedKeysTypes(std::vector<VecType>({ LongVecType() }));
    int32_t bufferedCols[] = {0};
    auto bufferedPageIndex = new DynamicPagesIndex(bufferedTypes);

    const int32_t DATA_SIZE = 6;
    // stream data0
    int64_t streamData0[] = {0, 1, 2, 3, 4, 5};
    VectorBatch *streamedVecBatch = std::make_unique<VectorBatch>(1, DATA_SIZE).release();
    streamedVecBatch->SetVector(0, CreateVector<LongVector, int64_t>(streamData0, DATA_SIZE));

    // buffer data0
    int64_t bufferData0[] =  {0, 1, 2, 3, 4, 5};
    VectorBatch *bufferedVecBatch = std::make_unique<VectorBatch>(1, DATA_SIZE).release();
    bufferedVecBatch->SetVector(0, CreateVector<LongVector, int64_t>(bufferData0, DATA_SIZE));

    // buffer data1
    int64_t bufferData1[] =  {5, 5, 5, 5, 5, 5};
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
    int64_t bufferData2[] =  {5, 6, 7, 7, 7, 7};
    VectorBatch *bufferedVecBatch2 = std::make_unique<VectorBatch>(1, DATA_SIZE).release();
    bufferedVecBatch2->SetVector(0, CreateVector<LongVector, int64_t>(bufferData2, DATA_SIZE));

    // add stream0
    streamedPageIndex->AddVecBatches(std::vector<VectorBatch *> { streamedVecBatch });

    // add buffer0
    bufferedPageIndex->AddVecBatches(std::vector<VectorBatch *> { bufferedVecBatch });

    auto scan = new SortMergeJoinScanner(streamedKeysTypes, streamedCols, 1, streamedPageIndex, bufferedKeysTypes,
        bufferedCols, bufferedPageIndex, OMNI_JOIN_TYPE_INNER, false);
    auto ret = scan->FindNextJoinRows();

    // get output
    std::vector<int64_t> streamedAddr;
    std::vector<int64_t> bufferedAddr;
    scan->GetMatchedValueAddresses(streamedAddr, bufferedAddr);

    // add buffer1
    bufferedPageIndex->AddVecBatches(std::vector<VectorBatch *> { bufferedVecBatch1 });
    ret = scan->FindNextJoinRows();
    // get output
    scan->GetMatchedValueAddresses(streamedAddr, bufferedAddr);

    // add buffer2
    bufferedPageIndex->AddVecBatches(std::vector<VectorBatch *> { bufferedVecBatch2 });
    ret = scan->FindNextJoinRows();
    // get output
    scan->GetMatchedValueAddresses(streamedAddr, bufferedAddr);

    // add stream1
    streamedPageIndex->AddVecBatches(std::vector<VectorBatch *> { streamedVecBatch1 });
    ret = scan->FindNextJoinRows();

    // get output
    scan->GetMatchedValueAddresses(streamedAddr, bufferedAddr);

    // add stream2
    streamedPageIndex->AddVecBatches(std::vector<VectorBatch *> { streamedVecBatch2 });
    ret = scan->FindNextJoinRows();
    // get output
    scan->GetMatchedValueAddresses(streamedAddr, bufferedAddr);

    // add buffer eof
    VectorBatch *eofVecBatch = std::make_unique<VectorBatch>(1, 0).release();
    bufferedPageIndex->AddVecBatches(std::vector<VectorBatch *> { eofVecBatch });
    ret = scan->FindNextJoinRows();
    scan->GetMatchedValueAddresses(streamedAddr, bufferedAddr);

    // add buffer eof
    bufferedPageIndex->AddVecBatches(std::vector<VectorBatch *> { eofVecBatch });
    ret = scan->FindNextJoinRows();
    scan->GetMatchedValueAddresses(streamedAddr, bufferedAddr);

    // add stream eof
    streamedPageIndex->AddVecBatches(std::vector<VectorBatch *> { eofVecBatch });
    ret = scan->FindNextJoinRows();
    scan->GetMatchedValueAddresses(streamedAddr, bufferedAddr);

    std::vector<int64_t> expectedStreamedAddr(
        { 0,          1,          2,          3,          4,          5,          5,          5,          5,
        5,          5,          5,          5,          4294967296, 4294967296, 4294967296, 4294967296, 4294967296,
        4294967296, 4294967296, 4294967296, 4294967297, 4294967297, 4294967297, 4294967297, 4294967297, 4294967297,
        4294967297, 4294967297, 4294967298, 4294967298, 4294967298, 4294967298, 4294967298, 4294967298, 4294967298,
        4294967298, 4294967299, 4294967299, 4294967299, 4294967299, 4294967299, 4294967299, 4294967299, 4294967299,
        4294967300, 4294967300, 4294967300, 4294967300, 4294967300, 4294967300, 4294967300, 4294967300, 4294967301,
        4294967301, 4294967301, 4294967301, 4294967301, 4294967301, 4294967301, 4294967301, 8589934592, 8589934592,
        8589934592, 8589934592, 8589934592, 8589934592, 8589934592, 8589934592, 8589934593, 8589934594, 8589934595,
        8589934595, 8589934595, 8589934595 });
    std::vector<int64_t> expectedBufferedAddr(
        { 0,          1,          2,          3,          4,          5,          4294967296, 4294967297, 4294967298,
        4294967299, 4294967300, 4294967301, 8589934592, 5,          4294967296, 4294967297, 4294967298, 4294967299,
        4294967300, 4294967301, 8589934592, 5,          4294967296, 4294967297, 4294967298, 4294967299, 4294967300,
        4294967301, 8589934592, 5,          4294967296, 4294967297, 4294967298, 4294967299, 4294967300, 4294967301,
        8589934592, 5,          4294967296, 4294967297, 4294967298, 4294967299, 4294967300, 4294967301, 8589934592,
        5,          4294967296, 4294967297, 4294967298, 4294967299, 4294967300, 4294967301, 8589934592, 5,
        4294967296, 4294967297, 4294967298, 4294967299, 4294967300, 4294967301, 8589934592, 5,          4294967296,
        4294967297, 4294967298, 4294967299, 4294967300, 4294967301, 8589934592, 8589934593, 8589934593, 8589934594,
        8589934595, 8589934596, 8589934597 });
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