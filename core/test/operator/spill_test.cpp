/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * @Description: spill test implementations
 */
#include <vector>
#include <ctime>
#include "gtest/gtest.h"
#include "operator/spill/vector_batch_spiller.h"
#include "operator/spill/spill_tracker.h"
#include "operator/spill/spill_iterator.h"
#include "vector/vector_helper.h"
#include "type/data_types.h"
#include "util/test_util.h"

using namespace omniruntime::op;
using namespace omniruntime::vec;
using namespace omniruntime::type;

namespace SpillTest {
TEST(SpillTest, TestWriteRead)
{
    const int32_t dataSize = 5;
    int32_t data1[dataSize] = {3, 5, 7, 9, 1};
    int64_t data2[dataSize] = {8, 4, 0, 2, 6};
    int16_t data3[dataSize] = {5, 4, 3, 2, 1};

    std::vector<DataTypePtr> types = { IntType(), LongType(), ShortType() };
    DataTypes sourceTypes(types);
    VectorBatch *vecBatch = TestUtil::CreateVectorBatch(sourceTypes, dataSize, data1, data2, data3);

    std::string path = TestUtil::GenerateSpillPath();
    mkdir(path.c_str(), S_IRUSR | S_IWUSR | S_IXUSR | S_IRWXG | S_IRWXO);
    ChildSpillTracker spillTracker(&(GetRootSpillTracker()));
    VectorBatchWriter writer(&spillTracker, sourceTypes);
    writer.CreateTempFile(path);
    VectorBatchUnitIter vectorBatchIterator(vecBatch);
    writer.WriteVecBatches(vectorBatchIterator);
    VectorBatchReader reader(writer.GetFd(), writer.GetFileLength(), &spillTracker);
    reader.ReadFileTailAndHead();
    while (reader.HasNext()) {
        auto result = reader.Next();
        auto resultVecBatch = result->GetVectorBatch();
        VectorHelper::PrintVecBatch(resultVecBatch, types);
        TestUtil::AssertVecBatchEquals(resultVecBatch, sourceTypes.GetSize(), types, dataSize, data1, data2, data3);
        VectorHelper::FreeVecBatch(resultVecBatch);
    }
    rmdir(path.c_str());
    VectorHelper::FreeVecBatch(vecBatch);
}

TEST(SpillTest, TestSpiller)
{
    const int32_t dataSize = 5;
    int32_t data1[dataSize] = {-7, -3, 1, 5, 9};
    int64_t data2[dataSize] = {6, 8, 4, 0, 2};
    int16_t data3[dataSize] = {-5, -3, 0, 2, 4};

    std::vector<DataTypePtr> types = { IntType(), LongType(), ShortType() };
    DataTypes sourceTypes(types);
    VectorBatch *vecBatch = TestUtil::CreateVectorBatch(sourceTypes, dataSize, data1, data2, data3);

    std::vector<int32_t> sortCols = { 0, 1, 2 };
    std::vector<int32_t> sortAscendings = { 1, 1, 1 };
    std::vector<int32_t> sortNullFirsts = { 0, 0, 0 };
    VecBatchWithPositionComparator comparator(sourceTypes, sortCols, sortAscendings, sortNullFirsts);

    auto spillTracker = dynamic_cast<ChildSpillTracker *>(GetRootSpillTracker().CreateSpillTracker());
    std::string path = TestUtil::GenerateSpillPath();
    mkdir(path.c_str(), S_IRUSR | S_IWUSR | S_IXUSR | S_IRWXG | S_IRWXO);
    VectorBatchSpiller spiller(path, sourceTypes, &comparator);
    spiller.SetSpillTracker(spillTracker);
    VectorBatchUnitIter diskVectorBatch(vecBatch);
    spiller.Spill(diskVectorBatch);
    ASSERT_TRUE(spillTracker->GetSpilledBytes() != 0);

    int32_t data4[dataSize] = {-9, -5, -1, 3, 7};
    int64_t data5[dataSize] = {-2, 0, -4, -8, -6};
    int16_t data6[dataSize] = {-6, -4, -2, 1, 3};

    VectorBatchUnitIter memoryIter(TestUtil::CreateVectorBatch(sourceTypes, dataSize, data4, data5, data6));
    spiller.MergeFromDiskAndMemory(memoryIter);
    while (spiller.HasNext()) {
        int32_t expectedDataSize = 2 * dataSize;
        int32_t expectedData1[] = {-9, -7, -5, -3, -1, 1, 3, 5, 7, 9};
        int64_t expectedData2[] = {-2, 6, 0, 8, -4, 4, -8, 0, -6, 2};
        int16_t expectedData3[] = {-6, -5, -4, -3, -2, 0, 1, 2, 3, 4};
        auto result = spiller.Next();
        auto resultVecBatch = result->GetVectorBatch();
        delete result;
        TestUtil::AssertVecBatchEquals(resultVecBatch, sourceTypes.GetSize(), types, expectedDataSize, expectedData1,
            expectedData2, expectedData3);
        VectorHelper::PrintVecBatch(resultVecBatch, types);
        VectorHelper::FreeVecBatch(resultVecBatch);
    }
    rmdir(path.c_str());
    VectorHelper::FreeVecBatch(vecBatch);
}

TEST(SpillTest, TestSpillNoneSinceExceededLimit)
{
    const int32_t dataSize = 5;
    int32_t data1[dataSize] = {-7, -3, 1, 5, 9};
    int64_t data2[dataSize] = {6, 8, 4, 0, 2};
    int16_t data3[dataSize] = {-5, -3, 0, 2, 4};

    DataTypes sourceTypes(std::vector<DataTypePtr>({ IntType(), LongType(), ShortType() }));
    VectorBatch *vecBatch = TestUtil::CreateVectorBatch(sourceTypes, dataSize, data1, data2, data3);

    std::vector<int32_t> sortCols = { 0, 1, 2 };
    std::vector<int32_t> sortAscendings = { 1, 1, 1 };
    std::vector<int32_t> sortNullFirsts = { 0, 0, 0 };
    VecBatchWithPositionComparator comparator(sourceTypes, sortCols, sortAscendings, sortNullFirsts);

    std::string path = TestUtil::GenerateSpillPath();
    mkdir(path.c_str(), S_IRUSR | S_IWUSR | S_IXUSR | S_IRWXG | S_IRWXO);
    InitRootSpillTracker(path, 50);
    auto *spillTracker = dynamic_cast<ChildSpillTracker *>(GetRootSpillTracker().CreateSpillTracker());
    VectorBatchSpiller spiller(path, sourceTypes, &comparator);
    spiller.SetSpillTracker(spillTracker);
    VectorBatchUnitIter diskVectorBatch(vecBatch);
    auto status = spiller.Spill(diskVectorBatch);
    VectorHelper::FreeVecBatch(vecBatch);
    ASSERT_EQ(status, ErrorCode::EXCEED_SPILL_THRESHOLD);
    ASSERT_EQ(spillTracker->GetSpilledBytes(), 0);
    rmdir(path.c_str());
}
}