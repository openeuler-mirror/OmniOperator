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
#include "../util/test_util.h"

using namespace omniruntime::op;
using namespace omniruntime::vec;
using namespace omniruntime::type;

namespace SpillTest {
TEST(SpillTest, TestWriteRead)
{
    const int32_t dataSize = 5;
    int32_t data1[dataSize] = {3, 5, 7, 9, 1};
    int64_t data2[dataSize] = {8, 4, 0, 2, 6};

    DataTypes sourceTypes(std::vector<DataType>({ IntDataType(), LongDataType() }));
    VectorBatch *vecBatch = TestUtil::CreateVectorBatch(sourceTypes, dataSize, data1, data2);

    std::string path = TestUtil::GenerateSpillPath();
    mkdir(path.c_str(), S_IRUSR | S_IWUSR | S_IXUSR | S_IRWXG | S_IRWXO);
    ChildSpillTracker spillTracker(&(GetRootSpillTracker()));
    VectorBatchWriter writer(&spillTracker);
    writer.CreateTempFile(path);
    VectorBatchUnitIter vectorBatchIterator(vecBatch);
    writer.WriteVecBatches(vectorBatchIterator);
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("spill_TestWriteRead");
    VectorBatchReader reader(writer.GetFd(), writer.GetFileLength(), &spillTracker, vecAllocator);
    reader.ReadFileTailAndHead();
    while (reader.HasNext()) {
        auto result = reader.Next();
        auto resultVecBatch = result->GetVectorBatch();
        VectorHelper::PrintVecBatch(resultVecBatch);
        TestUtil::AssertVecBatchEquals(resultVecBatch, sourceTypes.GetSize(), dataSize, data1, data2);
        VectorHelper::FreeVecBatch(resultVecBatch);
    }
    rmdir(path.c_str());
    VectorHelper::FreeVecBatch(vecBatch);
    delete vecAllocator;
}

TEST(SpillTest, TestSpiller)
{
    const int32_t dataSize = 5;
    int32_t data1[dataSize] = {-7, -3, 1, 5, 9};
    int64_t data2[dataSize] = {6, 8, 4, 0, 2};

    DataTypes sourceTypes(std::vector<DataType>({ IntDataType(), LongDataType() }));
    VectorBatch *vecBatch = TestUtil::CreateVectorBatch(sourceTypes, dataSize, data1, data2);

    std::vector<int32_t> sortCols = { 0, 1 };
    std::vector<int32_t> sortAscendings = { 1, 1 };
    std::vector<int32_t> sortNullFirsts = { 0, 0 };
    VecBatchWithPositionComparator comparator(sourceTypes, sortCols, sortAscendings, sortNullFirsts);

    auto spillTracker = dynamic_cast<ChildSpillTracker *>(GetRootSpillTracker().CreateSpillTracker());
    std::string path = TestUtil::GenerateSpillPath();
    mkdir(path.c_str(), S_IRUSR | S_IWUSR | S_IXUSR | S_IRWXG | S_IRWXO);
    VectorBatchSpiller spiller(path, sourceTypes, &comparator);
    spiller.SetSpillTracker(spillTracker);
    VectorBatchUnitIter diskVectorBatch(vecBatch);
    spiller.Spill(diskVectorBatch);
    ASSERT_TRUE(spillTracker->GetSpilledBytes() != 0);

    int32_t data3[dataSize] = {-9, -5, -1, 3, 7};
    int64_t data4[dataSize] = {-2, 0, -4, -8, -6};

    VectorBatchUnitIter memoryIter(TestUtil::CreateVectorBatch(sourceTypes, dataSize, data3, data4));
    spiller.MergeFromDiskAndMemory(memoryIter);
    if (spiller.HasNext()) {
        int32_t expectedDataSize = 2 * dataSize;
        int32_t expectedData1[] = {-9, -7, -5, -3, -1, 1, 3, 5, 7, 9};
        int64_t expectedData2[] = {-2, 6, 0, 8, -4, 4, -8, 0, -6, 2};
        auto result = spiller.Next();
        auto resultVecBatch = result->GetVectorBatch();
        delete result;
        TestUtil::AssertVecBatchEquals(resultVecBatch, sourceTypes.GetSize(), expectedDataSize, expectedData1,
            expectedData2);
        VectorHelper::PrintVecBatch(resultVecBatch);
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

    DataTypes sourceTypes(std::vector<DataType>({ IntDataType(), LongDataType() }));
    VectorBatch *vecBatch = TestUtil::CreateVectorBatch(sourceTypes, dataSize, data1, data2);

    std::vector<int32_t> sortCols = { 0, 1 };
    std::vector<int32_t> sortAscendings = { 1, 1 };
    std::vector<int32_t> sortNullFirsts = { 0, 0 };
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