/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2018. All rights reserved.
 */
#include "partitionedoutput.h"
#include <map>
#include "../../jit/annotation.h"
#include "../../vector/int_vector.h"
#include "../../vector/long_vector.h"
#include "../../vector/varchar_vector.h"
#include "../../vector/double_vector.h"
#include "../../vector/vector_helper.h"
#include "../optimization.h"
#include "../util/operator_util.h"

using namespace std;
using namespace omniruntime::vec;
namespace omniruntime {
namespace op {
PartitionedOutputOperatorFactory::PartitionedOutputOperatorFactory(
    int32_t *sourceTypes,
    int32_t sourceTypeCount,
    bool replicatesAnyRow,
    int32_t nullChannel,
    int32_t *partitionChannels,
    int32_t partitionChannelsCount,
    int32_t partitionCount,
    int32_t *bucketToPartition,
    int32_t bucketToPartitionCount)
    : sourceTypes(sourceTypes), sourceTypeCount(sourceTypeCount), partitionChannels(partitionChannels),
      partitionChannelsCount(partitionChannelsCount), bucketToPartition(bucketToPartition),
      replicatesAnyRow(replicatesAnyRow), partitionCount(partitionCount), nullChannel(nullChannel),
      bucketToPartitionCount(bucketToPartitionCount)
    {}

PartitionedOutputOperatorFactory::~PartitionedOutputOperatorFactory()
{
}

PartitionedOutputOperatorFactory *PartitionedOutputOperatorFactory::CreatePartitionedOutputOperatorFactory(
    int32_t *sourceTypes,
    int32_t sourceTypeCount,
    bool replicatesAnyRow,
    int32_t nullChannel,
    int32_t *partitionChannels,
    int32_t partitionChannelsCount,
    int32_t partitionCount,
    int32_t *bucketToPartition,
    int32_t bucketToPartitionCount)
    {
    PartitionedOutputOperatorFactory *operatorFactory = std::make_unique<PartitionedOutputOperatorFactory>(
            sourceTypes,
            sourceTypeCount,
            replicatesAnyRow,
            nullChannel,
            partitionChannels,
            partitionChannelsCount,
            partitionCount,
            bucketToPartition,
            bucketToPartitionCount).release();
    return operatorFactory;
}

Operator *PartitionedOutputOperatorFactory::CreateOperator()
{
    PartitionedOutputOperator *partitionedOutputOperator = std::make_unique<PartitionedOutputOperator>(
        sourceTypes,
        sourceTypeCount,
        replicatesAnyRow,
        nullChannel,
        partitionChannels,
        partitionChannelsCount,
        partitionCount,
        bucketToPartition,
        bucketToPartitionCount).release();
    return partitionedOutputOperator;
}

PartitionedOutputOperator::PartitionedOutputOperator(
    int32_t *sourceTypes,
    int32_t sourceTypeCount,
    bool replicatesAnyRow,
    int nullChannel,
    int32_t *partitionChannels,
    int32_t partitionChannelsCount,
    int32_t partitionCount,
    int32_t *bucketToPartition,
    int32_t bucketToPartitionCount)
    : sourceTypes(sourceTypes), sourceTypeCount(sourceTypeCount), partitionChannels(partitionChannels),
      partitionChannelsCount(partitionChannelsCount),
      bucketToPartition(bucketToPartition), replicatesAnyRow(replicatesAnyRow),
      partitionCount(partitionCount), nullChannel(nullChannel),
      bucketToPartitionCount(bucketToPartitionCount)
    {}

PartitionedOutputOperator::~PartitionedOutputOperator()
{
}

void PartitionedOutputOperator::MergeVectorBatch(VectorBatch *vecBatch, int32_t vecCount)
{
    for (int i = 0; i < partitionedMap.size(); ++i) {
        vector<int> &rowList = partitionedMap[i];
        int32_t currentVecBatchRowCount = rowList.size();
        BuildVecBatch(vecCount, currentVecBatchRowCount);
        VectorBatch *vectorBatch = vectorBatches[i];
        for (int j = 0; j < currentVecBatchRowCount; ++j) {
            int32_t rowIndex = rowList[j];
            for (int vecIdx = 0; vecIdx < vecCount; ++vecIdx) {
                void *value = nullptr;
                VectorHelper::GetValue(vecBatch->GetVector(vecIdx), rowIndex, &value);
                VectorHelper::SetValue(vectorBatch->GetVector(vecIdx), j, &value);
            }
        }
    }
}

int32_t PartitionedOutputOperator::AddInput(VectorBatch *vecBatch)
{
    int32_t rowCount = vecBatch->GetRowCount();
    int32_t vecCount = vecBatch->GetVectorCount() - partitionChannelsCount;

    for (int rowIdx = 0; rowIdx < rowCount; ++rowIdx) {
        bool shouldReplicate = (replicatesAnyRow && !hasAnyRowBeenReplicated) ||
            nullChannel >= -1 && vecBatch->GetVector(vecCount + nullChannel)->IsValueNull(rowIdx);
        if (shouldReplicate) {
            for (int partitionedIdx = 0; partitionedIdx < partitionCount; ++partitionedIdx) {
                partitionedMap[partitionedIdx].push_back(rowIdx);
            }
        } else {
            int32_t partition = GetPartition(vecBatch, vecCount, rowIdx);
            partitionedMap[partition].push_back(rowIdx);
        }
    }
    if (partitionedMap.size() > 0) {
        MergeVectorBatch(vecBatch, vecCount);
    }
    return OMNI_STATUS_FINISHED;
}

int32_t PartitionedOutputOperator::GetPartition(VectorBatch *vecBatch, int32_t vecCount, int32_t rowIdx)
{
    long rowHash;
    VectorHelper::GetValue(vecBatch->GetVector(vecCount), rowIdx, &rowHash);

    rowHash = rowHash & 0x7fffffffffffffffL;
    if (bucketToPartitionCount == 0) {
        bucketToPartitionCount = 1;
    }
    int32_t partition = rowHash % bucketToPartitionCount;
    return partition;
}

void PartitionedOutputOperator::BuildVecBatch(int32_t vecCount, int32_t rowCount)
{
    VectorBatch *vectorBatch = std::make_unique<VectorBatch>(vecCount, rowCount).release();
    vectorBatch->SetVectors(sourceTypes);
    vectorBatches.push_back(vectorBatch);
}

int32_t PartitionedOutputOperator::GetOutput(std::vector<omniruntime::vec::VectorBatch *> &outputPages)
{
    outputPages = vectorBatches;
    vectorBatches.clear();
    partitionedMap.clear();
    SetStatus(OMNI_STATUS_FINISHED);
    return OMNI_STATUS_FINISHED;
}
} // end of namespace op
} // end of namespace omniruntime