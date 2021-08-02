/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2018. All rights reserved.
 */
#ifndef __PARTITIONEDOUTPUT_H__
#define __PARTITIONEDOUTPUT_H__

#include "../operator_factory.h"
#include <vector>
#include <map>

namespace omniruntime {
namespace op {
using namespace std;
using namespace vec;
class PartitionedOutputOperatorFactory : public OperatorFactory {
public:
    PartitionedOutputOperatorFactory(
        int32_t *sourceTypes,
        int32_t sourceTypeCount,
        bool replicatesAnyRow,
        int nullChannel,
        int32_t *partitionChannels,
        int32_t partitionChannelsCount,
        int32_t partitionCount,
        int32_t *bucketToPartition,
        int32_t bucketToPartitionCount);

    ~PartitionedOutputOperatorFactory() override;

    static PartitionedOutputOperatorFactory *CreatePartitionedOutputOperatorFactory(
        int32_t *sourceTypes,
        int32_t sourceTypeCount,
        bool replicatesAnyRow,
        int nullChannel,
        int32_t *partitionChannels,
        int32_t partitionChannelsCount,
        int32_t partitionCount,
        int32_t *bucketToPartition,
        int32_t bucketToPartitionCount
    );

    Operator *CreateOperator() override;

    int32_t getSourceTypeCount()
    {
        return sourceTypeCount;
    }

    bool IsAeplicatesAnyRow() const
    {
        return replicatesAnyRow;
    }

    int32_t getNullChannel()
    {
        return nullChannel;
    }

    const int32_t *getPartitionChannels()
    {
        return partitionChannels;
    }

    int32_t getPartitionChannelsCount()
    {
        return partitionChannelsCount;
    }

    int32_t getPartitionCount()
    {
        return partitionCount;
    }

    const int32_t *getBucketToPartition()
    {
        return bucketToPartition;
    }

    int32_t getBucketToPartitionCount()
    {
        return bucketToPartitionCount;
    }

private:
    int32_t *sourceTypes;
    int32_t sourceTypeCount;
    bool replicatesAnyRow;
    int nullChannel;
    int32_t *partitionChannels;
    int32_t partitionChannelsCount;
    int32_t partitionCount;
    int32_t *bucketToPartition;
    int32_t bucketToPartitionCount;

};

class PartitionedOutputOperator : public Operator {
public:
    PartitionedOutputOperator(
        int32_t *sourceTypes,
        int32_t sourceTypeCount,
        bool replicatesAnyRow,
        int nullChannel,
        int32_t *partitionChannels,
        int32_t partitionChannelsCount,
        int32_t partitionCount,
        int32_t *bucketToPartition,
        int32_t bucketToPartitionCount);

    ~PartitionedOutputOperator() override;

    int32_t AddInput(VectorBatch *vecBatch) override;

    int32_t GetOutput(std::vector<omniruntime::vec::VectorBatch *> &outputVecBatch) override;

    int32_t getSourceTypeCount()
    {
        return sourceTypeCount;
    }

    bool IsAeplicatesAnyRow() const
    {
        return replicatesAnyRow;
    }

    int32_t getNullChannel()
    {
        return nullChannel;
    }

    const int32_t *getPartitionChannels()
    {
        return partitionChannels;
    }

    int32_t getPartitionChannelsCount()
    {
        return partitionChannelsCount;
    }

    int32_t getPartitionCount()
    {
        return partitionCount;
    }

    const int32_t *getBucketToPartition()
    {
        return bucketToPartition;
    }

    int32_t getBucketToPartitionCount()
    {
        return bucketToPartitionCount;
    }

    vector<VectorBatch *> &getVectorBatches()
    {
        return vectorBatches;
    }

private:
    int32_t *sourceTypes;
    int32_t sourceTypeCount;
    bool replicatesAnyRow;
    int nullChannel;
    int32_t *partitionChannels;
    int32_t partitionChannelsCount;
    int32_t partitionCount;
    int32_t *bucketToPartition;
    int32_t bucketToPartitionCount;
    bool hasAnyRowBeenReplicated = false;
    vector<VectorBatch *> vectorBatches = {};
    map<int, vector<int>> partitionedMap = {};
    void BuildVecBatch(int32_t vecCount, int32_t rowCount);
    int32_t GetPartition(VectorBatch *vecBatch, int32_t vecCount, int32_t rowIdx);
    void MergeVectorBatch(VectorBatch *vecBatch, int32_t vecCount);
};
}
}
#endif