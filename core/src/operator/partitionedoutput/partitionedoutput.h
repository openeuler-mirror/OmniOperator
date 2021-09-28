/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2018. All rights reserved.
 */
#ifndef __PARTITIONEDOUTPUT_H__
#define __PARTITIONEDOUTPUT_H__

#include "../operator_factory.h"
#include <vector>
#include <map>
#include "../../vector/vector_type_serializer.h"

namespace omniruntime {
namespace op {
using namespace std;
using namespace vec;
class PartitionedOutputOperatorFactory : public OperatorFactory {
public:
    PartitionedOutputOperatorFactory(const VecTypes &sourceTypes, int32_t sourceTypeCount, bool replicatesAnyRow,
        int nullChannel, int32_t *partitionChannels, int32_t partitionChannelsCount, int32_t partitionCount,
        int32_t *bucketToPartition, int32_t bucketToPartitionCount, bool isHashPrecomputed, int32_t *hashChannelTypes,
        int32_t hashChannelTypesCount, int32_t *hashChannels, int32_t hashChannelsCount);

    ~PartitionedOutputOperatorFactory() override;

    static PartitionedOutputOperatorFactory *CreatePartitionedOutputOperatorFactory(const VecTypes &sourceTypes,
        int32_t sourceTypeCount, bool replicatesAnyRow, int nullChannel, int32_t *partitionChannels,
        int32_t partitionChannelsCount, int32_t partitionCount, int32_t *bucketToPartition,
        int32_t bucketToPartitionCount, bool isHashPrecomputed, int32_t *hashChannelTypes,
        int32_t hashChannelTypesCount, int32_t *hashChannels, int32_t hashChannelsCount);

    Operator *CreateOperator() override;

    int32_t GetSourceTypeCount()
    {
        return sourceTypeCount;
    }

    bool IsAeplicatesAnyRow() const
    {
        return replicatesAnyRow;
    }

    int32_t GetNullChannel()
    {
        return nullChannel;
    }

    const int32_t *GetPartitionChannels()
    {
        return partitionChannels;
    }

    int32_t GetPartitionChannelsCount()
    {
        return partitionChannelsCount;
    }

    int32_t GetPartitionCount()
    {
        return partitionCount;
    }

    const int32_t *GetBucketToPartition()
    {
        return bucketToPartition;
    }

    int32_t GetBucketToPartitionCount()
    {
        return bucketToPartitionCount;
    }

    bool IsHashPrecomputed()
    {
        return hashPrecomputed;
    }

    int32_t *GetHashChannelTypes()
    {
        return hashChannelTypes;
    }

    int32_t GetHashChannelTypesCount()
    {
        return hashChannelTypesCount;
    }

    int32_t *GetHashChannels()
    {
        return hashChannels;
    }

    int32_t GetHashChannelsCount()
    {
        return hashChannelsCount;
    }

private:
    std::unique_ptr<VecTypes> sourceTypes;
    int32_t sourceTypeCount;
    bool replicatesAnyRow;
    int nullChannel;
    int32_t *partitionChannels;
    int32_t partitionChannelsCount;
    int32_t partitionCount;
    int32_t *bucketToPartition;
    int32_t bucketToPartitionCount;
    bool hashPrecomputed = true;
    int32_t *hashChannelTypes;
    int32_t hashChannelTypesCount;
    int32_t *hashChannels;
    int32_t hashChannelsCount;
};

class PartitionedOutputOperator : public Operator {
public:
    PartitionedOutputOperator(const VecTypes &sourceTypes, int32_t sourceTypeCount, bool replicatesAnyRow,
        int nullChannel, int32_t *partitionChannels, int32_t partitionChannelsCount, int32_t partitionCount,
        int32_t *bucketToPartition, int32_t bucketToPartitionCount, bool isHashPrecomputed, int32_t *hashChannelTypes,
        int32_t hashChannelTypesCount, int32_t *hashChannels, int32_t hashChannelsCount);

    ~PartitionedOutputOperator() override;

    int32_t AddInput(VectorBatch *vecBatch) override;

    int32_t GetOutput(std::vector<omniruntime::vec::VectorBatch *> &outputVecBatch) override;

    int32_t GetSourceTypeCount()
    {
        return sourceTypeCount;
    }

    bool IsAeplicatesAnyRow() const
    {
        return replicatesAnyRow;
    }

    int32_t GetNullChannel()
    {
        return nullChannel;
    }

    const int32_t *GetPartitionChannels()
    {
        return partitionChannels;
    }

    int32_t GetPartitionChannelsCount()
    {
        return partitionChannelsCount;
    }

    int32_t GetPartitionCount()
    {
        return partitionCount;
    }

    const int32_t *GetBucketToPartition()
    {
        return bucketToPartition;
    }

    int32_t GetBucketToPartitionCount()
    {
        return bucketToPartitionCount;
    }

    bool IsHashPrecomputed()
    {
        return hashPrecomputed;
    }

    vector<VectorBatch *> &getVectorBatches()
    {
        return vectorBatches;
    }

    int32_t *GetHashChannelTypes()
    {
        return hashChannelTypes;
    }

    int32_t GetHashChannelTypesCount()
    {
        return hashChannelTypesCount;
    }

    int32_t *GetHashChannels()
    {
        return hashChannels;
    }

    int32_t GetHashChannelsCount()
    {
        return hashChannelsCount;
    }

private:
    const VecTypes &sourceTypes;
    int32_t sourceTypeCount;
    bool replicatesAnyRow;
    int nullChannel;
    int32_t *partitionChannels;
    int32_t partitionChannelsCount;
    int32_t partitionCount;
    int32_t *bucketToPartition;
    int32_t bucketToPartitionCount;
    bool hasAnyRowBeenReplicated = false;
    bool hashPrecomputed = true;
    int32_t *hashChannelTypes;
    int32_t hashChannelTypesCount;
    int32_t *hashChannels;
    int32_t hashChannelsCount;
    vector<VectorBatch *> vectorBatches = {};
    map<int, vector<int>> partitionedMap = {};
    void BuildVecBatch(vector <VecType> &vecTypes, int32_t vecCount, int32_t rowCount);
    int32_t GetPartition(VectorBatch *vecBatch, int32_t startVecIndex, int32_t rowIndex);
    void MergeVectorBatch(VectorBatch *vecBatch, int32_t vecCount);
};
}
}
#endif