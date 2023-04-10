/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2018. All rights reserved.
 */
#ifndef __PARTITIONEDOUTPUT_H__
#define __PARTITIONEDOUTPUT_H__

#include <vector>
#include <map>
#include "operator/operator_factory.h"
#include "type/data_type_serializer.h"
#include "vector/vector_helper.h"

namespace omniruntime {
namespace op {
using namespace std;
using namespace vec;

class PartitionedOutputOperatorFactory : public OperatorFactory {
public:
    PartitionedOutputOperatorFactory(const DataTypes &sourceTypes, int32_t sourceTypeCount, bool replicatesAnyRow,
        int32_t nullChannel, int32_t *partitionChannels, int32_t partitionChannelsCount, int32_t partitionCount,
        int32_t *bucketToPartition, int32_t bucketToPartitionCount, bool isHashPrecomputed,
        const DataTypes &hashChannelTypes, int32_t *hashChannels, int32_t hashChannelsCount);

    ~PartitionedOutputOperatorFactory() override;

    static PartitionedOutputOperatorFactory *CreatePartitionedOutputOperatorFactory(const DataTypes &sourceTypesField,
        int32_t sourceTypeCountField, bool replicatesAnyRowField, int32_t nullChannelField,
        int32_t *partitionChannelsField, int32_t partitionChannelsCountField, int32_t partitionCountField,
        int32_t *bucketToPartitionField, int32_t bucketToPartitionCountField, bool hashPrecomputed,
        const DataTypes &hashChannelTypesField, int32_t *hashChannelsField, int32_t hashChannelsCountField);

    Operator *CreateOperator() override;

private:
    DataTypes sourceTypes;
    int32_t sourceTypeCount;
    bool replicatesAnyRow;
    int nullChannel;
    std::vector<int32_t> partitionChannels;
    int32_t partitionChannelsCount;
    int32_t partitionCount;
    std::vector<int32_t> bucketToPartition;
    int32_t bucketToPartitionCount;
    bool hashPrecomputed = true;
    DataTypes hashChannelTypes;
    std::vector<int32_t> hashChannels;
    int32_t hashChannelsCount;
};

class PartitionedOutputOperator : public Operator {
public:
    PartitionedOutputOperator(const DataTypes &sourceTypes, int32_t sourceTypeCount, bool replicatesAnyRow,
        int nullChannel, std::vector<int32_t> &partitionChannels, int32_t partitionChannelsCount,
        int32_t partitionCount, std::vector<int32_t> &bucketToPartition, int32_t bucketToPartitionCount,
        bool isHashPrecomputed, const DataTypes &hashChannelTypes, std::vector<int32_t> &hashChannels,
        int32_t hashChannelsCount);

    ~PartitionedOutputOperator() override;

    int32_t AddInput(VectorBatch *vecBatch) override;

    int32_t GetOutput(omniruntime::vec::VectorBatch **outputVecBatch) override;

    OmniStatus Close() override;

private:
    DataTypes sourceTypes;
    int32_t sourceTypeCount;
    bool replicatesAnyRow;
    int nullChannel;
    std::vector<int32_t> partitionChannels;
    int32_t partitionChannelsCount;
    int32_t partitionCount;
    std::vector<int32_t> bucketToPartition;
    int32_t bucketToPartitionCount;
    bool hasAnyRowBeenReplicated = false;
    bool hashPrecomputed = true;
    DataTypes hashChannelTypes;
    std::vector<int32_t> hashChannels;
    int32_t hashChannelsCount;
    vector<VectorBatch *> vectorBatches = {};
    map<int, vector<int>> partitionedMap = {};

    void BuildVecBatch(int32_t vecCount, int32_t rowCount);

    int32_t GetPartition(VectorBatch *vecBatch, int32_t startVecIndex, int32_t rowIndex);

    void MergeVectorBatch(VectorBatch *vecBatch, int32_t vecCount);

    long GetHash(int32_t rowIndex, Vector *vector);

    long GetContainerHash(int32_t rowIndex, ContainerVector *vector);

    void Insert(Vector *originVector, int32_t originRowIndex, Vector *currentVector, int32_t currentRowIndex);

    void ALWAYS_INLINE InsertVarchar(Vector *originVector, int32_t originRowIndex, Vector *currentVector,
        int32_t currentRowIndex);

    void ALWAYS_INLINE InsertContainer(Vector *originVector, int32_t originRowIndex, Vector *currentVector,
        int32_t currentRowIndex);

    // for iterative getOutput
    int32_t vecBatchCount = 0;
    int32_t vecBatchIndex = 0;
};
}
}
#endif