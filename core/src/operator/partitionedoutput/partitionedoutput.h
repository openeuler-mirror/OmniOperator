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

void ALWAYS_INLINE InsertContainer(Vector *origintVector, int32_t originRowIndex, Vector *currentVector,
    int32_t currentRowIndex)
{
    ContainerVector *containerVec = static_cast<ContainerVector *>(origintVector);
    auto *avgValVector = reinterpret_cast<DoubleVector *>(containerVec->GetValue(0));
    auto *avgCountVector = reinterpret_cast<LongVector *>(containerVec->GetValue(1));
    int64_t longValue = static_cast<LongVector *>(avgCountVector)->GetValue(originRowIndex);
    double doubleValue = static_cast<DoubleVector *>(avgValVector)->GetValue(originRowIndex);
    ContainerVector *currentContainerVec = static_cast<ContainerVector *>(currentVector);
    auto *currentAvgValVector = reinterpret_cast<DoubleVector *>(currentContainerVec->GetValue(0));
    auto *currentAvgCountVector = reinterpret_cast<LongVector *>(currentContainerVec->GetValue(1));
    static_cast<DoubleVector *>(currentAvgValVector)->SetValue(currentRowIndex, doubleValue);
    static_cast<LongVector *>(currentAvgCountVector)->SetValue(currentRowIndex, longValue);
}

void ALWAYS_INLINE InsertVarchar(Vector *origintVector, int32_t originRowIndex, Vector *currentVector,
    int32_t currentRowIndex)
{
    uint8_t *value = nullptr;
    int32_t length = static_cast<VarcharVector *>(origintVector)->GetValue(originRowIndex, &value);
    static_cast<VarcharVector *>(currentVector)->SetValue(currentRowIndex, value, length);
}

class PartitionedOutputOperatorFactory : public OperatorFactory {
public:
    PartitionedOutputOperatorFactory(const DataTypes &sourceTypes, int32_t sourceTypeCount, bool replicatesAnyRow,
        int32_t nullChannel, int32_t *partitionChannels, int32_t partitionChannelsCount, int32_t partitionCount,
        int32_t *bucketToPartition, int32_t bucketToPartitionCount, bool isHashPrecomputed, int32_t *hashChannelTypes,
        int32_t hashChannelTypesCount, int32_t *hashChannels, int32_t hashChannelsCount);

    ~PartitionedOutputOperatorFactory() override;

    static PartitionedOutputOperatorFactory *CreatePartitionedOutputOperatorFactory(const DataTypes &sourceTypesField,
        int32_t sourceTypeCountField, bool replicatesAnyRowField, int32_t nullChannelField,
        int32_t *partitionChannelsField, int32_t partitionChannelsCountField, int32_t partitionCountField,
        int32_t *bucketToPartitionField, int32_t bucketToPartitionCountField, bool hashPrecomputed,
        int32_t *hashChannelTypesField, int32_t hashChannelTypesCountField, int32_t *hashChannelsField,
        int32_t hashChannelsCountField);

    Operator *CreateOperator() override;

private:
    std::unique_ptr<DataTypes> sourceTypes;
    int32_t sourceTypeCount;
    bool replicatesAnyRow;
    int nullChannel;
    std::vector<int32_t> partitionChannels;
    int32_t partitionChannelsCount;
    int32_t partitionCount;
    std::vector<int32_t> bucketToPartition;
    int32_t bucketToPartitionCount;
    bool hashPrecomputed = true;
    std::vector<int32_t> hashChannelTypes;
    int32_t hashChannelTypesCount;
    std::vector<int32_t> hashChannels;
    int32_t hashChannelsCount;
};

class PartitionedOutputOperator : public Operator {
public:
    PartitionedOutputOperator(const DataTypes &sourceTypes, int32_t sourceTypeCount, bool replicatesAnyRow,
        int nullChannel, std::vector<int32_t> &partitionChannels, int32_t partitionChannelsCount,
        int32_t partitionCount, std::vector<int32_t> &bucketToPartition, int32_t bucketToPartitionCount,
        bool isHashPrecomputed, std::vector<int32_t> &hashChannelTypes, int32_t hashChannelTypesCount,
        std::vector<int32_t> &hashChannels, int32_t hashChannelsCount);

    ~PartitionedOutputOperator() override;

    int32_t AddInput(VectorBatch *vecBatch) override;

    int32_t GetOutput(std::vector<omniruntime::vec::VectorBatch *> &outputVecBatch) override;

    OmniStatus Close() override;

private:
    const DataTypes &sourceTypes;
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
    std::vector<int32_t> hashChannelTypes;
    int32_t hashChannelTypesCount;
    std::vector<int32_t> hashChannels;
    int32_t hashChannelsCount;
    vector<VectorBatch *> vectorBatches = {};
    map<int, vector<int>> partitionedMap = {};
    void BuildVecBatch(int32_t vecCount, int32_t rowCount);
    int32_t GetPartition(VectorBatch *vecBatch, int32_t startVecIndex, int32_t rowIndex);
    void MergeVectorBatch(VectorBatch *vecBatch, int32_t vecCount);
};
}
}
#endif