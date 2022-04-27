/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2018. All rights reserved.
 */

#include "partitionedoutput.h"
#include <map>
#include "operator/util/operator_util.h"
#include "operator/hash_util.h"

using namespace std;
using namespace omniruntime::vec;
namespace omniruntime {
namespace op {
PartitionedOutputOperatorFactory::PartitionedOutputOperatorFactory(const DataTypes &sourceTypes,
    int32_t sourceTypeCount, bool replicatesAnyRow, int32_t nullChannel, int32_t *partitionChannels,
    int32_t partitionChannelsCount, int32_t partitionCount, int32_t *bucketToPartition, int32_t bucketToPartitionCount,
    bool isHashPrecomputed, int32_t *hashChannelTypes, int32_t hashChannelTypesCount, int32_t *hashChannels,
    int32_t hashChannelsCount)
    : sourceTypeCount(sourceTypeCount),
      replicatesAnyRow(replicatesAnyRow),
      nullChannel(nullChannel),
      partitionChannelsCount(partitionChannelsCount),
      partitionCount(partitionCount),
      bucketToPartitionCount(bucketToPartitionCount),
      hashPrecomputed(isHashPrecomputed),
      hashChannelTypesCount(hashChannelTypesCount),
      hashChannelsCount(hashChannelsCount)
{
    if (partitionChannelsCount <= 0 || bucketToPartitionCount <= 0 || hashChannelTypesCount <= 0 ||
        hashChannelsCount <= 0) {
        throw std::exception();
    }

    this->sourceTypes = std::make_unique<DataTypes>(sourceTypes);

    this->partitionChannels = new int[partitionChannelsCount];
    for (int i = 0; i < partitionChannelsCount; ++i) {
        this->partitionChannels[i] = partitionChannels[i];
    }

    this->bucketToPartition = new int[bucketToPartitionCount];
    for (int i = 0; i < bucketToPartitionCount; ++i) {
        this->bucketToPartition[i] = bucketToPartition[i];
    }

    this->hashChannelTypes = new int[hashChannelTypesCount];
    for (int i = 0; i < hashChannelTypesCount; ++i) {
        this->hashChannelTypes[i] = hashChannelTypes[i];
    }

    this->hashChannels = new int[hashChannelsCount];
    for (int i = 0; i < hashChannelsCount; ++i) {
        this->hashChannels[i] = hashChannels[i];
    }
}

PartitionedOutputOperatorFactory::~PartitionedOutputOperatorFactory()
{
    delete[] partitionChannels;
    delete[] bucketToPartition;
    delete[] hashChannelTypes;
    delete[] hashChannels;
}

PartitionedOutputOperatorFactory *PartitionedOutputOperatorFactory::CreatePartitionedOutputOperatorFactory(
    const DataTypes &sourceTypesField, int32_t sourceTypeCountField, bool replicatesAnyRowField,
    int32_t nullChannelField, int32_t *partitionChannelsField, int32_t partitionChannelsCountField,
    int32_t partitionCountField, int32_t *bucketToPartitionField, int32_t bucketToPartitionCountField,
    bool hashPrecomputed, int32_t *hashChannelTypesField, int32_t hashChannelTypesCountField,
    int32_t *hashChannelsField, int32_t hashChannelsCountField)
{
    PartitionedOutputOperatorFactory *operatorFactory = new PartitionedOutputOperatorFactory(sourceTypesField,
        sourceTypeCountField, replicatesAnyRowField, nullChannelField, partitionChannelsField,
        partitionChannelsCountField, partitionCountField, bucketToPartitionField, bucketToPartitionCountField,
        hashPrecomputed, hashChannelTypesField, hashChannelTypesCountField, hashChannelsField, hashChannelsCountField);
    return operatorFactory;
}

Operator *PartitionedOutputOperatorFactory::CreateOperator()
{
    auto partitionedOutputOperator =
        new PartitionedOutputOperator(*(this->sourceTypes.get()), sourceTypeCount, replicatesAnyRow, nullChannel,
        partitionChannels, partitionChannelsCount, partitionCount, bucketToPartition, bucketToPartitionCount,
        hashPrecomputed, this->hashChannelTypes, hashChannelTypesCount, hashChannels, hashChannelsCount);
    return partitionedOutputOperator;
}

PartitionedOutputOperator::PartitionedOutputOperator(const DataTypes &sourceTypes, int32_t sourceTypeCount,
    bool replicatesAnyRow, int nullChannel, int32_t *partitionChannels, int32_t partitionChannelsCount,
    int32_t partitionCount, int32_t *bucketToPartition, int32_t bucketToPartitionCount, bool isHashPrecomputed,
    int32_t *hashChannelTypes, int32_t hashChannelTypesCount, int32_t *hashChannels, int32_t hashChannelsCount)
    : sourceTypes(sourceTypes),
      sourceTypeCount(sourceTypeCount),
      replicatesAnyRow(replicatesAnyRow),
      nullChannel(nullChannel),
      partitionChannels(partitionChannels),
      partitionChannelsCount(partitionChannelsCount),
      partitionCount(partitionCount),
      bucketToPartition(bucketToPartition),
      bucketToPartitionCount(bucketToPartitionCount),
      hashPrecomputed(isHashPrecomputed),
      hashChannelTypes(hashChannelTypes),
      hashChannelTypesCount(hashChannelTypesCount),
      hashChannels(hashChannels),
      hashChannelsCount(hashChannelsCount)
{}

PartitionedOutputOperator::~PartitionedOutputOperator() {}

static void Insert(Vector *origintVector, int32_t originRowIndex, Vector *currentVector, int32_t currentRowIndex)
{
    switch (origintVector->GetTypeId()) {
        case OMNI_INT:
        case OMNI_DATE32: {
            int32_t value = static_cast<IntVector *>(origintVector)->GetValue(originRowIndex);
            static_cast<IntVector *>(currentVector)->SetValue(currentRowIndex, value);
            break;
        }
        case OMNI_LONG:
        case OMNI_DECIMAL64: {
            int64_t value = static_cast<LongVector *>(origintVector)->GetValue(originRowIndex);
            static_cast<LongVector *>(currentVector)->SetValue(currentRowIndex, value);
            break;
        }
        case OMNI_DOUBLE: {
            double value = static_cast<DoubleVector *>(origintVector)->GetValue(originRowIndex);
            static_cast<DoubleVector *>(currentVector)->SetValue(currentRowIndex, value);
            break;
        }
        case OMNI_BOOLEAN: {
            bool value = static_cast<BooleanVector *>(origintVector)->GetValue(originRowIndex);
            static_cast<BooleanVector *>(currentVector)->SetValue(currentRowIndex, value);
            break;
        }
        // OMNI_VEC_ENCODING_DICTIONARY: The specific type in dictionary has been extracted before the call.
        case OMNI_VARCHAR:
        case OMNI_CHAR:
            InsertVarchar(origintVector, originRowIndex, currentVector, currentRowIndex);
            break;
        case OMNI_DECIMAL128: {
            Decimal128 value = static_cast<Decimal128Vector *>(origintVector)->GetValue(originRowIndex);
            static_cast<Decimal128Vector *>(currentVector)->SetValue(currentRowIndex, value);
            break;
        }
        case OMNI_CONTAINER:
            InsertContainer(origintVector, originRowIndex, currentVector, currentRowIndex);
            break;
        default: {
            LogError("No such data type %d", origintVector->GetTypeId());
            break;
        }
    }
}

void PartitionedOutputOperator::MergeVectorBatch(VectorBatch *vecBatch, int32_t vecCount)
{
    for (int i = 0; i < bucketToPartitionCount; ++i) {
        vector<int> rowList = partitionedMap[i];
        int32_t currentVecBatchRowCount = rowList.size();
        BuildVecBatch(vecCount, currentVecBatchRowCount);
        VectorBatch *vectorBatch = vectorBatches[i];
        for (int vecIdx = 0; vecIdx < vecCount; ++vecIdx) {
            for (int j = 0; j < currentVecBatchRowCount; ++j) {
                int32_t oldRowIndex = rowList[j];
                int32_t newRowIndex = j;
                int32_t originalOldRowIndex;
                int32_t originalNewRowIndex;
                Vector *oldVector =
                    VectorHelper::ExpandVectorAndIndex(vecBatch->GetVector(vecIdx), oldRowIndex, originalOldRowIndex);
                Vector *newVector = VectorHelper::ExpandVectorAndIndex(vectorBatch->GetVector(vecIdx), newRowIndex,
                    originalNewRowIndex);
                if (oldVector->IsValueNull(originalOldRowIndex)) {
                    if (newVector->GetTypeId() == OMNI_VARCHAR || newVector->GetTypeId() == OMNI_CHAR) {
                        static_cast<VarcharVector *>(newVector)->SetValueNull(originalNewRowIndex);
                    } else {
                        newVector->SetValueNull(originalNewRowIndex);
                    }
                    continue;
                }
                Insert(oldVector, originalOldRowIndex, newVector, originalNewRowIndex);
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
            nullChannel > -1 && vecBatch->GetVector(nullChannel)->IsValueNull(rowIdx);
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
    VectorHelper::FreeVecBatch(vecBatch);
    return OMNI_STATUS_FINISHED;
}

long GetHash(int32_t rowIndex, Vector *vector)
{
    switch (vector->GetTypeId()) {
        case OMNI_INT:
        case OMNI_DATE32:
            return HashUtil::HashValue(static_cast<IntVector *>(vector)->GetValue(rowIndex));
        case OMNI_LONG:
        case OMNI_SHORT:
            return HashUtil::HashValue(static_cast<LongVector *>(vector)->GetValue(rowIndex));
        case OMNI_DOUBLE:
            return HashUtil::HashValue(static_cast<DoubleVector *>(vector)->GetValue(rowIndex));
        case OMNI_BOOLEAN:
            return HashUtil::HashValue(static_cast<BooleanVector *>(vector)->GetValue(rowIndex));
        case OMNI_DECIMAL64:
            return HashUtil::HashDecimal64Value(static_cast<LongVector *>(vector)->GetValue(rowIndex));
        case OMNI_DECIMAL128: {
            Decimal128 decimal128Value = static_cast<Decimal128Vector *>(vector)->GetValue(rowIndex);
            return HashUtil::HashValue(decimal128Value.LowBits(), decimal128Value.HighBits());
        }
        case OMNI_CONTAINER: {
            long result = 1;
            ContainerVector *containerVec = static_cast<ContainerVector *>(vector);
            auto *avgValVector = reinterpret_cast<DoubleVector *>(containerVec->GetValue(0));
            result = HashUtil::CombineHash(result, GetHash(rowIndex, avgValVector));
            auto *avgCountVector = reinterpret_cast<LongVector *>(containerVec->GetValue(1));
            result = HashUtil::CombineHash(result, GetHash(rowIndex, avgCountVector));
            return result;
        }
        case OMNI_VARCHAR:
        case OMNI_CHAR: {
            uint8_t *varcharValue = nullptr;
            int32_t valueLength = static_cast<VarcharVector *>(vector)->GetValue(rowIndex, &varcharValue);
            return HashUtil::HashValue(reinterpret_cast<int8_t *>(varcharValue), valueLength);
        }
        // OMNI_VEC_ENCODING_DICTIONARY: The specific type in dictionary has been extracted before the call.
        default:
            return 0;
    }
}

int32_t PartitionedOutputOperator::GetPartition(VectorBatch *vecBatch, int32_t startVecIndex, int32_t rowIndex)
{
    long rowHash = 0;
    if (hashPrecomputed) {
        VectorHelper::GetValue(vecBatch->GetVector(startVecIndex), rowIndex, &rowHash); // partitioned page index 0
    } else {
        for (int i = 0; i < hashChannelsCount; ++i) {
            int32_t tmpRowIndex = rowIndex;
            int32_t originalTmpRowIndex;
            Vector *vector = vecBatch->GetVector(startVecIndex + hashChannels[i]);
            long hash = 0;
            vector = VectorHelper::ExpandVectorAndIndex(vector, tmpRowIndex, originalTmpRowIndex);
            if (!vector->IsValueNull(originalTmpRowIndex)) {
                hash = GetHash(originalTmpRowIndex, vector);
            }
            rowHash = HashUtil::CombineHash(rowHash, hash);
        }
    }

    uint64_t tempValue = static_cast<uint64_t>(rowHash);
    tempValue &= 0x7fffffffffffffffUL;
    rowHash = static_cast<uint64_t>(tempValue);
    if (bucketToPartitionCount == 0) {
        bucketToPartitionCount = 1;
    }
    int32_t partition = rowHash % bucketToPartitionCount;
    return partition;
}

void PartitionedOutputOperator::BuildVecBatch(int32_t vecCount, int32_t rowCount)
{
    VectorBatch *vectorBatch = new VectorBatch(vecCount, rowCount);
    vectorBatch->NewVectors(this->vecAllocator, sourceTypes.Get());
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