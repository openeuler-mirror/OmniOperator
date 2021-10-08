/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2018. All rights reserved.
 */

#include "partitionedoutput.h"
#include <map>
#include "../util/operator_util.h"
#include "../../vector/vector_helper.h"

using namespace std;
using namespace omniruntime::vec;
namespace omniruntime {
namespace op {
PartitionedOutputOperatorFactory::PartitionedOutputOperatorFactory(const VecTypes &sourceTypes, int32_t sourceTypeCount,
    bool replicatesAnyRow, int32_t nullChannel, int32_t *partitionChannels, int32_t partitionChannelsCount,
    int32_t partitionCount, int32_t *bucketToPartition, int32_t bucketToPartitionCount, bool isHashPrecomputed,
    int32_t *hashChannelTypes, int32_t hashChannelTypesCount, int32_t *hashChannels, int32_t hashChannelsCount)
    : sourceTypeCount(sourceTypeCount),
      partitionChannelsCount(partitionChannelsCount),
      replicatesAnyRow(replicatesAnyRow),
      partitionCount(partitionCount),
      nullChannel(nullChannel),
      bucketToPartitionCount(bucketToPartitionCount),
      hashPrecomputed(isHashPrecomputed),
      hashChannelTypesCount(hashChannelTypesCount),
      hashChannelsCount(hashChannelsCount)
{
    this->sourceTypes = std::make_unique<VecTypes>(sourceTypes);

    this->partitionChannels = std::make_unique<int[]>(partitionChannelsCount).release();
    for (int i = 0; i < partitionChannelsCount; ++i) {
        this->partitionChannels[i] = partitionChannels[i];
    }
    this->bucketToPartition = std::make_unique<int[]>(bucketToPartitionCount).release();
    for (int i = 0; i < bucketToPartitionCount; ++i) {
        this->bucketToPartition[i] = bucketToPartition[i];
    }

    this->hashChannelTypes = std::make_unique<int[]>(hashChannelTypesCount).release();
    for (int i = 0; i < hashChannelTypesCount; ++i) {
        this->hashChannelTypes[i] = hashChannelTypes[i];
    }

    this->hashChannels = std::make_unique<int[]>(hashChannelsCount).release();
    for (int i = 0; i < hashChannelsCount; ++i) {
        this->hashChannels[i] = hashChannels[i];
    }
}

PartitionedOutputOperatorFactory::~PartitionedOutputOperatorFactory() {}

PartitionedOutputOperatorFactory *PartitionedOutputOperatorFactory::CreatePartitionedOutputOperatorFactory(
    const VecTypes &sourceTypes, int32_t sourceTypeCount, bool replicatesAnyRow, int32_t nullChannel,
    int32_t *partitionChannels, int32_t partitionChannelsCount, int32_t partitionCount, int32_t *bucketToPartition,
    int32_t bucketToPartitionCount, bool isHashPrecomputed, int32_t *hashChannelTypes, int32_t hashChannelTypesCount,
    int32_t *hashChannels, int32_t hashChannelsCount)
{
    PartitionedOutputOperatorFactory *operatorFactory =
        std::make_unique<PartitionedOutputOperatorFactory>(sourceTypes, sourceTypeCount, replicatesAnyRow, nullChannel,
        partitionChannels, partitionChannelsCount, partitionCount, bucketToPartition, bucketToPartitionCount,
        isHashPrecomputed, hashChannelTypes, hashChannelTypesCount, hashChannels, hashChannelsCount)
            .release();
    return operatorFactory;
}

Operator *PartitionedOutputOperatorFactory::CreateOperator()
{
    auto partitionedOutputOperator = std::make_unique<PartitionedOutputOperator>(*(this->sourceTypes.get()),
        sourceTypeCount, replicatesAnyRow, nullChannel, partitionChannels, partitionChannelsCount, partitionCount,
        bucketToPartition, bucketToPartitionCount, hashPrecomputed, this->hashChannelTypes, hashChannelTypesCount,
        hashChannels, hashChannelsCount);
    return partitionedOutputOperator.release();
}

PartitionedOutputOperator::PartitionedOutputOperator(const VecTypes &sourceTypes, int32_t sourceTypeCount,
    bool replicatesAnyRow, int nullChannel, int32_t *partitionChannels, int32_t partitionChannelsCount,
    int32_t partitionCount, int32_t *bucketToPartition, int32_t bucketToPartitionCount, bool isHashPrecomputed,
    int32_t *hashChannelTypes, int32_t hashChannelTypesCount, int32_t *hashChannels, int32_t hashChannelsCount)
    : sourceTypes(sourceTypes),
      sourceTypeCount(sourceTypeCount),
      partitionChannels(partitionChannels),
      partitionChannelsCount(partitionChannelsCount),
      bucketToPartition(bucketToPartition),
      replicatesAnyRow(replicatesAnyRow),
      partitionCount(partitionCount),
      nullChannel(nullChannel),
      bucketToPartitionCount(bucketToPartitionCount),
      hashPrecomputed(isHashPrecomputed),
      hashChannelTypes(hashChannelTypes),
      hashChannelTypesCount(hashChannelTypesCount),
      hashChannels(hashChannels),
      hashChannelsCount(hashChannelsCount)
{}

PartitionedOutputOperator::~PartitionedOutputOperator()
{
    delete[] partitionChannels;
    delete[] bucketToPartition;
    delete[] hashChannelTypes;
    delete[] hashChannels;

    VectorHelper::FreeVecBatches(vectorBatches);
}

void ALWAYS_INLINE InsertContainer(Vector *origintVector, int32_t originRowIndex, Vector *currentVector,
    int32_t currentRowIndex)
{
    ContainerVector *containerVec = static_cast<ContainerVector *>(origintVector);
    auto *avgValVector = reinterpret_cast<DoubleVector *>(containerVec->getValue(0));
    auto *avgCountVector = reinterpret_cast<LongVector *>(containerVec->getValue(1));
    int64_t longValue = static_cast<LongVector *>(avgCountVector)->GetValue(originRowIndex);
    double doubleValue = static_cast<DoubleVector *>(avgValVector)->GetValue(originRowIndex);
    ContainerVector *currentContainerVec = static_cast<ContainerVector *>(currentVector);
    auto *currentAvgValVector = reinterpret_cast<DoubleVector *>(currentContainerVec->getValue(0));
    auto *currentAvgCountVector = reinterpret_cast<LongVector *>(currentContainerVec->getValue(1));
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

void ALWAYS_INLINE Insert(Vector *origintVector, int32_t originRowIndex, Vector *currentVector, int32_t currentRowIndex)
{
    switch (origintVector->GetTypeId()) {
        case OMNI_VEC_TYPE_INT:
        case OMNI_VEC_TYPE_DATE32: {
            int32_t value = static_cast<IntVector *>(origintVector)->GetValue(originRowIndex);
            static_cast<IntVector *>(currentVector)->SetValue(currentRowIndex, value);
            break;
        }
        case OMNI_VEC_TYPE_LONG:
        case OMNI_VEC_TYPE_DECIMAL64: {
            int64_t value = static_cast<LongVector *>(origintVector)->GetValue(originRowIndex);
            static_cast<LongVector *>(currentVector)->SetValue(currentRowIndex, value);
            break;
        }
        case OMNI_VEC_TYPE_DOUBLE: {
            double value = static_cast<DoubleVector *>(origintVector)->GetValue(originRowIndex);
            static_cast<DoubleVector *>(currentVector)->SetValue(currentRowIndex, value);
            break;
        }
        case OMNI_VEC_TYPE_BOOLEAN: {
            bool value = static_cast<BooleanVector *>(origintVector)->GetValue(originRowIndex);
            static_cast<BooleanVector *>(currentVector)->SetValue(currentRowIndex, value);
            break;
        }
        case OMNI_VEC_TYPE_DICTIONARY: {
            auto *dictionaryVector = static_cast<DictionaryVector *>(origintVector);
            Vector *dictionary = dictionaryVector->GetDictionary();
            int32_t id = dictionaryVector->GetIds()[originRowIndex];
            Insert(dictionary, id, currentVector, currentRowIndex);
            break;
        }
        case OMNI_VEC_TYPE_VARCHAR:
            InsertVarchar(origintVector, originRowIndex, currentVector, currentRowIndex);
            break;
        case OMNI_VEC_TYPE_DECIMAL128: {
            Decimal128 value = static_cast<Decimal128Vector *>(origintVector)->GetValue(originRowIndex);
            static_cast<Decimal128Vector *>(currentVector)->SetValue(currentRowIndex, value);
            break;
        }
        case OMNI_VEC_TYPE_CONTAINER:
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
    for (int i = 0; i < partitionedMap.size(); ++i) {
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
                Vector *oldVector = VectorHelper::ExpandVectorAndIndex(vecBatch->GetVector(vecIdx), oldRowIndex,
                                                                       originalOldRowIndex);
                Vector *newVector = VectorHelper::ExpandVectorAndIndex(vectorBatch->GetVector(vecIdx), newRowIndex,
                                                                       originalNewRowIndex);
                if (oldVector->IsValueNull(originalOldRowIndex)) {
                    newVector->SetValueNull(originalNewRowIndex);
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
    return OMNI_STATUS_FINISHED;
}

long GetHash(int32_t rowIndex, int type, Vector *vector)
{
    switch (vector->GetTypeId()) {
        case OMNI_VEC_TYPE_INT:
        case OMNI_VEC_TYPE_DATE32:
            return HashUtil::HashValue(static_cast<IntVector *>(vector)->GetValue(rowIndex));
        case OMNI_VEC_TYPE_LONG:
        case OMNI_VEC_TYPE_SHORT:
            return HashUtil::HashValue(static_cast<LongVector *>(vector)->GetValue(rowIndex));
        case OMNI_VEC_TYPE_DOUBLE:
            return HashUtil::HashValue(static_cast<DoubleVector *>(vector)->GetValue(rowIndex));
        case OMNI_VEC_TYPE_BOOLEAN:
            return HashUtil::HashValue(static_cast<BooleanVector *>(vector)->GetValue(rowIndex));
        case OMNI_VEC_TYPE_DECIMAL64:
            return HashUtil::HashDecimal64Value(static_cast<LongVector *>(vector)->GetValue(rowIndex));
        case OMNI_VEC_TYPE_DECIMAL128: {
            Decimal128 decimal128Value = static_cast<Decimal128Vector *>(vector)->GetValue(rowIndex);
            return HashUtil::HashValue(decimal128Value.LowBits(), decimal128Value.HighBits());
        }
        case OMNI_VEC_TYPE_CONTAINER: {
            long result = 1;
            ContainerVector *containerVec = static_cast<ContainerVector *>(vector);
            auto *avgValVector = reinterpret_cast<DoubleVector *>(containerVec->getValue(0));
            result = HashUtil::CombineHash(result, GetHash(rowIndex, avgValVector->GetTypeId(), avgValVector));
            auto *avgCountVector = reinterpret_cast<LongVector *>(containerVec->getValue(1));
            result =
                HashUtil::CombineHash(result, GetHash(rowIndex, avgCountVector->GetTypeId(), avgCountVector));
            return result;
        }
        case OMNI_VEC_TYPE_VARCHAR: {
            uint8_t *varcharValue = nullptr;
            int32_t valueLength = static_cast<VarcharVector *>(vector)->GetValue(rowIndex, &varcharValue);
            return HashUtil::HashValue(reinterpret_cast<int8_t *>(varcharValue), valueLength);
        }
        case OMNI_VEC_TYPE_DICTIONARY: {
            auto *dictionaryVector = static_cast<DictionaryVector *>(vector);
            Vector *vector = dictionaryVector->GetDictionary();
            rowIndex = dictionaryVector->GetIds()[rowIndex];
            return GetHash(rowIndex, vector->GetTypeId(), vector);
        }
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
            int type = hashChannelTypes[i];
            Vector *vector = vecBatch->GetVector(startVecIndex + hashChannels[i]);
            long hash = 0;
            vector = VectorHelper::ExpandVectorAndIndex(vector, tmpRowIndex, originalTmpRowIndex);
            if (!vector->IsValueNull(originalTmpRowIndex)) {
                hash = GetHash(originalTmpRowIndex, type, vector);
            }
            rowHash = HashUtil::CombineHash(rowHash, hash);
        }
    }

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