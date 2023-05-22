/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 */

#ifndef OMNI_RUNTIME_TOPN_SORT_H
#define OMNI_RUNTIME_TOPN_SORT_H

#include "operator/operator_factory.h"
#include "operator/topn/topn.h"
#include "operator/aggregation/group_hash_map/group_hash_map.h"
#include "type/string_ref.h"
#include "operator/aggregation/vector_marshaller.h"

namespace omniruntime::op {
using HashFunc = int64_t (*)(vec::BaseVector *vec, int32_t rowIdx);
using EqualFunc = bool (*)(vec::BaseVector *leftVec, int32_t leftPos, vec::BaseVector *rightVec, int32_t rightPos);
using CompareFunc = int32_t (*)(vec::BaseVector *leftVec, int32_t leftPosition, vec::BaseVector *rightVec,
    int32_t rightPosition);
using CreateVectorFunc = BaseVector *(*)(BaseVector *inputVec, int32_t inputPos);
using SetValueFunc = void (*)(vec::BaseVector *inputVec, int32_t inputPos, vec::BaseVector *outputVec,
    int32_t outputPos);

class PartitionKey {
public:
    PartitionKey(HashFunc *partitionHashFuncs, EqualFunc *partitionEqualFuncs, int32_t partitionColNum,
        vec::BaseVector **partitionVectors, int32_t rowIdx)
        : partitionHashFuncs(partitionHashFuncs),
          partitionEqualFuncs(partitionEqualFuncs),
          partitionColNum(partitionColNum),
          partitionVectors(partitionVectors),
          rowIdx(rowIdx)
    {}

    ~PartitionKey()
    {
        for (int32_t i = 0; i < partitionColNum; i++) {
            delete partitionVectors[i];
        }
        delete[] partitionVectors;
    }

    HashFunc *partitionHashFuncs;
    EqualFunc *partitionEqualFuncs;
    int32_t partitionColNum;
    vec::BaseVector **partitionVectors;
    int32_t rowIdx;
};

class PartitionKeyHash {
public:
    std::size_t operator () (const PartitionKey &key) const
    {
        // calculate hash
        auto partitionHashFuncs = key.partitionHashFuncs;
        auto partitionColNum = key.partitionColNum;
        auto partitionVectors = key.partitionVectors;
        auto rowIdx = key.rowIdx;
        int64_t result = 0;
        for (int32_t i = 0; i < partitionColNum; i++) {
            auto vec = partitionVectors[i];
            if (vec->IsNull(rowIdx)) {
                continue;
            }
            auto hash = partitionHashFuncs[i](vec, rowIdx);
            result = HashUtil::CombineHash(result, hash);
        }
        return result;
    }
};

class PartitionKeyEqual {
public:
    bool operator () (const PartitionKey &left, const PartitionKey &right) const
    {
        auto partitionEqualFuncs = left.partitionEqualFuncs;
        auto partitionColNum = left.partitionColNum;
        auto leftVectors = left.partitionVectors;
        auto leftRowIdx = left.rowIdx;
        auto rightVectors = right.partitionVectors;
        auto rightRowIdx = right.rowIdx;
        for (int32_t i = 0; i < partitionColNum; i++) {
            auto result = partitionEqualFuncs[i](leftVectors[i], leftRowIdx, rightVectors[i], rightRowIdx);
            if (!result) {
                return false;
            }
        }
        return true;
    }
};

class PartitionValue {
public:
    PartitionValue(int32_t vecBatchCount) : vecBatches(new vec::VectorBatch *[vecBatchCount]()), currentSize(0) {}

    ~PartitionValue()
    {
        for (int32_t i = 0; i < currentSize; i++) {
            vec::VectorHelper::FreeVecBatch(vecBatches[i]);
        }
        delete[] vecBatches;
    }

    vec::VectorBatch **vecBatches; // the row count of vecBatch is n
    int32_t currentSize;
};

class TopNSortOperator : public Operator {
public:
    TopNSortOperator(const type::DataTypes &sourceTypes, int32_t n, const std::vector<int32_t> &partitionCols,
        const std::vector<int32_t> &sortCols, const std::vector<int32_t> &sortAscendings,
        const std::vector<int32_t> &sortNullFirsts);

    ~TopNSortOperator() override = default;

    int32_t AddInput(omniruntime::vec::VectorBatch *inputVecBatch) override;

    int32_t GetOutput(omniruntime::vec::VectorBatch **outputVecBatch) override;

    OmniStatus Close() override;

private:
    void Prepare(vec::VectorBatch *inputVecBatch);

    void InsertNewPartition(const PartitionKey &key, vec::BaseVector **inputVectors, int32_t inputColNum,
        int32_t rowIdx, HashFunc *partitionHashFuncs, EqualFunc *partitionEqualFuncs);

    void InsertNewValue(PartitionValue &value, vec::BaseVector **inputVectors, int32_t inputColNum,
        vec::BaseVector **sortVectors, int32_t rowIdx);

    void UpdatePartitionValue(PartitionValue &value, vec::BaseVector **inputVectors, int32_t inputColNum,
        vec::BaseVector **sortVectors, int32_t rowIdx);

    // insert sort vector is flat vector or dictionary vector
    int32_t CompareForSortCols(vec::BaseVector **insertSortVectors, int32_t insertRowIdx, VectorBatch *vecBatch)
    {
        int32_t result;
        for (int32_t sortColIdx = 0; sortColIdx < sortColNum; sortColIdx++) {
            auto leftVec = insertSortVectors[sortColIdx];
            auto rightVec = vecBatch->Get(sortCols[sortColIdx]);
            auto leftNull = leftVec->IsNull(insertRowIdx);
            auto rightNull = rightVec->IsNull(0);
            auto sortNullFirst = sortNullFirsts[sortColIdx];

            if (leftNull && rightNull) {
                // both left and right are null
                result = 0;
            } else if (leftNull) {
                // left is null, but right is not null
                result = sortNullFirst ? 1 : -1;
                break;
            } else if (rightNull) {
                // left is not null, but right is null
                result = sortNullFirst ? -1 : 1;
                break;
            } else {
                // both left and right are not null
                result = sortCompareFuncs[sortColIdx](leftVec, insertRowIdx, rightVec, 0);
                if (result != 0) {
                    result = sortAscendings[sortColIdx] ? result : -result;
                    break;
                }
            }
        }
        return result;
    }

    int32_t FindInsertPosition(BaseVector **insertSortVectors, int32_t insertRowIdx, VectorBatch **vecBatches,
        int32_t position);

    type::DataTypes sourceTypes;
    int32_t n;
    std::vector<int32_t> partitionCols;
    std::vector<int32_t> sortCols;
    std::vector<int32_t> sortAscendings;
    std::vector<int32_t> sortNullFirsts;
    std::vector<int32_t> sortColTypes;
    std::vector<CompareFunc> sortCompareFuncs;
    int32_t sortColNum;
    std::vector<HashFunc> partitionHashFuncs;
    std::vector<EqualFunc> partitionEqualFuncs;
    int32_t partitionColNum;
    std::unique_ptr<ExecutionContext> executionContext;
    std::unordered_map<PartitionKey, PartitionValue *, PartitionKeyHash, PartitionKeyEqual> partitionedMap;
    std::vector<VectorSerializer> serializers;
    std::vector<VectorDeSerializer> deserializers;
    std::vector<CreateVectorFunc> createVectorFuncs;
    std::vector<SetValueFunc> updatePartitionValueFuncs; // this is for update value in partitionMap
    std::vector<SetValueFunc> setOutputValueFuncs;       // this is for construct output from partitionMap
    int32_t maxRowCount = 0;
    std::unordered_map<PartitionKey, PartitionValue *, PartitionKeyHash, PartitionKeyEqual>::iterator currentIter;
};

class TopNSortOperatorFactory : public OperatorFactory {
public:
    TopNSortOperatorFactory(const type::DataTypes &sourceTypes, int32_t n, const std::vector<int32_t> &partitionCols,
        const std::vector<int32_t> &sortCols, const std::vector<int32_t> &sortAscendings,
        const std::vector<int32_t> &sortNullFirsts)
        : sourceTypes(sourceTypes),
          n(n),
          partitionCols(partitionCols),
          sortCols(sortCols),
          sortAscendings(sortAscendings),
          sortNullFirsts(sortNullFirsts)
    {}

    ~TopNSortOperatorFactory() override = default;

    Operator *CreateOperator() override
    {
        return new TopNSortOperator(sourceTypes, n, partitionCols, sortCols, sortAscendings, sortNullFirsts);
    }

private:
    type::DataTypes sourceTypes;
    int32_t n;
    std::vector<int32_t> partitionCols;
    std::vector<int32_t> sortCols;
    std::vector<int32_t> sortAscendings;
    std::vector<int32_t> sortNullFirsts;
};
}
#endif // OMNI_RUNTIME_TOPN_SORT_H
