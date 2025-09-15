/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 * Description: Hash Aggregation Sort Header
 */

#ifndef OMNI_RUNTIME_GROUP_AGGREATION_SORT_H
#define OMNI_RUNTIME_GROUP_AGGREATION_SORT_H

#include <string>
#include "aggregator/aggregator.h"
#include "type/data_types.h"
#include "type/string_ref.h"

namespace omniruntime::op {
class AggregationSort {
public:
    explicit AggregationSort(std::vector<std::unique_ptr<Aggregator>> &aggregators) : aggregators(aggregators)
    {
        for (auto &aggregator : aggregators) {
            aggVectorCounts.emplace_back(aggregator->GetSpillType().size());
        }
    }

    void ResizeKvVector(size_t size)
    {
        kvVec.resize(size);
        kvString.resize(size);
        groupCount = size;
    }

    void ParseHashMapToVector(const omniruntime::type::StringRef &key, AggregateState *value, size_t groupIndex)
    {
        auto &kv = kvVec[groupIndex];
        kv.keyAddr = const_cast<char *>(key.data);
        kv.keyLen = key.size;
        kv.value = value;
    }

    template<typename T>
    void ParseHashMapToVector(const T &key, AggregateState *value, size_t groupIndex)
    {
        auto &kv = kvVec[groupIndex];
        kvString[groupIndex] = std::to_string(key);
        kv.keyAddr = const_cast<char *>(kvString[groupIndex].c_str());
        kv.keyLen = kvString[groupIndex].size();
        kv.value = value;
    }

    void ClearVector()
    {
        kvVec.clear();
        kvString.clear();
    }

    size_t GetRowCount()
    {
        return groupCount;
    }

    void SortKvVector()
    {
        std::sort(kvVec.begin(), kvVec.end(), HashKeyCompare);
    }

    void SetSpillVectorBatch(vec::VectorBatch *spillVecBatch, uint64_t rowOffset);

private:
    std::vector<std::unique_ptr<Aggregator>> &aggregators;
    std::vector<omniruntime::op::KeyValue> kvVec;
    std::vector<std::string> kvString;
    std::vector<AggregateState *> groupStates;
    size_t groupCount = 0;
    std::vector<int32_t> aggVectorCounts;

    static ALWAYS_INLINE bool HashKeyCompare(const omniruntime::op::KeyValue &a, omniruntime::op::KeyValue &b)
    {
        int ret = memcmp(a.keyAddr, b.keyAddr, std::min(a.keyLen, b.keyLen));
        if (ret == 0) {
            return a.keyLen < b.keyLen;
        } else {
            return ret < 0;
        }
    }
};
}


#endif // OMNI_RUNTIME_GROUP_AGGREATION_SORT_H
