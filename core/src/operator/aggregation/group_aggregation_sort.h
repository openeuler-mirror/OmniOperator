/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 * Description: Hash Aggregation Sort Header
 */

#ifndef OMNI_RUNTIME_GROUP_AGGREATION_SORT_H
#define OMNI_RUNTIME_GROUP_AGGREATION_SORT_H

#include "operator/hashmap/column_marshaller.h"
#include "aggregator/aggregator.h"
#include "type/data_types.h"

namespace omniruntime::op {
class AggregationSort {
public:
    std::vector<omniruntime::op::KeyValue> &GetKvVector()
    {
        return kvVec;
    }
    void ClearVector()
    {
        kvVec.clear();
    }
    size_t GetRowCount()
    {
        return kvVec.size();
    }
    void SortKvVector()
    {
        std::sort(kvVec.begin(), kvVec.end(), HashKeyCompare);
    }
    void SetSpillVectorBatch(vec::VectorBatch *spillVecBatch, uint64_t rowOffset, std::vector<std::unique_ptr<Aggregator>> &aggregators);

private:
    std::vector<omniruntime::op::KeyValue> kvVec;
    static bool HashKeyCompare(const omniruntime::op::KeyValue &a, omniruntime::op::KeyValue &b)
    {
        int ret = memcmp(a.keyAddr, b.keyAddr, std::min(a.keyLen, b.keyLen));
        return ret < 0;
    }
};
}


#endif // OMNI_RUNTIME_GROUP_AGGREATION_SORT_H
