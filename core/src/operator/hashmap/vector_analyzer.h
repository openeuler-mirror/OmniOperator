/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef OMNI_RUNTIME_VECTOR_ANALYZER_H
#define OMNI_RUNTIME_VECTOR_ANALYZER_H

#include <cstdint>
#include "operator/aggregation/aggregation.h"
#include "operator/hashmap/array_map.h"
#include "operator/omni_id_type_vector_traits.h"

namespace omniruntime {
namespace op {

enum class HashTableType {
    NORMAL_HASH_TABLE,
    ARRAY_HASH_TABLE
};

struct ArrayRange {
    bool init = false;
    int64_t min = std::numeric_limits<int64_t>::max();
    int64_t max = std::numeric_limits<int64_t>::min();
};

class VectorAnalyzer {
public:
    explicit VectorAnalyzer(std::vector<ColumnIndex> &groupByCols) : groupByCols(groupByCols)
    {
    }

    bool IsArrayHashTableType();

    bool DecideHashMode(omniruntime::vec::VectorBatch *&vectorBatch);

    ALWAYS_INLINE int64_t MinValue() const
    {
        return groupbyColsRange.min;
    }

    ALWAYS_INLINE bool MinMaxChanged() const
    {
        return minMaxChanged;
    }

    ALWAYS_INLINE void SetNormalHashTable()
    {
        hashMode = HashTableType::NORMAL_HASH_TABLE;
    }

    ALWAYS_INLINE uint64_t GetRange() const
    {
        // range length max - min + 1 and null slot
        return groupbyColsRange.max - groupbyColsRange.min + 2;
    }

    ALWAYS_INLINE uint64_t ComputeKey(const int64_t value) const
    {
        return value - groupbyColsRange.min + 1;
    }

private:
    template<typename T>
    bool CheckArrayMap(Vector<T> *vector, size_t length);

    template<type::DataTypeId typeId>
    bool HandleInputValues(omniruntime::vec::VectorBatch *&vectorBatch, int32_t idx);

    bool minMaxChanged = false;
    std::vector<ColumnIndex> groupByCols;
    ArrayRange groupbyColsRange;
    HashTableType hashMode = HashTableType::ARRAY_HASH_TABLE;
};

}
} // end of namespace omniruntime::op
#endif