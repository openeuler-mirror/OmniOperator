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
    int64_t min = 1;
    int64_t max = 0;
};

class VectorAnalyzer {
public:
    explicit VectorAnalyzer(std::vector<ColumnIndex> &groupByCols) : groupByCols(groupByCols) {
    }

    bool IsArrayHashTableType();

    bool DecideHashMode(omniruntime::vec::VectorBatch*& vectorBatch);

    template <typename T>
    inline int64_t ToInt64(T value) const
    {
        return value;
    }

private:
    template <typename T>
    bool CheckArrayMap(const T* values, const size_t length);

    template <typename T>
    T ComputerKey(const T value);

    template <type::DataTypeId typeId>
    bool HandleInputValues(omniruntime::vec::VectorBatch*& vectorBatch, int32_t idx);

    std::vector<ColumnIndex> groupByCols;

    ArrayRange groupbyColsRange;
    HashTableType hashMode = HashTableType::ARRAY_HASH_TABLE;
};

}
} // end of namespace omniruntime::op
#endif