/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#ifndef OMNI_RUNTIME_GROUP_COLUMN_MARSHALLER_H
#define OMNI_RUNTIME_GROUP_COLUMN_MARSHALLER_H
#include <cstdint>
#include "vector/vector_helper.h"
#include "type/string_ref.h"
#include "group_hash_map/group_hash_map.h"

namespace omniruntime {
namespace op {
using namespace vec;
enum class GroupByFieldHandleType {
    serialize,
    fixed256Bytes,
    onlyOneKey
};

template <typename Hashmap>
class GroupbyColumnSerializeHandler {
public:
    Hashmap hashmap;
    using Result = typename Hashmap::ResultType;

    type::StringRef
    HandleOneRow(size_t rowId, VectorBatch *groupVectors, ExecutionContext &executionContext)
    {
        const uint8_t *ptr = nullptr;
        uint32_t len = 0;
        for (int i = 0; i<groupVectors->GetVectorCount(); i++) {
            auto *curVector = groupVectors->GetVector(i);
            auto strRef = curVector->SerializeValue(rowId, *executionContext.GetArena(), ptr);
            len += strRef.size ;
        }

        return {ptr, len};
    }

    Result InsertValueToHashmap(size_t rowId, VectorBatch *groupVectors, ExecutionContext &executionContext)
    {
        auto emplaceKey = HandleOneRow(rowId, groupVectors, executionContext);
        return hashmap.Emplace(emplaceKey);
    }

    static void ParseKeyToCols(
        const StringRef &key, VectorBatch *vectorBatch, const int32_t start, const int32_t length, const int rowId)
    {
        auto *pos = reinterpret_cast<const uint8_t *>(key.data);
        const int32_t end = start + length;
        for (int32_t i = start; i < end; ++i) {
            pos = vectorBatch->GetVector(i)->DeserializeValueIntoThis(rowId, pos);
        }
    }
};
}
}
#endif // OMNI_RUNTIME_GROUP_COLUMN_MARSHALLER_H
