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
        const char *ptr = nullptr;
        uint32_t len = 0;
        for (int i = 0; i<groupVectors->GetVectorCount(); i++) {
            auto *curVector = groupVectors->GetVector(i);
            auto strRef = curVector->SerializeValue(rowId, executionContext, ptr);
            len += strRef.size ;
        }

        return {ptr, len};
    }

    Result InsertValueToHashmap(size_t rowId, VectorBatch *groupVectors, ExecutionContext &executionContext)
    {
        auto emplaceKey = HandleOneRow(rowId, groupVectors, executionContext);
        return hashmap.Emplace(emplaceKey);
    }

    static void ParseKeyToCols(const StringRef &key, VectorBatch &aggVectors, int rowId)
    {
        auto pos = key.data ;
        for (int i = 0; i<aggVectors.GetVectorCount(); i++) {
            auto *curVec = aggVectors.GetVector(i);
            pos = curVec->DeserializeValueIntoThis(rowId, pos);
        }
    }
};

}
}
#endif // OMNI_RUNTIME_GROUP_COLUMN_MARSHALLER_H
