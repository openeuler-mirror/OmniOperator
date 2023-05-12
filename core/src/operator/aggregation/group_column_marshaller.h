/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#ifndef OMNI_RUNTIME_GROUP_COLUMN_MARSHALLER_H
#define OMNI_RUNTIME_GROUP_COLUMN_MARSHALLER_H
#include <cstdint>
#include "vector/vector_helper.h"
#include "type/string_ref.h"
#include "group_hash_map/group_hash_map.h"
#include "operator/execution_context.h"
#include "vector_marshaller.h"

namespace omniruntime {
namespace op {
using namespace vec;
enum class GroupByFieldHandleType {
    serialize,
    fixed256Bytes,
    onlyOneKey
};

template <typename Hashmap> class GroupbyColumnSerializeHandler {
public:
    Hashmap hashmap;
    using Result = typename Hashmap::ResultType;

    type::StringRef HandleOneRow(size_t rowId, BaseVector **groupVectors, int32_t groupColNum,
        mem::SimpleArenaAllocator &arenaAllocator)
    {
        const char *ptr = nullptr;
        uint32_t len = 0;
        for (int i = 0; i < groupColNum; i++) {
            auto curVector = groupVectors[i];
            auto &curFunc = serializers[i];
            auto strRef = curFunc(curVector, rowId, arenaAllocator, ptr);
            len += strRef.size;
        }

        return { ptr, len };
    }

    Result InsertValueToHashmap(size_t rowId, BaseVector **groupVectors, int32_t groupColNum,
        mem::SimpleArenaAllocator &arenaAllocator)
    {
        auto emplaceKey = HandleOneRow(rowId, groupVectors, groupColNum, arenaAllocator);
        return hashmap.Emplace(emplaceKey);
    }

    void ParseKeyToCols(const StringRef &key, VectorBatch *vectorBatch, const int32_t start, const int32_t length,
        const int rowId)
    {
        auto *pos = key.data;
        const int32_t end = start + length;
        for (int32_t i = start; i < end; ++i) {
            auto curVectorPtr = vectorBatch->Get(i);
            auto deserializeFunc = deserializers[i];
            pos = deserializeFunc(curVectorPtr, rowId, pos);
        }
    }
    
    void InitSize(int groupBySize)
    {
        serializers.reserve(groupBySize);
        deserializers.reserve(groupBySize);
    }

    void ResetSerializer()
    {
        serializers.clear();
        deserializers.clear();
    }

    void PushBackSerializer(VectorSerializer &serializer)
    {
        serializers.push_back(serializer);
    }

    void PushBackDeSerializer(VectorDeSerializer &deserializer)
    {
        deserializers.push_back(deserializer);
    }

private:
    std::vector<VectorSerializer> serializers;
    std::vector<VectorDeSerializer> deserializers;
};
}
}
#endif // OMNI_RUNTIME_GROUP_COLUMN_MARSHALLER_H
