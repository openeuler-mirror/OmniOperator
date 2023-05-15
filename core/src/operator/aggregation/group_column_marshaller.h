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

    Result InsertValueToHashmap(BaseVector **groupVectors, int32_t groupColNum, int32_t rowIdx,
        mem::SimpleArenaAllocator &arenaAllocator)
    {
        type::StringRef key;
        for (int32_t groupColIdx = 0; groupColIdx < groupColNum; groupColIdx++) {
            auto curVector = groupVectors[groupColIdx];
            auto &curFunc = serializers[groupColIdx];
            curFunc(curVector, rowIdx, arenaAllocator, key);
        }
        return hashmap.Emplace(key);
    }

    void ParseKeyToCols(const StringRef &key, std::vector<vec::BaseVector *> &groupOutputVectors, int32_t groupColNum,
        const int rowIdx)
    {
        auto *pos = key.data;
        for (int32_t i = 0; i < groupColNum; ++i) {
            auto curVectorPtr = groupOutputVectors[i];
            auto deserializeFunc = deserializers[i];
            pos = deserializeFunc(curVectorPtr, rowIdx, pos);
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
