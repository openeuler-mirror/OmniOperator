/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2023. All rights reserved.
 */

#ifndef OMNI_RUNTIME_COLUMN_MARSHALLER_H
#define OMNI_RUNTIME_COLUMN_MARSHALLER_H
#include <cstdint>
#include "vector/vector_helper.h"
#include "type/string_ref.h"
#include "operator/hashmap/base_hash_map.h"
#include "operator/execution_context.h"
#include "vector_marshaller.h"

namespace omniruntime {
namespace op {
using namespace vec;
enum class HandleType {
    serialize,
    fixed256Bytes,
    onlyOneKey
};

template <typename Hashmap> class ColumnSerializeHandler {
public:
    Hashmap hashmap;
    using KeyType = typename Hashmap::Keys;
    using ValueType = typename Hashmap::Values;
    using Result = typename Hashmap::ResultType;
    ColumnSerializeHandler(uint8_t initDegree = 16) : hashmap(initDegree) {}

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

    ALWAYS_INLINE void TryToInsertJoinKeysToHashmap(BaseVector **joinVectors, int32_t joinColNum, int32_t rowIdx,
        int32_t i, mem::SimpleArenaAllocator &arenaAllocator, std::vector<type::StringRef> &keys,
        std::vector<int8_t> &isNotNullKeys)
    {
        keys[i].size = 0;
        keys[i].data = nullptr;
        for (int32_t joinColIdx = 0; joinColIdx < joinColNum; joinColIdx++) {
            auto curVector = joinVectors[joinColIdx];
            auto &curFunc = ignoreNullSerializers[joinColIdx];
            if (UNLIKELY(!curFunc(curVector, rowIdx, arenaAllocator, keys[i]))) {
                isNotNullKeys[i] = false;
                return;
            }
        }
        isNotNullKeys[i] = true;
    }

    ALWAYS_INLINE void TryToInsertFixedJoinKeysToHashmap(BaseVector **joinVectors, int32_t joinColNum, int32_t rowIdx,
        type::StringRef &key, bool &isNotNullKey)
    {
        size_t pos = 0;
        for (int32_t groupColIdx = 0; groupColIdx < joinColNum; groupColIdx++) {
            auto curVector = joinVectors[groupColIdx];
            auto &curFunc = fixedKeysIgnoreNullSerializers[groupColIdx];
            if (UNLIKELY(!curFunc(curVector, rowIdx, key, pos))) {
                isNotNullKey = false;
                return;
            }
        }
        isNotNullKey = true;
    }

    ALWAYS_INLINE void BatchCalculateHash(std::vector<KeyType> &keys, std::vector<int8_t> &isNotNullKeys,
        std::vector<size_t> &hashes, int32_t maxStep)
    {
        for (int i = 0; i < maxStep; ++i) {
            if (LIKELY(isNotNullKeys[i])) {
                hashes[i] = hashmap.CalculateHash(keys[i]);
            }
        }
    }

    ALWAYS_INLINE Result InsertJoinKeysToHashmap(KeyType &key)
    {
        return hashmap.EmplaceNotNullKey(key);
    }

    ALWAYS_INLINE Result InsertNullKeysToHashmap(KeyType &key)
    {
        return hashmap.EmplaceNullValue(key);
    }

    ALWAYS_INLINE Result InsertJoinKeysToHashmap(KeyType &key, size_t &hashValue)
    {
        return hashmap.EmplaceNotNullKey(key, hashValue);
    }

    void ParseKeyToCols(const KeyType &key, std::vector<vec::BaseVector *> &groupOutputVectors, int32_t groupColNum,
        const int rowIdx)
    {
        auto *pos = key.data;
        for (int32_t i = 0; i < groupColNum; ++i) {
            auto curVectorPtr = groupOutputVectors[i];
            auto deserializeFunc = deserializers[i];
            pos = deserializeFunc(curVectorPtr, rowIdx, pos);
        }
    }
    void PraseValueToCols(const ValueType &value, std::vector<vec::BaseVector *> &aggSpillVectors, int32_t aggColNum,
        const std::vector<type::DataTypePtr> &spillTypes , const int rowIdx)
    {
        // if constexpr (AggType == OMNI_AGGREGATION_TYPE_SUM || AggType == OMNI_AGGREGATION_TYPE_COUNT_COLUMN ||
        //    AggType == OMNI_AGGREGATION_TYPE_COUNT_ALL || AggType == OMNI_AGGREGATION_TYPE_AVG ||
        //    AggType == OMNI_AGGREGATION_TYPE_MAX || OMNI_AGGREGATION_TYPE_MIN) {
        // }
        //for (uint32_t i = 0; i < aggColNum; i++) {
        //    if(spillTypes[i]->GetId() == DataTypeId::OMNI_CHAR) {
        //       auto BaseVector = VectorHelper::CreateStringVector(vectorSize);
        //    }
        //}
    }

    ALWAYS_INLINE Result FindValueFromHashmap(KeyType &key)
    {
        return hashmap.FindMatchPosition(key);
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

    void ResetIgnoreNullSerializer()
    {
        ignoreNullSerializers.clear();
    }

    void ResetFixedKeysIgnoreNullSerializer()
    {
        fixedKeysIgnoreNullSerializers.clear();
    }

    void PushBackSerializer(VectorSerializer &serializer)
    {
        serializers.push_back(serializer);
    }

    void PushBackIgnoreNullSerializer(VectorSerializerIgnoreNull &serializer)
    {
        ignoreNullSerializers.push_back(serializer);
    }

    void PushBackFixedKeysIgnoreNullSerializer(FixedKeyVectorSerializerIgnoreNull &serializer)
    {
        fixedKeysIgnoreNullSerializers.push_back(serializer);
    }

    void PushBackDeSerializer(VectorDeSerializer &deserializer)
    {
        deserializers.push_back(deserializer);
    }

    size_t GetElementsSize() const
    {
        return hashmap.GetElementsSize();
    }

private:
    std::vector<VectorSerializer> serializers;
    std::vector<VectorDeSerializer> deserializers;

    std::vector<VectorSerializerIgnoreNull> ignoreNullSerializers;
    std::vector<FixedKeyVectorSerializerIgnoreNull> fixedKeysIgnoreNullSerializers;
};
}
}
#endif // OMNI_RUNTIME_COLUMN_MARSHALLER_H
