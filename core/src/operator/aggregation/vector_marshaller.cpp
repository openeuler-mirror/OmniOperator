/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#include "vector_marshaller.h"
#include "operator/omni_id_type_vector_traits.h"
#include "vector/unsafe_vector.h"

namespace omniruntime {
namespace op {
template <type::DataTypeId id>
type::StringRef NullVariableTypeSerializer(mem::SimpleArenaAllocator &arenaAllocator, const char *&begin)
{
    static_assert(id == type::OMNI_CHAR || id == type::OMNI_VARCHAR);
    StringRef res{};
    std::string_view::size_type stringLen = std::string_view::npos;
    res.size = sizeof(stringLen);
    auto *pos = arenaAllocator.AllocateContinue(res.size, (const uint8_t *&)begin);
    std::copy(reinterpret_cast<uint8_t *>(&stringLen), reinterpret_cast<uint8_t *>(&stringLen) + sizeof(stringLen),
        pos);
    return res;
}


template <DataTypeId id> const char *VariableTypeDeserializer(BaseVector *baseVector, size_t rowId, const char *pos)
{
    using RealVector = typename NativeAndVectorType<id>::vector;
    auto realVector = reinterpret_cast<RealVector *>(baseVector);
    std::string_view::size_type stringSize = 0;
    std::copy(pos, pos + sizeof(std::string_view::size_type), reinterpret_cast<uint8_t *>(&stringSize));
    pos += sizeof(stringSize);

    if (stringSize == std::string_view::npos) {
        // string_size < 0 means null pointer
        realVector->SetNull(static_cast<int>(rowId));
        return pos;
    } else if (stringSize >= 0) {
        auto *copyPointer = static_cast<const char *>(pos);
        std::string_view strView(copyPointer, stringSize);
        realVector->SetValue(rowId, strView);
        return pos + stringSize;
    }
}

template <type::DataTypeId id>
type::StringRef VariableTypeSerializer(void *inValuePtr, mem::SimpleArenaAllocator &arenaAllocator, const char *&begin)
{
    static_assert(id == type::OMNI_CHAR || id == type::OMNI_VARCHAR);
    StringRef res{};
    std::string_view::size_type stringLen;
    std::string_view strView = *(reinterpret_cast<std::string_view *>(inValuePtr));

    stringLen = strView.size();
    res.size = sizeof(stringLen) + stringLen;

    auto *pos = arenaAllocator.AllocateContinue(res.size, (const uint8_t *&)begin);
    std::copy(reinterpret_cast<uint8_t *>(&stringLen), reinterpret_cast<uint8_t *>(&stringLen) + sizeof(stringLen),
        pos);
    std::copy(strView.data(), strView.data() + stringLen, pos + sizeof(stringLen));
    res.data = reinterpret_cast<const char *>(pos);
    return res;
}

template <DataTypeId id> const char *FixedLenTypeDeserializer(BaseVector *baseVector, size_t rowId, const char *pos)
{
    using RawDataType = typename NativeAndVectorType<id>::type;
    using RealVector = typename NativeAndVectorType<id>::vector;
    static constexpr uint8_t RawDataSize = sizeof(RawDataType);
    auto realVector = reinterpret_cast<RealVector *>(baseVector);
    bool isNull = *(reinterpret_cast<const bool *>(pos));
    auto *copyPointer = reinterpret_cast<const RawDataType *>(pos + 1);
    if (not isNull) {
        // must copy value
        auto value = *copyPointer;
        realVector->SetValue(rowId, value);
    } else {
        realVector->SetNull(rowId);
    }
    return pos + RawDataSize + sizeof(bool);
}

template <DataTypeId typeId>
type::StringRef FixedLenTypeSerializer(void *inValuePtr, mem::SimpleArenaAllocator &arenaAllocator, const char *&begin)
{
    using RawDataType = typename NativeAndVectorType<typeId>::type;
    static constexpr uint8_t RawDataSize = sizeof(RawDataType);
    StringRef res;
    res.size = sizeof(bool) + RawDataSize;
    auto *pos = arenaAllocator.AllocateContinue(res.size, (const uint8_t *&)begin);
    (*pos) = false;
    auto *valuePtr = reinterpret_cast<RawDataType *>((pos + sizeof(bool)));
    *valuePtr = *(reinterpret_cast<RawDataType *>(inValuePtr));
    res.data = reinterpret_cast<const char *>(pos);
    return res;
}

template <DataTypeId typeId>
type::StringRef NullFixedLenTypeSerializer(mem::SimpleArenaAllocator &arenaAllocator, const char *&begin)
{
    using RawDataType = typename NativeAndVectorType<typeId>::type;
    static constexpr uint8_t RawDataSize = sizeof(RawDataType);
    StringRef res;
    res.size = sizeof(bool) + RawDataSize;
    auto *pos = arenaAllocator.AllocateContinue(res.size, (const uint8_t *&)begin);
    (*pos) = true;
    memset_sp(pos + sizeof(bool), RawDataSize, 0, RawDataSize);
    res.data = reinterpret_cast<const char *>(pos);
    ;
    return res;
}

template <type::DataTypeId id>
omniruntime::type::StringRef SerializeValueIntoArena(BaseVector *baseVector, int rowId,
    mem::SimpleArenaAllocator &arenaAllocator, const char *&begin)
{
    using RawDataType = typename NativeAndVectorType<id>::type;

    if (!baseVector->IsNull(rowId)) {
        using RawVectorType = typename NativeAndVectorType<id>::vector;

        // not dictionary,just use cast to RawVector
        auto rawVector = reinterpret_cast<RawVectorType *>(baseVector);
        auto value = rawVector->GetValue(rowId);

        // the analysis of const expr  will be in compile stage
        if constexpr (std::is_same_v<RawDataType, std::string_view>) {
            return VariableTypeSerializer<id>(static_cast<void *>(&value), arenaAllocator, begin);
        } else {
            return FixedLenTypeSerializer<id>(static_cast<void *>(&value), arenaAllocator, begin);
        }
    } else {
        if constexpr (std::is_same_v<RawDataType, std::string_view>) {
            return NullVariableTypeSerializer<id>(arenaAllocator, begin);
        } else {
            return NullFixedLenTypeSerializer<id>(arenaAllocator, begin);
        }
    }
}

template <type::DataTypeId id>
omniruntime::type::StringRef SerializeDictionaryValueIntoArena(BaseVector *baseVector, int rowId,
    mem::SimpleArenaAllocator &arenaAllocator, const char *&begin)
{
    using RawDataType = typename NativeAndVectorType<id>::type;
    if (!baseVector->IsNull(rowId)) {
        auto dictionaryVector = reinterpret_cast<Vector<DictionaryContainer<RawDataType>> *>(baseVector);

        auto value = dictionaryVector->GetValue(rowId);
        // the analysis of const expr  will be in compile stage
        if constexpr (std::is_same_v<RawDataType, std::string_view>) {
            return VariableTypeSerializer<id>(static_cast<void *>(&value), arenaAllocator, begin);
        } else {
            return FixedLenTypeSerializer<id>(static_cast<void *>(&value), arenaAllocator, begin);
        }
    }

    if constexpr (std::is_same_v<RawDataType, std::string_view>) {
        return NullVariableTypeSerializer<id>(arenaAllocator, begin);
    } else {
        return NullFixedLenTypeSerializer<id>(arenaAllocator, begin);
    }
}

template <type::DataTypeId id> const char *DeserializeFromPointer(BaseVector *baseVector, int rowId, const char *&begin)
{
    using RawDataType = typename NativeAndVectorType<id>::type;
    // the analysis of const expr  will be in compile stage
    if constexpr (std::is_same_v<RawDataType, std::string_view>) {
        return VariableTypeDeserializer<id>(baseVector, rowId, begin);
    } else {
        return FixedLenTypeDeserializer<id>(baseVector, rowId, begin);
    }
}

std::vector<VectorSerializer> vectorSerializerCenter = {
    nullptr,                                        // OMNI_NONE,
    SerializeValueIntoArena<type::OMNI_INT>,        // OMNI_INT
    SerializeValueIntoArena<type::OMNI_LONG>,       // OMNI_LONG
    SerializeValueIntoArena<type::OMNI_DOUBLE>,     // OMNI_DOUBLE
    SerializeValueIntoArena<type::OMNI_BOOLEAN>,    // OMNI_BOOLEAN
    SerializeValueIntoArena<type::OMNI_SHORT>,      // OMNI_SHORT
    SerializeValueIntoArena<type::OMNI_LONG>,       // OMNI_DECIMAL64,
    SerializeValueIntoArena<type::OMNI_DECIMAL128>, // OMNI_DECIMAL128
    SerializeValueIntoArena<type::OMNI_INT>,        // OMNI_DATE32
    SerializeValueIntoArena<type::OMNI_LONG>,       // OMNI_DATE64
    SerializeValueIntoArena<type::OMNI_INT>,        // OMNI_TIME32
    SerializeValueIntoArena<type::OMNI_LONG>,       // OMNI_TIME64
    nullptr,                                        // OMNI_TIMESTAMP
    nullptr,                                        // OMNI_INTERVAL_MONTHS
    nullptr,                                        // OMNI_INTERVAL_DAY_TIME
    SerializeValueIntoArena<type::OMNI_VARCHAR>,    // OMNI_VARCHAR
    SerializeValueIntoArena<type::OMNI_VARCHAR>,    // OMNI_CHAR,
    nullptr                                         // OMNI_CONTAINER,
};

std::vector<VectorSerializer> dicVectorSerializerCenter = {
    nullptr,                                                  // OMNI_NONE,
    SerializeDictionaryValueIntoArena<type::OMNI_INT>,        // OMNI_INT
    SerializeDictionaryValueIntoArena<type::OMNI_LONG>,       // OMNI_LONG
    SerializeDictionaryValueIntoArena<type::OMNI_DOUBLE>,     // OMNI_DOUBLE
    SerializeDictionaryValueIntoArena<type::OMNI_BOOLEAN>,    // OMNI_BOOLEAN
    SerializeDictionaryValueIntoArena<type::OMNI_SHORT>,      // OMNI_SHORT
    SerializeDictionaryValueIntoArena<type::OMNI_LONG>,       // OMNI_DECIMAL64,
    SerializeDictionaryValueIntoArena<type::OMNI_DECIMAL128>, // OMNI_DECIMAL128
    SerializeDictionaryValueIntoArena<type::OMNI_INT>,        // OMNI_DATE32
    SerializeDictionaryValueIntoArena<type::OMNI_LONG>,       // OMNI_DATE64
    SerializeDictionaryValueIntoArena<type::OMNI_INT>,        // OMNI_TIME32
    SerializeDictionaryValueIntoArena<type::OMNI_LONG>,       // OMNI_TIME64
    nullptr,                                                  // OMNI_TIMESTAMP
    nullptr,                                                  // OMNI_INTERVAL_MONTHS
    nullptr,                                                  // OMNI_INTERVAL_DAY_TIME
    SerializeDictionaryValueIntoArena<type::OMNI_VARCHAR>,    // OMNI_VARCHAR
    SerializeDictionaryValueIntoArena<type::OMNI_VARCHAR>,    // OMNI_CHAR,
    nullptr                                                   // OMNI_CONTAINER,
};

std::vector<VectorDeSerializer> vectorDeSerializerCenter = {
    nullptr,                                       // OMNI_NONE,
    DeserializeFromPointer<type::OMNI_INT>,        // OMNI_INT
    DeserializeFromPointer<type::OMNI_LONG>,       // OMNI_LONG
    DeserializeFromPointer<type::OMNI_DOUBLE>,     // OMNI_DOUBLE
    DeserializeFromPointer<type::OMNI_BOOLEAN>,    // OMNI_BOOLEAN
    DeserializeFromPointer<type::OMNI_SHORT>,      // OMNI_SHORT
    DeserializeFromPointer<type::OMNI_LONG>,       // OMNI_DECIMAL64,
    DeserializeFromPointer<type::OMNI_DECIMAL128>, // OMNI_DECIMAL128
    DeserializeFromPointer<type::OMNI_INT>,        // OMNI_DATE32
    DeserializeFromPointer<type::OMNI_LONG>,       // OMNI_DATE64
    DeserializeFromPointer<type::OMNI_INT>,        // /OMNI_TIME32
    DeserializeFromPointer<type::OMNI_LONG>,       // OMNI_TIME64
    nullptr,                                       // OMNI_TIMESTAMP
    nullptr,                                       // OMNI_INTERVAL_MONTHS
    nullptr,                                       // OMNI_INTERVAL_DAY_TIME
    DeserializeFromPointer<type::OMNI_VARCHAR>,    // OMNI_VARCHAR
    DeserializeFromPointer<type::OMNI_VARCHAR>,    // OMNI_CHAR,
    nullptr                                        // OMNI_CONTAINER,
};
}
}