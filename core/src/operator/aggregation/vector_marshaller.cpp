/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#include "vector_marshaller.h"
#include "operator/omni_id_type_vector_traits.h"
#include "vector/unsafe_vector.h"

namespace omniruntime {
namespace op {
template <DataTypeId id> const char *VariableTypeDeserializer(BaseVector *baseVector, size_t rowIdx, const char *pos)
{
    using RealVector = typename NativeAndVectorType<id>::vector;
    auto realVector = reinterpret_cast<RealVector *>(baseVector);
    auto stringSize = *reinterpret_cast<const int32_t *>(pos);
    pos += sizeof(int32_t);

    if (stringSize < 0) {
        // string_size < 0 means null pointer
        realVector->SetNull(static_cast<int32_t>(rowIdx));
        return pos;
    } else {
        std::string_view strView(pos, stringSize);
        realVector->SetValue(rowIdx, strView);
        return pos + stringSize;
    }
}

void VariableTypeSerializer(std::string_view &inValue, mem::SimpleArenaAllocator &arenaAllocator, StringRef &result)
{
    auto stringLen = static_cast<int32_t>(inValue.size());
    auto resLen = sizeof(int32_t) + stringLen;
    auto *&data = result.data;
    auto pos = arenaAllocator.AllocateContinue(resLen, (const uint8_t *&)(data));
    *reinterpret_cast<int32_t *>(pos) = stringLen;
    std::copy(inValue.data(), inValue.data() + stringLen, pos + sizeof(int32_t));
    result.size += resLen;
}

void NullVariableTypeSerializer(mem::SimpleArenaAllocator &arenaAllocator, StringRef &result)
{
    auto *&data = result.data;
    auto *pos = arenaAllocator.AllocateContinue(sizeof(int32_t), (const uint8_t *&)(data));
    *reinterpret_cast<int32_t *>(pos) = -1;
    result.size += sizeof(int32_t);
}

template <DataTypeId id> const char *FixedLenTypeDeserializer(BaseVector *baseVector, size_t rowIdx, const char *pos)
{
    using RawDataType = typename NativeAndVectorType<id>::type;
    using RealVector = typename NativeAndVectorType<id>::vector;
    static constexpr uint8_t RawDataSize = sizeof(RawDataType);
    auto realVector = reinterpret_cast<RealVector *>(baseVector);
    bool isNull = *(reinterpret_cast<const bool *>(pos));
    if (not isNull) {
        // must copy value
        auto *copyPointer = reinterpret_cast<const RawDataType *>(pos + 1);
        auto value = *copyPointer;
        realVector->SetValue(rowIdx, value);
    } else {
        realVector->SetNull(rowIdx);
    }
    return pos + RawDataSize + sizeof(bool);
}

void Decimal128Serializer(Decimal128 &value, mem::SimpleArenaAllocator &arenaAllocator, StringRef &result)
{
    static constexpr uint8_t RawDataSize = sizeof(Decimal128);
    auto resSize = sizeof(bool) + RawDataSize;
    auto *&data = result.data;
    auto *pos = arenaAllocator.AllocateContinue(resSize, (const uint8_t *&)(data));
    (*pos) = false;
    *reinterpret_cast<Decimal128 *>((pos + sizeof(bool))) = value;
    result.size += resSize;
}

template <typename RawDataType>
void FixedLenTypeSerializer(RawDataType value, mem::SimpleArenaAllocator &arenaAllocator, StringRef &result)
{
    static constexpr uint8_t RawDataSize = sizeof(RawDataType);
    auto resSize = sizeof(bool) + RawDataSize;
    auto *&data = result.data;
    auto *pos = arenaAllocator.AllocateContinue(resSize, (const uint8_t *&)(data));
    (*pos) = false;
    *reinterpret_cast<RawDataType *>(pos + sizeof(bool)) = value;
    result.size += resSize;
}

void NullDecimal128Serializer(mem::SimpleArenaAllocator &arenaAllocator, StringRef &result)
{
    static constexpr uint8_t RawDataSize = sizeof(Decimal128);
    auto resSize = sizeof(bool) + RawDataSize;
    auto *&data = result.data;
    auto *pos = arenaAllocator.AllocateContinue(resSize, (const uint8_t *&)(data));
    (*pos) = true;
    memset_sp(pos + sizeof(bool), RawDataSize, 0, RawDataSize);
    result.size += resSize;
}

template <typename RawDataType>
void NullFixedLenTypeSerializer(mem::SimpleArenaAllocator &arenaAllocator, StringRef &result)
{
    static constexpr uint8_t RawDataSize = sizeof(RawDataType);
    auto resSize = sizeof(bool) + RawDataSize;
    auto *&data = result.data;
    auto *pos = arenaAllocator.AllocateContinue(resSize, (const uint8_t *&)(data));
    (*pos) = true;
    *reinterpret_cast<RawDataType *>(pos + sizeof(bool)) = 0;
    result.size += resSize;
}

template <type::DataTypeId id>
void SerializeValueIntoArena(BaseVector *baseVector, int32_t rowIdx, mem::SimpleArenaAllocator &arenaAllocator,
    StringRef &result)
{
    using RawDataType = typename NativeAndVectorType<id>::type;

    if (!baseVector->IsNull(rowIdx)) {
        using RawVectorType = typename NativeAndVectorType<id>::vector;

        // not dictionary,just use cast to RawVector
        auto rawVector = reinterpret_cast<RawVectorType *>(baseVector);
        auto value = rawVector->GetValue(rowIdx);

        // the analysis of const expr  will be in compile stage
        if constexpr (std::is_same_v<RawDataType, std::string_view>) {
            VariableTypeSerializer(value, arenaAllocator, result);
        } else if constexpr (std::is_same_v<RawDataType, Decimal128>) {
            Decimal128Serializer(value, arenaAllocator, result);
        } else {
            FixedLenTypeSerializer<RawDataType>(value, arenaAllocator, result);
        }
    } else {
        if constexpr (std::is_same_v<RawDataType, std::string_view>) {
            NullVariableTypeSerializer(arenaAllocator, result);
        } else if constexpr (std::is_same_v<RawDataType, Decimal128>) {
            NullDecimal128Serializer(arenaAllocator, result);
        } else {
            NullFixedLenTypeSerializer<RawDataType>(arenaAllocator, result);
        }
    }
}

template <type::DataTypeId id>
void SerializeDictionaryValueIntoArena(BaseVector *baseVector, int32_t rowIdx,
    mem::SimpleArenaAllocator &arenaAllocator, StringRef &result)
{
    using RawDataType = typename NativeAndVectorType<id>::type;
    if (!baseVector->IsNull(rowIdx)) {
        auto dictionaryVector = reinterpret_cast<Vector<DictionaryContainer<RawDataType>> *>(baseVector);

        auto value = dictionaryVector->GetValue(rowIdx);
        // the analysis of const expr  will be in compile stage
        if constexpr (std::is_same_v<RawDataType, std::string_view>) {
            VariableTypeSerializer(value, arenaAllocator, result);
        } else if constexpr (std::is_same_v<RawDataType, Decimal128>) {
            Decimal128Serializer(value, arenaAllocator, result);
        } else {
            FixedLenTypeSerializer<RawDataType>(value, arenaAllocator, result);
        }
        return;
    }

    if constexpr (std::is_same_v<RawDataType, std::string_view>) {
        NullVariableTypeSerializer(arenaAllocator, result);
    } else if constexpr (std::is_same_v<RawDataType, Decimal128>) {
        NullDecimal128Serializer(arenaAllocator, result);
    } else {
        NullFixedLenTypeSerializer<RawDataType>(arenaAllocator, result);
    }
}

template <type::DataTypeId id>
const char *DeserializeFromPointer(BaseVector *baseVector, int32_t rowIdx, const char *&begin)
{
    using RawDataType = typename NativeAndVectorType<id>::type;
    // the analysis of const expr  will be in compile stage
    if constexpr (std::is_same_v<RawDataType, std::string_view>) {
        return VariableTypeDeserializer<id>(baseVector, rowIdx, begin);
    } else {
        return FixedLenTypeDeserializer<id>(baseVector, rowIdx, begin);
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