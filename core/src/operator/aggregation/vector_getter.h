/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2023. All rights reserved.
 */
#ifndef OMNI_RUNTIME_VECTOR_GETTER_H
#define OMNI_RUNTIME_VECTOR_GETTER_H

#include <cstddef>
#include "vector/vector_batch.h"
#include "vector/dictionary_container.h"
#include "memory/simple_arena_allocator.h"
#include "type/string_ref.h"
#include "type/data_type.h"
#include "vector/unsafe_vector.h"
#include "operator/aggregation/container_vector.h"
#include "vector/vector_helper.h"
#include "definitions.h"
#include "util/type_util.h"

namespace omniruntime {
using namespace type;
namespace op {
using GetIdsWithOffFunction =
    std::function<int32_t *(BaseVector *vector, int32_t *IdsWithOffset, int offset, int rowCount)>;


using GetValuesFunction = std::function<void *(BaseVector *)>;

template <type::DataTypeId OmniId> void *GetValuesFromVector(BaseVector *vector)
{
    void *ptr = nullptr;
    using T = typename NativeType<OmniId>::type;
    if constexpr (std::is_same_v<std::string_view, T> || std::is_same_v<T, uint8_t>) {
        // TODO: need offsets for varChar, only return values
        auto largeStringVector =
            reinterpret_cast<Vector<LargeStringContainer<std::string_view>> *>(vector);
        ptr = unsafe::UnsafeStringVector::GetValues(largeStringVector);
    } else {
        auto rawVector = reinterpret_cast<Vector<T> *>(vector);
        ptr = unsafe::UnsafeVector::GetRawValues<T>(rawVector);
    }
    return ptr;
}

template <type::DataTypeId OmniId> void *GetValuesFromDict(BaseVector *vector)
{
    using T = typename NativeType<OmniId>::type;
    if constexpr (std::is_same_v<std::string_view, T> || std::is_same_v<T, uint8_t>) {
        // TODO: need offsets for varChar, only return values
        auto *stringDictVector =
            reinterpret_cast<Vector<DictionaryContainer<std::string_view>> *>(vector);
        return unsafe::UnsafeDictionaryVector::GetVarCharDictionary(stringDictVector);
    } else {
        auto *dictVector = reinterpret_cast<Vector<DictionaryContainer<T>> *>(vector);
        return unsafe::UnsafeDictionaryVector::GetDictionary(dictVector);
    }
}

using NewUniqueVectorFunction = std::function<void(VectorBatch *, int)>;

template <type::DataTypeId OmniId> ALWAYS_INLINE static void NewUniqueVector(VectorBatch *vectorBatch, int size)
{
    auto vector = VectorHelper::CreateVector(OMNI_FLAT, OmniId, size);
    vectorBatch->Append(vector.release());
}

template <>
ALWAYS_INLINE void NewUniqueVector<type::DataTypeId::OMNI_CONTAINER>(VectorBatch *vectorBatch, int size)
{
    auto doubleVector = new Vector<double>(size);
    auto longVector = new Vector<int64_t>(size);
    // container is used in average final stage , the inputs only include doubleVector and longVector
    std::vector<int64_t> vectorAddresses(AVG_VECTOR_COUNT);
    vectorAddresses[0] = reinterpret_cast<int64_t>(doubleVector);
    vectorAddresses[1] = reinterpret_cast<int64_t>(longVector);
    std::vector<DataTypePtr> dataTypes { DoubleType(), LongType() };
    auto containerVector = std::make_unique<ContainerVector>(size, vectorAddresses, dataTypes);
    vectorBatch->Append(containerVector.release());
}

template <typename RAW_DATA_TYPE>
static int32_t *GetIdsWithOffset(BaseVector *vector, int32_t *idsWithOffset, int offset, int rowCount)
{
    auto dictVector = reinterpret_cast<Vector<DictionaryContainer<RAW_DATA_TYPE>> *>(vector);
    return unsafe::UnsafeDictionaryVector::GetIdsWithOffset(dictVector, idsWithOffset, offset, rowCount);
}

template <> int32_t *GetIdsWithOffset<void>(BaseVector *vector, int32_t *idsWithOffset, int offset, int rowCount)
{
    return nullptr;
}

template <> int32_t *GetIdsWithOffset<std::string_view>(BaseVector *vector, int32_t *idsWithOffset, int offset,
    int rowCount)
{
    // We use the API in Vector<DictionaryContainer<T>> to obtain the value in the dictionary of string_view type,
    // instead of getting the value after obtaining valueAddress and expanding the dictionary ids.
    // Therefore, we specialize the dictionary type of string_view, and return nullptr directly,
    // which reduces the overhead of expanding ids.
    return nullptr;
}

const std::vector<GetIdsWithOffFunction> getIdsWithOffsetFunctions {
    GetIdsWithOffset<void>,             // OMNI_NONE
    GetIdsWithOffset<int32_t>,          // OMNI_INT
    GetIdsWithOffset<int64_t>,          // OMNI_LONG
    GetIdsWithOffset<double>,           // OMNI_DOUBLE
    GetIdsWithOffset<int8_t>,           // OMNI_BOOLEAN
    GetIdsWithOffset<int16_t>,          // OMNI_SHORT
    GetIdsWithOffset<int64_t>,          // OMNI_DECIMAL64
    GetIdsWithOffset<type::Decimal128>, // OMNI_DECIMAL128
    GetIdsWithOffset<int32_t>,          // OMNI_DATE32
    GetIdsWithOffset<int64_t>,          // OMNI_DATE64
    GetIdsWithOffset<int32_t>,          // OMNI_TIME32
    GetIdsWithOffset<int64_t>,          // OMNI_TIME64
    nullptr,                            // OMNI_TIMESTAMP
    nullptr,                            // OMNI_INTERVAL_MONTHS
    nullptr,                            // OMNI_INTERVAL_DAY_TIME
    GetIdsWithOffset<std::string_view>, // OMNI_VARCHAR
    GetIdsWithOffset<std::string_view>, // OMNI_CHAR
    nullptr                             // OMNI_CONTAINER
};

const std::vector<GetValuesFunction> getValuesFromDictFunctions {
    nullptr,                            // OMNI_NONE
    GetValuesFromDict<OMNI_INT>,        // OMNI_INT
    GetValuesFromDict<OMNI_LONG>,       // OMNI_LONG
    GetValuesFromDict<OMNI_DOUBLE>,     // OMNI_DOUBLE
    GetValuesFromDict<OMNI_BOOLEAN>,    // OMNI_BOOLEAN
    GetValuesFromDict<OMNI_SHORT>,      // OMNI_SHORT
    GetValuesFromDict<OMNI_DECIMAL64>,  // OMNI_DECIMAL64
    GetValuesFromDict<OMNI_DECIMAL128>, // OMNI_DECIMAL128
    GetValuesFromDict<OMNI_DATE32>,     // OMNI_DATE32
    GetValuesFromDict<OMNI_DATE64>,     // OMNI_DATE64
    GetValuesFromDict<OMNI_INT>,        // OMNI_TIME32
    GetValuesFromDict<OMNI_TIME64>,      // OMNI_TIME64
    nullptr,                             // OMNI_TIMESTAMP
    nullptr,                             // OMNI_INTERVAL_MONTHS
    nullptr,                             // OMNI_INTERVAL_DAY_TIME
    nullptr,                             // OMNI_VARCHAR
    nullptr,                             // OMNI_CHAR
    nullptr                              // OMNI_CONTAINER
};
const std::vector<GetValuesFunction> getValuesFromVectorFunctions {
    nullptr,                              // OMNI_NONE
    GetValuesFromVector<OMNI_INT>,        // OMNI_INT
    GetValuesFromVector<OMNI_LONG>,       // OMNI_LONG
    GetValuesFromVector<OMNI_DOUBLE>,     // OMNI_DOUBLE
    GetValuesFromVector<OMNI_BOOLEAN>,    // OMNI_BOOLEAN
    GetValuesFromVector<OMNI_SHORT>,      // OMNI_SHORT
    GetValuesFromVector<OMNI_DECIMAL64>,  // OMNI_DECIMAL64
    GetValuesFromVector<OMNI_DECIMAL128>, // OMNI_DECIMAL128
    GetValuesFromVector<OMNI_DATE32>,     // OMNI_DATE32
    GetValuesFromVector<OMNI_DATE64>,     // OMNI_DATE64
    GetValuesFromVector<OMNI_INT>,        // OMNI_TIME32
    GetValuesFromVector<OMNI_TIME64>,     // OMNI_TIME64
    nullptr,                               // OMNI_TIMESTAMP
    nullptr,                               // OMNI_INTERVAL_MONTHS
    nullptr,                               // OMNI_INTERVAL_DAY_TIME
    nullptr,                               // OMNI_VARCHAR
    nullptr,                               // OMNI_CHAR
    nullptr                                // OMNI_CONTAINER
};


const std::vector<NewUniqueVectorFunction> newUniqueVectorFunctions {
    nullptr,                          // OMNI_NONE
    NewUniqueVector<OMNI_INT>,        // OMNI_INT
    NewUniqueVector<OMNI_LONG>,       // OMNI_LONG
    NewUniqueVector<OMNI_DOUBLE>,     // OMNI_DOUBLE
    NewUniqueVector<OMNI_BOOLEAN>,    // OMNI_BOOLEAN
    NewUniqueVector<OMNI_SHORT>,      // OMNI_SHORT
    NewUniqueVector<OMNI_DECIMAL64>,  // OMNI_DECIMAL64
    NewUniqueVector<OMNI_DECIMAL128>, // OMNI_DECIMAL128
    NewUniqueVector<OMNI_DATE32>,     // OMNI_DATE32
    NewUniqueVector<OMNI_DATE64>,     // OMNI_DATE64
    NewUniqueVector<OMNI_TIME32>,     // OMNI_TIME32
    NewUniqueVector<OMNI_TIME64>,     // OMNI_TIME64
    nullptr,                          // OMNI_TIMESTAMP
    nullptr,                          // OMNI_INTERVAL_MONTHS
    nullptr,                          // OMNI_INTERVAL_DAY_TIME
    NewUniqueVector<OMNI_VARCHAR>,    // OMNI_VARCHAR
    NewUniqueVector<OMNI_CHAR>,       // OMNI_CHAR
    NewUniqueVector<OMNI_CONTAINER>   // OMNI_CONTAINER
};

static ALWAYS_INLINE void GetDecimalValue(BaseVector *vector, const int32_t &dataTypeId, const int32_t &rowIndex,
    int128 &decimalValue)
{
    if (vector->GetEncoding() == OMNI_DICTIONARY) {
        if (dataTypeId == OMNI_DECIMAL64) {
            decimalValue = reinterpret_cast<Vector<DictionaryContainer<long>> *>(vector)->GetValue(rowIndex);
        } else if (dataTypeId == OMNI_DECIMAL128) {
            decimalValue = reinterpret_cast<Vector<DictionaryContainer<Decimal128>> *>(vector)->GetValue(rowIndex)
                .ToInt128();
        }
    } else {
        if (dataTypeId == OMNI_DECIMAL64) {
            decimalValue = reinterpret_cast<Vector<int64_t> *>(vector)->GetValue(rowIndex);
        } else if (dataTypeId == OMNI_DECIMAL128) {
            decimalValue = reinterpret_cast<Vector<Decimal128> *>(vector)->GetValue(rowIndex).ToInt128();
        }
    }
}

}
}
#endif // OMNI_RUNTIME_VECTOR_GETTER_H
