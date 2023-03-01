/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#include "gtest/gtest.h"
#include "vector/unsafe_vector.h"
#include "test.h"
#include "vector/vector_helper.h"

namespace omniruntime::vec::test {
using namespace omniruntime::vec::unsafe;

template <typename T>
std::shared_ptr<Vector<T>> CreateVectorAndSetValue(int32_t size)
{
    auto vector = std::make_shared<Vector<T>>(size);
    for (int32_t i = 0; i < size; i++) {
        T value;
        if constexpr (std::is_same_v<std::string, T>) {
            value = "string " + std::to_string(i);
        } else {
            value = (T)i * 2 / 3;
        }
        vector->SetValue(i, value);
    }
    return vector;
}

template <typename T>
std::shared_ptr<Vector<DictionaryContainer<T>>> CreateDictionaryVector()
{
    int dictionary_size = 10, value_size = 100;
    int *values = new int[value_size];
    std::shared_ptr<bool[]> nulls = std::shared_ptr<bool[]>(new bool[value_size]);
    for (int i = 0; i < value_size; i++) {
        nulls[i] = false;
        values[i] = i % dictionary_size;
    }

    using DICTIONARY_DATA_TYPE = typename TYPE_UTIL<T>::DICTIONARY_TYPE;
    auto dictionary = createDictionary<DICTIONARY_DATA_TYPE>(dictionary_size);

    auto container = std::make_shared<DictionaryContainer<T>>(values, value_size, dictionary, dictionary_size, 0);
    auto vector = std::make_shared<Vector<DictionaryContainer<T>>>(value_size, container, nulls);
    delete[] values;
    return vector;
}

template<typename CONTAINER>
std::shared_ptr<BaseVector> CreateStringTestVector()
{
    int value_size = 1000;
    uint32_t stringWidth = OMNI_LARGE_WIDTH;

    std::string valuePrefix;
    if constexpr (std::is_same_v<SmallStringContainer<std::string_view>, CONTAINER>) {
        stringWidth = OMNI_SMALL_WIDTH;
        valuePrefix = "hello__";
    } else {
        valuePrefix = "hello_world__";
    }

    auto baseVector = VectorHelper::CreateStringVector(value_size, stringWidth);
    auto *vector = (Vector<CONTAINER> *)baseVector.get();

    for (int i = 0; i < value_size; i++) {
        std::string value = valuePrefix + std::to_string(i);
        std::string_view input(value.data(), value.size());
        vector->SetValue(i, input);
    }
    return baseVector;
}

void VectorSetNull(BaseVector *vector)
{
    int size = 100;
    for (int i = 0; i < size; i++) {
        if (i % 2) {
            vector->SetNull(i);
        }
    }
}

template <typename DATA_TYPE>
void vector_get_values_and_nulls()
{
    int size = 100;
    auto vector = CreateVectorAndSetValue<DATA_TYPE>(size);
    VectorSetNull(vector.get());

    bool *vectorNulls = UnsafeBaseVector::GetNulls(vector.get());

    DATA_TYPE *vectorValues = UnsafeVector::GetRawValues(vector.get());

    for (int i = 0; i < size; i++) {
        if (i % 2) {
            EXPECT_TRUE(vectorNulls[i]);
        } else {
            EXPECT_EQ(vectorValues[i], vector->GetValue(i));
        }
    }
}

template <typename DATA_TYPE>
void vector_slice_get_values_and_nulls()
{
    int size = 100;
    int offSet = 50;
    int sliceLength = 50;

    auto vector = CreateVectorAndSetValue<DATA_TYPE>(size);
    VectorSetNull(vector.get());

    auto vector2 = vector->Slice(offSet, sliceLength);

    bool *vectorNulls = UnsafeBaseVector::GetNulls(vector2.get());
    DATA_TYPE *vectorValues = UnsafeVector::GetRawValues(vector2.get());

    for (int i = 0; i < sliceLength; i++) {
        if (i % 2) {
            EXPECT_TRUE(vectorNulls[i]);
        } else {
            EXPECT_EQ(vectorValues[i], vector2->GetValue(i));
        }
    }
}

template <typename DATA_TYPE>
void dictionary_vector_get_values_and_nulls()
{
    int size = 100;

    auto vector = CreateDictionaryVector<DATA_TYPE>();
    VectorSetNull(vector.get());

    bool *vectorNulls = UnsafeBaseVector::GetNulls(vector.get());
    DATA_TYPE *vectorValues = UnsafeDictionaryVector::GetDictionary(vector.get());
    int *valuesIds = UnsafeDictionaryVector::GetIds(vector.get());

    for (int i = 0; i < size; i++) {
        if (i % 2) {
            EXPECT_TRUE(vectorNulls[i]);
        } else {
            EXPECT_EQ(vectorValues[valuesIds[i]], vector->GetValue(i));
        }
    }
}

void dictionary_string_vector_get_values_and_nulls()
{
    int dictionary_size = 10;
    auto vector = VectorHelper::CreateStringVector(dictionary_size);
    auto stringVector = reinterpret_cast<Vector<LargeStringContainer<std::string_view>> *>(vector.get());

    for (int i = 0; i < dictionary_size; i++) {
        if (i % 2 == 0) {
            stringVector->SetNull(i);
            continue;
        }
        std::string value = "hello___world" + std::to_string(i);
        std::string_view input(value.data(), value.size());
        stringVector->SetValue(i, input);
    }

    int size = 100;
    int *values = new int[size];
    for (int i = 0; i < size; i++) {
        values[i] = i % dictionary_size;
    }

    std::unique_ptr<BaseVector> vectorPtr = VectorHelper::CreateStringDictionary(values, size, stringVector);
    auto dictVector =
        reinterpret_cast<Vector<DictionaryContainer<std::string_view, LargeStringContainer>> *>(vectorPtr.get());

    bool *vectorNulls = UnsafeBaseVector::GetNulls(dictVector);
    char *vectorValues = UnsafeDictionaryVector::GetVarCharDictionary(dictVector);
    int *valuesIds = UnsafeDictionaryVector::GetIds(dictVector);
    auto vectorOffsets = UnsafeDictionaryVector::GetDictionaryOffsets(dictVector);
    for (int i = 0; i < size; i++) {
        if (i % 2 == 0) {
            EXPECT_TRUE(vectorNulls[i]);
        } else {
            EXPECT_EQ(vectorValues + vectorOffsets[valuesIds[i]], dictVector->GetValue(i).data());
        }
    }
    delete[] values;
}

template <typename DATA_TYPE>
void dictionary_vector_slice_get_values_and_nulls()
{
    int offSet = 50;
    int sliceLength = 50;

    auto vector = CreateDictionaryVector<DATA_TYPE>();
    VectorSetNull(vector.get());

    auto vector2 = vector->Slice(offSet, sliceLength);

    bool *vectorNulls = UnsafeBaseVector::GetNulls(vector2.get());
    DATA_TYPE *vectorValues = UnsafeDictionaryVector::GetDictionary(vector2.get());
    int *valuesIds = UnsafeDictionaryVector::GetIds(vector2.get());

    for (int i = 0; i < sliceLength; i++) {
        if (i % 2) {
            EXPECT_TRUE(vectorNulls[i]);
        } else {
            EXPECT_EQ(vectorValues[valuesIds[i]], vector2->GetValue(i));
        }
    }
}

template<typename CONTAINER>
void string_vector_get_values_and_nulls()
{
    int size = 100;

    auto baseVector = CreateStringTestVector<CONTAINER>();
    VectorSetNull(baseVector.get());

    bool *vectorNulls = UnsafeBaseVector::GetNulls(baseVector.get());
    auto *vector = (Vector<CONTAINER> *)baseVector.get();
    char *vectorValues = UnsafeStringVector::GetValues(vector);
    auto valueOffsets = reinterpret_cast<int32_t *>(VectorHelper::GetOffsetsAddr(vector, OMNI_VARCHAR));
    for (int i = 0; i < size; i++) {
        if (i % 2) {
            EXPECT_TRUE(vectorNulls[i]);
        } else {
            EXPECT_EQ(vectorValues + valueOffsets[i], vector->GetValue(i).data());
        }
    }
}

template<typename CONTAINER>
void string_vector_slice_get_values_and_nulls()
{
    int offSet = 50;
    int sliceLength = 50;

    auto baseVector = CreateStringTestVector<CONTAINER>();
    VectorSetNull(baseVector.get());

    auto *vector = (Vector<CONTAINER> *)baseVector.get();
    auto vector2 = vector->Slice(offSet, sliceLength);

    bool *vectorNulls = UnsafeBaseVector::GetNulls(vector2.get());
    char *vectorValues = UnsafeStringVector::GetValues(vector2.get());
    auto valueOffsets = reinterpret_cast<int32_t *>(VectorHelper::GetOffsetsAddr(vector2.get(), OMNI_VARCHAR));
    for (int i = 0; i < sliceLength; i++) {
        if (i % 2) {
            EXPECT_TRUE(vectorNulls[i]);
        } else {
            EXPECT_EQ(vectorValues + valueOffsets[i], vector2->GetValue(i).data());
        }
    }
}

template<typename CONTAINER>
void string_vector_get_string_buffer()
{
    auto baseVector = CreateStringTestVector<CONTAINER>();
    auto *vector = (Vector<CONTAINER> *)baseVector.get();
    auto vectorCapacityInBytes = UnsafeStringContainer::GetCapacityInBytes(
            unsafe::UnsafeStringVector::GetContainer(vector).get());

    auto real = UnsafeStringContainer::GetStringBufferAddr(
            unsafe::UnsafeStringVector::GetContainer(vector).get());

    auto expect = UnsafeStringContainer::GetBufferWithSpace(
            unsafe::UnsafeStringVector::GetContainer(vector).get(), vectorCapacityInBytes);

    EXPECT_EQ(expect, real);

    auto newCapacityInBytes = UnsafeStringContainer::GetCapacityInBytes(
            unsafe::UnsafeStringVector::GetContainer(vector).get());
    EXPECT_EQ(vectorCapacityInBytes, newCapacityInBytes);
}

template<typename CONTAINER>
void string_vector_get_string_expand_buffer()
{
    int requestSize = INITIAL_STRING_SIZE + 10;
    auto baseVector = CreateStringTestVector<CONTAINER>();
    auto *vector = (Vector<CONTAINER> *)baseVector.get();
    auto vectorCapacityInBytes = UnsafeStringContainer::GetCapacityInBytes(
            unsafe::UnsafeStringVector::GetContainer(vector).get());

    auto expect = UnsafeStringContainer::GetBufferWithSpace(
            unsafe::UnsafeStringVector::GetContainer(vector).get(), vectorCapacityInBytes + requestSize);

    auto real = UnsafeStringContainer::GetStringBufferAddr(
            unsafe::UnsafeStringVector::GetContainer(vector).get());
    EXPECT_EQ(expect, real);

    auto newCapacityInBytes = UnsafeStringContainer::GetCapacityInBytes(
            unsafe::UnsafeStringVector::GetContainer(vector).get());
    EXPECT_GE(newCapacityInBytes, vectorCapacityInBytes + requestSize);
}

template<typename CONTAINER>
void string_vector_get_string_first_buffer()
{
    int value_size = 1000;

    auto baseVector = VectorHelper::CreateStringVector(value_size);
    auto *vector = (Vector<CONTAINER> *)baseVector.get();

    int requestSize = 10;
    auto expect = UnsafeStringContainer::GetBufferWithSpace(
            unsafe::UnsafeStringVector::GetContainer(vector).get(), requestSize);

    auto real = UnsafeStringContainer::GetStringBufferAddr(
            unsafe::UnsafeStringVector::GetContainer(vector).get());
    EXPECT_EQ(expect, real);

    auto newCapacityInBytes = UnsafeStringContainer::GetCapacityInBytes(
            unsafe::UnsafeStringVector::GetContainer(vector).get());
    EXPECT_EQ(newCapacityInBytes, INITIAL_STRING_SIZE);
}

TEST(unsafe_vector, int_get_values_and_nulls)
{
    vector_get_values_and_nulls<int32_t>();
}

TEST(unsafe_vector, int_slice_get_values_and_nulls)
{
    vector_slice_get_values_and_nulls<int32_t>();
}

TEST(unsafe_vector, string_get_values_and_nulls)
{
    vector_get_values_and_nulls<std::string>();
}

TEST(unsafe_vector, string_slice_get_values_and_nulls)
{
    vector_slice_get_values_and_nulls<std::string>();
}

TEST(unsafe_vector, int_dictionary_get_values_and_nulls)
{
    dictionary_vector_get_values_and_nulls<int32_t>();
}

TEST(unsafe_vector, string_dictionary_get_values_and_nulls)
{
    dictionary_string_vector_get_values_and_nulls();
}

TEST(unsafe_vector, int_dictionary_slice_get_values_and_nulls)
{
    dictionary_vector_slice_get_values_and_nulls<int32_t>();
}

TEST(unsafe_vector, small_string_get_values_and_nulls)
{
    // TODO: small string with offsets
    //  string_vector_get_values_and_nulls<SmallStringContainer<std::string_view>>();
}

TEST(unsafe_vector, small_string_slice_get_values_and_nulls)
{
    // TODO: small string with offsets
    //  string_vector_slice_get_values_and_nulls<SmallStringContainer<std::string_view>>();
}

TEST(unsafe_vector, large_string_get_values_and_nulls)
{
    string_vector_get_values_and_nulls<LargeStringContainer<std::string_view>>();
}

TEST(unsafe_vector, large_string_slice_get_values_and_nulls)
{
    string_vector_slice_get_values_and_nulls<LargeStringContainer<std::string_view>>();
}

TEST(unsafe_vector, string_vector_get_string_buffer)
{
    string_vector_get_string_buffer<LargeStringContainer<std::string_view>>();
}

TEST(unsafe_vector, string_vector_get_string_expand_buffer)
{
    string_vector_get_string_expand_buffer<LargeStringContainer<std::string_view>>();
}

TEST(unsafe_vector, string_vector_get_string_first_buffer)
{
    string_vector_get_string_first_buffer<LargeStringContainer<std::string_view>>();
}
}
