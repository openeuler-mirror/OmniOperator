/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 * Description: test
 */
#ifndef OMNI_RUNTIME_VECTOR_TEST_UTIL_H
#define OMNI_RUNTIME_VECTOR_TEST_UTIL_H

#include "vector/vector_helper.h"
#include "boost/multiprecision/cpp_dec_float.hpp"
#include "boost/multiprecision/number.hpp"

namespace omniruntime::vec::test {
using boost_dec32 = boost::multiprecision::number<boost::multiprecision::cpp_dec_float<32>>;
using boost_dec64 = boost::multiprecision::number<boost::multiprecision::cpp_dec_float<64>>;
using boost_dec128 = boost::multiprecision::number<boost::multiprecision::cpp_dec_float<128>>;

template <typename T> struct TYPE_UTIL {
    using DICTIONARY_TYPE = std::conditional_t<std::is_same_v<T, std::string_view>, std::string, T>;
};


template <typename RAW_DATA_TYPE, typename DICTIONARY_DATA_TYPE = typename TYPE_UTIL<RAW_DATA_TYPE>::DICTIONARY_TYPE>
std::shared_ptr<AlignedBuffer<DICTIONARY_DATA_TYPE>> CreateDictionary(int size)
{
    std::shared_ptr<AlignedBuffer<DICTIONARY_DATA_TYPE>> dictionaryBuffer(
        new AlignedBuffer<DICTIONARY_DATA_TYPE>(size));
    auto dictionary = dictionaryBuffer->GetBuffer();
    for (int i = 0; i < size; i++) {
        dictionary[i] = static_cast<RAW_DATA_TYPE>(i) * 2 / 3;
    }
    return dictionaryBuffer;
}

template <typename T, template <typename> typename CONTAINER>
std::shared_ptr<CONTAINER<T>> CreateStringDictionary(int size)
{
    auto container = std::make_shared<CONTAINER<T>>(size);
    for (int i = 0; i < size; i++) {
        if constexpr (std::is_same_v<std::string_view, T>) {
            std::string data = "hello world " + std::to_string(i);
            std::string_view view(data.data(), data.length());
            container->SetValue(i, view);
        }
    }
    return container;
}

template <typename T> std::unique_ptr<Vector<T>> CreateVectorAndSetValue(int32_t size)
{
    auto vector = std::make_unique<Vector<T>>(size);
    for (int32_t i = 0; i < size; i++) {
        T value = static_cast<T>(i) * 2 / 3;
        vector->SetValue(i, value);
    }
    return vector;
}

template <typename T>
std::unique_ptr<Vector<DictionaryContainer<T>>> CreateDictionaryVector(int dictionarySize, int valueSize)
{
    int *values = new int[valueSize];
    std::unique_ptr<NullsBuffer> nullsBuffer = std::make_unique<NullsBuffer>(valueSize);
    for (int i = 0; i < valueSize; i++) {
        nullsBuffer->SetNull(i, false);
        values[i] = i % dictionarySize;
    }

    using DICTIONARY_DATA_TYPE = typename TYPE_UTIL<T>::DICTIONARY_TYPE;
    auto dictionary = CreateDictionary<DICTIONARY_DATA_TYPE>(dictionarySize);

    auto container = std::make_shared<DictionaryContainer<T>>(values, valueSize, dictionary, dictionarySize, 0);
    auto vector = std::make_unique<Vector<DictionaryContainer<T>>>(valueSize, container, nullsBuffer.get());
    delete[] values;
    return vector;
}

template <typename T>
std::unique_ptr<Vector<DictionaryContainer<T>>> CreateStringDictionaryVector(int dictionarySize, int valueSize)
{
    int *values = new int[valueSize];
    std::unique_ptr<NullsBuffer> nullsBuffer = std::make_unique<NullsBuffer>(valueSize);
    for (int i = 0; i < valueSize; i++) {
        nullsBuffer->SetNull(i, false);
        values[i] = i % dictionarySize;
    }

    auto dictionary = CreateStringDictionary<std::string_view, LargeStringContainer>(dictionarySize);
    auto container = std::make_shared<DictionaryContainer<std::string_view, LargeStringContainer>>(values, valueSize,
        dictionary, dictionarySize, 0);
    auto vector = std::make_unique<Vector<DictionaryContainer<std::string_view, LargeStringContainer>>>(valueSize,
        container, nullsBuffer.get());
    delete[] values;
    return vector;
}

template <typename CONTAINER> BaseVector *CreateStringTestVector(int valueSize)
{
    std::string valuePrefix = "hello_world__";

    auto baseVector = VectorHelper::CreateStringVector(valueSize);
    auto *vector = (Vector<CONTAINER> *)baseVector;

    for (int i = 0; i < valueSize; i++) {
        std::string value = valuePrefix + std::to_string(i);
        std::string_view input(value.data(), value.size());
        vector->SetValue(i, input);
    }
    return baseVector;
}
}
#endif // OMNI_RUNTIME_VECTOR_TEST_UTIL_H
