/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * Description: dict_container_test
 */
#include "gtest/gtest.h"
#include "vector/dictionary_container.h"
#include "test.h"
//
// Created by ken on 2022-09-19.
//

namespace omniruntime::vec::test {
template <typename T> void dict_container_get_set_value()
{
    int dictionarySize = 10;
    int valueSize = 100;
    int *values = new int[valueSize];
    for (int i = 0; i < valueSize; i++) {
        values[i] = i % dictionarySize;
    }

    using DICTIONARY_DATA_TYPE = typename TYPE_UTIL<T>::DICTIONARY_TYPE;

    auto dictionary = createDictionary<DICTIONARY_DATA_TYPE>(dictionarySize);

    omniruntime::vec::DictionaryContainer<T> container(values, valueSize, dictionary, dictionarySize, 0);

    for (int i = 0; i < valueSize; i++) {
        T value = dictionary[(i + 1) % dictionarySize];
        container.SetValue(i, value);
        EXPECT_EQ(value, container.GetValue(i));
    }
    delete[] values;
}

template <typename T> void proxy_dict_container_get_set_value()
{
    int dictionarySize = 10;
    int valueSize = 100;
    int *values = new int[valueSize];
    for (int i = 0; i < valueSize; i++) {
        values[i] = i % dictionarySize;
    }

    using DICTIONARY_DATA_TYPE = typename TYPE_UTIL<T>::DICTIONARY_TYPE;

    auto dictionary = createDictionary<DICTIONARY_DATA_TYPE>(dictionarySize);

    omniruntime::vec::DictionaryContainer<T> container(values, valueSize, dictionary, dictionarySize);
    for (int i = 0; i < valueSize; i++) {
        T value = dictionary[(i + 1) % dictionarySize];
        container.SetValue(i, value);
        T getvalue = container.GetValue(i);
        EXPECT_EQ(value, getvalue);
    }
}

TEST(vector2, dict_container_get_set_value_int32)
{
    dict_container_get_set_value<int32_t>();
}
}