#include "gtest/gtest.h"
#include "vector/dictionary_container.h"
#include "test.h"
//
// Created by ken on 2022-09-19.
//

namespace omniruntime::vec::test {
    template<typename T>
    void dict_container_get_set_value() {
        int dictionary_size = 10, value_size = 100;
        int *values = new int[value_size];
        for (int i = 0; i < value_size; i++) values[i] = i % dictionary_size;

        using DICTIONARY_DATA_TYPE = typename TYPE_UTIL<T>::DICTIONARY_TYPE;

        auto dictionary = createDictionary<DICTIONARY_DATA_TYPE>(dictionary_size);

        omniruntime::vec::DictionaryContainer<T> container(values, value_size, dictionary, dictionary_size, 0);

        for (int i = 0; i < value_size; i++) {
            T value = dictionary[(i + 1) % dictionary_size];
            container.SetValue(i, value);
            EXPECT_EQ(value, container.GetValue(i));
        }
        delete[] values;
    }

    template<typename T>
    void proxy_dict_container_get_set_value() {
        int dictionary_size = 10, value_size = 100;
        int *values = new int[value_size];
        for (int i = 0; i < value_size; i++) values[i] = i % dictionary_size;

        using DICTIONARY_DATA_TYPE = typename TYPE_UTIL<T>::DICTIONARY_TYPE;

        auto dictionary = createDictionary<DICTIONARY_DATA_TYPE>(dictionary_size);

        omniruntime::vec::DictionaryContainer<T> container(values, value_size, dictionary, dictionary_size);
        for (int i = 0; i < value_size; i++) {
            T value = dictionary[(i + 1) % dictionary_size];
            container.SetValue(i, value);
            T getvalue = container.GetValue(i);
            EXPECT_EQ(value, getvalue);
        }
    }

    TEST(vector2, dict_container_get_set_value_int32) {
        dict_container_get_set_value<int32_t>();
    }
}