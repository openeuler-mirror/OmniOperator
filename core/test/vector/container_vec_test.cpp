#include "gtest/gtest.h"
#include "vector/vector.h"
#include "vector/dictionary_container.h"
#include "test.h"
//
// Created by ken on 2022-09-19.
//

namespace omniruntime::vec::test {
template <typename T> void container_vector_get_set_value()
{
    int dictionary_size = 10, value_size = 100;
    int *values = new int[value_size];
    std::shared_ptr<bool[]> nulls = std::shared_ptr<bool[]>(new bool[value_size]);
    for (int i = 0; i < value_size; i++) {
        values[i] = i % dictionary_size;
        nulls[i] = false;
    }

    using DICTIONARY_DATA_TYPE = typename TYPE_UTIL<T>::DICTIONARY_TYPE;

    auto dictionary = createDictionary<DICTIONARY_DATA_TYPE>(dictionary_size);
    auto container = std::make_shared<DictionaryContainer<T>>(values, value_size, dictionary, dictionary_size, 0);
    Vector<DictionaryContainer<T>> vector(value_size, container, nulls);
    T value;
    for (int i = 0; i < value_size; i++) {
        value = vector.GetValue(i);
        EXPECT_EQ(dictionary[i % dictionary_size], value);
    }

    for (int i = 0; i < value_size; i++) {
        value = dictionary[(i + 1) % dictionary_size];
        vector.SetValue(i, value);
        EXPECT_EQ(value, vector.GetValue(i));
    }

    delete[] values;
}

TEST(vector2, container_vector_get_set_int32)
{
    container_vector_get_set_value<int32_t>();
}
TEST(vector2, container_vector_get_set_double)
{
    container_vector_get_set_value<double>();
}
}