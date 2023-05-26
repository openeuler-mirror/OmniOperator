/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * Description: slice_test
 */
#include "gtest/gtest.h"
#include "vector/vector.h"
#include "test.h"
#include "vector/dictionary_container.h"

namespace omniruntime::vec::test {
int g_vecSize = 100;
int g_offset = 50;
int g_len = 10;

template <typename T> void v2_slice_get_set_value()
{
    auto parent = new Vector<T>(g_vecSize);
    for (int i = 0; i < g_vecSize; i++) {
        T value;
        if constexpr (std::is_same_v<std::string, T>) {
            value = "string " + std::to_string(i);
        } else {
            value = T(i * 2 / 3);
        }
        parent->SetValue(i, value);
    }

    auto vector = parent->Slice(g_offset, g_len, false);
    EXPECT_EQ(vector->GetDataTypeId(), parent->GetDataTypeId());
    delete parent;

    for (int i = 0; i < g_len; i++) {
        T value;
        if constexpr (std::is_same_v<std::string, T>) {
            value = "string " + std::to_string(i + g_offset);
        } else {
            value = T((i + g_offset) * 2 / 3);
        }
        EXPECT_EQ(value, vector->GetValue(i));
    }
    delete vector;
}

TEST(vector2, v2_slice_get_set_value_int32)
{
    v2_slice_get_set_value<int32_t>();
}
TEST(vector2, v2_slice_get_set_value_int64)
{
    v2_slice_get_set_value<int64_t>();
}
TEST(vector2, v2_slice_get_set_value_double)
{
    v2_slice_get_set_value<double>();
}
TEST(vector2, v2_slice_get_set_value_string)
{
    v2_slice_get_set_value<std::string>();
}
TEST(vector2, v2_slice_get_set_value_dec32)
{
    v2_slice_get_set_value<boost_dec32>();
}
TEST(vector2, v2_slice_get_set_value_dec64)
{
    v2_slice_get_set_value<boost_dec64>();
}
TEST(vector2, v2_slice_get_set_value_dec128)
{
    v2_slice_get_set_value<boost_dec128>();
}

template <typename T> void v2_slice_container()
{
    int dictionary_size = 10;
    int value_size = 100;
    int *values = new int[value_size];
    std::shared_ptr<bool[]> nulls = std::shared_ptr<bool[]>(new bool[value_size]);
    for (int i = 0; i < value_size; i++) {
        values[i] = i % dictionary_size;
        nulls[i] = false;
    }

    using DICTIONARY_DATA_TYPE = typename TYPE_UTIL<T>::DICTIONARY_TYPE;

    auto dictionary = createDictionary<DICTIONARY_DATA_TYPE>(dictionary_size);
    auto container = std::make_shared<DictionaryContainer<T>>(values, value_size, dictionary, dictionary_size, 0);
    auto parent = new Vector<DictionaryContainer<T>>(value_size, container, nulls, TYPE_ID<T>);

    auto vector = parent->Slice(g_offset, g_len);
    delete parent; // purposely deleting parent;

    T value;
    for (int i = 0; i < g_len; i++) {
        value = vector->GetValue(i);
        EXPECT_EQ(dictionary[i % dictionary_size], value);
    }

    delete[] values;
    delete vector;
}

TEST(vector2, slice_container_int32)
{
    v2_slice_container<int32_t>();
}

TEST(vector2, slice_container_int64)
{
    v2_slice_container<int64_t>();
}

TEST(vector2, slice_container_double)
{
    v2_slice_container<double>();
}
}