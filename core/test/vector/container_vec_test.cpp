/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 * @Description: container vec test implementations
 */

#include "gtest/gtest.h"
#include "vector/vector.h"
#include "vector/dictionary_container.h"
#include "vector_test_util.h"

namespace omniruntime::vec::test {
template <typename T> void container_vector_get_set_value()
{
    int dictionarySize = 10;
    int valueSize = 100;
    int *values = new int[valueSize];
    std::unique_ptr<NullsBuffer> nullsBuffer = std::make_unique<NullsBuffer>(valueSize);
    for (int i = 0; i < valueSize; i++) {
        values[i] = i % dictionarySize;
        nullsBuffer->SetNull(i, false);
    }

    using DICTIONARY_DATA_TYPE = typename TYPE_UTIL<T>::DICTIONARY_TYPE;

    auto dictionary = CreateDictionary<DICTIONARY_DATA_TYPE>(dictionarySize);
    auto container = std::make_shared<DictionaryContainer<T>>(values, valueSize, dictionary, dictionarySize, 0);
    Vector<DictionaryContainer<T>> vector(valueSize, container, nullsBuffer.get());
    T value;
    for (int i = 0; i < valueSize; i++) {
        value = vector.GetValue(i);
        EXPECT_EQ(dictionary->GetValue(i % dictionarySize), value);
    }

    for (int i = 0; i < valueSize; i++) {
        value = dictionary->GetValue((i + 1) % dictionarySize);
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