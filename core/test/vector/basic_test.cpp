/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * Description: basic_test
 */
#include "gtest/gtest.h"
#include "vector/vector.h"
#include "test.h"
#include "boost/multiprecision/number.hpp"
#include "vector/dictionary_container.h"
#include "vector/vector_helper.h"
#include "type/data_type.h"

namespace omniruntime::vec::test {
template <typename T> void vector_get_set_value()
{
    int vec_size = 100;
    auto vector = new Vector<T>(vec_size);
    for (int i = 0; i < vec_size; i++) {
        T value;
        if constexpr (std::is_same_v<std::string, T>) {
            value = "string " + std::to_string(i);
        } else {
            value = (T)i * 2 / 3;
        }
        vector->SetValue(i, value);
    }

    for (int i = 0; i < vec_size; i++) {
        T value;
        if constexpr (std::is_same_v<std::string, T>) {
            value = "string " + std::to_string(i);
        } else {
            value = (T)i * 2 / 3;
        }
        EXPECT_EQ(value, vector->GetValue(i));
    }
    delete vector;
}

template <typename T> void vector_has_null()
{
    int vec_size = 100;
    auto vector = new Vector<T>(vec_size);
    for (int i = 0; i < vec_size; i++) {
        T value;
        if constexpr (std::is_same_v<std::string, T>) {
            value = "string " + std::to_string(i);
        } else {
            value = (T)i * 2 / 3;
        }
        vector->SetValue(i, value);
    }

    bool hasNull = vector->HasNull();
    EXPECT_EQ(hasNull, false);

    vector->SetNull(vec_size - 1);
    hasNull = vector->HasNull();
    EXPECT_EQ(hasNull, true);

    for (int i = 0; i < vec_size; i++) {
        if (i % 2 == 0) {
            vector->SetNull(i);
            continue;
        }

        T value;
        if constexpr (std::is_same_v<std::string, T>) {
            value = "string " + std::to_string(i);
        } else {
            value = (T)i * 2 / 3;
        }
        vector->SetValue(i, value);
    }

    hasNull = vector->HasNull();
    EXPECT_EQ(hasNull, true);

    for (int i = 0; i < vec_size; i++) {
        vector->SetNull(i);
    }

    hasNull = vector->HasNull();
    EXPECT_EQ(hasNull, true);
    delete vector;
}

template <typename T> void dict_vector_get_set_value()
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

    auto container = std::make_shared<DictionaryContainer<T>>(values, value_size, dictionary, dictionary_size);
    Vector<DictionaryContainer<T>> vector(value_size, container, nulls);

    T value;
    for (int i = 0; i < value_size; i++) {
        value = vector.GetValue(i);
        EXPECT_EQ(dictionary[i % dictionary_size], value);
    }

    for (int i = 0; i < value_size; i++) {
        value = dictionary[(i + 1) % dictionary_size];
        vector.SetValue(i, value);
        EXPECT_EQ(dictionary[(i + 1) % dictionary_size], value);
    }
}

template <typename T> void dict_vector_get_value_with_null()
{
    int dicSize = 10, valueSize = 100;
    int *values = new int[valueSize];
    for (int i = 0; i < valueSize; i++) {
        values[i] = i % dicSize;
    }

    std::shared_ptr<Vector<T>> dictionary = std::make_shared<Vector<T>>(dicSize);
    for (int i = 0; i < dicSize; i++) {
        if (i % 2 == 0) {
            dictionary.get()->SetNull(i);
            continue;
        }
        T value = (T)(i * 2 / 3);
        dictionary->SetValue(i, value);
    }

    std::shared_ptr<BaseVector> vectorPtr = VectorHelper::CreateDictionary(values, valueSize, dictionary.get());
    auto *vector = reinterpret_cast<Vector<DictionaryContainer<T>> *>(vectorPtr.get());

    T value;
    for (int i = 0; i < valueSize; i++) {
        if (values[i] % 2 == 0) {
            EXPECT_EQ(dictionary.get()->IsNull(values[i]), vector->IsNull(i));
            continue;
        }
        value = vector->GetValue(i);
        EXPECT_EQ(dictionary->GetValue(i % dicSize), value);
    }
    delete[] values;
}

template <typename T, template <typename> typename CONTAINER> void dict_vector_get_value_string_with_null()
{
    int dicSize = 10, valueSize = 100;
    auto *values = new int32_t[valueSize];
    for (int i = 0; i < valueSize; i++) {
        values[i] = i % dicSize;
    }

    std::shared_ptr<Vector<CONTAINER<T>>> dictionary = std::make_shared<Vector<CONTAINER<T>>>(dicSize);

    std::string valuePrefix;
    if constexpr (std::is_same_v<SmallStringContainer<std::string_view>, CONTAINER<T>>) {
        valuePrefix = "hello__";
    } else {
        valuePrefix = "hello_world__";
    }

    for (int i = 0; i < dicSize; i++) {
        if (i % 2 == 0) {
            dictionary->SetNull(i);
            continue;
        }
        std::string value = valuePrefix + std::to_string(i);
        std::string_view input(value.data(), value.length());
        dictionary->SetValue(i, input);
    }

    std::shared_ptr<BaseVector> vectorPtr =
        VectorHelper::CreateDictionary(values, valueSize, reinterpret_cast<Vector<T> *>(dictionary.get()));
    auto *vector = reinterpret_cast<Vector<DictionaryContainer<T, CONTAINER>> *>(vectorPtr.get());

    for (int i = 0; i < valueSize; i++) {
        if (values[i] % 2 == 0) {
            EXPECT_EQ(dictionary->IsNull(values[i]), vector->IsNull(i));
            continue;
        }
        std::string_view output = vector->GetValue(i);
        EXPECT_EQ(dictionary->GetValue(i % dicSize), output);
    }
    delete[] values;
}

template <typename T> T GetTestValue(int32_t index)
{
    T value;
    if constexpr (std::is_same_v<std::string, T> || std::is_same_v<std::string_view, T>) {
        value = "string " + std::to_string(index);
    } else {
        value = (T)index * 2 / 3;
    }
    return value;
}

template <typename T> void vector_append_value()
{
    int vecSize = 5;
    Vector<T> v1{ vecSize };
    std::vector<T> expected;
    for (int32_t i = 0; i < vecSize; i++) {
        T value = GetTestValue<T>(i);
        v1.SetValue(i, value);
        expected.template emplace_back(value);
    }

    int32_t appendedVecSize = 15;
    Vector<T> appended{ appendedVecSize };
    appended.Append(&v1, 0, vecSize);

    Vector<T> v2WithNull{ vecSize };
    for (int32_t i = 0; i < vecSize; i++) {
        if (i % 2 == 0) {
            v2WithNull.SetNull(i);
            continue;
        }
        v2WithNull.SetValue(i, expected[i]);
    }
    appended.Append(&v2WithNull, 5, 5);

    Vector<T> v3Emtpy{ 0 };
    appended.Append(&v3Emtpy, 10, 0);

    Vector<T> v4OverBounds{ vecSize };
    for (int32_t i = 0; i < vecSize; i++) {
        v4OverBounds.SetValue(i, expected[i]);
    }
    appended.Append(&v4OverBounds, 10, vecSize + 1);

    std::vector<bool> expectedNull{ false, false, false, false, false, true, false, true, false, true };
    for (int32_t i = 0; i < appendedVecSize; i++) {
        // append empty vector or beyond the bound
        if (i >= 10) {
            EXPECT_FALSE(appended.IsNull(i));
            // for number it is random value
            if constexpr (std::is_same_v<std::string, T>) {
                EXPECT_EQ("", appended.GetValue(i));
            }
            continue;
        }
        // append success for value check
        if (appended.IsNull(i)) {
            EXPECT_EQ(expectedNull[i], appended.IsNull(i));
            continue;
        }
        EXPECT_EQ(expected[i % 5], appended.GetValue(i));
    }
}

template <typename T> void vector_copy_positions_value()
{
    int vecSize = 10;
    Vector<T> vector{ vecSize };
    std::vector<T> expected;
    for (int32_t i = 0; i < vecSize; i++) {
        T value = GetTestValue<T>(i);
        expected.template emplace_back(value);
        if (i % 2 == 0) {
            vector.SetNull(i);
            continue;
        }
        vector.SetValue(i, value);
    }

    int index[] = {2, 3, 4, 5, 6, 7};
    int offset1 = 0;
    int offset2 = 1;
    int copySize = 4;
    auto v1OffsetZero = vector.CopyPositions(index, offset1, copySize);
    auto v2OffsetNotZero = vector.CopyPositions(index, offset2, copySize);

    for (int32_t i = 0; i < copySize; i++) {
        if (i % 2 == 0) {
            EXPECT_EQ(v1OffsetZero->IsNull(i), true);
            EXPECT_EQ(v2OffsetNotZero->GetValue(i), expected[index[i + offset2]]);
            continue;
        }
        EXPECT_EQ(v2OffsetNotZero->IsNull(i), true);
        EXPECT_EQ(v1OffsetZero->GetValue(i), expected[index[i + offset1]]);
    }

    auto v3Empty = vector.CopyPositions(index, offset2, 0);
    EXPECT_EQ(v3Empty->GetSize(), 0);
    auto v4OverBounds = vector.CopyPositions(index, offset1, -1);
    EXPECT_EQ(v4OverBounds, nullptr);
}

template <typename T> void dict_copy_positions_value()
{
    int dicSize = 10, valueSize = 7;
    std::shared_ptr<Vector<T>> dictionary = std::make_shared<Vector<T>>(dicSize);
    for (int i = 0; i < dicSize; i++) {
        if (i % 2 == 0) {
            dictionary->SetNull(i);
            continue;
        }
        T value = (T)(i);
        dictionary->SetValue(i, value);
    }

    int32_t values[] = {2, 3, 4, 5, 6, 8, 9};
    std::unique_ptr<BaseVector> vectorPtr = VectorHelper::CreateDictionary(values, valueSize, dictionary.get());
    auto vector = reinterpret_cast<Vector<DictionaryContainer<T>> *>(vectorPtr.get());

    int32_t positions[] = {1, 3, 5, 6};
    int32_t offset = 1;
    int32_t newValueSize = 3;
    auto copyPositions = vector->CopyPositions(positions, offset, newValueSize);

    T value;
    for (int i = 0; i < newValueSize; i++) {
        if (values[positions[i + offset]] % 2 == 0) {
            EXPECT_EQ(dictionary->IsNull(values[positions[i + offset]]), copyPositions->IsNull(i));
            continue;
        }
        value = copyPositions->GetValue(i);
        EXPECT_EQ(dictionary->GetValue(values[positions[i + offset]]), value);
    }

    auto v3Empty = vector->CopyPositions(positions, offset, 0);
    EXPECT_EQ(v3Empty->GetSize(), 0);
    auto v4OverBounds = vector->CopyPositions(positions, offset, -1);
    EXPECT_EQ(v4OverBounds, nullptr);
}

template <> void dict_copy_positions_value<std::string_view>()
{
    int dicSize = 10, valueSize = 7;
    std::shared_ptr<Vector<LargeStringContainer<std::string_view>>> dictionary =
        std::make_shared<Vector<LargeStringContainer<std::string_view>>>(dicSize);
    for (int i = 0; i < dicSize; i++) {
        if (i % 2 == 0) {
            dictionary->SetNull(i);
            continue;
        }
        auto str = GetTestValue<std::string>(i);
        std::string_view value(str.data(), str.length());
        dictionary->SetValue(i, value);
    }

    int32_t values[] = {2, 3, 4, 5, 6, 8, 9};
    std::unique_ptr<BaseVector> vectorPtr = VectorHelper::CreateDictionary(values, valueSize,
        reinterpret_cast<Vector<std::string_view> *>(dictionary.get()));
    auto vector =
        reinterpret_cast<Vector<DictionaryContainer<std::string_view, LargeStringContainer>> *>(vectorPtr.get());

    int32_t positions[] = {1, 3, 5, 6};
    int32_t offset = 1;
    int32_t newValueSize = 3;
    auto copyPositions = vector->CopyPositions(positions, offset, newValueSize);

    std::string_view value;
    for (int i = 0; i < newValueSize; i++) {
        if (values[positions[i + offset]] % 2 == 0) {
            EXPECT_EQ(dictionary->IsNull(values[positions[i + offset]]), copyPositions->IsNull(i));
            continue;
        }
        value = copyPositions->GetValue(i);
        EXPECT_EQ(dictionary->GetValue(values[positions[i + offset]]), value);
    }
}

TEST(vector2, vector_get_set_value_int32)
{
    vector_get_set_value<int32_t>();
}

TEST(vector2, vector_get_set_value_int64)
{
    vector_get_set_value<int64_t>();
}

TEST(vector2, vector_get_set_value_double)
{
    vector_get_set_value<double>();
}

TEST(vector2, vector_get_set_value_boost_dec128)
{
    vector_get_set_value<boost_dec128>();
}

TEST(vector2, vector_get_set_value_string)
{
    vector_get_set_value<std::string>();
}

TEST(vector2, vector_has_null_int32)
{
    vector_has_null<int32_t>();
}

TEST(vector2, vector_has_null_double)
{
    vector_has_null<double>();
}

TEST(vector2, odd_vector_has_null)
{
    int vec_size = 25;
    bool hasNull;
    auto vector = std::make_shared<Vector<int32_t>>(vec_size);
    vector->SetNull(vec_size / 2);
    hasNull = vector->HasNull();
    EXPECT_EQ(hasNull, true);
}

TEST(vector2, string_vec_any_size)
{
    for (int i = 1; i < 100; i++) {
        auto vec = std::make_shared<Vector<std::string>>(i);
        int idx = rand() % i;
        std::string value = "hello";
        vec->SetValue(idx, value);
        EXPECT_EQ(value, vec->GetValue(idx));
    }
}

TEST(vector2, string_vec_size_0)
{
    auto vec = std::make_shared<Vector<std::string>>(0);
}

TEST(vector2, append_int32)
{
    vector_append_value<int32_t>();
}

TEST(vector2, append_string)
{
    vector_append_value<std::string>();
}

TEST(vector2, copy_positions_int32)
{
    vector_copy_positions_value<int32_t>();
}

TEST(vector2, copy_positions_string)
{
    vector_copy_positions_value<std::string>();
    // fixme: Add UT after the new Varchar Encoding code is merged.
}

TEST(vector2, dict_get_value_with_null)
{
    dict_vector_get_value_with_null<int32_t>();
}

TEST(vector2, dict_decimal128_get_value_with_null)
{
    dict_vector_get_value_with_null<type::Decimal128>();
}

TEST(vector2, dict_get_value_string_with_null)
{
    dict_vector_get_value_string_with_null<std::string_view, LargeStringContainer>();
}

TEST(vector2, dict_copy_position_int32_with_null)
{
    dict_copy_positions_value<int32_t>();
}

TEST(vector2, dict_copy_position_int64_with_null)
{
    dict_copy_positions_value<int64_t>();
}

TEST(vector2, dict_copy_position_varchar_with_null)
{
    dict_copy_positions_value<std::string_view>();
}
}