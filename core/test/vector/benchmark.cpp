/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 * @Description: benchmark implementations
 */

#include "gtest/gtest.h"
#include "vector/vector.h"
#include "vector/dictionary_container.h"
#include "boost/multiprecision/number.hpp"
#include "benchmark/benchmark.h"
#include "test.h"

class Vector;

namespace omniruntime::vec::test {
template <typename T> void bm_vector_create_with_share_ptr(benchmark::State &state)
{
    int vecSize = 10000;
    for (auto _ : state) {
        auto vector = std::make_shared<Vector<T>>(vecSize);
    }
}

template <typename T> void bm_vector_create(benchmark::State &state)
{
    int vecSize = 10000;
    for (auto _ : state) {
        auto vector = new Vector<T>(vecSize);
        delete vector;
    }
}

template <typename T> void bm_vector_setvalue(benchmark::State &state)
{
    int vecSize = 10000;
    auto vector = std::make_shared<Vector<T>>(vecSize);
    for (auto _ : state) {
        for (int i = 0; i < 1'000'000; i++) {
            T assign(i * 2);
            vector->SetValue(i % vecSize, assign);
        }
    }
}

template <typename T> void bm_vector_getvalue(benchmark::State &state)
{
    int vecSize = 10000;
    auto vector = new Vector<T>(vecSize);
    for (int i = 0; i < vecSize; i++) {
        vector->SetValue(i, i * 2);
    }
    long total = 0;
    for (auto _ : state) {
        for (int i = 0; i < 1'000'000; i++) {
            total += vector->GetValue(i % vecSize);
        }
    }

    std::cerr << "total: " << total << std::endl;
    delete vector;
}

template <typename T> void bm_vector_has_no_null(benchmark::State &state)
{
    int vecSize = 1'000'001;
    bool hasNull;
    auto vector = std::make_shared<Vector<T>>(vecSize);
    for (auto _ : state) {
        hasNull = vector->HasNull();
    }
    if (hasNull) {
        // nothing, only avoid the compiler optimiztion for unused variable
    }
}

template <typename T> void bm_vector_has_null(benchmark::State &state)
{
    int vecSize = 1'000'001;
    bool hasNull;
    auto vector = std::make_shared<Vector<T>>(vecSize);
    vector->SetNull(vecSize / 2);
    for (auto _ : state) {
        hasNull = vector->HasNull();
    }
    if (hasNull) {
        // nothing, only avoid the compiler optimiztion for unused variable
    }
}

template <typename T> void bm_slice_vector_getvalue(benchmark::State &state)
{
    int parent_vec_size = 20000;
    int vecSize = 10000;
    int offset = 867;
    auto parent = new Vector<T>(parent_vec_size);
    for (int i = 0; i < parent_vec_size; i++) {
        T value(i * 2);
        parent->SetValue(i, value);
    }

    auto vector = parent->Slice(offset, vecSize);
    T total = 0;
    for (auto _ : state) {
        for (int i = 0; i < 1'000'000; i++) {
            total += vector->GetValue(i % vecSize);
        }
    }

    std::cerr << "total: " << total << std::endl;
    delete parent;
    delete vector;
}

template <> void bm_slice_vector_getvalue<std::string>(benchmark::State &state)
{
    int parent_vec_size = 20000;
    int vecSize = 10000;
    int offset = 867;
    Vector<std::string> *vector = nullptr;
    { // scope to ensure deletion of parent
        auto parent = std::make_unique<Vector<std::string>>(parent_vec_size);
        for (int i = 0; i < parent_vec_size; i++) {
            std::string value{ "hello hello hello hello hello: " + std::to_string(i % 100000) };
            parent->SetValue(i, value);
        }

        vector = parent->Slice(offset, vecSize);
    }
    long total = 0;
    for (auto _ : state) {
        for (int i = 0; i < 1'000'000; i++) {
            total += vector->GetValue(i % vecSize).length();
        }
    }
    std::cerr << "total: " << total << std::endl;
}

template <> void bm_vector_setvalue<std::string>(benchmark::State &state)
{
    int vecSize = 10000;
    std::string prefix = "hello: ";
    if (state.range(0)) {
        prefix = "hello hello hello hello hello: ";
    }
    for (auto _ : state) {
        auto vector = new Vector<std::string>(vecSize);
        for (int i = 0; i < 1'000'000; i++) {
            std::string value{ prefix + std::to_string(i % 100000) };
            vector->SetValue(i % vecSize, value);
        }
        delete vector;
    }
}

template <> void bm_vector_getvalue<std::string>(benchmark::State &state)
{
    int vecSize = 10000;
    std::string prefix = "hello: ";
    if (state.range(0)) {
        prefix = "hello hello hello hello hello: ";
    }
    auto vector = new Vector<std::string>(vecSize);
    for (int i = 0; i < vecSize; i++) {
        std::string value{ prefix + std::to_string(i % 100000) };
        vector->SetValue(i % vecSize, value);
    }

    std::string output;
    for (auto _ : state) {
        for (int i = 0; i < vecSize; i++) {
            std::string getValue(vector->GetValue(i));
        }
    }

    delete vector;
}

template <typename T> static void bm_dictvector_setvalue(benchmark::State &state)
{
    int dictionary_size = 100;
    int value_size = 10000;
    int *values = new int[value_size];
    std::shared_ptr<bool[]> nulls = std::shared_ptr<bool[]>(new bool[value_size]);
    for (int i = 0; i < value_size; i++) {
        values[i] = i % dictionary_size;
        nulls[i] = false;
    }

    auto dictionary = createDictionary<T>(dictionary_size);
    auto container = std::make_shared<DictionaryContainer<T>>(values, value_size, dictionary, dictionary_size, 0);
    Vector<DictionaryContainer<T>> vector{ value_size, container, nulls };

    for (auto _ : state) {
        for (int i = 0; i < 1'000'000; i++) {
            vector.SetValue(1, 1);
        }
    }

    delete[] values;
}

template <typename T> static void bm_dictvector_getvalue(benchmark::State &state)
{
    int dictionary_size = 100;
    int value_size = 10000;
    int *values = new int[value_size];
    std::shared_ptr<bool[]> nulls = std::shared_ptr<bool[]>(new bool[value_size]);
    for (int i = 0; i < value_size; i++) {
        values[i] = i % dictionary_size;
        nulls[i] = false;
    }

    auto dictionary = createDictionary<T>(dictionary_size);
    auto container = std::make_shared<DictionaryContainer<T>>(values, value_size, dictionary, dictionary_size, 0);
    Vector<DictionaryContainer<T>> vector{ value_size, container, nulls };
    long total = 0;
    for (auto _ : state) {
        for (int i = 0; i < 1'000'000; i++) {
            // the following is required otherwise implicit conversion is not invoked
            if constexpr (std::is_same_v<std::string, T>) {
                total += vector.GetValue(i % value_size).length();
            } else {
                total += vector.GetValue(i % value_size);
            }
        }
    }

    std::cerr << "total: " << total << std::endl;

    delete[] values;
}

static void bm_dictvector_getvalue_string(benchmark::State &state)
{
    int dictionary_size = 100;
    int value_size = 10000;
    int *values = new int[value_size];
    std::shared_ptr<bool[]> nulls = std::shared_ptr<bool[]>(new bool[value_size]);
    for (int i = 0; i < value_size; i++) {
        values[i] = i % dictionary_size;
        nulls[i] = false;
    }

    auto dictionary = createStringDictionary<std::string_view, LargeStringContainer>(dictionary_size);
    auto container = std::make_shared<DictionaryContainer<std::string_view, LargeStringContainer>>(values, value_size,
        dictionary, dictionary_size, 0);
    Vector<DictionaryContainer<std::string_view, LargeStringContainer>> vector{ value_size, container, nulls };
    long total = 0;
    for (auto _ : state) {
        for (int i = 0; i < 1'000'000; i++) {
            // the following is required otherwise implicit conversion is not invoked
            total += vector.GetValue(i % value_size).length();
        }
    }

    std::cerr << "total: " << total << std::endl;
    delete[] values;
}

template <typename T> void bm_vector_copypositions(benchmark::State &state)
{
    int originSize = 20000;
    int positionsCnt = 10000;
    int positions[positionsCnt];
    int copySize = state.range(0);
    for (int j = 0; j < positionsCnt; j++) {
        positions[j] = 2 * j;
    }

    Vector<T> vector{ originSize };
    for (int i = 0; i < originSize; i++) {
        T value(i * 2);
        vector.SetValue(i, value);
    }

    for (auto _ : state) {
        auto copyVector = vector.CopyPositions(positions, 56, copySize);
    }
}

template <> void bm_vector_copypositions<std::string>(benchmark::State &state)
{
    int originSize = 20000;
    int positionsCnt = 10000;
    int positions[positionsCnt];
    int copySize = state.range(0);
    for (int j = 0; j < positionsCnt; j++) {
        positions[j] = 2 * j;
    }

    Vector<std::string> vector{ originSize };
    for (int i = 0; i < originSize; i++) {
        std::string value{ "hello" + std::to_string(i) };
        vector.SetValue(i, value);
    }

    for (auto _ : state) {
        auto copyVector = vector.CopyPositions(positions, 56, copySize);
    }
}

BENCHMARK_TEMPLATE(bm_vector_setvalue, std::string)->Arg(0)->ArgName("small");
BENCHMARK_TEMPLATE(bm_vector_setvalue, std::string)->Arg(1)->ArgName("large");

BENCHMARK_TEMPLATE(bm_vector_setvalue, int16_t);
BENCHMARK_TEMPLATE(bm_vector_setvalue, int32_t);
BENCHMARK_TEMPLATE(bm_vector_setvalue, int64_t);
BENCHMARK_TEMPLATE(bm_vector_setvalue, float);
BENCHMARK_TEMPLATE(bm_vector_setvalue, double);

BENCHMARK_TEMPLATE(bm_vector_setvalue, boost_dec32);
BENCHMARK_TEMPLATE(bm_vector_setvalue, boost_dec64);
BENCHMARK_TEMPLATE(bm_vector_setvalue, boost_dec128);

BENCHMARK_TEMPLATE(bm_dictvector_setvalue, int16_t);

BENCHMARK_TEMPLATE(bm_vector_getvalue, int16_t);
BENCHMARK_TEMPLATE(bm_vector_getvalue, int32_t);
BENCHMARK_TEMPLATE(bm_vector_getvalue, double);

BENCHMARK_TEMPLATE(bm_dictvector_getvalue, int16_t);
BENCHMARK_TEMPLATE(bm_dictvector_getvalue, double);
BENCHMARK_TEMPLATE(bm_dictvector_getvalue, int32_t);
BENCHMARK_TEMPLATE(bm_dictvector_getvalue, int64_t);

BENCHMARK(bm_dictvector_getvalue_string);

BENCHMARK_TEMPLATE(bm_vector_create_with_share_ptr, int32_t);
BENCHMARK_TEMPLATE(bm_vector_create, int32_t);
BENCHMARK_TEMPLATE(bm_vector_create_with_share_ptr, int32_t);
BENCHMARK_TEMPLATE(bm_vector_create, int32_t);
BENCHMARK_TEMPLATE(bm_vector_create_with_share_ptr, int32_t);
BENCHMARK_TEMPLATE(bm_vector_create, int32_t);

BENCHMARK_TEMPLATE(bm_slice_vector_getvalue, int32_t);
BENCHMARK_TEMPLATE(bm_slice_vector_getvalue, int64_t);
BENCHMARK_TEMPLATE(bm_slice_vector_getvalue, std::string);
BENCHMARK_TEMPLATE(bm_slice_vector_getvalue, double);
BENCHMARK_TEMPLATE(bm_slice_vector_getvalue, boost_dec32);
BENCHMARK_TEMPLATE(bm_slice_vector_getvalue, boost_dec64);
BENCHMARK_TEMPLATE(bm_slice_vector_getvalue, boost_dec128);

BENCHMARK_TEMPLATE(bm_vector_getvalue, std::string)->Arg(0)->ArgName("small");
BENCHMARK_TEMPLATE(bm_vector_getvalue, std::string)->Arg(1)->ArgName("large");

BENCHMARK_TEMPLATE(bm_vector_copypositions, int32_t)->Arg(3000)->Arg(6000)->Arg(9000);
BENCHMARK_TEMPLATE(bm_vector_copypositions, int64_t)->Arg(3000)->Arg(6000)->Arg(9000);
BENCHMARK_TEMPLATE(bm_vector_copypositions, std::string)->Arg(3000)->Arg(6000)->Arg(9000);

BENCHMARK_TEMPLATE(bm_vector_has_no_null, int32_t);
BENCHMARK_TEMPLATE(bm_vector_has_null, int32_t);
}
