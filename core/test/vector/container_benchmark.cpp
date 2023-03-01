//
// Created by ken on 2022-09-19.
//

#include "gtest/gtest.h"
#include "benchmark/benchmark.h"
#include "vector/vector.h"
#include "vector/dictionary_container.h"
#include "vector/vector_helper.h"

class Vector;

namespace omniruntime::vec::test {
template <typename CONTAINER> void bm_vector_setvalue_string(benchmark::State &state)
{
    int vec_size = 10000;
    uint32_t stringWidth = OMNI_LARGE_WIDTH;

    std::string valuePrefix;
    if constexpr (std::is_same_v<SmallStringContainer<std::string_view>, CONTAINER>) {
        valuePrefix = "hello: ";
        stringWidth = OMNI_SMALL_WIDTH;
    } else {
        valuePrefix = "hello hello hello hello hello: ";
    }

    for (auto _ : state) {
        auto baseVector = VectorHelper::CreateStringVector(vec_size, stringWidth);
        auto *vector = (Vector<CONTAINER> *)baseVector.get();

        for (int i = 0; i < 1'000'000; i++) {
            std::string value{ valuePrefix + std::to_string(i % 100000) };
            std::string_view input(value.data(), value.size());
            vector->SetValue(i % vec_size, input);
        }
    }
}

template <typename CONTAINER> void bm_vector_getvalue_string(benchmark::State &state)
{
    int vec_size = 10000;
    uint32_t stringWidth = OMNI_LARGE_WIDTH;

    std::string valuePrefix;
    if constexpr (std::is_same_v<SmallStringContainer<std::string_view>, CONTAINER>) {
        valuePrefix = "hello: ";
        stringWidth = OMNI_SMALL_WIDTH;
    } else {
        valuePrefix = "hello hello hello hello hello: ";
    }

    std::shared_ptr<BaseVector> baseVector = VectorHelper::CreateStringVector(vec_size, stringWidth);
    auto *vector = (Vector<CONTAINER> *)baseVector.get();

    for (int i = 0; i < vec_size; i++) {
        std::string value{ valuePrefix + std::to_string(i % 100000) };
        std::string_view input(value.data(), value.size());
        vector->SetValue(i % vec_size, input);
    }

    for (auto _ : state) {
        for (int i = 0; i < vec_size; i++) {
            std::string_view getValue = vector->GetValue(i);
            std::string output(getValue);
        }
    }
}

template <typename CONTAINER> void bm_vector_create(benchmark::State &state)
{
    int vec_size = 10000;
    uint32_t stringWidth = OMNI_LARGE_WIDTH;

    if constexpr (std::is_same_v<SmallStringContainer<std::string_view>, CONTAINER>) {
        stringWidth = OMNI_SMALL_WIDTH;
    }

    for (auto _ : state) {
        auto baseVector = VectorHelper::CreateStringVector(vec_size, stringWidth);
        auto *vector = (Vector<CONTAINER> *)baseVector.get();
        vector->GetStringEncoding();
    }
}

BENCHMARK_TEMPLATE(bm_vector_setvalue_string, SmallStringContainer<std::string_view>);
BENCHMARK_TEMPLATE(bm_vector_setvalue_string, LargeStringContainer<std::string_view>);

BENCHMARK_TEMPLATE(bm_vector_getvalue_string, SmallStringContainer<std::string_view>);
BENCHMARK_TEMPLATE(bm_vector_getvalue_string, LargeStringContainer<std::string_view>);

BENCHMARK_TEMPLATE(bm_vector_create, SmallStringContainer<std::string_view>);
BENCHMARK_TEMPLATE(bm_vector_create, LargeStringContainer<std::string_view>);
}
