/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 * Description: test
 */
#ifndef OMNI_RUNTIME_TEST_H
#define OMNI_RUNTIME_TEST_H

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
std::shared_ptr<DICTIONARY_DATA_TYPE[]> createDictionary(int size)
{
    std::shared_ptr<DICTIONARY_DATA_TYPE[]> dictionary(new DICTIONARY_DATA_TYPE[size]);
    for (int i = 0; i < size; i++) {
        if constexpr (std::is_same_v<std::string, RAW_DATA_TYPE>) {
            dictionary[i] = "hello world " + std::to_string(i);
        } else {
            dictionary[i] = static_cast<RAW_DATA_TYPE>(i) * 2 / 3;
        }
    }
    return dictionary;
}

template <typename T, template <typename> typename CONTAINER>
std::shared_ptr<CONTAINER<T>> createStringDictionary(int size)
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
}

#endif // OMNI_RUNTIME_TEST_H
