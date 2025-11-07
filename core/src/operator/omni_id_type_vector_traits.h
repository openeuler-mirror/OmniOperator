/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 */

#ifndef OMNI_RUNTIME_OMNI_ID_TYPE_VECTOR_TRAITS_H
#define OMNI_RUNTIME_OMNI_ID_TYPE_VECTOR_TRAITS_H

#include <type/data_type.h>
#include <vector/vector.h>
#include "vector/large_string_container.h"
#include "type/string_ref.h"
namespace omniruntime {
namespace op {
/**
 * NativeAndVector is a datatype traits
 * to trait type, vector and dictVector from dataTypeId
 */
template <type::DataTypeId dataTypeId> struct NativeAndVectorType {};

template <> struct NativeAndVectorType<type::DataTypeId::OMNI_BOOLEAN> {
    using type = bool;
    using vector = vec::Vector<type>;
    using dictVector = vec::Vector<vec::DictionaryContainer<type>>;
};

template <> struct NativeAndVectorType<type::DataTypeId::OMNI_SHORT> {
    using type = int16_t;
    using vector = vec::Vector<type>;
    using dictVector = vec::Vector<vec::DictionaryContainer<type>>;
};

template <> struct NativeAndVectorType<type::DataTypeId::OMNI_INT> {
    using type = int32_t;
    using vector = vec::Vector<type>;
    using dictVector = vec::Vector<vec::DictionaryContainer<type>>;
};
template <> struct NativeAndVectorType<type::DataTypeId::OMNI_DATE32> {
    using type = int32_t;
    using vector = vec::Vector<type>;
    using dictVector = vec::Vector<vec::DictionaryContainer<type>>;
};
template <> struct NativeAndVectorType<type::DataTypeId::OMNI_TIME32> {
    using type = int32_t;
    using vector = vec::Vector<type>;
    using dictVector = vec::Vector<vec::DictionaryContainer<type>>;
};

template <> struct NativeAndVectorType<type::DataTypeId::OMNI_LONG> {
    using type = int64_t;
    using vector = vec::Vector<type>;
    using dictVector = vec::Vector<vec::DictionaryContainer<type>>;
};
template <> struct NativeAndVectorType<type::DataTypeId::OMNI_DATE64> {
    using type = int64_t;
    using vector = vec::Vector<type>;
    using dictVector = vec::Vector<vec::DictionaryContainer<type>>;
};
template <> struct NativeAndVectorType<type::DataTypeId::OMNI_TIME64> {
    using type = int64_t;
    using vector = vec::Vector<type>;
    using dictVector = vec::Vector<vec::DictionaryContainer<type>>;
};
template <> struct NativeAndVectorType<type::DataTypeId::OMNI_TIMESTAMP> {
    using type = int64_t;
    using vector = vec::Vector<type>;
    using dictVector = vec::Vector<vec::DictionaryContainer<type>>;
};
template <> struct NativeAndVectorType<type::DataTypeId::OMNI_DOUBLE> {
    using type = double;
    using vector = vec::Vector<type>;
    using dictVector = vec::Vector<vec::DictionaryContainer<type>>;
};
template <> struct NativeAndVectorType<type::DataTypeId::OMNI_DECIMAL64> {
    using type = int64_t;
    using vector = vec::Vector<type>;
    using dictVector = vec::Vector<vec::DictionaryContainer<type>>;
};
template <> struct NativeAndVectorType<type::DataTypeId::OMNI_DECIMAL128> {
    using type = type::Decimal128;
    using vector = vec::Vector<type>;
    using dictVector = vec::Vector<vec::DictionaryContainer<type>>;
};
template <> struct NativeAndVectorType<type::DataTypeId::OMNI_VARCHAR> {
    using type = std::string_view;
    using vector = vec::Vector<vec::LargeStringContainer<std::string_view>>;
    using dictVector = vec::Vector<vec::DictionaryContainer<type>>;
};
template <> struct NativeAndVectorType<type::DataTypeId::OMNI_CHAR> {
    using type = std::string_view;
    using vector = vec::Vector<vec::LargeStringContainer<std::string_view>>;
    using dictVector = vec::Vector<vec::DictionaryContainer<type>>;
};
}
}
#endif // OMNI_RUNTIME_OMNI_ID_TYPE_VECTOR_TRAITS_H
