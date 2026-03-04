/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2023. All rights reserved.
 */

#ifndef OMNI_RUNTIME_GROUP_HASHER_H
#define OMNI_RUNTIME_GROUP_HASHER_H
#include <operator/hash_util.h>
#include <type/string_ref.h>
#include <type/decimal128.h>
#include <functional>
#include <string>
#include "crc_hasher.h"

namespace omniruntime {
namespace op {
template <class T> struct GroupbyHashCalculator {
    size_t operator () (const T &data) const
    {
        return std::hash<T>()(data);
    }
};

template <> struct GroupbyHashCalculator<omniruntime::type::StringRef> {
    size_t operator () (const omniruntime::type::StringRef &str) const
    {
        return omniruntime::op::HashUtil::HashValue((int8_t *)str.data, str.size);
    }
};

// For collect_set_varchar_aggregator: key stored as std::string.
template <> struct GroupbyHashCalculator<std::string> {
    size_t operator () (const std::string &str) const
    {
        return static_cast<size_t>(omniruntime::op::HashUtil::HashValue(
            reinterpret_cast<int8_t *>(const_cast<char *>(str.data())), static_cast<int32_t>(str.size())));
    }
};

template <> struct GroupbyHashCalculator<int32_t> {
    size_t operator () (const int32_t data) const
    {
        return omniruntime::simdutil::CRC32HasherForInt(data);
    }
};

template <> struct GroupbyHashCalculator<int64_t> {
    size_t operator() (const int64_t data) const
    {
        return omniruntime::simdutil::CRC32HasherForInt(data);
    }
};

template<>
struct GroupbyHashCalculator<int16_t> {
    size_t operator()(const int16_t data) const
    {
        return omniruntime::simdutil::CRC32HasherForInt(data);
    }
};

// Custom hash for Decimal128 so DefaultHashMap<Decimal128, ...> can be used (e.g. CollectSetAggregator).
template <>
struct GroupbyHashCalculator<omniruntime::type::Decimal128> {
    size_t operator()(const omniruntime::type::Decimal128 &val) const
    {
        return static_cast<size_t>(HashUtil::HashValue(static_cast<int64_t>(val.LowBits()), val.HighBits()));
    }
};
}
}

#endif // OMNI_RUNTIME_GROUP_BY_HASHER_H
