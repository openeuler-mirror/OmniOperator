/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2023. All rights reserved.
 */

#ifndef OMNI_RUNTIME_GROUP_HASHER_H
#define OMNI_RUNTIME_GROUP_HASHER_H
#include <operator/hash_util.h>
#include <type/string_ref.h>
#include <functional>
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
}
}

#endif // OMNI_RUNTIME_GROUP_BY_HASHER_H
