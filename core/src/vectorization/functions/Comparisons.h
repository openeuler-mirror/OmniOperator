/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: visitor class for expressions
 */

#pragma once
#include "util/compiler_util.h"
#include "vectorization/VectorFunction.h"

namespace omniruntime::vectorization {
template <typename T>
struct Not {
    ALWAYS_INLINE void call(bool &result, const bool &a)
    {
        result = not a;
    }
};

template <typename T>
struct And {
    ALWAYS_INLINE void call(bool &result, const bool &a, const bool &b)
    {
        result = a && b;
    }
};

template <typename T>
static ALWAYS_INLINE bool isNan(const T &value)
{
    if constexpr (std::is_floating_point_v<T>) {
        return std::isnan(value);
    } else {
        return false;
    }
}

template <typename T>
struct Less {
    constexpr bool operator()(const T &a, const T &b) const
    {
        if (isNan(a)) {
            return false;
        }
        if (isNan(b)) {
            return true;
        }
        return a < b;
    }
};

template <typename T>
struct Greater : private Less<T> {
    constexpr bool operator()(const T &a, const T &b) const
    {
        return Less<T>::operator()(b, a);
    }
};

template <typename T>
struct Equal {
    constexpr bool operator()(const T &a, const T &b) const
    {
        // In SparkSQL, NaN is defined to equal NaN.
        if (isNan(a)) {
            return isNan(b);
        }
        return a == b;
    }
};

template <typename T>
struct LessOrEqual {
    constexpr bool operator()(const T &a, const T &b) const
    {
        Less<T> less;
        Equal<T> equal;
        return less(a, b) || equal(a, b);
    }
};

template <typename T>
struct GreaterOrEqual : private Less<T> {
    constexpr bool operator()(const T &a, const T &b) const
    {
        Less<T> less;
        Equal<T> equal;
        return less(b, a) || equal(a, b);
    }
};

std::shared_ptr<VectorFunction> makeEqualTo(const std::string &name, const std::vector<type::DataTypeId> &inputArgs,
    const config::QueryConfig &);

std::shared_ptr<VectorFunction> makeLessThan(const std::string &name, const std::vector<type::DataTypeId> &inputArgs,
    const config::QueryConfig &);

std::shared_ptr<VectorFunction> makeGreaterThan(const std::string &name, const std::vector<type::DataTypeId> &inputArgs,
    const config::QueryConfig &);

std::shared_ptr<VectorFunction> makeLessThanOrEqual(const std::string &name,
    const std::vector<type::DataTypeId> &inputArgs, const config::QueryConfig &);

std::shared_ptr<VectorFunction> makeGreaterThanOrEqual(const std::string &name,
    const std::vector<type::DataTypeId> &inputArgs, const config::QueryConfig &);

inline std::vector<std::shared_ptr<codegen::FunctionSignature>> ComparisonSignatures(const std::string &name)
{
    std::vector<std::shared_ptr<codegen::FunctionSignature>> signatures;
    for (const auto &inputType : {
             type::OMNI_BYTE,
             type::OMNI_INT,
             type::OMNI_LONG,
             type::OMNI_VARCHAR,
             type::OMNI_DOUBLE
         }) {
        signatures.emplace_back(
            codegen::FunctionSignatureBuilder().FuncName(name).ReturnType(type::OMNI_BOOLEAN).ArgumentType(inputType).
            ArgumentType(inputType).Build());
    }
    return signatures;
}
}
