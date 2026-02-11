/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: visitor class for expressions
 */

#pragma once
#include "util/compiler_util.h"
#include "vectorization/VectorFunction.h"
#include "../registration/SimpleFunctionRegistry.h"

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
struct Or {
    ALWAYS_INLINE void call(bool &result, const bool &a, const bool &b)
    {
        result = a || b;
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
static ALWAYS_INLINE bool equal(const T &a, const T &b)
{
    // In SparkSQL, NaN is defined to equal NaN.
    if (isNan(a)) {
        return isNan(b);
    }
    return a == b;
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
            type::OMNI_BOOLEAN,
            type::OMNI_BYTE,
            type::OMNI_SHORT,
            type::OMNI_INT,
            type::OMNI_LONG,
            type::OMNI_FLOAT,
            type::OMNI_DOUBLE,
            type::OMNI_VARCHAR,
            type::OMNI_CHAR,
            type::OMNI_VARBINARY,
            type::OMNI_DATE32,
            type::OMNI_DATE64,
            type::OMNI_TIMESTAMP,
            type::OMNI_DECIMAL64,
            type::OMNI_DECIMAL128
         }) {
        signatures.emplace_back(
            codegen::FunctionSignatureBuilder().FuncName(name).ReturnType(type::OMNI_BOOLEAN).ArgumentType(inputType).
            ArgumentType(inputType).Build());
    }
    return signatures;
}

class EqualNullSafeFunction : public VectorFunction {
public:
    void Apply(std::stack<BaseVector *> &args, const DataTypePtr &outputType, BaseVector *&result,
        op::ExecutionContext *context) const override
    {
        const auto left = args.top();
        args.pop();
        const auto right = args.top();
        args.pop();
        auto inputTypeId = left->GetTypeId();
        auto size = left->GetSize();
        if (result == nullptr) {
            result = VectorHelper::CreateFlatVector(outputType->GetId(), size);
        }
        for (size_t i = 0; i < size; i++) {
            if (left->IsNull(i) && right->IsNull(i)) {
                dynamic_cast<Vector<bool> *>(result)->SetValue(i, true);
            } else if (left->IsNull(i) || right->IsNull(i)) {
                dynamic_cast<Vector<bool> *>(result)->SetValue(i, false);
            } else {
                switch (inputTypeId) {
                    case OMNI_BOOLEAN:
                        dynamic_cast<Vector<bool> *>(result)->SetValue(i,
                            dynamic_cast<Vector<bool> *>(left)->GetValue(i) == dynamic_cast<Vector<bool> *>(right)->GetValue(i));
                        break;
                    case OMNI_BYTE:
                        dynamic_cast<Vector<bool> *>(result)->SetValue(i,
                            dynamic_cast<Vector<int8_t> *>(left)->GetValue(i) == dynamic_cast<Vector<int8_t> *>(right)->GetValue(i));
                        break;
                    case OMNI_SHORT:
                        dynamic_cast<Vector<bool> *>(result)->SetValue(i,
                            dynamic_cast<Vector<int16_t> *>(left)->GetValue(i) == dynamic_cast<Vector<int16_t> *>(right)->GetValue(i));
                        break;
                    case OMNI_INT:
                    case OMNI_DATE32:
                        dynamic_cast<Vector<bool> *>(result)->SetValue(i,
                            dynamic_cast<Vector<int32_t> *>(left)->GetValue(i) == dynamic_cast<Vector<int32_t> *>(right)->GetValue(i));
                        break;
                    case OMNI_LONG:
                    case OMNI_TIMESTAMP:
                    case OMNI_DECIMAL64:
                        dynamic_cast<Vector<bool> *>(result)->SetValue(i,
                            dynamic_cast<Vector<int64_t> *>(left)->GetValue(i) == dynamic_cast<Vector<int64_t> *>(right)->GetValue(i));
                        break;
                    case OMNI_FLOAT:
                        dynamic_cast<Vector<bool> *>(result)->SetValue(i,
                            equal(dynamic_cast<Vector<float> *>(left)->GetValue(i),dynamic_cast<Vector<float> *>(right)->GetValue(i)));
                        break;
                    case OMNI_DOUBLE:
                        dynamic_cast<Vector<bool> *>(result)->SetValue(i,
                            equal(dynamic_cast<Vector<double> *>(left)->GetValue(i), dynamic_cast<Vector<double> *>(right)->GetValue(i)));
                        break;
                    case OMNI_CHAR:
                    case OMNI_VARCHAR:
                    case OMNI_VARBINARY:
                        dynamic_cast<Vector<bool> *>(result)->SetValue(i,
                            dynamic_cast<Vector<LargeStringContainer<std::string_view>> *>(left)->GetValue(i) == dynamic_cast<Vector<LargeStringContainer<std::string_view>> *>(right)->GetValue(i));
                        break;
                    case OMNI_DECIMAL128:
                        dynamic_cast<Vector<bool> *>(result)->SetValue(i,
                            dynamic_cast<Vector<Decimal128> *>(left)->GetValue(i) == dynamic_cast<Vector<Decimal128> *>(right)->GetValue(i));
                        break;
                    default: ;
                }
            }
        }
        delete left;
        delete right;
    }
};

inline void registerEqualNullSafeFunction(const std::string &prefix)
{
    auto equalNullSafeFunction = std::make_shared<EqualNullSafeFunction>();
    VectorFunction::RegisterVectorFunction(prefix + "equal_null_safe", {OMNI_BOOLEAN, OMNI_BOOLEAN}, OMNI_BOOLEAN, equalNullSafeFunction);
    VectorFunction::RegisterVectorFunction(prefix + "equal_null_safe", {OMNI_BYTE, OMNI_BYTE}, OMNI_BOOLEAN, equalNullSafeFunction);
    VectorFunction::RegisterVectorFunction(prefix + "equal_null_safe", {OMNI_SHORT, OMNI_SHORT}, OMNI_BOOLEAN, equalNullSafeFunction);
    VectorFunction::RegisterVectorFunction(prefix + "equal_null_safe", {OMNI_INT, OMNI_INT}, OMNI_BOOLEAN, equalNullSafeFunction);
    VectorFunction::RegisterVectorFunction(prefix + "equal_null_safe", {OMNI_LONG, OMNI_LONG}, OMNI_BOOLEAN, equalNullSafeFunction);
    VectorFunction::RegisterVectorFunction(prefix + "equal_null_safe", {OMNI_FLOAT, OMNI_FLOAT}, OMNI_BOOLEAN, equalNullSafeFunction);
    VectorFunction::RegisterVectorFunction(prefix + "equal_null_safe", {OMNI_DOUBLE, OMNI_DOUBLE}, OMNI_BOOLEAN, equalNullSafeFunction);
    VectorFunction::RegisterVectorFunction(prefix + "equal_null_safe", {OMNI_VARCHAR, OMNI_VARCHAR}, OMNI_BOOLEAN, equalNullSafeFunction);
    VectorFunction::RegisterVectorFunction(prefix + "equal_null_safe", {OMNI_DATE32, OMNI_DATE32}, OMNI_BOOLEAN, equalNullSafeFunction);
    VectorFunction::RegisterVectorFunction(prefix + "equal_null_safe", {OMNI_TIMESTAMP, OMNI_TIMESTAMP}, OMNI_BOOLEAN, equalNullSafeFunction);
    VectorFunction::RegisterVectorFunction(prefix + "equal_null_safe", {OMNI_DECIMAL64, OMNI_DECIMAL64}, OMNI_BOOLEAN, equalNullSafeFunction);
    VectorFunction::RegisterVectorFunction(prefix + "equal_null_safe", {OMNI_DECIMAL128, OMNI_DECIMAL128}, OMNI_BOOLEAN, equalNullSafeFunction);
    VectorFunction::RegisterVectorFunction(prefix + "equal_null_safe", {OMNI_VARBINARY, OMNI_VARBINARY}, OMNI_BOOLEAN, equalNullSafeFunction);
}
}
