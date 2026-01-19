/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: visitor class for expressions
 */

#pragma once
#include <type_traits>

namespace omniruntime::vectorization {
struct AnyType {};

template <typename T>
struct UnwrapCustomType {
    using type = T;
};

template <typename T>
struct is_shared_ptr : std::false_type {};

template <typename T>
struct is_shared_ptr<std::shared_ptr<T>> : std::true_type {};

// T must be a struct with T::type being a built-in type and T::typeName
// type name to use in FunctionSignature.
// providesCustomComparison must be set to true to ensure values are wrapped in
// a view that exposes the custom comparison operations in Simple Functions.
template <typename T, bool providesCustomComparison_ = false>
struct CustomType {
    static constexpr bool providesCustomComparison = providesCustomComparison_;

private:
    CustomType() {}
};

template <typename T, bool providesCustomComparison>
struct UnwrapCustomType<CustomType<T, providesCustomComparison>> {
    using type = typename T::type;
};

template <typename T = AnyType, bool comparable = false, bool orderable = false>
struct Generic {
    Generic() = delete;

    static_assert(!(orderable && !comparable), "Orderable implies comparable.");
};

using Any = Generic<>;

template <typename UNDERLYING_TYPE>
struct Variadic {
    using underlying_type = UNDERLYING_TYPE;

    Variadic() = delete;
};

template <typename>
struct isVariadicType : public std::false_type {};

template <typename T>
struct isVariadicType<Variadic<T>> : public std::true_type {};

template <typename ELEMENT>
struct Array {
    using element_type = ELEMENT;

    static_assert(!isVariadicType<element_type>::value, "Array elements cannot be Variadic");

private:
    Array() {}
};

/// MaterializeType template.

template <typename T>
struct MaterializeType {
    using null_free_t = T;
    using nullable_t = T;
    static constexpr bool requiresMaterialization = false;
};

template <typename V>
struct MaterializeType<Array<V>> {
    using null_free_t = std::vector<typename MaterializeType<V>::null_free_t>;
    using nullable_t = std::vector<std::optional<typename MaterializeType<V>::nullable_t>>;
    static constexpr bool requiresMaterialization = true;
};

template <typename T>
struct MaterializeType<std::shared_ptr<T>> {
    using nullable_t = T;
    using null_free_t = T;
    static constexpr bool requiresMaterialization = false;
};
}
