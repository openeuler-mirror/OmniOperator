/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: visitor class for expressions
 */
#pragma once
#include "type/data_type.h"

namespace omniruntime::vectorization {
template <bool nullable, typename V>
class ArrayView;

template <typename T>
using NullableArrayView = ArrayView<true, T>;

namespace detail {
template <typename T>
struct resolver {
    using in_type = T;
};

template <typename V>
struct resolver<Array<V>> {
    using in_type = NullableArrayView<V>;
};
}

struct VectorExec {
    template <typename T>
    using resolver = typename detail::template resolver<T>;
};
}
