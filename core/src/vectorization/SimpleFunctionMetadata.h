/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: visitor class for expressions
 */

#pragma once

#include <optional>
#include <tuple>
#include <util/compiler_util.h>

namespace omniruntime {
// wraps a UDF object to provide the inheritance
// this is basically just boilerplate-avoidance
template <typename Fun, typename TReturn, typename... TArgs>
class FunctionHolder {
    Fun instance_;

public:
    using return_type = TReturn;
    using arg_types = std::tuple<TArgs...>;
    template <size_t N>
    using type_at = std::tuple_element_t<N, arg_types>;
    static constexpr int num_args = std::tuple_size_v<arg_types>;
    using udf_struct_t = Fun;
    using exec_return_type = TReturn;
    using exec_arg_types = std::tuple<TArgs...>;

    ALWAYS_INLINE bool callImpl(TReturn &out, bool &notNull, const TArgs &... args)
    {
        instance_.call(out, args...);
        notNull = true;
        return false;
    }

    ALWAYS_INLINE bool call(TReturn &out, bool &notNull, const TArgs &... args)
    {
        return callImpl(out, notNull, args...);
    }
};
}
