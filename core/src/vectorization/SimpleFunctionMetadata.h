/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: visitor class for expressions
 */

#pragma once

#include <tuple>
#include <util/compiler_util.h>
#include "vectorization/Status.h"
#include <vector>
#include "type/data_type.h"
#include "util/config/QueryConfig.h"

namespace omniruntime::vectorization {
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

    ALWAYS_INLINE Status callImpl(TReturn &out, bool &notNull, const TArgs &... args)
    {
        if constexpr (std::is_same_v<decltype(instance_.call(out, args...)), Status>) {
            notNull = true;
            return instance_.call(out, args...);
        } else {
            notNull = true;
            instance_.call(out, args...);
            return Status::OK();
        }
    }

    ALWAYS_INLINE Status call(TReturn &out, bool &notNull, const TArgs &... args)
    {
        return callImpl(out, notNull, args...);
    }

    template <typename... Args>
    void initialize(const std::vector<type::DataTypePtr>& inputTypes, const config::QueryConfig& config, Args&&... args)
    {
        instance_.initialize(inputTypes, config, std::forward<Args>(args)...);
    }
};
}
