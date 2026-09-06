/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 */
#pragma once

#include <cstdint>
#include <memory>
#include <string>

#include "type/data_type.h"
#include "util/config/QueryConfig.h"
#include "vector/vector.h"

namespace omniruntime::reader::text {

class TextValueConverter {
public:
    explicit TextValueConverter(std::string sessionTimezone);

    std::unique_ptr<vec::BaseVector> DecodeColumn(
        std::unique_ptr<vec::BaseVector> strings,
        const type::DataTypePtr& targetType) const;

    std::unique_ptr<vec::BaseVector> EncodeColumn(
        vec::BaseVector* source,
        const type::DataTypePtr& sourceType,
        int64_t start,
        int64_t end) const;

private:
    std::unique_ptr<vec::BaseVector> CastOwned(
        std::unique_ptr<vec::BaseVector> input,
        const type::DataTypePtr& fromType,
        const type::DataTypePtr& toType) const;

    config::QueryConfig queryConfig_;
};

} // namespace omniruntime::reader::text
