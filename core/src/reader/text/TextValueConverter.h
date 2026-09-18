/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 */
#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "type/data_type.h"
#include "util/config/QueryConfig.h"
#include "vector/vector.h"

namespace omniruntime::reader::text {

class TextValueConverter {
public:
    explicit TextValueConverter(std::string sessionTimezone);

    std::unique_ptr<vec::BaseVector> DecodeColumn(
        std::unique_ptr<vec::BaseVector> strings,
        const type::DataTypePtr& targetType,
        const std::string& dateFormat = {},
        const std::vector<std::string>& timestampFormats = {}) const;

    std::unique_ptr<vec::BaseVector> EncodeColumn(
        vec::BaseVector* source,
        const type::DataTypePtr& sourceType,
        int64_t start,
        int64_t end,
        const std::string& dateFormat = {},
        const std::string& timestampFormat = {}) const;

private:
    std::unique_ptr<vec::BaseVector> CastOwned(
        std::unique_ptr<vec::BaseVector> input,
        const type::DataTypePtr& fromType,
        const type::DataTypePtr& toType) const;

    std::unique_ptr<vec::BaseVector> ApplyFormatFunction(
        const std::string& functionName,
        std::unique_ptr<vec::BaseVector> input,
        type::DataTypeId inputType,
        type::DataTypeId outputType,
        const std::string& format) const;

    std::unique_ptr<vec::BaseVector> DecodeTimestampFormats(
        std::unique_ptr<vec::BaseVector> strings,
        const std::vector<std::string>& formats) const;

    config::QueryConfig queryConfig_;
};

} // namespace omniruntime::reader::text
