/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 */
#include "reader/text/TextCodec.h"

#include <stdexcept>

#include "reader/text/LazySimpleSerdeCodec.h"
#include "reader/text/RawLineCodec.h"

namespace omniruntime::reader::text {

std::unique_ptr<TextCodec> CreateTextCodec(const TextFormatOptions& options)
{
    if (options.IsRawLine()) {
        return std::make_unique<RawLineCodec>();
    }
    if (options.IsLazySimple()) {
        return std::make_unique<LazySimpleSerdeCodec>(options.LazySimple());
    }
    throw std::runtime_error("Unsupported Text codec.");
}

std::unique_ptr<TextCodec> CreateTextCodec(
    const TextFormatOptions& options,
    const std::vector<int32_t>& projectedFieldIndices)
{
    if (options.IsLazySimple()) {
        return std::make_unique<LazySimpleSerdeCodec>(
            options.LazySimple(), projectedFieldIndices);
    }
    throw std::runtime_error("Projected Text decoding requires LazySimple codec.");
}

} // namespace omniruntime::reader::text
