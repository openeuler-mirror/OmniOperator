/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 */
#include "reader/text/RawLineCodec.h"

#include <stdexcept>

namespace omniruntime::reader::text {

void RawLineCodec::DecodeRecord(std::string_view record, DecodedTextRecord& output) const
{
    output.Reset();
    output.fields.push_back({false, record});
}

void RawLineCodec::EncodeRecord(
    const std::vector<TextFieldView>& fields, std::string& output) const
{
    if (fields.size() != 1) {
        throw std::runtime_error("RawLineCodec requires exactly one field.");
    }
    output.clear();
    if (!fields[0].isNull) {
        output.assign(fields[0].value.data(), fields[0].value.size());
    }
}

} // namespace omniruntime::reader::text
