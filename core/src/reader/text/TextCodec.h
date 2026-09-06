/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 */
#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "reader/text/TextFormatOptions.h"

namespace omniruntime::reader::text {

struct TextFieldView {
    bool isNull = false;
    std::string_view value;
};

struct DecodedTextRecord {
    std::vector<TextFieldView> fields;
    // Codecs that must unescape or normalize input can own transformed bytes here. RawLineCodec
    // leaves it empty and points directly at the Reader's complete record buffer.
    std::string storage;

    void Reset()
    {
        fields.clear();
        storage.clear();
    }
};

class TextCodec {
public:
    virtual ~TextCodec() = default;

    virtual void DecodeRecord(std::string_view record, DecodedTextRecord& output) const = 0;

    virtual void EncodeRecord(
        const std::vector<TextFieldView>& fields, std::string& output) const = 0;
};

std::unique_ptr<TextCodec> CreateTextCodec(const TextFormatOptions& options);

std::unique_ptr<TextCodec> CreateTextCodec(
    const TextFormatOptions& options,
    const std::vector<int32_t>& projectedFieldIndices);

} // namespace omniruntime::reader::text
