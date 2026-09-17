/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 */
#pragma once

#include <cstddef>
#include <cstdint>
#include <utility>
#include <vector>

#include "reader/text/TextCodec.h"

namespace omniruntime::reader::text {

class LazySimpleSerdeCodec final : public TextCodec {
public:
    explicit LazySimpleSerdeCodec(LazySimpleOptions options) : options_(std::move(options)) {}

    LazySimpleSerdeCodec(
        LazySimpleOptions options,
        const std::vector<int32_t>& projectedFieldIndices,
        size_t fileFieldCount = 0);

    void DecodeRecord(std::string_view record, DecodedTextRecord& output) const override;

    void EncodeRecord(const std::vector<TextFieldView>& fields, std::string& output) const override;

private:
    struct ProjectedField {
        size_t sourceIndex;
        size_t outputIndex;
    };

    void DecodeField(
        std::string_view raw,
        DecodedTextRecord& output,
        TextFieldView& field) const;
    void AppendDecodedField(std::string_view raw, DecodedTextRecord& output) const;
    void DecodeProjectedRecord(std::string_view record, DecodedTextRecord& output) const;

    LazySimpleOptions options_;
    bool projectionEnabled_ = false;
    size_t fileFieldCount_ = 0;
    std::vector<ProjectedField> projectedFields_;
};

} // namespace omniruntime::reader::text
