/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 */
#pragma once

#include "reader/text/TextCodec.h"

namespace omniruntime::reader::text {

class CsvCodec final : public TextCodec {
public:
    explicit CsvCodec(const TextFormatOptions& options);
    CsvCodec(const TextFormatOptions& options, const std::vector<int32_t>& projection);

    void DecodeRecord(std::string_view record, DecodedTextRecord& output) const override;
    void EncodeRecord(const std::vector<TextFieldView>& fields, std::string& output) const override;

private:
    void DecodeHiveRecord(std::string_view record, DecodedTextRecord& output) const;
    struct ProjectedField {
        size_t source;
        size_t output;
    };

    CsvOptions options_;
    bool hive_;
    bool projected_ = false;
    std::vector<ProjectedField> projection_;
};

} // namespace omniruntime::reader::text
