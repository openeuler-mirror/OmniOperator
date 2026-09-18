/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 */
#pragma once

#include "reader/text/TextCodec.h"

namespace omniruntime::reader::text {

class RawLineCodec final : public TextCodec {
public:
    void DecodeRecord(std::string_view record, DecodedTextRecord& output) const override;

    void EncodeRecord(
        const std::vector<TextFieldView>& fields, std::string& output) const override;
};

} // namespace omniruntime::reader::text
