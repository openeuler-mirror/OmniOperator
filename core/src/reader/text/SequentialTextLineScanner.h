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

#include "reader/text/TextCompressionStream.h"
#include "reader/text/TextLineScanner.h"

namespace omniruntime::reader::text {

class SequentialTextLineScanner final {
public:
    SequentialTextLineScanner(std::unique_ptr<TextSequentialInput> input,
        int64_t bufferSize = DEFAULT_TEXT_READ_BUFFER_SIZE,
        bool stripUtf8Bom = false);

    bool NextLine(std::string_view& line);
    uint64_t CountRows(uint64_t maxRows, bool skipBlankLines = false);

private:
    bool FillBuffer();
    void StripUtf8Bom();

    std::unique_ptr<TextSequentialInput> input_;
    std::vector<uint8_t> buffer_;
    size_t position_ = 0;
    size_t size_ = 0;
    bool eof_ = false;
    std::string scratch_;
};

} // namespace omniruntime::reader::text
