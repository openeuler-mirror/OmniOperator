/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 */
#pragma once

#include <cstdint>
#include <memory>

#include <arrow/io/interfaces.h>

#include "reader/text/TextFormatOptions.h"

namespace omniruntime::reader::text {

class TextSequentialInput {
public:
    virtual ~TextSequentialInput() = default;
    virtual int64_t Read(uint8_t* output, int64_t maxBytes) = 0;
};

class TextOutputSink {
public:
    virtual ~TextOutputSink() = default;
    virtual void Write(const uint8_t* data, int64_t size) = 0;
    virtual void Finish() = 0;
    virtual void Close() = 0;
};

std::unique_ptr<TextSequentialInput> CreateTextSequentialInput(
    std::shared_ptr<arrow::io::RandomAccessFile> file,
    TextCompressionKind codec);

std::unique_ptr<TextOutputSink> CreateTextOutputSink(
    std::shared_ptr<arrow::io::OutputStream> output,
    TextCompressionKind codec,
    uint32_t blockSize);

} // namespace omniruntime::reader::text
