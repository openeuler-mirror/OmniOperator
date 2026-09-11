/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 */
#include "reader/text/SequentialTextLineScanner.h"

#include <algorithm>
#include <stdexcept>
#include <utility>

namespace omniruntime::reader::text {
namespace {

const uint8_t* FindLineSeparator(const uint8_t* data, size_t size)
{
    const auto* end = data + size;
    while (data < end) {
        if (*data == '\n' || *data == '\r') {
            return data;
        }
        ++data;
    }
    return nullptr;
}

} // namespace

SequentialTextLineScanner::SequentialTextLineScanner(
    std::unique_ptr<TextSequentialInput> input, int64_t bufferSize, bool stripUtf8Bom)
    : input_(std::move(input)),
      buffer_(static_cast<size_t>(std::max<int64_t>(bufferSize, 1)))
{
    if (input_ == nullptr) {
        throw std::runtime_error("Sequential Text line scanner input is null.");
    }
    if (bufferSize <= 0) {
        throw std::runtime_error("Sequential Text line scanner buffer size must be positive.");
    }
    if (stripUtf8Bom) {
        StripUtf8Bom();
    }
}

bool SequentialTextLineScanner::FillBuffer()
{
    if (eof_) {
        return false;
    }
    const auto bytes = input_->Read(buffer_.data(), buffer_.size());
    if (bytes < 0 || bytes > static_cast<int64_t>(buffer_.size())) {
        throw std::runtime_error("Sequential Text input returned an invalid byte count.");
    }
    position_ = 0;
    size_ = static_cast<size_t>(bytes);
    eof_ = bytes == 0;
    return !eof_;
}

void SequentialTextLineScanner::StripUtf8Bom()
{
    if (!FillBuffer() || size_ < 3) {
        return;
    }
    if (buffer_[0] == 0xef && buffer_[1] == 0xbb && buffer_[2] == 0xbf) {
        position_ = 3;
    }
}

bool SequentialTextLineScanner::NextLine(std::string_view& line)
{
    line = {};
    scratch_.clear();
    bool usesScratch = false;
    while (position_ < size_ || FillBuffer()) {
        const auto* data = buffer_.data() + position_;
        const auto available = size_ - position_;
        const auto* separator = FindLineSeparator(data, available);
        if (separator == nullptr) {
            scratch_.append(reinterpret_cast<const char*>(data), available);
            usesScratch = true;
            position_ = size_;
            continue;
        }

        const auto contentSize = static_cast<size_t>(separator - data);
        if (usesScratch) {
            scratch_.append(reinterpret_cast<const char*>(data), contentSize);
            line = std::string_view(scratch_.data(), scratch_.size());
        } else {
            line = std::string_view(reinterpret_cast<const char*>(data), contentSize);
        }
        position_ += contentSize + 1;
        if (*separator == '\r') {
            if (position_ == size_) {
                if (!usesScratch) {
                    scratch_.assign(line.data(), line.size());
                    line = std::string_view(scratch_.data(), scratch_.size());
                }
                FillBuffer();
            }
            if (position_ < size_ && buffer_[position_] == '\n') {
                ++position_;
            }
        }
        return true;
    }
    if (usesScratch) {
        line = std::string_view(scratch_.data(), scratch_.size());
        return true;
    }
    return false;
}

uint64_t SequentialTextLineScanner::CountRows(uint64_t maxRows, bool skipBlankLines)
{
    uint64_t rows = 0;
    bool recordStarted = false;
    bool nonBlank = false;
    while (rows < maxRows && (position_ < size_ || FillBuffer())) {
        const auto value = buffer_[position_++];
        recordStarted = true;
        if (value != '\n' && value != '\r') {
            nonBlank = nonBlank || value > ' ';
            continue;
        }
        if (value == '\r') {
            if (position_ == size_) {
                FillBuffer();
            }
            if (position_ < size_ && buffer_[position_] == '\n') {
                ++position_;
            }
        }
        if (!skipBlankLines || nonBlank) {
            ++rows;
        }
        recordStarted = false;
        nonBlank = false;
    }
    if (rows < maxRows && recordStarted && (!skipBlankLines || nonBlank)) {
        ++rows;
    }
    return rows;
}

} // namespace omniruntime::reader::text
