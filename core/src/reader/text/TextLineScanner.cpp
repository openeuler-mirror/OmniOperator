/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 */
#include "reader/text/TextLineScanner.h"

#include <arrow/result.h>

#include <algorithm>
#include <limits>
#include <stdexcept>
#include <utility>

#include "util/omni_exception.h"

namespace omniruntime::reader::text {

using omniruntime::exception::OmniException;

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

TextLineScanner::TextLineScanner(
    std::shared_ptr<arrow::io::RandomAccessFile> file,
    int64_t fileSize,
    int64_t splitStart,
    int64_t splitEnd,
    int64_t bufferSize,
    bool stripUtf8Bom)
    : file_(std::move(file)),
      fileSize_(std::max<int64_t>(0, fileSize)),
      splitEnd_(splitEnd < 0 || splitEnd == std::numeric_limits<int64_t>::max()
              ? fileSize_
              : std::min(splitEnd, fileSize_)),
      cursor_(std::min(std::max<int64_t>(0, splitStart), fileSize_)),
      bufferSize_(bufferSize)
{
    if (file_ == nullptr) {
        throw std::runtime_error("Text line scanner file is null.");
    }
    if (bufferSize_ <= 0) {
        throw std::runtime_error("Text line scanner buffer size must be positive.");
    }
    if (stripUtf8Bom && cursor_ == 0 && fileSize_ >= 3) {
        uint8_t first = 0;
        uint8_t second = 0;
        uint8_t third = 0;
        if (ReadByte(0, first) && ReadByte(1, second) && ReadByte(2, third) &&
            first == 0xef && second == 0xbb && third == 0xbf) {
            cursor_ = 3;
            return;
        }
    }
    AlignToSplitStart();
}

bool TextLineScanner::CanStartRecord() const
{
    return cursor_ < fileSize_ && cursor_ < splitEnd_;
}

bool TextLineScanner::LoadBuffer(int64_t position)
{
    if (position < 0 || position >= fileSize_) {
        return false;
    }
    if (buffer_ != nullptr && position >= bufferStart_ &&
        position < bufferStart_ + buffer_->size()) {
        return true;
    }

    bufferStart_ = position;
    const auto readSize = std::min(bufferSize_, fileSize_ - position);
    auto result = file_->ReadAt(position, readSize);
    if (!result.ok()) {
        throw OmniException(result.status().ToString().c_str());
    }
    buffer_ = std::move(result).ValueUnsafe();
    return buffer_ != nullptr && buffer_->size() > 0;
}

bool TextLineScanner::ReadByte(int64_t position, uint8_t& value)
{
    if (!LoadBuffer(position)) {
        return false;
    }
    value = buffer_->data()[position - bufferStart_];
    return true;
}

void TextLineScanner::ConsumeLfAfterCr()
{
    uint8_t next = 0;
    if (ReadByte(cursor_, next) && next == '\n') {
        ++cursor_;
    }
}

bool TextLineScanner::SkipCurrentLine()
{
    if (cursor_ >= fileSize_) {
        return false;
    }
    while (cursor_ < fileSize_) {
        if (!LoadBuffer(cursor_)) {
            return false;
        }
        const auto offset = cursor_ - bufferStart_;
        const auto* data = buffer_->data() + offset;
        const auto available = static_cast<size_t>(buffer_->size() - offset);
        const auto* separator = FindLineSeparator(data, available);
        if (separator == nullptr) {
            cursor_ += static_cast<int64_t>(available);
            continue;
        }

        cursor_ += static_cast<int64_t>(separator - data) + 1;
        if (*separator == '\r') {
            ConsumeLfAfterCr();
        }
        return true;
    }
    return true;
}

void TextLineScanner::AlignToSplitStart()
{
    if (cursor_ == 0 || cursor_ >= fileSize_) {
        return;
    }
    uint8_t previous = 0;
    if (!ReadByte(cursor_ - 1, previous)) {
        return;
    }
    if (previous == '\n') {
        return;
    }
    if (previous == '\r') {
        uint8_t current = 0;
        if (ReadByte(cursor_, current) && current == '\n') {
            ++cursor_;
        }
        return;
    }
    SkipCurrentLine();
}

bool TextLineScanner::NextLine(std::string_view& line)
{
    line = {};
    if (!CanStartRecord()) {
        return false;
    }

    scratch_.clear();
    bool usesScratch = false;
    while (cursor_ < fileSize_) {
        if (!LoadBuffer(cursor_)) {
            return false;
        }
        const auto offset = cursor_ - bufferStart_;
        const auto* data = buffer_->data() + offset;
        const auto available = static_cast<size_t>(buffer_->size() - offset);
        const auto* separator = FindLineSeparator(data, available);
        if (separator != nullptr) {
            const auto contentSize = static_cast<size_t>(separator - data);
            if (usesScratch) {
                scratch_.append(reinterpret_cast<const char*>(data), contentSize);
                line = std::string_view(scratch_.data(), scratch_.size());
            } else {
                line = std::string_view(reinterpret_cast<const char*>(data), contentSize);
            }

            cursor_ += static_cast<int64_t>(contentSize) + 1;
            if (*separator == '\r') {
                const bool reloadsBuffer = cursor_ < fileSize_ &&
                    cursor_ >= bufferStart_ + buffer_->size();
                if (reloadsBuffer && !usesScratch) {
                    scratch_.assign(line.data(), line.size());
                    line = std::string_view(scratch_.data(), scratch_.size());
                    usesScratch = true;
                }
                ConsumeLfAfterCr();
            }
            return true;
        }

        if (cursor_ + static_cast<int64_t>(available) >= fileSize_) {
            if (usesScratch) {
                scratch_.append(reinterpret_cast<const char*>(data), available);
                line = std::string_view(scratch_.data(), scratch_.size());
            } else {
                line = std::string_view(reinterpret_cast<const char*>(data), available);
            }
            cursor_ = fileSize_;
            return true;
        }

        scratch_.append(reinterpret_cast<const char*>(data), available);
        usesScratch = true;
        cursor_ += static_cast<int64_t>(available);
    }
    return false;
}

uint64_t TextLineScanner::CountRows(uint64_t maxRows, bool skipBlankLines)
{
    return skipBlankLines ? CountRowsImpl<true>(maxRows) : CountRowsImpl<false>(maxRows);
}

template <bool skipBlankLines>
uint64_t TextLineScanner::CountRowsImpl(uint64_t maxRows)
{
    uint64_t rows = 0;
    bool recordStarted = false;
    bool nonBlank = false;
    while (rows < maxRows) {
        if (!recordStarted) {
            if (!CanStartRecord()) {
                break;
            }
            recordStarted = true;
        }
        if (!LoadBuffer(cursor_)) {
            break;
        }

        const auto offset = cursor_ - bufferStart_;
        const auto* current = buffer_->data() + offset;
        const auto* end = buffer_->data() + buffer_->size();
        while (current < end) {
            const auto value = *current++;
            const bool separatorAtBufferEnd = value == '\r' && current == end;
            ++cursor_;
            if (value != '\n' && value != '\r') {
                if constexpr (skipBlankLines) {
                    nonBlank = nonBlank || value > ' ';
                }
                continue;
            }
            if (value == '\r') {
                if (current < end) {
                    if (*current == '\n') {
                        ++current;
                        ++cursor_;
                    }
                } else {
                    ConsumeLfAfterCr();
                }
            }

            if (!skipBlankLines || nonBlank) {
                ++rows;
            }
            nonBlank = false;
            recordStarted = false;
            if (rows >= maxRows || !CanStartRecord()) {
                return rows;
            }
            recordStarted = true;
            if (separatorAtBufferEnd) {
                break;
            }
        }

        if (cursor_ >= fileSize_) {
            if (recordStarted && (!skipBlankLines || nonBlank)) {
                ++rows;
            }
            break;
        }
    }
    return rows;
}

} // namespace omniruntime::reader::text
