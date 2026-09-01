/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 */
#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

#include <arrow/buffer.h>
#include <arrow/io/interfaces.h>

namespace omniruntime::reader::text {

inline constexpr int64_t DEFAULT_TEXT_READ_BUFFER_SIZE = 64 * 1024;

class TextLineScanner final {
public:
    TextLineScanner(
        std::shared_ptr<arrow::io::RandomAccessFile> file,
        int64_t fileSize,
        int64_t splitStart,
        int64_t splitEnd,
        int64_t bufferSize = DEFAULT_TEXT_READ_BUFFER_SIZE);

    // The returned view remains valid until the next scanner call.
    bool NextLine(std::string_view& line);

    // Counts records without materializing their contents.
    uint64_t CountRows(uint64_t maxRows);

private:
    bool CanStartRecord() const;
    bool LoadBuffer(int64_t position);
    bool ReadByte(int64_t position, uint8_t& value);
    bool SkipCurrentLine();
    void AlignToSplitStart();
    void ConsumeLfAfterCr();

    std::shared_ptr<arrow::io::RandomAccessFile> file_;
    int64_t fileSize_ = 0;
    int64_t splitEnd_ = 0;
    int64_t cursor_ = 0;
    int64_t bufferSize_ = 0;
    int64_t bufferStart_ = -1;
    std::shared_ptr<arrow::Buffer> buffer_;
    std::string scratch_;
};

} // namespace omniruntime::reader::text
