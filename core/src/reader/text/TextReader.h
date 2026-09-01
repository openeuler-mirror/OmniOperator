/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 */
#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <arrow/filesystem/filesystem.h>
#include <arrow/io/interfaces.h>

#include "reader/Reader.h"
#include "reader/text/TextCodec.h"
#include "reader/text/TextFormatOptions.h"
#include "reader/text/TextLineScanner.h"

namespace omniruntime::reader::text {

class TextReader;

class TextRowReader final : public RowReader {
public:
    explicit TextRowReader(TextReader& reader);

    uint64_t Next(uint64_t size, vec::VectorPtr& result) override;

    uint64_t NextDirect(std::vector<BaseVector*>* batch, int* omniTypeId, uint64_t batchLen) override;

    uint64_t Next(std::vector<BaseVector*>** batch, int* omniTypeId, uint64_t batchLen) override;

private:
    TextReader& reader_;
    std::unique_ptr<TextCodec> codec_;
    std::unique_ptr<TextLineScanner> lineScanner_;
};

class TextReader final : public Reader {
public:
    TextReader(std::shared_ptr<ReaderOptions> options, TextFormatOptions textOptions);

    void InitReader();

    std::unique_ptr<RowReader> CreateRowReader() override;

    const std::shared_ptr<arrow::io::RandomAccessFile>& GetFile() const;
    int64_t GetFileSize() const;
    const TextFormatOptions& GetTextOptions() const;
    const std::shared_ptr<ReaderOptions>& GetOptions() const;

private:
    TextFormatOptions textOptions_;
    std::shared_ptr<arrow::fs::FileSystem> fileSystem_;
    std::shared_ptr<arrow::io::RandomAccessFile> file_;
    int64_t fileSize_ = 0;
};

} // namespace omniruntime::reader::text
