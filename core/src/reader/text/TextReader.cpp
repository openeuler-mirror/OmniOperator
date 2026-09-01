/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 */
#include "reader/text/TextReader.h"

#include <arrow/result.h>

#include <stdexcept>
#include <utility>

#include "reader/arrowadapter/FileSystemAdapter.h"
#include "util/omni_exception.h"
#include "vector/vector_helper.h"

namespace omniruntime::reader::text {

using omniruntime::exception::OmniException;

TextReader::TextReader(std::shared_ptr<ReaderOptions> options, TextFormatOptions textOptions)
    : textOptions_(std::move(textOptions))
{
    options_ = std::move(options);
}

void TextReader::InitReader()
{
    auto uri = options_->GetUri();
    if (uri == nullptr) {
        throw std::runtime_error("Text reader URI is null.");
    }
    std::string fileSystemPath;
    auto fileSystemResult = arrow_adapter::FileSystemFromUriOrPath(*uri, &fileSystemPath);
    if (!fileSystemResult.ok()) {
        throw OmniException(fileSystemResult.status().ToString().c_str());
    }
    fileSystem_ = std::move(fileSystemResult).ValueUnsafe();
    auto fileResult = fileSystem_->OpenInputFile(fileSystemPath);
    if (!fileResult.ok()) {
        throw OmniException(fileResult.status().ToString().c_str());
    }
    file_ = std::move(fileResult).ValueUnsafe();
    auto sizeResult = file_->GetSize();
    if (!sizeResult.ok()) {
        throw OmniException(sizeResult.status().ToString().c_str());
    }
    fileSize_ = std::move(sizeResult).ValueUnsafe();
}

std::unique_ptr<RowReader> TextReader::CreateRowReader()
{
    return std::make_unique<TextRowReader>(*this);
}

const std::shared_ptr<arrow::io::RandomAccessFile>& TextReader::GetFile() const
{
    return file_;
}

int64_t TextReader::GetFileSize() const
{
    return fileSize_;
}

const TextFormatOptions& TextReader::GetTextOptions() const
{
    return textOptions_;
}

const std::shared_ptr<ReaderOptions>& TextReader::GetOptions() const
{
    return options_;
}

TextRowReader::TextRowReader(TextReader& reader)
    : reader_(reader)
{
    options_ = reader.GetOptions();
    rowType_ = options_->GetRowType();
    fileRowType_ = options_->GetFileRowType();
    lineScanner_ = std::make_unique<TextLineScanner>(
        reader.GetFile(),
        reader.GetFileSize(),
        options_->GetSplitStart(),
        options_->GetSplitEnd());
    if (fileRowType_ != nullptr && fileRowType_->size() > 0) {
        codec_ = CreateTextCodec(reader.GetTextOptions().codecKind);
    }
}

uint64_t TextRowReader::Next(uint64_t, vec::VectorPtr&)
{
    return 0;
}

uint64_t TextRowReader::NextDirect(
    std::vector<BaseVector*>* batch, int*, uint64_t batchLen)
{
    if (batch == nullptr || batchLen == 0) {
        return 0;
    }
    if (fileRowType_ == nullptr || fileRowType_->size() > 1) {
        throw std::runtime_error("Text reader requires zero or one projected file column.");
    }
    if (fileRowType_->size() == 0) {
        return lineScanner_->CountRows(batchLen);
    }
    if (fileRowType_->size() == 1) {
        auto typeId = fileRowType_->childAt(0)->GetId();
        if (typeId != type::OMNI_VARCHAR && typeId != type::OMNI_CHAR) {
            throw std::runtime_error("Text reader projected column must be String.");
        }
    }

    std::vector<std::string> records;
    records.reserve(batchLen);
    while (records.size() < batchLen) {
        std::string_view record;
        if (!lineScanner_->NextLine(record)) {
            break;
        }
        records.emplace_back(record);
    }
    if (records.empty()) {
        return 0;
    }

    if (fileRowType_->size() == 1) {
        std::unique_ptr<vec::BaseVector> outputBase(
            vec::VectorHelper::CreateStringVector(records.size()));
        auto* output = reinterpret_cast<vec::Vector<vec::LargeStringContainer<std::string_view>>*>(
            outputBase.get());
        DecodedTextRecord decoded;
        for (size_t index = 0; index < records.size(); ++index) {
            codec_->DecodeRecord(records[index], decoded);
            if (decoded.fields.size() != 1) {
                throw std::runtime_error("RawLineCodec must decode exactly one field.");
            }
            if (decoded.fields[0].isNull) {
                output->SetNull(static_cast<int32_t>(index));
            } else {
                output->SetValue(static_cast<int32_t>(index), decoded.fields[0].value);
            }
        }
        batch->push_back(outputBase.release());
    }
    return records.size();
}

uint64_t TextRowReader::Next(
    std::vector<BaseVector*>** batch, int* omniTypeId, uint64_t batchLen)
{
    auto output = std::make_unique<std::vector<BaseVector*>>();
    auto rows = NextDirect(output.get(), omniTypeId, batchLen);
    *batch = rows == 0 ? nullptr : output.release();
    return rows;
}

} // namespace omniruntime::reader::text
