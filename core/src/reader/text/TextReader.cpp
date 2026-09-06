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
#include "vector/unsafe_vector.h"
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
    if (rowType_ == nullptr) {
        throw std::runtime_error("Text reader projected row type is missing.");
    }
    if (reader_.GetTextOptions().IsLazySimple() && rowType_->size() > 0) {
        if (fileRowType_ == nullptr) {
            throw std::runtime_error("LazySimple reader requires projected and full file schemas.");
        }
        valueConverter_ = std::make_unique<TextValueConverter>(
            reader.GetTextOptions().common.sessionTimezone);
        std::vector<int32_t> projectedFieldIndices;
        projectedFieldIndices.reserve(rowType_->size());
        for (const auto& name : rowType_->names()) {
            const auto index = fileRowType_->getChildIdxIfExists(name);
            if (!index.has_value()) {
                throw std::runtime_error("LazySimple projected column is missing from the file schema: " + name);
            }
            projectedFieldIndices.push_back(static_cast<int32_t>(*index));
        }
        codec_ = CreateTextCodec(reader.GetTextOptions(), projectedFieldIndices);
    } else if (rowType_->size() > 0) {
        codec_ = CreateTextCodec(reader.GetTextOptions());
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
    const auto& textOptions = reader_.GetTextOptions();
    if (rowType_->size() == 0) {
        return lineScanner_->CountRows(batchLen);
    }
    if (textOptions.IsRawLine()) {
        if (rowType_->size() > 1) {
            throw std::runtime_error("RawLine reader requires zero or one projected file column.");
        }
        if (rowType_->size() == 1) {
            auto typeId = rowType_->childAt(0)->GetId();
            if (typeId != type::OMNI_VARCHAR && typeId != type::OMNI_CHAR) {
                throw std::runtime_error("RawLine reader projected column must be String.");
            }
        }
    }

    if (textOptions.IsRawLine() && rowType_->size() == 1) {
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
        return records.size();
    }
    if (textOptions.IsLazySimple() && rowType_->size() > 0) {
        using StringVector = vec::Vector<vec::LargeStringContainer<std::string_view>>;
        std::vector<std::unique_ptr<vec::BaseVector>> stringColumns;
        std::vector<StringVector*> writableColumns;
        stringColumns.reserve(rowType_->size());
        writableColumns.reserve(rowType_->size());
        for (int32_t column = 0; column < rowType_->size(); ++column) {
            std::unique_ptr<vec::BaseVector> stringColumn(
                vec::VectorHelper::CreateStringVector(static_cast<uint32_t>(batchLen)));
            writableColumns.push_back(reinterpret_cast<StringVector*>(stringColumn.get()));
            stringColumns.emplace_back(std::move(stringColumn));
        }

        DecodedTextRecord decoded;
        uint64_t rows = 0;
        std::string_view record;
        while (rows < batchLen && lineScanner_->NextLine(record)) {
            codec_->DecodeRecord(record, decoded);
            if (decoded.fields.size() != writableColumns.size()) {
                throw std::runtime_error("LazySimpleCodec output does not match projected schema.");
            }
            for (size_t column = 0; column < writableColumns.size(); ++column) {
                if (decoded.fields[column].isNull) {
                    writableColumns[column]->SetNull(static_cast<int32_t>(rows));
                } else {
                    writableColumns[column]->SetValue(
                        static_cast<int32_t>(rows), decoded.fields[column].value);
                }
            }
            ++rows;
        }
        if (rows == 0) {
            return 0;
        }
        for (int32_t column = 0; column < rowType_->size(); ++column) {
            vec::unsafe::UnsafeBaseVector::SetSize(
                stringColumns[column].get(), static_cast<int32_t>(rows));
            auto converted = valueConverter_->DecodeColumn(
                std::move(stringColumns[column]), rowType_->childAt(column));
            batch->push_back(converted.release());
        }
        return rows;
    }
    throw std::runtime_error("Unsupported Text reader codec.");
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
