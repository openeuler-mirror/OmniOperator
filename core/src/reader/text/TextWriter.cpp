/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 */
#include "reader/text/TextWriter.h"

#include <arrow/result.h>

#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <string>

#include "reader/arrowadapter/FileSystemAdapter.h"
#include "reader/common/Directories.h"
#include "util/omni_exception.h"
#include "vector/vector_helper.h"

namespace omniruntime::reader::text {

using omniruntime::exception::OmniException;

namespace {

TextFormatOptions DefaultRawLineOptions()
{
    TextFormatOptions options;
    options.sourceKind = TextSourceKind::SPARK_TEXT;
    options.codecKind = TextCodecKind::RAW_LINE;
    options.common.charset = "UTF-8";
    options.common.compressionCodec = "NONE";
    options.dialect = RawLineOptions{};
    return options;
}

bool IsCompatibleVectorType(type::DataTypeId actual, type::DataTypeId expected)
{
    if (actual == expected) {
        return true;
    }
    return (expected == type::OMNI_VARCHAR && actual == type::OMNI_CHAR) ||
        (expected == type::OMNI_DATE32 && actual == type::OMNI_INT) ||
        (expected == type::OMNI_TIMESTAMP && actual == type::OMNI_LONG) ||
        (expected == type::OMNI_DECIMAL64 && actual == type::OMNI_LONG);
}

type::RowTypePtr DefaultRawLineSchema()
{
    return ROW(
        std::vector<std::string>{"value"},
        std::vector<type::DataTypePtr>{std::make_shared<type::VarcharDataType>()});
}

} // namespace

TextWriter::TextWriter() : TextWriter(DefaultRawLineOptions(), DefaultRawLineSchema()) {}

TextWriter::TextWriter(TextFormatOptions options, type::RowTypePtr rowType)
    : options_(std::move(options)),
      rowType_(std::move(rowType)),
      codec_(CreateTextCodec(options_)),
      valueConverter_(options_.common.sessionTimezone)
{
    options_.Validate();
    if (rowType_ == nullptr || rowType_->size() == 0) {
        throw std::runtime_error("Text writer schema is empty.");
    }
    if (options_.IsRawLine()) {
        if (rowType_->size() != 1 || rowType_->childAt(0)->GetId() != type::OMNI_VARCHAR) {
            throw std::runtime_error("RawLine writer requires exactly one String column.");
        }
        return;
    }
    for (int32_t column = 0; column < rowType_->size(); ++column) {
        const auto& child = rowType_->childAt(column);
        switch (child->GetId()) {
            case type::OMNI_BOOLEAN:
            case type::OMNI_BYTE:
            case type::OMNI_SHORT:
            case type::OMNI_INT:
            case type::OMNI_LONG:
            case type::OMNI_FLOAT:
            case type::OMNI_DOUBLE:
            case type::OMNI_VARCHAR:
            case type::OMNI_DECIMAL64:
            case type::OMNI_DECIMAL128:
            case type::OMNI_DATE32:
            case type::OMNI_TIMESTAMP:
                break;
            default:
                throw std::runtime_error("LazySimple writer schema contains an unsupported type.");
        }
    }
}

TextWriter::~TextWriter()
{
    if (outputSink_ != nullptr && !closed_) {
        try {
            outputSink_->Close();
        } catch (...) {
        }
    } else if (output_ != nullptr && !closed_) {
        output_->Close();
    }
}

void TextWriter::Init(const UriInfo& uri)
{
    std::string fileSystemPath;
    auto fileSystemResult = arrow_adapter::FileSystemFromUriOrPath(uri, &fileSystemPath);
    if (!fileSystemResult.ok()) {
        throw OmniException(fileSystemResult.status().ToString().c_str());
    }
    fileSystem_ = std::move(fileSystemResult).ValueUnsafe();
    if (uri.Scheme() == UriInfo::LOCAL_FILE) {
        const auto parentPath = common::getParentPath(fileSystemPath);
        if (!parentPath.empty() && common::createDirectories(parentPath) != 0) {
            OMNI_FAIL("Create local directories fail, path: {}, err msg: {}", parentPath, strerror(errno));
        }
    }
    auto outputResult = fileSystem_->OpenOutputStream(fileSystemPath);
    if (!outputResult.ok()) {
        throw OmniException(outputResult.status().ToString().c_str());
    }
    output_ = std::move(outputResult).ValueUnsafe();
    outputSink_ = CreateTextOutputSink(output_, options_.Compression(),
        options_.common.compressionBlockSize);
    closed_ = false;
    if (options_.IsCsv() && options_.Csv().delimited.emitHeader) {
        std::vector<TextFieldView> fields;
        for (const auto& name : rowType_->names()) {
            fields.push_back({false, name});
        }
        std::string header;
        codec_->EncodeRecord(fields, header);
        header.push_back('\n');
        outputSink_->Write(reinterpret_cast<const uint8_t*>(header.data()), header.size());
    }
}

void TextWriter::Write(vec::BaseVector* vector, int64_t start, int64_t end)
{
    Write(std::vector<vec::BaseVector*>{vector}, start, end);
}

void TextWriter::Write(
    const std::vector<vec::BaseVector*>& vectors, int64_t start, int64_t end)
{
    if (outputSink_ == nullptr || closed_) {
        throw std::runtime_error("Text writer is not open.");
    }
    if (vectors.size() != static_cast<size_t>(rowType_->size())) {
        throw std::runtime_error("Text writer vector count does not match its schema.");
    }
    if (vectors.empty() || vectors.front() == nullptr ||
        start < 0 || end < start || end > vectors.front()->GetSize()) {
        throw std::runtime_error("Text writer row range is invalid.");
    }
    if (start == end) {
        return;
    }
    for (size_t column = 0; column < vectors.size(); ++column) {
        if (vectors[column] == nullptr || vectors[column]->GetSize() != vectors.front()->GetSize()) {
            throw std::runtime_error("Text writer input vectors have inconsistent sizes.");
        }
        if (!IsCompatibleVectorType(
                vectors[column]->GetTypeId(),
                rowType_->childAt(static_cast<int32_t>(column))->GetId())) {
            throw std::runtime_error("Text writer input vector type does not match its schema.");
        }
    }

    static constexpr char LINE_FEED = '\n';
    auto writeRecord = [this](const std::string& encoded) {
        if (!encoded.empty()) {
            outputSink_->Write(reinterpret_cast<const uint8_t*>(encoded.data()), encoded.size());
        }
        outputSink_->Write(reinterpret_cast<const uint8_t*>(&LINE_FEED), 1);
    };

    std::string encoded;
    if (options_.IsRawLine()) {
        std::vector<TextFieldView> fields(1);
        for (int64_t row = start; row < end; ++row) {
            fields[0] = vectors[0]->IsNull(static_cast<int32_t>(row))
                ? TextFieldView{true, {}}
                : TextFieldView{false, vec::VectorHelper::GetStringValueFromVector(
                    vectors[0], static_cast<int32_t>(row))};
            codec_->EncodeRecord(fields, encoded);
            writeRecord(encoded);
        }
        return;
    }

    std::vector<std::unique_ptr<vec::BaseVector>> stringColumns;
    stringColumns.reserve(vectors.size());
    const bool useCsvFormats = options_.sourceKind == TextSourceKind::SPARK_CSV;
    const auto& timestampFormats = options_.temporal.timestampFormats;
    for (size_t column = 0; column < vectors.size(); ++column) {
        stringColumns.emplace_back(valueConverter_.EncodeColumn(
            vectors[column], rowType_->childAt(static_cast<int32_t>(column)), start, end,
            useCsvFormats ? options_.temporal.dateFormat : std::string{},
            useCsvFormats && !timestampFormats.empty() ? timestampFormats.front() : std::string{}));
    }

    std::vector<TextFieldView> fields(vectors.size());
    for (int64_t row = 0; row < end - start; ++row) {
        for (size_t column = 0; column < stringColumns.size(); ++column) {
            if (stringColumns[column]->IsNull(static_cast<int32_t>(row))) {
                fields[column] = {true, {}};
            } else {
                fields[column] = {false, vec::VectorHelper::GetStringValueFromVector(
                    stringColumns[column].get(), static_cast<int32_t>(row))};
            }
        }
        codec_->EncodeRecord(fields, encoded);
        writeRecord(encoded);
    }
}

void TextWriter::Close()
{
    if (outputSink_ == nullptr || closed_) {
        return;
    }
    outputSink_->Close();
    closed_ = true;
}

} // namespace omniruntime::reader::text
