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

TextWriter::TextWriter(TextCodecKind codecKind) : codec_(CreateTextCodec(codecKind)) {}

TextWriter::~TextWriter()
{
    if (output_ != nullptr && !closed_) {
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
    closed_ = false;
}

void TextWriter::Write(vec::BaseVector* vector, int64_t start, int64_t end)
{
    if (output_ == nullptr || closed_) {
        throw std::runtime_error("Text writer is not open.");
    }
    if (vector == nullptr) {
        throw std::runtime_error("Text writer input vector is null.");
    }
    if (vector->GetTypeId() != type::OMNI_VARCHAR && vector->GetTypeId() != type::OMNI_CHAR) {
        throw std::runtime_error("Text writer requires a String vector.");
    }
    if (start < 0 || end < start || end > vector->GetSize()) {
        throw std::runtime_error("Text writer row range is invalid.");
    }

    static constexpr char LINE_FEED = '\n';
    std::string encoded;
    std::vector<TextFieldView> fields(1);
    for (int64_t row = start; row < end; ++row) {
        if (vector->IsNull(static_cast<int32_t>(row))) {
            fields[0] = {true, {}};
        } else {
            auto value = vec::VectorHelper::GetStringValueFromVector(vector, static_cast<int32_t>(row));
            fields[0] = {false, value};
        }
        codec_->EncodeRecord(fields, encoded);
        if (!encoded.empty()) {
            auto status = output_->Write(encoded.data(), encoded.size());
            if (!status.ok()) {
                throw OmniException(status.ToString().c_str());
            }
        }
        auto status = output_->Write(&LINE_FEED, 1);
        if (!status.ok()) {
            throw OmniException(status.ToString().c_str());
        }
    }
}

void TextWriter::Close()
{
    if (output_ == nullptr || closed_) {
        return;
    }
    auto status = output_->Close();
    if (!status.ok()) {
        throw OmniException(status.ToString().c_str());
    }
    closed_ = true;
}

} // namespace omniruntime::reader::text
