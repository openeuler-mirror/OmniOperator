/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 */
#include "reader/text/LazySimpleSerdeCodec.h"

#include <algorithm>
#include <stdexcept>

namespace omniruntime::reader::text {

LazySimpleSerdeCodec::LazySimpleSerdeCodec(
    LazySimpleOptions options,
    const std::vector<int32_t>& projectedFieldIndices,
    size_t fileFieldCount)
    : options_(std::move(options)), projectionEnabled_(true), fileFieldCount_(fileFieldCount)
{
    if (options_.lastColumnTakesRest && fileFieldCount_ == 0) {
        throw std::runtime_error("LazySimple last-column-takes-rest requires the full file schema.");
    }
    projectedFields_.reserve(projectedFieldIndices.size());
    for (size_t outputIndex = 0; outputIndex < projectedFieldIndices.size(); ++outputIndex) {
        if (projectedFieldIndices[outputIndex] < 0) {
            throw std::runtime_error("LazySimple projected field index must be non-negative.");
        }
        projectedFields_.push_back(
            {static_cast<size_t>(projectedFieldIndices[outputIndex]), outputIndex});
    }
    std::stable_sort(
        projectedFields_.begin(),
        projectedFields_.end(),
        [](const ProjectedField& left, const ProjectedField& right) {
            return left.sourceIndex < right.sourceIndex;
        });
}

void LazySimpleSerdeCodec::DecodeField(
    std::string_view raw,
    DecodedTextRecord& output,
    TextFieldView& field) const
{
    const auto& delimited = options_.delimited;
    if (raw == delimited.nullLiteral) {
        field = {true, {}};
        return;
    }
    if (!delimited.escapeEnabled ||
        raw.find(delimited.escapeChar) == std::string_view::npos) {
        field = {false, raw};
        return;
    }

    const auto offset = output.storage.size();
    for (size_t index = 0; index < raw.size(); ++index) {
        if (raw[index] == delimited.escapeChar && index + 1 < raw.size()) {
            ++index;
        }
        output.storage.push_back(raw[index]);
    }
    field = {false, std::string_view(output.storage).substr(offset)};
}

void LazySimpleSerdeCodec::AppendDecodedField(
    std::string_view raw, DecodedTextRecord& output) const
{
    output.fields.emplace_back();
    DecodeField(raw, output, output.fields.back());
}

void LazySimpleSerdeCodec::DecodeProjectedRecord(
    std::string_view record, DecodedTextRecord& output) const
{
    output.Reset();
    output.storage.reserve(record.size());
    output.fields.resize(projectedFields_.size(), TextFieldView{true, {}});
    if (projectedFields_.empty()) {
        return;
    }

    const auto& delimited = options_.delimited;
    auto projected = projectedFields_.cbegin();
    size_t sourceIndex = 0;
    size_t fieldStart = 0;
    auto decodeIfProjected = [&](std::string_view raw) {
        if (projected == projectedFields_.cend() || projected->sourceIndex != sourceIndex) {
            return false;
        }
        TextFieldView field;
        DecodeField(raw, output, field);
        do {
            output.fields[projected->outputIndex] = field;
            ++projected;
        } while (projected != projectedFields_.cend() &&
            projected->sourceIndex == sourceIndex);
        return projected == projectedFields_.cend();
    };

    if (options_.lastColumnTakesRest && fileFieldCount_ == 1) {
        decodeIfProjected(record);
        return;
    }

    for (size_t index = 0; index < record.size(); ++index) {
        if (delimited.escapeEnabled && record[index] == delimited.escapeChar &&
            index + 1 < record.size()) {
            ++index;
            continue;
        }
        if (record[index] != delimited.fieldDelimiter) {
            continue;
        }
        if (decodeIfProjected(record.substr(fieldStart, index - fieldStart))) {
            return;
        }
        ++sourceIndex;
        fieldStart = index + 1;
        if (options_.lastColumnTakesRest && sourceIndex + 1 == fileFieldCount_) {
            break;
        }
    }
    decodeIfProjected(record.substr(fieldStart));
}

void LazySimpleSerdeCodec::DecodeRecord(
    std::string_view record, DecodedTextRecord& output) const
{
    if (projectionEnabled_) {
        DecodeProjectedRecord(record, output);
        return;
    }
    output.Reset();
    output.storage.reserve(record.size());
    const auto& delimited = options_.delimited;
    size_t fieldStart = 0;
    for (size_t index = 0; index < record.size(); ++index) {
        if (delimited.escapeEnabled && record[index] == delimited.escapeChar &&
            index + 1 < record.size()) {
            ++index;
            continue;
        }
        if (record[index] == delimited.fieldDelimiter) {
            AppendDecodedField(record.substr(fieldStart, index - fieldStart), output);
            fieldStart = index + 1;
        }
    }
    AppendDecodedField(record.substr(fieldStart), output);
}

void LazySimpleSerdeCodec::EncodeRecord(
    const std::vector<TextFieldView>& fields, std::string& output) const
{
    output.clear();
    const auto& delimited = options_.delimited;
    for (size_t fieldIndex = 0; fieldIndex < fields.size(); ++fieldIndex) {
        if (fieldIndex > 0) {
            output.push_back(delimited.fieldDelimiter);
        }
        const auto& field = fields[fieldIndex];
        if (field.isNull) {
            output.append(delimited.nullLiteral);
            continue;
        }
        for (const auto value : field.value) {
            if (delimited.escapeEnabled &&
                (value == delimited.fieldDelimiter ||
                    (options_.collectionDelimiter != '\0' &&
                        value == options_.collectionDelimiter) ||
                    (options_.mapKeyDelimiter != '\0' &&
                        value == options_.mapKeyDelimiter) ||
                    value == delimited.escapeChar)) {
                output.push_back(delimited.escapeChar);
            }
            output.push_back(value);
        }
    }
}

} // namespace omniruntime::reader::text
