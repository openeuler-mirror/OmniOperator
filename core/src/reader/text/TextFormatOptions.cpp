/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 */
#include "reader/text/TextFormatOptions.h"

#include <stdexcept>

namespace omniruntime::reader::text {
namespace {

TextSourceKind ParseSourceKind(const std::string& value)
{
    if (value == "SPARK_TEXT") {
        return TextSourceKind::SPARK_TEXT;
    }
    if (value == "HIVE_TEXT") {
        return TextSourceKind::HIVE_TEXT;
    }
    if (value == "SPARK_CSV") {
        return TextSourceKind::SPARK_CSV;
    }
    return TextSourceKind::UNKNOWN;
}

TextCodecKind ParseCodecKind(const std::string& value)
{
    if (value == "RAW_LINE") {
        return TextCodecKind::RAW_LINE;
    }
    if (value == "LAZY_SIMPLE") {
        return TextCodecKind::LAZY_SIMPLE;
    }
    if (value == "CSV") {
        return TextCodecKind::CSV;
    }
    return TextCodecKind::UNKNOWN;
}

char ParseSingleByte(const nlohmann::json& json, const char* key, bool allowEmpty)
{
    const auto value = json.value(key, std::string{});
    if (value.empty() && allowEmpty) {
        return '\0';
    }
    if (value.size() != 1) {
        throw std::runtime_error(std::string("Text option ") + key + " must contain one byte.");
    }
    return value.front();
}

} // namespace

TextFormatOptions TextFormatOptions::FromJson(const std::shared_ptr<nlohmann::json>& json)
{
    if (json == nullptr) {
        throw std::runtime_error("Text options JSON is null.");
    }
    TextFormatOptions options;
    options.sourceKind = ParseSourceKind(json->value("text.source_kind", std::string{}));
    options.codecKind = ParseCodecKind(json->value("text.codec_kind", std::string{}));
    options.common.charset = json->value("text.charset", std::string{});
    options.common.lineSeparator = json->value("text.line_separator", std::string{});
    options.common.compressionCodec = json->value("text.compression_codec", std::string{});
    options.common.sessionTimezone = json->value("text.session_timezone", std::string{});
    options.common.splitable = json->value("text.splitable", std::string("true")) == "true";

    if (options.codecKind == TextCodecKind::RAW_LINE) {
        RawLineOptions raw;
        raw.wholeText = json->value("text.whole_text", std::string("false")) == "true";
        options.dialect = raw;
    } else if (options.codecKind == TextCodecKind::LAZY_SIMPLE) {
        LazySimpleOptions lazy;
        lazy.delimited.fieldDelimiter = ParseSingleByte(*json, "text.field_delimiter", false);
        lazy.delimited.nullLiteral = json->value("text.null_literal", std::string("\\N"));
        lazy.delimited.escapeEnabled =
            json->value("text.escape_enabled", std::string("false")) == "true";
        lazy.delimited.escapeChar = ParseSingleByte(*json, "text.escape_char", true);
        lazy.delimited.skipInputLines = static_cast<uint32_t>(
            std::stoul(json->value("text.skip_input_lines", std::string("0"))));
        lazy.delimited.emitHeader =
            json->value("text.emit_header", std::string("false")) == "true";
        lazy.lastColumnTakesRest =
            json->value("text.last_column_takes_rest", std::string("false")) == "true";
        const auto collection = json->value("text.collection_delimiter", std::string{});
        const auto mapKey = json->value("text.map_key_delimiter", std::string{});
        lazy.collectionDelimiter = collection.size() == 1 ? collection.front() : '\0';
        lazy.mapKeyDelimiter = mapKey.size() == 1 ? mapKey.front() : '\0';
        options.dialect = lazy;
    }
    return options;
}

bool TextFormatOptions::IsRawLine() const
{
    return sourceKind == TextSourceKind::SPARK_TEXT && codecKind == TextCodecKind::RAW_LINE;
}

bool TextFormatOptions::IsLazySimple() const
{
    return sourceKind == TextSourceKind::HIVE_TEXT && codecKind == TextCodecKind::LAZY_SIMPLE;
}

const RawLineOptions& TextFormatOptions::RawLine() const
{
    return std::get<RawLineOptions>(dialect);
}

const LazySimpleOptions& TextFormatOptions::LazySimple() const
{
    return std::get<LazySimpleOptions>(dialect);
}

void TextFormatOptions::Validate() const
{
    if (common.charset != "UTF-8") {
        throw std::runtime_error("Native Text supports UTF-8 only.");
    }
    if (!common.lineSeparator.empty()) {
        throw std::runtime_error("Native Text does not support custom line separators.");
    }
    if (common.compressionCodec != "NONE") {
        throw std::runtime_error("Native Text does not support compression yet.");
    }
    if (!common.splitable) {
        throw std::runtime_error("Native Text supports splitable input only.");
    }
    if (IsRawLine()) {
        if (RawLine().wholeText) {
            throw std::runtime_error("RawLine reader does not support whole-text mode.");
        }
        return;
    }
    if (IsLazySimple()) {
        const auto& lazy = LazySimple();
        if (lazy.delimited.fieldDelimiter == '\0') {
            throw std::runtime_error("LazySimple field delimiter must contain one byte.");
        }
        if (lazy.delimited.escapeEnabled && lazy.delimited.escapeChar == '\0') {
            throw std::runtime_error("LazySimple escape is enabled without an escape byte.");
        }
        if (lazy.delimited.skipInputLines != 0) {
            throw std::runtime_error("LazySimple skip header is not supported by Spark Hive scan.");
        }
        if (lazy.delimited.emitHeader) {
            throw std::runtime_error("LazySimple writer does not emit headers.");
        }
        if (lazy.lastColumnTakesRest) {
            throw std::runtime_error("LazySimple last-column-takes-rest is not supported.");
        }
        return;
    }
    throw std::runtime_error("Unsupported Text source/codec combination.");
}

} // namespace omniruntime::reader::text
