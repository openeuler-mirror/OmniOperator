/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 */
#include "reader/text/TextFormatOptions.h"

#include <algorithm>
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
    options.temporal.dateFormat = json->value("text.date_format", std::string{});
    const auto timestampFormatCount = static_cast<size_t>(
        std::stoul(json->value("text.timestamp_format_count", std::string("0"))));
    options.temporal.timestampFormats.reserve(timestampFormatCount);
    for (size_t index = 0; index < timestampFormatCount; ++index) {
        options.temporal.timestampFormats.emplace_back(
            json->value("text.timestamp_format_" + std::to_string(index), std::string{}));
    }

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
    } else if (options.codecKind == TextCodecKind::CSV) {
        CsvOptions csv;
        csv.delimited.fieldDelimiter = ParseSingleByte(*json, "text.field_delimiter", false);
        csv.delimited.nullLiteral = json->value("text.null_literal", std::string{});
        csv.delimited.escapeEnabled = true;
        csv.delimited.escapeChar = ParseSingleByte(*json, "text.escape_char", false);
        csv.delimited.skipInputLines = static_cast<uint32_t>(
            std::stoul(json->value("text.skip_input_lines", std::string("0"))));
        csv.delimited.emitHeader = json->value("text.emit_header", std::string("false")) == "true";
        csv.quote = ParseSingleByte(*json, "text.quote", false);
        csv.parseMode = json->value("text.parse_mode", std::string("PERMISSIVE"));
        options.dialect = csv;
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

bool TextFormatOptions::IsCsv() const
{
    return codecKind == TextCodecKind::CSV &&
        (sourceKind == TextSourceKind::SPARK_CSV || sourceKind == TextSourceKind::HIVE_TEXT);
}

const CsvOptions& TextFormatOptions::Csv() const
{
    return std::get<CsvOptions>(dialect);
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
    if (std::any_of(temporal.timestampFormats.begin(), temporal.timestampFormats.end(),
            [](const std::string& format) { return format.empty(); })) {
        throw std::runtime_error("Text timestamp format must not be empty.");
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
    if (IsCsv()) {
        const auto& csv = Csv();
        const auto delimiter = csv.delimited.fieldDelimiter;
        const auto hiveEscape = csv.delimited.escapeChar == '"' ? '\\' : csv.delimited.escapeChar;
        if (sourceKind == TextSourceKind::HIVE_TEXT &&
            (delimiter == hiveEscape || csv.quote == hiveEscape)) {
            throw std::runtime_error("OpenCSV reader delimiter, quote and escape must be different.");
        }
        if (delimiter == '\0' || delimiter == '\r' || delimiter == '\n' ||
            csv.quote == '\0' || csv.quote == '\r' || csv.quote == '\n' ||
            csv.delimited.escapeChar == '\0' || csv.delimited.escapeChar == '\r' ||
            csv.delimited.escapeChar == '\n' || delimiter == csv.quote ||
            delimiter == csv.delimited.escapeChar) {
            throw std::runtime_error("Unsupported CSV delimiter/quote/escape combination.");
        }
        if (csv.parseMode != "PERMISSIVE" || csv.delimited.skipInputLines > 1 ||
            (sourceKind == TextSourceKind::HIVE_TEXT &&
                (csv.delimited.skipInputLines != 0 || csv.delimited.emitHeader))) {
            throw std::runtime_error("Unsupported CSV mode or header option.");
        }
        return;
    }
    throw std::runtime_error("Unsupported Text source/codec combination.");
}

} // namespace omniruntime::reader::text
