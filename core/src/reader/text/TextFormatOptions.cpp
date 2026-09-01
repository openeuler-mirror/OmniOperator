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

} // namespace

TextFormatOptions TextFormatOptions::FromJson(const std::shared_ptr<nlohmann::json>& json)
{
    if (json == nullptr) {
        throw std::runtime_error("Text options JSON is null.");
    }
    TextFormatOptions options;
    options.sourceKind = ParseSourceKind(json->value("text.source_kind", std::string{}));
    options.codecKind = ParseCodecKind(json->value("text.codec_kind", std::string{}));
    options.charset = json->value("text.charset", std::string{});
    options.lineSeparator = json->value("text.line_separator", std::string{});
    options.compressionCodec = json->value("text.compression_codec", std::string{});
    options.wholeText = json->value("text.whole_text", std::string("false")) == "true";
    return options;
}

void TextFormatOptions::ValidatePhaseOne() const
{
    if (sourceKind != TextSourceKind::SPARK_TEXT || codecKind != TextCodecKind::RAW_LINE) {
        throw std::runtime_error("Phase-one Text reader requires SPARK_TEXT with RAW_LINE.");
    }
    if (charset != "UTF-8") {
        throw std::runtime_error("Phase-one Text reader supports UTF-8 only.");
    }
    if (!lineSeparator.empty()) {
        throw std::runtime_error("Phase-one Text reader does not support custom line separators.");
    }
    if (compressionCodec != "NONE") {
        throw std::runtime_error("Phase-one Text reader does not support compression.");
    }
    if (wholeText) {
        throw std::runtime_error("Phase-one Text reader does not support whole-text mode.");
    }
}

} // namespace omniruntime::reader::text
