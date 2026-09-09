/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 */
#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <variant>
#include <vector>

#include <nlohmann/json.hpp>

namespace omniruntime::reader::text {

enum class TextSourceKind {
    UNKNOWN = 0,
    SPARK_TEXT,
    HIVE_TEXT,
    SPARK_CSV
};

enum class TextCodecKind {
    UNKNOWN = 0,
    RAW_LINE,
    LAZY_SIMPLE,
    CSV
};

struct CommonTextOptions {
    std::string charset;
    std::string lineSeparator;
    std::string compressionCodec;
    std::string sessionTimezone;
    bool splitable = true;
};

struct TemporalTextOptions {
    std::string dateFormat;
    std::vector<std::string> timestampFormats;
};

struct RawLineOptions {
    bool wholeText = false;
};

struct DelimitedOptions {
    char fieldDelimiter = '\0';
    std::string nullLiteral;
    bool escapeEnabled = false;
    char escapeChar = '\0';
    uint32_t skipInputLines = 0;
    bool emitHeader = false;
};

struct LazySimpleOptions {
    DelimitedOptions delimited;
    char collectionDelimiter = '\0';
    char mapKeyDelimiter = '\0';
    bool lastColumnTakesRest = false;
};

struct CsvOptions {
    DelimitedOptions delimited;
    char quote = '"';
    std::string parseMode;
};

using TextDialectOptions = std::variant<RawLineOptions, LazySimpleOptions, CsvOptions>;

struct TextFormatOptions {
    TextSourceKind sourceKind = TextSourceKind::UNKNOWN;
    TextCodecKind codecKind = TextCodecKind::UNKNOWN;
    CommonTextOptions common;
    TemporalTextOptions temporal;
    TextDialectOptions dialect = RawLineOptions{};

    static TextFormatOptions FromJson(const std::shared_ptr<nlohmann::json>& json);

    void Validate() const;
    bool IsRawLine() const;
    bool IsLazySimple() const;
    bool IsCsv() const;
    const RawLineOptions& RawLine() const;
    const LazySimpleOptions& LazySimple() const;
    const CsvOptions& Csv() const;
};

} // namespace omniruntime::reader::text
