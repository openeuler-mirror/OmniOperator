/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 */
#pragma once

#include <memory>
#include <string>

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

struct TextFormatOptions {
    TextSourceKind sourceKind = TextSourceKind::UNKNOWN;
    TextCodecKind codecKind = TextCodecKind::UNKNOWN;
    std::string charset;
    std::string lineSeparator;
    std::string compressionCodec;
    bool wholeText = false;

    static TextFormatOptions FromJson(const std::shared_ptr<nlohmann::json>& json);

    void ValidatePhaseOne() const;
};

} // namespace omniruntime::reader::text
