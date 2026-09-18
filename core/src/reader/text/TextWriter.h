/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 */
#pragma once

#include <cstdint>
#include <memory>
#include <vector>

#include <arrow/filesystem/filesystem.h>
#include <arrow/io/interfaces.h>

#include "reader/common/UriInfo.h"
#include "reader/text/TextCodec.h"
#include "reader/text/TextCompressionStream.h"
#include "reader/text/TextValueConverter.h"
#include "type/data_type.h"
#include "vector/vector.h"

namespace omniruntime::reader::text {

class TextWriter {
public:
    TextWriter();
    TextWriter(TextFormatOptions options, type::RowTypePtr rowType);
    ~TextWriter();

    void Init(const UriInfo& uri);

    void Write(vec::BaseVector* vector, int64_t start, int64_t end);

    void Write(const std::vector<vec::BaseVector*>& vectors, int64_t start, int64_t end);

    void Close();

private:
    TextFormatOptions options_;
    type::RowTypePtr rowType_;
    std::unique_ptr<TextCodec> codec_;
    TextValueConverter valueConverter_;
    std::shared_ptr<arrow::fs::FileSystem> fileSystem_;
    std::shared_ptr<arrow::io::OutputStream> output_;
    std::unique_ptr<TextOutputSink> outputSink_;
    bool closed_ = false;
};

} // namespace omniruntime::reader::text
