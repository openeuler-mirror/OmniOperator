/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 */
#pragma once

#include <cstdint>
#include <memory>

#include <arrow/filesystem/filesystem.h>
#include <arrow/io/interfaces.h>

#include "reader/common/UriInfo.h"
#include "reader/text/TextCodec.h"
#include "vector/vector.h"

namespace omniruntime::reader::text {

class TextWriter {
public:
    explicit TextWriter(TextCodecKind codecKind = TextCodecKind::RAW_LINE);
    ~TextWriter();

    void Init(const UriInfo& uri);

    void Write(vec::BaseVector* vector, int64_t start, int64_t end);

    void Close();

private:
    std::unique_ptr<TextCodec> codec_;
    std::shared_ptr<arrow::fs::FileSystem> fileSystem_;
    std::shared_ptr<arrow::io::OutputStream> output_;
    bool closed_ = false;
};

} // namespace omniruntime::reader::text
