/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 */
#pragma once

#include "reader/ReaderFactory.h"

namespace omniruntime::reader::text {

class TextReaderFactory final : public ReaderFactory {
public:
    std::unique_ptr<Reader> CreateReader(std::shared_ptr<ReaderOptions>& options) override;
};

} // namespace omniruntime::reader::text
