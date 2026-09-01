/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 */
#include "reader/text/TextReaderFactory.h"

#include <utility>

#include "reader/text/TextFormatOptions.h"
#include "reader/text/TextReader.h"

namespace omniruntime::reader::text {

std::unique_ptr<Reader> TextReaderFactory::CreateReader(std::shared_ptr<ReaderOptions>& options)
{
    auto textOptions = TextFormatOptions::FromJson(options->GetEnhancementJson());
    textOptions.ValidatePhaseOne();
    auto reader = std::make_unique<TextReader>(options, std::move(textOptions));
    reader->InitReader();
    return reader;
}

} // namespace omniruntime::reader::text
