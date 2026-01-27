/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#pragma once

#include <limits>
#include <unordered_set>
#include <memory>
#include "memory/memory_pool.h"

namespace omniruntime::codegen {

enum class FileFormat {
    UNKNOWN = 0,
    DWRF = 1, // DWRF
    RC = 2, // RC with unknown serialization
    RC_TEXT = 3, // RC with text serialization
    RC_BINARY = 4, // RC with binary serialization
    TEXT = 5,
    JSON = 6,
    PARQUET = 7,
    NIMBLE = 8,
    ORC = 9,
    SST = 10, // rocksdb sst format
};

enum ColumnSelection {
    ColumnSelection_NONE = 0,
    ColumnSelection_NAMES = 1,
    ColumnSelection_FIELD_IDS = 2,
    ColumnSelection_TYPE_IDS = 3,
};

FileFormat toFileFormat(std::string_view s);
std::string_view toString(FileFormat fmt);
} // namespace omniruntime::codegen

