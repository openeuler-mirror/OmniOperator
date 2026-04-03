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

#include "HiveConnectorSplit.h"

#include <memory>
#include <string>
#include <unordered_map>

namespace omniruntime::connector::hive {

/** Must match [[org.apache.gluten.execution.GlutenOmniScanProperties.InternalHudiDatasourceMetadataKey]]. */
inline constexpr const char kGlutenOmniInternalHudiDatasourceKey[] =
    "__gluten_omni_internal__.hudi_datasource";

inline bool splitMarksGlutenOmniHudiDatasource(const std::shared_ptr<const HiveConnectorSplit> &split)
{
    if (split == nullptr) {
        return false;
    }
    const auto it = split->infoColumns.find(kGlutenOmniInternalHudiDatasourceKey);
    return it != split->infoColumns.end() && it->second == "true";
}

/**
 * Hudi commit/record metadata and similar: values come from split info, not base file vectors.
 * `_hoodie_*` is only treated as split-backed when [[splitMarksGlutenOmniHudiDatasource]] is true
 * for this scan (Gluten marks Hudi data sources explicitly).
 */
inline bool columnMaterializedFromSplitInfo(
    const std::string &colName,
    const std::unordered_map<std::string, std::shared_ptr<HiveColumnHandle>> *infoHandles,
    bool hudiDatasourceScan)
{
    if (colName.size() >= 8 && colName.compare(0, 8, "_hoodie_") == 0) {
        return hudiDatasourceScan;
    }
    if (infoHandles == nullptr) {
        return false;
    }
    const auto hIt = infoHandles->find(colName);
    if (hIt == infoHandles->end()) {
        return false;
    }
    const auto *handle = static_cast<const HiveColumnHandle *>(hIt->second.get());
    return handle->columnType() == HiveColumnHandle::ColumnType::kSynthesized;
}

} // namespace omniruntime::connector::hive
