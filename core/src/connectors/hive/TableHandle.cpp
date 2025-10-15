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

#include "connectors/hive/TableHandle.h"

namespace omniruntime::connector::hive {

namespace {
std::unordered_map<HiveColumnHandle::ColumnType, std::string> columnTypeNames()
{
    return {
        {HiveColumnHandle::ColumnType::kPartitionKey, "PartitionKey"},
        {HiveColumnHandle::ColumnType::kRegular, "Regular"},
        {HiveColumnHandle::ColumnType::kSynthesized, "Synthesized"},
        {HiveColumnHandle::ColumnType::kRowIndex, "RowIndex"},
    };
}

template <typename K, typename V>
std::unordered_map<V, K> invertMap(const std::unordered_map<K, V>& mapping)
{
    std::unordered_map<V, K> inverted;
    for (const auto& [key, value] : mapping) {
        inverted.emplace(value, key);
    }
    return inverted;
}

} // namespace

std::string HiveColumnHandle::columnTypeName(
    HiveColumnHandle::ColumnType type)
{
    static const auto ctNames = columnTypeNames();
    return ctNames.at(type);
}

HiveColumnHandle::ColumnType HiveColumnHandle::columnTypeFromName(
    const std::string& name)
{
    static const auto nameColumnTypes = invertMap(columnTypeNames());
    return nameColumnTypes.at(name);
}

HiveTableHandle::HiveTableHandle(
    std::string connectorId,
    const std::string& tableName,
    bool filterPushdownEnabled,
    std::string enhancementJson,
    const type::RowTypePtr& dataColumns,
    const std::unordered_map<std::string, std::string>& tableParameters)
    : ConnectorTableHandle(std::move(connectorId)),
      tableName_(tableName),
      filterPushdownEnabled_(filterPushdownEnabled),
      dataColumns_(dataColumns),
      tableParameters_(tableParameters),
     enhancementJson_{enhancementJson}{}

} // namespace omniruntime::connector::hive
