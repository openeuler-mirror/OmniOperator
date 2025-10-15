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

#include <optional>
#include <unordered_map>
#include "connectors/Connector.h"
#include "TableHandle.h"
#include "FileProperties.h"
#include "codegen/Options.h"

namespace omniruntime::connector::hive {

// A bucket conversion that should happen on the split.  This happens when we
// increase the bucket count of a table, but the old partitions are still
// generated using the old bucket count, so that multiple new buckets can exist
// in the same file, and we need to apply extra filter when we read these files
// to make sure we read the rows corresponding to the selected bucket number
// only.
struct RowIdProperties {
    int64_t metadataVersion;
    int64_t partitionId;
    std::string tableGuid;
};

struct HiveConnectorSplit : public connector::ConnectorSplit {
    const std::string filePath;
    codegen::FileFormat fileFormat;
    const uint64_t start;
    const uint64_t length;

    // Mapping from partition keys to values. Values are specified as strings
    // formatted the same way as CAST(x as VARCHAR). Null values are specified as
    // std::nullopt. Date values must be formatted using ISO 8601 as YYYY-MM-DD.
    // All scalar types and date type are supported.
    const std::unordered_map<std::string, std::optional<std::string>>
            partitionKeys;
    std::unordered_map<std::string, std::string> customSplitInfo;
    std::shared_ptr<std::string> extraFileInfo;
    // Parameters that are provided as the serialization options.
    std::unordered_map<std::string, std::string> serdeParameters;
    // Parameters that are provided as the physical storage properties.
    std::unordered_map<std::string, std::string> storageParameters;

    /// These represent columns like $file_size, $file_modified_time that are
    /// associated with the HiveSplit.
    std::unordered_map<std::string, std::string> infoColumns;

    /// These represent file properties like file size that are used while opening
    /// the file handle.
    std::optional<FileProperties> properties;

    std::optional<RowIdProperties> rowIdProperties;

    HiveConnectorSplit(
            const std::string& connectorId,
            const std::string& _filePath,
            codegen::FileFormat _fileFormat,
            uint64_t _start = 0,
            uint64_t _length = std::numeric_limits<uint64_t>::max(),
            const std::unordered_map<std::string, std::optional<std::string>>&
            _partitionKeys = {},
            const std::unordered_map<std::string, std::string>& _customSplitInfo = {},
            const std::shared_ptr<std::string>& _extraFileInfo = {},
            const std::unordered_map<std::string, std::string>& _serdeParameters = {},
            const std::unordered_map<std::string, std::string>& _storageParameters = {},
            int64_t splitWeight = 0,
            bool cacheable = true,
            const std::unordered_map<std::string, std::string>& _infoColumns = {},
            std::optional<FileProperties> _properties = std::nullopt,
            std::optional<RowIdProperties> _rowIdProperties = std::nullopt)
                : ConnectorSplit(connectorId, splitWeight, cacheable),
                  filePath(_filePath),
                  fileFormat(_fileFormat),
                  start(_start),
                  length(_length),
                  partitionKeys(_partitionKeys),
                  customSplitInfo(_customSplitInfo),
                  extraFileInfo(_extraFileInfo),
                  serdeParameters(_serdeParameters),
                  storageParameters(_storageParameters),
                  infoColumns(_infoColumns),
                  properties(_properties),
                  rowIdProperties(_rowIdProperties) {}

    std::string getFileName() const;

    std::string getFilePath() const;

    codegen::FileFormat getFileFormat() const;

    static std::shared_ptr<HiveConnectorSplit> create(const nlohmann::json& obj);
};

class HiveConnectorSplitBuilder {
public:
    explicit HiveConnectorSplitBuilder(std::string filePath)
        : filePath_{std::move(filePath)}
    {
        infoColumns_["$path"] = filePath_;
    }

    HiveConnectorSplitBuilder& start(uint64_t start)
    {
        start_ = start;
        return *this;
    }

    HiveConnectorSplitBuilder& length(uint64_t length)
    {
        length_ = length;
        return *this;
    }

    HiveConnectorSplitBuilder& splitWeight(int64_t splitWeight)
    {
        splitWeight_ = splitWeight;
        return *this;
    }

    HiveConnectorSplitBuilder& cacheable(bool cacheable)
    {
        cacheable_ = cacheable;
        return *this;
    }

    HiveConnectorSplitBuilder& fileFormat(codegen::FileFormat format)
    {
        fileFormat_ = format;
        return *this;
    }

    HiveConnectorSplitBuilder& infoColumn(
        const std::string& name,
        const std::string& value)
    {
        infoColumns_.emplace(std::move(name), std::move(value));
        return *this;
    }

    HiveConnectorSplitBuilder& partitionKey(
        std::string name,
        std::optional<std::string> value)
    {
        partitionKeys_.emplace(std::move(name), std::move(value));
        return *this;
    }

    HiveConnectorSplitBuilder& customSplitInfo(
        const std::unordered_map<std::string, std::string>& customSplitInfo)
    {
        customSplitInfo_ = customSplitInfo;
        return *this;
    }

    HiveConnectorSplitBuilder& extraFileInfo(
        const std::shared_ptr<std::string>& extraFileInfo)
    {
        extraFileInfo_ = extraFileInfo;
        return *this;
    }

    HiveConnectorSplitBuilder& serdeParameters(
        const std::unordered_map<std::string, std::string>& serdeParameters)
    {
        serdeParameters_ = serdeParameters;
        return *this;
    }

    HiveConnectorSplitBuilder& storageParameters(
        const std::unordered_map<std::string, std::string>& storageParameters)
    {
        storageParameters_ = storageParameters;
        return *this;
    }

    HiveConnectorSplitBuilder& connectorId(const std::string& connectorId)
    {
        connectorId_ = connectorId;
        return *this;
    }

    HiveConnectorSplitBuilder& fileProperties(FileProperties fileProperties)
    {
        fileProperties_ = fileProperties;
        return *this;
    }

    HiveConnectorSplitBuilder& rowIdProperties(
        const RowIdProperties& rowIdProperties)
    {
        rowIdProperties_ = rowIdProperties;
        return *this;
    }

    std::shared_ptr<connector::hive::HiveConnectorSplit> build() const
    {
        return std::make_shared<connector::hive::HiveConnectorSplit>(
                connectorId_,
                filePath_,
                fileFormat_,
                start_,
                length_,
                partitionKeys_,
                customSplitInfo_,
                extraFileInfo_,
                serdeParameters_,
                storageParameters_,
                splitWeight_,
                cacheable_,
                infoColumns_,
                fileProperties_,
                rowIdProperties_);
    }

private:
    const std::string filePath_;
    codegen::FileFormat fileFormat_{codegen::FileFormat::ORC};
    uint64_t start_{0};
    uint64_t length_{std::numeric_limits<uint64_t>::max()};
    std::unordered_map<std::string, std::optional<std::string>> partitionKeys_;
    std::unordered_map<std::string, std::string> customSplitInfo_ = {};
    std::shared_ptr<std::string> extraFileInfo_ = {};
    std::unordered_map<std::string, std::string> serdeParameters_ = {};
    std::unordered_map<std::string, std::string> storageParameters_ = {};
    std::unordered_map<std::string, std::string> infoColumns_ = {};
    std::string connectorId_;
    int64_t splitWeight_{0};
    bool cacheable_{true};
    std::optional<FileProperties> fileProperties_;
    std::optional<RowIdProperties> rowIdProperties_ = std::nullopt;
};
} // namespace omniruntime::connector::hive
