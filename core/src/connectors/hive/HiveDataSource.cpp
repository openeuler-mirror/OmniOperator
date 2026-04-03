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

#include "HiveDataSource.h"

#include <string>
#include <unordered_map>

namespace omniruntime::connector::hive {

class HiveTableHandle;
class HiveColumnHandle;

HiveDataSource::HiveDataSource(
    const type::RowTypePtr &outputType,
    const std::shared_ptr <connector::ConnectorTableHandle> &tableHandle,
    const std::unordered_map <std::string, std::shared_ptr<connector::ColumnHandle>> &columnHandles,
    const std::shared_ptr <HiveConfig> &hiveConfig) : hiveConfig_(hiveConfig), outputType_(outputType) {
    // Column handled keyed on the column alias, the name used in the query.
    for (const auto &[canonicalizedName, columnHandle]: columnHandles) {
        auto handle = std::dynamic_pointer_cast<HiveColumnHandle>(columnHandle);
        switch (handle->columnType()) {
            case HiveColumnHandle::ColumnType::kRegular:
                break;
            case HiveColumnHandle::ColumnType::kPartitionKey:
                partitionKeys_.emplace(handle->name(), handle);
                break;
            case HiveColumnHandle::ColumnType::kSynthesized:
                infoColumns_.emplace(handle->name(), handle);
                break;
            case HiveColumnHandle::ColumnType::kRowIndex:
                specialColumns_.rowIndex = handle->name();
                break;
            case HiveColumnHandle::ColumnType::kRowId:
                specialColumns_.rowId = handle->name();
                break;
        }
    }

    std::vector <std::string> readColumnNames;
    auto readColumnTypes = outputType_->children_();
    for (const auto &outputName: outputType_->names()) {
        auto it = columnHandles.find(outputName);

        auto *handle = static_cast<const HiveColumnHandle *>(it->second.get());
        readColumnNames.push_back(handle->name());
        for (auto &subfield: handle->requiredSubfields()) {
            subfields_[handle->name()].push_back(&subfield);
        }
    }

    hiveTableHandle_ = std::dynamic_pointer_cast<HiveTableHandle>(tableHandle);
    readerOutputType_ =
        ROW(std::move(readColumnNames), std::move(readColumnTypes));
    scanSpec_ = makeScanSpec(readerOutputType_, subfields_, hiveTableHandle_->dataColumns(),
                             partitionKeys_, infoColumns_, specialColumns_);
}

std::unique_ptr<SplitReader> HiveDataSource::createSplitReader() {
    return SplitReader::create(
            split_,
            hiveTableHandle_,
            &partitionKeys_,
            &infoColumns_,
            hiveConfig_,
            readerOutputType_,
            scanSpec_,
            specialColumns_);
}

void HiveDataSource::addSplit(std::shared_ptr<ConnectorSplit> split, uint64_t size) {
    split_ = std::dynamic_pointer_cast<HiveConnectorSplit>(split);

    if (splitReader_) {
        splitReader_.reset();
    }

    splitReader_ = createSplitReader();
    splitReader_->prepareSplit(readerOutputType_, size);
}

std::optional<vec::VectorBatch *> HiveDataSource::next(uint64_t size) {
    const std::vector <std::shared_ptr<omniruntime::type::DataType>> children = readerOutputType_->Children();
    std::vector<int> dataTypeIdVector;
    for (int i = 0; i < children.size(); i++) {
        auto dataType = children[i];
        dataTypeIdVector.push_back(dataType->GetId());
    }

    const auto rowsScanned = splitReader_->next(&output_, dataTypeIdVector.data(), size);
    auto rowsRemaining = output_->GetRowCount();
    if (rowsRemaining == 0) {
        // no rows passed the pushed down filters.
        return getEmptyOutput();
    }
    return output_;
}

} // namespace connector::hive
