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

#include "SplitReader.h"

namespace omniruntime::connector::hive {
SplitReader::SplitReader(
    const std::shared_ptr<const hive::HiveConnectorSplit> &hiveSplit,
    const std::shared_ptr<const HiveTableHandle> &hiveTableHandle,
    const std::unordered_map <std::string, std::shared_ptr<HiveColumnHandle>> * partitionKeys,
    const std::shared_ptr<const HiveConfig> &hiveConfig,
    const type::RowTypePtr &readerOutputType,
    const std::shared_ptr <codegen::ScanSpec> &scanSpec)
    : hiveSplit_(hiveSplit),
      hiveTableHandle_(hiveTableHandle),
      partitionKeys_(partitionKeys),
      hiveConfig_(hiveConfig),
      readerOutputType_(readerOutputType),
      pool_(omniruntime::mem::GetMemoryPool()),
      scanSpec_(scanSpec),
      emptySplit_(false)
{
    baseReaderOpts_ = std::make_shared<ReaderOptions>();
}

std::unique_ptr <SplitReader> SplitReader::create(
    const std::shared_ptr <hive::HiveConnectorSplit> &hiveSplit,
    const std::shared_ptr<const HiveTableHandle> &hiveTableHandle,
    const std::unordered_map <std::string, std::shared_ptr<HiveColumnHandle>> *partitionKeys,
    const std::shared_ptr<const HiveConfig> &hiveConfig,
    const type::RowTypePtr &readerOutputType,
    const std::shared_ptr <codegen::ScanSpec> &scanSpec)
{
    return std::make_unique<SplitReader>(
        hiveSplit,
        hiveTableHandle,
        partitionKeys,
        hiveConfig,
        readerOutputType,
        scanSpec);
}

void SplitReader::prepareSplit(omniruntime::type::RowTypePtr &rowType, uint64_t batchLen)
{
    baseReaderOpts_->SetBatchLen(batchLen);
    baseReaderOpts_->SetCoalesceMaxBytes(hiveConfig_->maxCoalescedBytes());
    baseReaderOpts_->SetCoalesceMaxDistance(hiveConfig_->maxCoalescedDistance());
    baseReaderOpts_->SetFilePreloadThreshold(static_cast<int64_t>(hiveConfig_->filePreloadThreshold()));
    createReader();
    createRowReader(rowType, batchLen);
}

uint64_t SplitReader::next(vec::VectorBatch **output_, int *omniTypeId, uint64_t batchLen)
{
    std::vector<omniruntime::vec::BaseVector *> *recordBatch;
    uint64_t batchRowSize = baseRowReader_->Next(&recordBatch, omniTypeId, batchLen);

    auto output = new vec::VectorBatch(batchRowSize);
    *output_ = output;
    if (batchRowSize <= 0) {
        return batchRowSize;
    }
    for (int i = 0; i < recordBatch->size(); ++i) {
        output->Append(recordBatch->at(i));
    }
    if (fileRowType_->size() > output->GetVectorCount()) {
        for (int i = output->GetVectorCount(); i < fileRowType_->size(); ++i) {
            auto missingFieldVec = vec::VectorHelper::CreateFlatVector(fileRowType_->children_()[i]->GetId(),
                batchRowSize);
            for (int j = 0; j < batchRowSize; ++j) {
                omniruntime::vec::VectorHelper::SetValue(missingFieldVec, j, nullptr);
            }
            output->Append(missingFieldVec);
        }
    }
    // Partition columns: not stored in data files, injected from split metadata
    if (rowType_->size() > fileRowType_->size()) {
        for (int i = fileRowType_->size(); i < rowType_->size(); ++i) {
            auto dataTypeId = rowType_->children_()[i]->GetId();
            auto it = hiveSplit_->partitionKeys.find(rowType_->nameOf(i));
            vec::BaseVector *partitionVec = nullptr;
            if (it->second.has_value()) {
                const std::string &val = it->second.value();
                partitionVec = createConstPartitionVec(dataTypeId, batchRowSize, val);
            } else {
                partitionVec = createNullConstVec(dataTypeId, batchRowSize);
            }
            output->Append(partitionVec);
        }
    }
    return batchRowSize;
}

void SplitReader::createReader()
{
    baseReaderOpts_->ParseEnhanceJson(hiveTableHandle_->GetEnhancementJson(), hiveSplit_->fileFormat);
    configureReaderOptions(hiveConfig_, hiveSplit_, baseReaderOpts_);
    baseReader_ = omniruntime::reader::GetReaderFactory(hiveSplit_->fileFormat)
        ->CreateReader(baseReaderOpts_);
}

void SplitReader::createRowReader(omniruntime::type::RowTypePtr &rowType, uint64_t batchLen)
{
    std::vector <std::string> readColumnNames;
    std::vector <std::shared_ptr<omniruntime::type::DataType>> readColumnTypes;
    for (int i = 0; i < rowType->names().size(); i++) {
        const auto &outputName = rowType->names()[i];
        auto it = partitionKeys_->find(outputName);
        if (it == partitionKeys_->end()) {
            readColumnNames.push_back(outputName);
            readColumnTypes.push_back(rowType->children_()[i]);
            continue;
        }
        auto *handle = static_cast<const HiveColumnHandle *>(it->second.get());
        if (handle->columnType() == HiveColumnHandle::ColumnType::kPartitionKey) {
            continue;
        }
    }
    fileRowType_ = ROW(std::move(readColumnNames), std::move(readColumnTypes));
    rowType_ = rowType;

    // baseReaderOpts_->SetBatchLen(static_cast<int32_t>(batchLen));
    configureRowReaderOptions(
        hiveTableHandle_,
        rowType_,
        fileRowType_,
        scanSpec_,
        hiveSplit_,
        hiveConfig_,
        baseReaderOpts_);
    baseRowReader_ = baseReader_->CreateRowReader();
}
} // namespace hive
