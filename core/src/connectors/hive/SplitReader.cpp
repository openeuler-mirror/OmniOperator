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

std::unique_ptr <SplitReader> SplitReader::create(
    const std::shared_ptr <hive::HiveConnectorSplit> &hiveSplit,
    const std::shared_ptr<const HiveTableHandle> &hiveTableHandle,
    const std::unordered_map <std::string, std::shared_ptr<HiveColumnHandle>> *
    partitionKeys,
    const std::shared_ptr<const HiveConfig> &hiveConfig,
    const type::RowTypePtr &readerOutputType,
    const std::shared_ptr <codegen::ScanSpec> &scanSpec)
{
    return std::unique_ptr<SplitReader>(new SplitReader(
        hiveSplit,
        hiveTableHandle,
        partitionKeys,
        hiveConfig,
        readerOutputType,
        scanSpec));
}

void SplitReader::prepareSplit(omniruntime::type::RowTypePtr& rowType, uint64_t batchLen)
{
    createReader();
    createRowReader(rowType,batchLen);
}

SplitReader::SplitReader(
    const std::shared_ptr<const hive::HiveConnectorSplit> &hiveSplit,
    const std::shared_ptr<const HiveTableHandle> &hiveTableHandle,
    const std::unordered_map <std::string, std::shared_ptr<HiveColumnHandle>> *
    partitionKeys,
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
    emptySplit_(false) {}

void clearRecordBatch(std::vector<omniruntime::vec::BaseVector *> &recordBatch)
{
    for (auto vec: recordBatch) {
        delete vec;
    }
    recordBatch.clear();
}

uint64_t filterData(uint8_t *bitMark, std::vector<omniruntime::vec::BaseVector *> *recordBatch, int32_t vectorSize,
    const std::set <int32_t> &isNullSet, const std::set <int32_t> &isNotNullSet)
{
    std::vector < omniruntime::vec::BaseVector * > resultBatch;
    if (common::GetFlatBaseVectorsFromBitMark(*recordBatch, resultBatch, bitMark, vectorSize, isNullSet,
                                              isNotNullSet)) {
        clearRecordBatch(*recordBatch);
        *recordBatch = std::move(resultBatch);
        return (*recordBatch)[0]->GetSize();
    }
    // 失败返回原始的
    clearRecordBatch(resultBatch);
    return vectorSize;
}

bool SplitReader::readAndFilterData(std::unique_ptr <omniruntime::reader::RowReader> &rowReaderPtr,
    std::vector<omniruntime::vec::BaseVector *> *recordBatch, uint64_t &batchRowSize, int *omniTypeId,
    uint64_t batchLen)
{
    batchRowSize = rowReaderPtr->Next(recordBatch, omniTypeId, batchLen);
    std::unique_ptr <common::PredicateCondition> &predicateCondition = rowReaderPtr->GetPredicatePtr();
    if (batchRowSize == 0 || predicateCondition == nullptr) {
        return false;
    }
    try {
        uint8_t *bitMark = predicateCondition->compute(*recordBatch);
        int32_t vectorSize = (*recordBatch)[0]->GetSize();
        // 数据被全部过滤完，需要重新读取
        if (omniruntime::BitUtil::CountBits(reinterpret_cast<const uint64_t *>(bitMark), 0, vectorSize) == 0) {
            clearRecordBatch(*recordBatch);
            return true;
        }
        batchRowSize = filterData(bitMark, recordBatch, vectorSize, predicateCondition->getIsAllNullColumns(),
                                  predicateCondition->getIsAllNotNullColumns());
    } catch (const std::exception &e) {
        LogError("filterData fail: %s", e.what());
    }
    return false;
}

uint64_t SplitReader::next(vec::VectorBatch **output_, int *omniTypeId, uint64_t batchLen)
{
    std::vector < omniruntime::vec::BaseVector * > recordBatch = {};
    uint64_t batchRowSize = 0;
    bool needReadAgain = readAndFilterData(baseRowReader_, &recordBatch, batchRowSize, omniTypeId, batchLen);
    while (needReadAgain) {
        needReadAgain = readAndFilterData(baseRowReader_, &recordBatch, batchRowSize, omniTypeId, batchLen);
    }

    auto output = new vec::VectorBatch(batchRowSize);
    *output_ = output;
    for (int i = 0; i < recordBatch.size(); ++i) {
        output->Append((recordBatch)[i]);
    }
    if (rowType_->size() > fileRowType_->size()) {
        for (int i = fileRowType_->size(); i < rowType_->size(); ++i) {
            auto partitionVec = vec::VectorHelper::CreateFlatVector(rowType_->children_()[i]->GetId(),
                                                                    batchRowSize);
            auto it = hiveSplit_->partitionKeys.find(rowType_->nameOf(i));
            auto optionalVal = it->second;
            if (optionalVal.has_value()) {
                auto val = it->second.value();
                for (int j = 0; j < batchRowSize; ++j) {
                    setPartitionVal(partitionVec, j, val);
                }
            } else {
                for (int j = 0; j < batchRowSize; ++j) {
                    omniruntime::vec::VectorHelper::SetValue(partitionVec, j, nullptr);
                }
            }
            output->Append(partitionVec);
        }
    }
    if (batchRowSize <= 0) {
        return batchRowSize;
    }
    for (int i = 0; i < fileRowType_->size(); ++i) {
        if (fileRowType_->childAt(i)->GetId() == type::DataTypeId::OMNI_DATE32) {
            auto vector = output->Get(i);
            for (int j = 0; j < batchRowSize; ++j) {
                auto intVector = reinterpret_cast<Vector<int32_t> *>(vector);
                auto srcVal = intVector->GetValue(j);
                auto finalVal = (baseRowReader_->GetJulianDaysPtr()->RebaseJulianToGregorianDays(srcVal));
                intVector->SetValue(j, finalVal);
            }
        }
    }
    return batchRowSize;
}

void SplitReader::createReader()
{
    configureReaderOptions(hiveConfig_, hiveSplit_, baseReaderOpts_);
    baseReaderOpts_->SetFileFormat(hiveSplit_->getFileFormat());
    auto baseFileInput = createBufferedInput(baseReaderOpts_, hiveSplit_);
    baseReader_ = omniruntime::reader::GetReaderFactory(omniruntime::codegen::FileFormat::ORC)
        ->CreateReader(std::move(baseFileInput), baseReaderOpts_);
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

    auto enhancementJson = hiveTableHandle_->GetEnhancementJson();
    auto jsonCondition = nlohmann::json::parse(enhancementJson);

    std::unique_ptr <common::JulianGregorianRebase> julianPtr;
    std::unique_ptr <common::PredicateCondition> predicate;
    std::unique_ptr<::orc::SearchArgument> searchArgument;
    ParseEnhanceJson(jsonCondition, julianPtr, predicate, searchArgument);
    if (predicate != nullptr) {
        predicate->init(batchLen);
    }

    omniruntime::connector::hive::configureRowReaderOptions(
        hiveTableHandle_->tableParameters(),
        fileRowType_,
        scanSpec_,
        hiveSplit_,
        hiveConfig_,
        searchArgument,
        baseRowReaderOpts_);
    baseRowReader_ = baseReader_->CreateRowReader(baseRowReaderOpts_, julianPtr, predicate);
}

} // namespace hive
