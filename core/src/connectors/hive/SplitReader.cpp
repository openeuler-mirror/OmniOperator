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
#include "SplitReaderMetadata.h"
#include "codegen/Options.h"
#include "vector/dictionary_container.h"
#include "vector/vector_helper.h"

#include <functional>
#include <string>

namespace omniruntime::connector::hive {
namespace {
/**
 * Parquet physical type (e.g. STRING) may differ from Spark/Hudi table schema (INT/LONG).
 * The native reader decodes by file type, so we coerce to the output schema type here.
 *
 * String columns may be flat varchar or dictionary-encoded varchar; both must be handled.
 */
vec::BaseVector *coercePhysicalVecToSchemaType(vec::BaseVector *vec, type::DataTypeId expected)
{
    using type::DataTypeId;
    if (vec == nullptr) {
        return nullptr;
    }
    const DataTypeId actual = vec->GetTypeId();
    if (actual == expected) {
        return vec;
    }

    using FlatStrVec = vec::Vector<vec::LargeStringContainer<std::string_view>>;
    using DictStrVec = vec::Vector<vec::DictionaryContainer<std::string_view>>;

    int32_t n = 0;
    std::function<bool(int32_t)> rowIsNull;
    std::function<std::string_view(int32_t)> rowString;

    if (auto *sv = dynamic_cast<FlatStrVec *>(vec)) {
        n = sv->GetSize();
        rowIsNull = [sv](int32_t i) { return sv->IsNull(i); };
        rowString = [sv](int32_t i) { return sv->GetValue(i); };
    } else if (auto *dv = dynamic_cast<DictStrVec *>(vec)) {
        n = dv->GetSize();
        rowIsNull = [dv](int32_t i) { return dv->IsNull(i); };
        rowString = [dv](int32_t i) { return dv->GetValue(i); };
    } else {
        return vec;
    }

    auto parseInt32 = [&]() -> vec::BaseVector * {
        auto *out = new vec::Vector<int32_t>(n, expected);
        for (int32_t i = 0; i < n; ++i) {
            if (rowIsNull(i)) {
                out->SetNull(i);
                continue;
            }
            try {
                std::string_view v = rowString(i);
                int32_t val = static_cast<int32_t>(std::stol(std::string(v)));
                omniruntime::vec::VectorHelper::SetValue(out, i, &val);
            } catch (...) {
                out->SetNull(i);
            }
        }
        delete vec;
        return out;
    };
    auto parseInt64 = [&]() -> vec::BaseVector * {
        auto *out = new vec::Vector<int64_t>(n, expected);
        for (int32_t i = 0; i < n; ++i) {
            if (rowIsNull(i)) {
                out->SetNull(i);
                continue;
            }
            try {
                std::string_view v = rowString(i);
                int64_t val = std::stoll(std::string(v));
                omniruntime::vec::VectorHelper::SetValue(out, i, &val);
            } catch (...) {
                out->SetNull(i);
            }
        }
        delete vec;
        return out;
    };

    switch (expected) {
        case DataTypeId::OMNI_INT:
        case DataTypeId::OMNI_DATE32:
            return parseInt32();
        case DataTypeId::OMNI_LONG:
        case DataTypeId::OMNI_TIMESTAMP:
        case DataTypeId::OMNI_DATE64:
            return parseInt64();
        case DataTypeId::OMNI_SHORT: {
            auto *out = new vec::Vector<int16_t>(n, expected);
            for (int32_t i = 0; i < n; ++i) {
                if (rowIsNull(i)) {
                    out->SetNull(i);
                    continue;
                }
                try {
                    std::string_view v = rowString(i);
                    int16_t val = static_cast<int16_t>(std::stoi(std::string(v)));
                    omniruntime::vec::VectorHelper::SetValue(out, i, &val);
                } catch (...) {
                    out->SetNull(i);
                }
            }
            delete vec;
            return out;
        }
        case DataTypeId::OMNI_BYTE: {
            auto *out = new vec::Vector<int8_t>(n, expected);
            for (int32_t i = 0; i < n; ++i) {
                if (rowIsNull(i)) {
                    out->SetNull(i);
                    continue;
                }
                try {
                    std::string_view v = rowString(i);
                    int8_t val = static_cast<int8_t>(std::stoi(std::string(v)));
                    omniruntime::vec::VectorHelper::SetValue(out, i, &val);
                } catch (...) {
                    out->SetNull(i);
                }
            }
            delete vec;
            return out;
        }
        case DataTypeId::OMNI_FLOAT: {
            auto *out = new vec::Vector<float>(n, expected);
            for (int32_t i = 0; i < n; ++i) {
                if (rowIsNull(i)) {
                    out->SetNull(i);
                    continue;
                }
                try {
                    std::string_view v = rowString(i);
                    float val = std::stof(std::string(v));
                    omniruntime::vec::VectorHelper::SetValue(out, i, &val);
                } catch (...) {
                    out->SetNull(i);
                }
            }
            delete vec;
            return out;
        }
        case DataTypeId::OMNI_DOUBLE: {
            auto *out = new vec::Vector<double>(n, expected);
            for (int32_t i = 0; i < n; ++i) {
                if (rowIsNull(i)) {
                    out->SetNull(i);
                    continue;
                }
                try {
                    std::string_view v = rowString(i);
                    double val = std::stod(std::string(v));
                    omniruntime::vec::VectorHelper::SetValue(out, i, &val);
                } catch (...) {
                    out->SetNull(i);
                }
            }
            delete vec;
            return out;
        }
        default:
            return vec;
    }
}
std::string topLevelColumnName(const std::string &col)
{
    const size_t dotPos = col.find('.');
    if (dotPos != std::string::npos) {
        return col.substr(0, dotPos);
    }
    return col;
}

void buildParquetFileVectorIndexMap(
    const std::shared_ptr<omniruntime::reader::ReaderOptions> &opts,
    std::unordered_map<std::string, size_t> &out)
{
    out.clear();
    const auto &cols = opts->GetParquetIncludedColumns();
    for (size_t i = 0; i < cols.size(); ++i) {
        out[topLevelColumnName(cols[i])] = i;
    }
}
} // namespace
SplitReader::SplitReader(
    const std::shared_ptr<const hive::HiveConnectorSplit> &hiveSplit,
    const std::shared_ptr<const HiveTableHandle> &hiveTableHandle,
    const std::unordered_map <std::string, std::shared_ptr<HiveColumnHandle>> * partitionKeys,
    const std::unordered_map <std::string, std::shared_ptr<HiveColumnHandle>> * infoColumnHandles,
    const std::shared_ptr<const HiveConfig> &hiveConfig,
    const type::RowTypePtr &readerOutputType,
    const std::shared_ptr <codegen::ScanSpec> &scanSpec,
    SpecialColumnNames specialColumns)
    : hiveSplit_(hiveSplit),
      hiveTableHandle_(hiveTableHandle),
      partitionKeys_(partitionKeys),
      infoColumnHandles_(infoColumnHandles),
      hiveConfig_(hiveConfig),
      readerOutputType_(readerOutputType),
      pool_(omniruntime::mem::GetMemoryPool()),
      scanSpec_(scanSpec),
      emptySplit_(false),
      specialColumns_(std::move(specialColumns))
{
    baseReaderOpts_ = std::make_shared<ReaderOptions>();
}

bool SplitReader::isSpecialNonFileColumn(const std::string &colName) const
{
    if (specialColumns_.rowIndex.has_value() && specialColumns_.rowIndex.value() == colName) {
        return true;
    }
    if (specialColumns_.rowId.has_value() && specialColumns_.rowId.value() == colName) {
        return true;
    }
    return false;
}

std::unique_ptr <SplitReader> SplitReader::create(
    const std::shared_ptr <hive::HiveConnectorSplit> &hiveSplit,
    const std::shared_ptr<const HiveTableHandle> &hiveTableHandle,
    const std::unordered_map <std::string, std::shared_ptr<HiveColumnHandle>> *partitionKeys,
    const std::unordered_map <std::string, std::shared_ptr<HiveColumnHandle>> *infoColumnHandles,
    const std::shared_ptr<const HiveConfig> &hiveConfig,
    const type::RowTypePtr &readerOutputType,
    const std::shared_ptr <codegen::ScanSpec> &scanSpec,
    const SpecialColumnNames &specialColumns)
{
    return std::make_unique<SplitReader>(
        hiveSplit,
        hiveTableHandle,
        partitionKeys,
        infoColumnHandles,
        hiveConfig,
        readerOutputType,
        scanSpec,
        specialColumns);
}

void SplitReader::prepareSplit(omniruntime::type::RowTypePtr &rowType, uint64_t batchLen)
{
    baseReaderOpts_->SetBatchLen(batchLen);
    createReader();
    createRowReader(rowType, batchLen);
}

uint64_t SplitReader::next(vec::VectorBatch **output_, int *omniTypeId, uint64_t batchLen)
{
    (void)omniTypeId;
    std::vector<int> fileTypeIdVector;
    fileTypeIdVector.reserve(fileRowType_->children_().size());
    for (const auto &child : fileRowType_->children_()) {
        fileTypeIdVector.push_back(child->GetId());
    }
    std::vector<omniruntime::vec::BaseVector *> *recordBatch;
    uint64_t batchRowSize = baseRowReader_->Next(&recordBatch, fileTypeIdVector.data(), batchLen);

    auto output = new vec::VectorBatch(batchRowSize);
    *output_ = output;
    if (batchRowSize <= 0) {
        return batchRowSize;
    }
    const bool hudiDatasourceScan = splitMarksGlutenOmniHudiDatasource(hiveSplit_);
    int fileVecIdx = 0;
    for (int i = 0; i < rowType_->size(); ++i) {
        const std::string &colName = rowType_->nameOf(i);
        const auto dataTypeId = rowType_->children_()[i]->GetId();
        const auto pkIt = hiveSplit_->partitionKeys.find(colName);
        if (pkIt != hiveSplit_->partitionKeys.end()) {
            vec::BaseVector *partitionVec = nullptr;
            if (pkIt->second.has_value()) {
                partitionVec = createConstPartitionVec(dataTypeId, batchRowSize, pkIt->second.value());
            } else {
                partitionVec = createNullConstVec(dataTypeId, batchRowSize);
            }
            output->Append(partitionVec);
            continue;
        }
        if (isSpecialNonFileColumn(colName)) {
            output->Append(createNullConstVec(dataTypeId, static_cast<int32_t>(batchRowSize)));
            continue;
        }
        if (columnMaterializedFromSplitInfo(colName, infoColumnHandles_, hudiDatasourceScan)) {
            const auto infoValIt = hiveSplit_->infoColumns.find(colName);
            vec::BaseVector *infoVec = nullptr;
            if (infoValIt != hiveSplit_->infoColumns.end() && !infoValIt->second.empty()) {
                infoVec = createConstPartitionVec(dataTypeId, batchRowSize, infoValIt->second);
            } else {
                infoVec = createNullConstVec(dataTypeId, batchRowSize);
            }
            output->Append(infoVec);
            continue;
        }
        if (!parquetFileVectorIndexByName_.empty()) {
            std::string lookupName = colName;
            const size_t dotPos = lookupName.find('.');
            if (dotPos != std::string::npos) {
                lookupName = lookupName.substr(0, dotPos);
            }
            const auto idxIt = parquetFileVectorIndexByName_.find(lookupName);
            if (idxIt != parquetFileVectorIndexByName_.end()
                && idxIt->second < recordBatch->size()) {
                vec::BaseVector *fileCol = recordBatch->at(idxIt->second);
                fileCol = coercePhysicalVecToSchemaType(fileCol, dataTypeId);
                output->Append(fileCol);
            } else {
                output->Append(createNullConstVec(dataTypeId, batchRowSize));
            }
            continue;
        }
        if (fileVecIdx < static_cast<int>(recordBatch->size())) {
            vec::BaseVector *fileCol = recordBatch->at(fileVecIdx++);
            fileCol = coercePhysicalVecToSchemaType(fileCol, dataTypeId);
            output->Append(fileCol);
        } else {
            auto missingFieldVec =
                createNullConstVec(dataTypeId, batchRowSize);
            output->Append(missingFieldVec);
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
    const bool hudiDatasourceScan = splitMarksGlutenOmniHudiDatasource(hiveSplit_);
    std::vector <std::string> readColumnNames;
    std::vector <std::shared_ptr<omniruntime::type::DataType>> readColumnTypes;
    for (size_t i = 0; i < rowType->names().size(); i++) {
        const auto &outputName = rowType->names()[i];
        auto it = partitionKeys_->find(outputName);
        if (it != partitionKeys_->end()) {
            auto *handle = static_cast<const HiveColumnHandle *>(it->second.get());
            if (handle->columnType() == HiveColumnHandle::ColumnType::kPartitionKey) {
                continue;
            }
        }
        if (columnMaterializedFromSplitInfo(outputName, infoColumnHandles_, hudiDatasourceScan)) {
            continue;
        }
        if (isSpecialNonFileColumn(outputName)) {
            continue;
        }
        readColumnNames.push_back(outputName);
        readColumnTypes.push_back(rowType->children_()[i]);
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
    if (hiveSplit_->getFileFormat() == codegen::FileFormat::PARQUET) {
        buildParquetFileVectorIndexMap(baseReaderOpts_, parquetFileVectorIndexByName_);
    } else {
        parquetFileVectorIndexByName_.clear();
    }
    baseRowReader_ = baseReader_->CreateRowReader();
}
} // namespace hive
