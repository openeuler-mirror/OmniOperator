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

#include "connectors/hive/HiveConnectorUtil.h"
#include "connectors/hive/HiveConfig.h"
#include "connectors/hive/TableHandle.h"
#include <limits>
#include <cstdint>
#include "reader/orc/OrcFileOverride.hh"


namespace omniruntime::connector::hive {
namespace {

struct SubfieldSpec {
  const type::Subfield *subfield;
  bool filterOnly;
};

inline bool isSynthesizedColumn(
    const std::string &name,
    const std::unordered_map<std::string, std::shared_ptr<HiveColumnHandle>> &infoColumns)
{
  return infoColumns.count(name) != 0;
}

bool isSpecialColumn(const std::string &name,
                     const std::optional<std::string> &specialName)
{
  return specialName.has_value() && name == *specialName;
}

// Recursively add subfields to scan spec.
void addSubfields(const type::DataType &type,
    std::vector <SubfieldSpec> &subfields, int level,
    codegen::ScanSpec &spec)
{
    int newSize = 0;
    for (int i = 0; i < subfields.size(); ++i) {
        if (level < subfields[i].subfield->path().size()) {
            subfields[newSize++] = subfields[i];
        } else if (!subfields[i].filterOnly) {
            spec.addAllChildFields(type);
            return;
        }
    }
    subfields.resize(newSize);
    switch (type.GetId()) {
        case type::DataTypeId::OMNI_ROW: {
            std::unordered_map <std::string, std::vector<SubfieldSpec>> required;
            for (auto &subfield: subfields) {
                auto *element = subfield.subfield->path()[level].get();
                auto *nestedField =
                    dynamic_cast<const codegen::Subfield::NestedField *>(element);
                required[nestedField->name()].push_back(subfield);
            }
            auto &rowType = static_cast<const type::RowType &>(type);
            for (int i = 0; i < rowType.size(); ++i) {
                auto &childName = rowType.nameOf(i);
                auto &childType = rowType.childAt(i);
                auto *child = spec.addField(childName, i);
                auto it = required.find(childName);
                addSubfields(*childType, it->second, level + 1, *child);
            }
            break;
        }
        default:
            break;
    }
}

} // namespace

namespace {

void processFieldSpec(const type::RowTypePtr &dataColumns,
    const type::DataTypePtr &outputType,
    codegen::ScanSpec &fieldSpec)
{
    fieldSpec.visit(*outputType,
                    [](const type::DataType &type, codegen::ScanSpec &spec) {});
    if (dataColumns) {
        auto i = dataColumns->getChildIdxIfExists(fieldSpec.fieldName());
        if (i.has_value()) {
            if (dataColumns->childAt(*i)->GetId() == type::DataTypeId::OMNI_MAP &&
                outputType->GetId() == type::DataTypeId::OMNI_ROW) {
                fieldSpec.setFlatMapAsStruct(true);
            }
        }
    }
}

} // namespace

std::shared_ptr <codegen::ScanSpec> makeScanSpec(
    const vec::RowTypePtr &rowType,
    const std::unordered_map <std::string, std::vector<const type::Subfield *>>
    &outputSubfields,
    const vec::RowTypePtr &dataColumns,
    const std::unordered_map <std::string, std::shared_ptr<HiveColumnHandle>>
    &partitionKeys,
    const std::unordered_map <std::string, std::shared_ptr<HiveColumnHandle>>
    &infoColumns,
    const SpecialColumnNames &specialColumns)
{
    auto spec = std::make_shared<codegen::ScanSpec>("root");
    std::vector <SubfieldSpec> subfieldSpecs;
    // Process columns that will be projected out.
    for (int i = 0; i < rowType->size(); ++i) {
        auto &name = rowType->nameOf(i);
        auto &type = rowType->childAt(i);
        if (isSpecialColumn(name, specialColumns.rowIndex)) {
            auto *fieldSpec = spec->addField(name, i);
            fieldSpec->setColumnType(codegen::ScanSpec::ColumnType::kRowIndex);
            continue;
        }
        if (isSpecialColumn(name, specialColumns.rowId)) {
            auto &rowIdType = static_cast<const type::RowType &>(*type);
            auto *fieldSpec = spec->addFieldRecursively(name, rowIdType, i);
            fieldSpec->setColumnType(codegen::ScanSpec::ColumnType::kComposite);
            fieldSpec->childByName(rowIdType.nameOf(0))
                ->setColumnType(codegen::ScanSpec::ColumnType::kRowIndex);
            continue;
        }
        auto it = outputSubfields.find(name);
        if (it == outputSubfields.end()) {
            auto *fieldSpec = spec->addFieldRecursively(name, *type, i);
            processFieldSpec(dataColumns, type, *fieldSpec);
            continue;
        }
        for (auto *subfield: it->second) {
            subfieldSpecs.push_back({subfield, false});
        }
        auto *fieldSpec = spec->addField(name, i);
        addSubfields(*type, subfieldSpecs, 1, *fieldSpec);
        processFieldSpec(dataColumns, type, *fieldSpec);
        subfieldSpecs.clear();
    }
    return spec;
}

void configureReaderOptions(
    const std::shared_ptr<const HiveConfig> &hiveConfig,
    const std::shared_ptr<const HiveConnectorSplit> &hiveSplit,
    std::shared_ptr <omniruntime::reader::ReaderOptions> &baseReaderOpts_)
{
    switch (hiveSplit->getFileFormat()) {
        case omniruntime::codegen::FileFormat::ORC: {
            auto readerOptions = std::make_unique<orc::ReaderOptions>();
            orc::MemoryPool *pool = orc::getDefaultPool();
            readerOptions->setMemoryPool(*pool);
            readerOptions->setTailLocation(std::numeric_limits<uint64_t>::max());
            const std::string &SerializedFileTail = "";
            readerOptions->setSerializedFileTail(SerializedFileTail);
            auto orcReaderOptions = std::make_shared<OrcReaderOptions>(
                std::shared_ptr<orc::ReaderOptions>(std::move(readerOptions))
            );
            baseReaderOpts_ = orcReaderOptions;
            break;
        }
        case omniruntime::codegen::FileFormat::PARQUET: {
            throw std::runtime_error("Unsupported format PARQUET");
            break;
        }
        default: {
            throw std::runtime_error("Unsupported format");
            break;
        }
    }
}

void configureRowReaderOptions(
    const std::unordered_map <std::string, std::string> &tableParameters,
    const omniruntime::type::RowTypePtr &rowType,
    const std::shared_ptr <omniruntime::codegen::ScanSpec> &scanSpec,
    const std::shared_ptr<const HiveConnectorSplit> &hiveSplit,
    const std::shared_ptr<const HiveConfig> &hiveConfig,
    std::unique_ptr<::orc::SearchArgument> &searchArgument,
    std::shared_ptr <omniruntime::reader::RowReaderOptions> &baseRowReaderOpts_)
{
    switch (hiveSplit->getFileFormat()) {
        case omniruntime::codegen::FileFormat::ORC: {
            auto rowReaderOptions = std::make_unique<orc::RowReaderOptions>();
            rowReaderOptions->range(hiveSplit->start, hiveSplit->length);

            std::list <std::string> includedColumnsLenArray;
            for (int i = 0; i < rowType->size(); i++) {
                std::string name = rowType->nameOf(i);
                includedColumnsLenArray.push_back(name);
            }
            rowReaderOptions->include(includedColumnsLenArray);
            rowReaderOptions->searchArgument(std::unique_ptr<::orc::SearchArgument>(searchArgument.release()));

            auto orcRowReaderOptions =
                std::make_shared<omniruntime::reader::OrcRowReaderOptions>(
                    std::shared_ptr<orc::RowReaderOptions>(std::move(rowReaderOptions)));
            baseRowReaderOpts_ = orcRowReaderOptions;
            break;
        }
        case omniruntime::codegen::FileFormat::PARQUET: {
            throw std::runtime_error("Unsupported format PARQUET");
            break;
        }
        default: {
            throw std::runtime_error("Unsupported format");
            break;
        }
    }
}

UriInfo stringToUriInfo(std::string uriString)
{
    // 1. 提取 scheme (协议)
    size_t scheme_end = uriString.find("://");
    if (scheme_end == std::string::npos) {
        throw std::runtime_error("invalid scheme");
    }
    std::string schemaStr = uriString.substr(0, scheme_end);

    // 2. 提取 authority 部分 (host:port)
    size_t authority_start = scheme_end + 3; // 跳过 "://"
    size_t path_start = uriString.find('/', authority_start);

    std::string authority;
    std::string fileStr;
    if (path_start == std::string::npos) {
        // 没有路径部分
        authority = uriString.substr(authority_start);
        fileStr = ""; // 或根据需求设置为 "/"
    } else {
        authority = uriString.substr(authority_start, path_start - authority_start);
        fileStr = uriString.substr(path_start);
    }

    // 3. 从 authority 分离 host 和 port
    size_t port_pos = authority.find(':');
    std::string hostStr;
    int port;
    if (port_pos != std::string::npos) {
        hostStr = authority.substr(0, port_pos);
        std::string port_str = authority.substr(port_pos + 1);

        // 验证端口是否为数字
        if (!port_str.empty() &&
            std::all_of(port_str.begin(), port_str.end(), [](char c) {
                return std::isdigit(static_cast<unsigned char>(c));
            })) {
            port = std::stoi(port_str);
        } else {
            throw std::runtime_error("invalid port"); // 无效的端口
        }
    } else {
        hostStr = authority;
        // 没有指定端口
    }

    UriInfo uriInfo{schemaStr, fileStr, hostStr, std::to_string(port)};
    return uriInfo;
}

std::unique_ptr <omniruntime::reader::BufferInput> createBufferedInput(
    std::shared_ptr <omniruntime::reader::ReaderOptions> readerOpts,
    const std::shared_ptr<const HiveConnectorSplit> &hiveSplit)
{
    switch (readerOpts->GetFormat()) {
        case omniruntime::codegen::FileFormat::ORC: {
            UriInfo uri = stringToUriInfo(hiveSplit->getFilePath());
            std::unique_ptr<::orc::InputStream> inputStream = omniruntime::reader::readFileOverride(uri);
            return std::make_unique<omniruntime::reader::ORCBufferInput>(
                std::move(inputStream));
        }
        case omniruntime::codegen::FileFormat::PARQUET: {
            throw std::runtime_error("Unsupported format PARQUET");
            break;
        }
        default: {
            throw std::runtime_error("Unsupported format");
            break;
        }
    }
}

}
