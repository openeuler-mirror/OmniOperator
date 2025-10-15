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

#ifndef OMNI_RUNTIME_SPLITREADER_H
#define OMNI_RUNTIME_SPLITREADER_H

#include "HiveConfig.h"
#include "HiveConnectorSplit.h"
#include "HiveConnectorUtil.h"
#include "vector/vector.h"
#include "type/data_type.h"
#include "TableHandle.h"
#include "memory/memory_pool.h"
#include <type/data_type.h>
#include "reader/Reader.h"
#include "reader/jni/OrcColumnarBatchJniReader.h"
#include <iostream>
#include <chrono>
#include <sstream>
#include <iomanip>
#include <stdexcept>

namespace omniruntime::codegen {
class ScanSpec;
} // namespace omniruntime::codegen

namespace omniruntime::connector::hive {

struct HiveConnectorSplit;
class HiveTableHandle;
class HiveColumnHandle;
class HiveConfig;

class SplitReader {
public:
    static std::unique_ptr <SplitReader> create(
        const std::shared_ptr <hive::HiveConnectorSplit> &hiveSplit,
        const std::shared_ptr<const HiveTableHandle> &hiveTableHandle,
        const std::unordered_map <std::string, std::shared_ptr<HiveColumnHandle>> *
        partitionKeys,
        const std::shared_ptr<const HiveConfig> &hiveConfig,
        const type::RowTypePtr &readerOutputType,
        const std::shared_ptr <codegen::ScanSpec> &scanSpec);

    bool readAndFilterData(std::unique_ptr <omniruntime::reader::RowReader> &rowReaderPtr,
        std::vector<omniruntime::vec::BaseVector *> *recordBatch, uint64_t &batchRowSize, int *omniTypeId,
        uint64_t batchLen);

    virtual ~SplitReader() = default;

    uint64_t next(vec::VectorBatch **output_, int *omniTypeId, uint64_t batchLen);

    void prepareSplit(omniruntime::type::RowTypePtr &rowType, uint64_t batchLen);

    std::string toString() const;

    void createReader();

protected:
    SplitReader(
        const std::shared_ptr<const hive::HiveConnectorSplit> &hiveSplit_,
        const std::shared_ptr<const HiveTableHandle> &hiveTableHandle_,
        const std::unordered_map <std::string, std::shared_ptr<HiveColumnHandle>> *
        partitionKeys_,
        const std::shared_ptr<const HiveConfig> &hiveConfig_,
        const type::RowTypePtr &readerOutputType_,
        const std::shared_ptr <codegen::ScanSpec> &scanSpec_);

protected:
    std::shared_ptr<const HiveConnectorSplit> hiveSplit_;
    const std::shared_ptr<const HiveTableHandle> hiveTableHandle_;
    const std::unordered_map <
    std::string,
    std::shared_ptr<HiveColumnHandle>> *const partitionKeys_;
    const std::shared_ptr<const HiveConfig> hiveConfig_;
    type::RowTypePtr readerOutputType_;
    mem::MemoryPool *const pool_;
    std::shared_ptr <codegen::ScanSpec> scanSpec_;
    std::unique_ptr <omniruntime::reader::Reader> baseReader_;
    std::unique_ptr <omniruntime::reader::RowReader> baseRowReader_;
    std::shared_ptr <omniruntime::reader::ReaderOptions> baseReaderOpts_;
    std::shared_ptr <omniruntime::reader::RowReaderOptions> baseRowReaderOpts_;
    bool emptySplit_;
    omniruntime::type::RowTypePtr rowType_;
    omniruntime::type::RowTypePtr fileRowType_;

    void createRowReader(omniruntime::type::RowTypePtr &rowType, uint64_t batchLen);

    int64_t StringToTimestamp(const std::string &timeStr) {
        std::tm tm = {};
        std::istringstream ss(timeStr);

        ss >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S");
        if (ss.fail()) {
            throw std::runtime_error("Failed to parse time string: " + timeStr);
        }
        tm.tm_isdst = 0;

        std::time_t time = timegm(&tm);
        if (time == -1) {
            throw std::runtime_error("UTC time conversion failed");
        }
        auto tp = std::chrono::system_clock::from_time_t(time);

        auto duration = tp.time_since_epoch();
        auto micros = std::chrono::duration_cast<std::chrono::microseconds>(duration);

        return static_cast<int64_t>(micros.count());
    }

    void setPartitionVal(vec::BaseVector *vector, int32_t index, std::string &val) {
        auto dataTypeId = vector->GetTypeId();
        switch (dataTypeId) {
            case omniruntime::type::OMNI_INT:
            case omniruntime::type::OMNI_DATE32: {
                auto item = std::stoi(val);
                omniruntime::vec::VectorHelper::SetValue(vector, index, &item);
                break;
            }
            case omniruntime::type::OMNI_LONG:
            case omniruntime::type::OMNI_DECIMAL64: {
                auto item = std::stol(val);
                omniruntime::vec::VectorHelper::SetValue(vector, index, &item);
                break;
            }
            case omniruntime::type::OMNI_DOUBLE: {
                auto item = std::stod(val);
                omniruntime::vec::VectorHelper::SetValue(vector, index, &item);
                break;
            }
            case omniruntime::type::OMNI_BOOLEAN: {
                auto item = std::strcmp("true", val.c_str()) == 0;
                omniruntime::vec::VectorHelper::SetValue(vector, index, &item);
                break;
            }
            case omniruntime::type::OMNI_CHAR:
            case omniruntime::type::OMNI_VARCHAR: {
                auto item = val;
                omniruntime::vec::VectorHelper::SetValue(vector, index, &item);
                break;
            }
            case omniruntime::type::OMNI_DECIMAL128: {
                auto item = omniruntime::type::Decimal128(val);

                omniruntime::vec::VectorHelper::SetValue(vector, index, &item);
                break;
            }
            case omniruntime::type::OMNI_TIMESTAMP: {
                auto item = StringToTimestamp(val);

                omniruntime::vec::VectorHelper::SetValue(vector, index, &item);
                break;
            }
            default:
                LogError("No such data type %d", dataTypeId);
                break;
        }
    }
};

void createReader();

} // namespace hive


#endif // OMNI_RUNTIME_SPLITREADER_H
