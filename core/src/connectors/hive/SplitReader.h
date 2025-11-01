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
#include <string_view>

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
    SplitReader(
        const std::shared_ptr<const hive::HiveConnectorSplit> &hiveSplit_,
        const std::shared_ptr<const HiveTableHandle> &hiveTableHandle_,
        const std::unordered_map<std::string, std::shared_ptr<HiveColumnHandle>> *partitionKeys_,
        const std::shared_ptr<const HiveConfig> &hiveConfig_,
        const type::RowTypePtr &readerOutputType_,
        const std::shared_ptr<codegen::ScanSpec> &scanSpec_);

    static std::unique_ptr <SplitReader> create(
        const std::shared_ptr <hive::HiveConnectorSplit> &hiveSplit,
        const std::shared_ptr<const HiveTableHandle> &hiveTableHandle,
        const std::unordered_map <std::string, std::shared_ptr<HiveColumnHandle>> *partitionKeys,
        const std::shared_ptr<const HiveConfig> &hiveConfig,
        const type::RowTypePtr &readerOutputType,
        const std::shared_ptr <codegen::ScanSpec> &scanSpec);

    virtual ~SplitReader() = default;

    uint64_t next(vec::VectorBatch **output_, int *omniTypeId, uint64_t batchLen);

    void prepareSplit(omniruntime::type::RowTypePtr &rowType, uint64_t batchLen);

    std::string toString() const;

    void createReader();

protected:
    std::shared_ptr<const HiveConnectorSplit> hiveSplit_;
    const std::shared_ptr<const HiveTableHandle> hiveTableHandle_;
    const std::unordered_map <std::string, std::shared_ptr<HiveColumnHandle>> *const partitionKeys_;
    const std::shared_ptr<const HiveConfig> hiveConfig_;
    type::RowTypePtr readerOutputType_;
    mem::MemoryPool *const pool_;
    std::shared_ptr <codegen::ScanSpec> scanSpec_;
    std::unique_ptr <omniruntime::reader::Reader> baseReader_;
    std::unique_ptr <omniruntime::reader::RowReader> baseRowReader_;
    std::shared_ptr <ReaderOptions> baseReaderOpts_;
    bool emptySplit_;
    omniruntime::type::RowTypePtr rowType_;
    omniruntime::type::RowTypePtr fileRowType_;

    void createRowReader(omniruntime::type::RowTypePtr &rowType, uint64_t batchLen);

    int64_t StringToTimestamp(const std::string &timeStr)
    {
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

    // Convert "YYYY-MM-DD" to the number of days since 1970-01-01 (consistent with DATE32 semantics)
    // Convert YYYY-MM-DD to the number of days since 1970-01-01 (DATE32)
    constexpr bool isLeap(int year) {
        return (year % 4 == 0) && (year % 100 != 0 || year % 400 == 0);
    }

    // Days in each month (non-leap year)
    static constexpr int kDaysInMonth[] = {
        31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31
    };

    int32_t ParseDate32(std::string_view s) {
        if (s.size() != 10 || s[4] != '-' || s[7] != '-') {
            throw std::invalid_argument("Invalid date format: expected YYYY-MM-DD");
        }

        auto parse2 = [&](size_t i) -> int {
            return (s[i] - '0') * 10 + (s[i + 1] - '0');
        };
        int year  = (s[0] - '0') * 1000 + (s[1] - '0') * 100 + (s[2] - '0') * 10 + (s[3] - '0');
        int month = parse2(5);  // 1-based
        int day   = parse2(8);  // 1-based

        if (month < 1 || month > 12 || day < 1) {
            throw std::invalid_argument("Invalid month or day");
        }

        // Check if day exceeds the maximum for the given month
        int maxDay = kDaysInMonth[month - 1];
        if (month == 2 && isLeap(year)) maxDay = 29;
        if (day > maxDay) {
            throw std::invalid_argument("Invalid day for month");
        }

        // Calculate the number of days from 1970-01-01 to (year, month, day)
        int32_t days = 0;

        // Add full years first
        for (int y = 1970; y < year; ++y) {
            days += isLeap(y) ? 366 : 365;
        }

        // Then add months in the current year
        for (int m = 1; m < month; ++m) {
            days += kDaysInMonth[m - 1];
            if (m == 2 && isLeap(year)) days += 1;
        }

        // Finally add days (1st of the month is day 0, so add (day - 1))
        days += (day - 1);

        return days;
    }

    void setPartitionVal(vec::BaseVector *vector, int32_t index, std::string &val) {
        auto dataTypeId = vector->GetTypeId();
        switch (dataTypeId) {
            case omniruntime::type::OMNI_BYTE:{
                int8_t item = static_cast<int8_t>(std::stoi(val));
                omniruntime::vec::VectorHelper::SetValue(vector, index, &item);
                break;
            }
            case omniruntime::type::OMNI_SHORT:{
                int16_t item = static_cast<int16_t>(std::stoi(val));
                omniruntime::vec::VectorHelper::SetValue(vector, index, &item);
                break;
            }
            case omniruntime::type::OMNI_INT: {
                auto item = std::stoi(val);
                omniruntime::vec::VectorHelper::SetValue(vector, index, &item);
                break;
            }
            case omniruntime::type::OMNI_DATE32: {
                auto item = ParseDate32(val);
                omniruntime::vec::VectorHelper::SetValue(vector, index, &item);
                break;
            }
            case omniruntime::type::OMNI_LONG: {
                auto item = std::stol(val);
                omniruntime::vec::VectorHelper::SetValue(vector, index, &item);
                break;
            }
            case omniruntime::type::OMNI_FLOAT: {
                float item = std::stof(val);
                omniruntime::vec::VectorHelper::SetValue(vector, index, &item);
                break;
            }
            case omniruntime::type::OMNI_DOUBLE: {
                auto item = std::stod(val);
                omniruntime::vec::VectorHelper::SetValue(vector, index, &item);
                break;
            }
            case omniruntime::type::OMNI_BOOLEAN: {
                bool item = (val == "true" || val == "1");
                omniruntime::vec::VectorHelper::SetValue(vector, index, &item);
                break;
            }
            case omniruntime::type::OMNI_CHAR:
            case omniruntime::type::OMNI_VARCHAR: {
                omniruntime::vec::VectorHelper::SetValue(vector, index, &val);
                break;
            }
            case omniruntime::type::OMNI_DECIMAL64:
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
