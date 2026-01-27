/**
 * Copyright (C) 2024-2025. Huawei Technologies Co., Ltd. All rights reserved.
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <parquet/exception.h>
#include "TimeRebaseInfo.h"

namespace common {
    int64_t TimeRebaseInfo::timestampRebaseFunc(int64_t micros)
    {
        if (timestampRebaseMode == MODE_CORRECTED || timestampRebasePtr == nullptr) {
            return micros;
        }
        switch (timestampRebaseMode) {
            case MODE_EXCEPTION: {
                if (micros < lastSwitchJulianTs) {
                   throw parquet::ParquetStatusException(::arrow::Status::RError(
                        R"(The files may be written by Spark 2.x or legacy versions of Hive, reading timestamps before 1900-01-01T00:00:00Z from Parquet files can be ambiguous. You can set the SQL config "spark.sql.parquet.datetimeRebaseModeInRead" or the datasource option "datetimeRebaseMode" to "LEGACY" to rebase the datetime values w.r.t. the calendar difference during reading. To read the datetime values as it is, set the SQL config "spark.sql.parquet.datetimeRebaseModeInRead" or the datasource option "datetimeRebaseMode" to 'CORRECTED'.)"));
                }
                return micros;
            }
            case MODE_LEGACY:
                return timestampRebasePtr->RebaseJulianToGregorianMicros(micros);
            default:
                throw parquet::ParquetStatusException(::arrow::Status::Invalid("Invalid timestampRebaseMode"));
        }
    }

    int64_t TimeRebaseInfo::int96RebaseFunc(int64_t micros)
    {
        if (int96RebaseMode == MODE_CORRECTED || int96RebasePtr == nullptr) {
            return micros;
        }
        switch (int96RebaseMode) {
            case MODE_EXCEPTION: {
                if (micros < lastSwitchJulianTs) {
                    throw parquet::ParquetStatusException(::arrow::Status::RError(
                        R"(The files may be written by Spark 2.x or legacy versions of Hive, reading timestamps before 1900-01-01T00:00:00Z from Parquet INT96 files can be ambiguous. You can set the SQL config "spark.sql.parquet.int96RebaseModeInRead" or the datasource option "int96RebaseMode" to "LEGACY" to rebase the datetime values w.r.t. the calendar difference during reading. To read the datetime values as it is, set the SQL config "spark.sql.parquet.int96RebaseModeInRead" or the datasource option "int96RebaseMode" to 'CORRECTED'.)"));
                }
                return micros;
            }
            case MODE_LEGACY:
                return int96RebasePtr->RebaseJulianToGregorianMicros(micros);
            default:
                throw parquet::ParquetStatusException(::arrow::Status::Invalid("Invalid int96RebaseMode"));
        }
    }

    std::unique_ptr<TimeRebaseInfo> BuildTimeRebaseInfo(nlohmann::json &json)
    {
        std::unique_ptr<JulianGregorianRebase> timestampRebasePtr;
        LegacyBehaviorPolicy timestampRebaseMode = MODE_INVALID;

        if (json.contains("timestampRebase") && !json["timestampRebase"].is_null()) {
            auto& tsRebaseJson = json["timestampRebase"];
            timestampRebasePtr = BuildJulianGregorianRebase(tsRebaseJson);
            if (tsRebaseJson.contains("mode")) {
                timestampRebaseMode = static_cast<LegacyBehaviorPolicy>(tsRebaseJson["mode"].get<int>());
            }
        }

        std::unique_ptr<JulianGregorianRebase> int96RebasePtr;
        LegacyBehaviorPolicy int96RebaseMode = MODE_INVALID;

        if (json.contains("int96Rebase") && !json["int96Rebase"].is_null()) {
            auto& int96RebaseJson = json["int96Rebase"];
            int96RebasePtr = BuildJulianGregorianRebase(int96RebaseJson);
            if (int96RebaseJson.contains("mode")) {
                int96RebaseMode = static_cast<LegacyBehaviorPolicy>(int96RebaseJson["mode"].get<int>());
            }
        }

        int64_t lastSwitchJulianTs = std::numeric_limits<int64_t>::min();
        if (json.contains("lastSwitchJulianTs") &&
            (json.contains("timestampRebase") || json.contains("int96Rebase"))) {
            lastSwitchJulianTs = json["lastSwitchJulianTs"].get<int64_t>();
        }

        return std::make_unique<TimeRebaseInfo>(timestampRebasePtr, timestampRebaseMode, int96RebasePtr,
            int96RebaseMode, lastSwitchJulianTs);
    }
}
