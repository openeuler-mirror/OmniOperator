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

#include "JulianGregorianRebase.h"
#include "reader/jni/jni_common.h"
#include <stdexcept>
#include <sstream>
#include <algorithm>

namespace common {
    static constexpr int64_t ONE_DAY_MICROS = 86400000000L;
    static constexpr int64_t JULIAN_100YEAR_MICROS= 3155760000000000L;
    static constexpr int32_t ONE_DAY_DAYS = 1;
    static constexpr int32_t JULIAN_100YEAR_DAYS= 36500;

    JulianGregorianRebase::JulianGregorianRebase(std::string tz, const std::vector<int64_t> &switches,
        const std::vector<int64_t> &diffs) : tz(std::move(tz)), switches(switches), diffs(diffs)
    {
        offset = diffs[0] - ONE_DAY_MICROS * 2L;
        lastSwitch = switches[switches.size() - 1];
    }

    int64_t JulianGregorianRebase::RebaseJulianToGregorianMicros(int64_t micros)
    {
        if (micros >= lastSwitch) {
            return micros;
        }
        if (micros < switches[0]) {
            return micros + (ONE_DAY_MICROS * CalculateJulianDayOffset(micros)) + offset;
        }
        uint64_t i = switches.size() - 1;
        while (i > 0 && micros < switches[i]) {
            i -= 1;
        }
        return micros + diffs[i];
    }

    int64_t JulianGregorianRebase::CalculateJulianDayOffset(int64_t micros)
    {
        int64_t dayOffset = 2;
        int64_t switchMicros = switches[1];
        int64_t microsOffset = switchMicros - micros - JULIAN_100YEAR_MICROS;
        if (microsOffset <= 0) {
            return dayOffset;
        }
        int64_t step = microsOffset / JULIAN_100YEAR_MICROS - (microsOffset % JULIAN_100YEAR_MICROS == 0 ? 1 : 0);
        dayOffset += step - (step / 4);
        return dayOffset;
    }

    std::string JulianGregorianRebase::GetTz()
    {
        return tz;
    }

    std::unique_ptr<JulianGregorianRebase> BuildJulianGregorianRebase(nlohmann::json& json)
    {
        if (!json.contains("tz") || !json["tz"].is_string()) {
            return nullptr;
        }
        const std::string tz = json["tz"].get<std::string>();

        auto parseCommaSeparatedInts = [](const std::string& str, const std::string& fieldName) {
            std::vector<int64_t> result;
            std::stringstream ss(str);
            std::string token;

            while (std::getline(ss, token, ',')) {
                token.erase(0, token.find_first_not_of(" \t\n\r\f\v"));
                token.erase(token.find_last_not_of(" \t\n\r\f\v") + 1);

                if (token.empty()) continue;

                try {
                    result.push_back(std::stoll(token));
                } catch (const std::invalid_argument& e) {
                    throw std::runtime_error("'" + fieldName + "' contains invalid integer: '" + token + "'");
                } catch (const std::out_of_range& e) {
                    throw std::runtime_error("'" + fieldName + "' contains integer out of range: '" + token + "'");
                }
            }

            return result;
        };

        if (!json.contains("switches") || !json["switches"].is_string()) {
            throw std::runtime_error("Missing or invalid 'switches' field (must be a string)");
        }
        const std::string switches_str = json["switches"].get<std::string>();
        const std::vector<int64_t> switches = parseCommaSeparatedInts(switches_str, "switches");

        if (!json.contains("diffs") || !json["diffs"].is_string()) {
            throw std::runtime_error("Missing or invalid 'diffs' field (must be a string)");
        }
        const std::string diffs_str = json["diffs"].get<std::string>();
        const std::vector<int64_t> diffs = parseCommaSeparatedInts(diffs_str, "diffs");

        if (switches.size() != diffs.size()) {
            throw std::runtime_error("'switches' and 'diffs' must have the same number of elements");
        }

        return std::make_unique<JulianGregorianRebase>(tz, switches, diffs);
    }

    JulianGregorianRebaseDays::JulianGregorianRebaseDays()
    {
        switches = {
            -719164, -682945, -646420, -609895, -536845, -500320, -463795,
            -390745, -354220, -317695, -244645, -208120, -171595, -141427
        };
        diffs = {
            2, 1, 0, -1, -2, -3, -4, -5, -6, -7, -8, -9, -10, 0
        };
        offset = diffs[0] - ONE_DAY_DAYS * 2;
        lastSwitch = switches[switches.size() - 1];
    }

    int32_t JulianGregorianRebaseDays::RebaseJulianToGregorianDays(int32_t days)
    {
        if (days >= lastSwitch) {
            return days;
        }
        if (days < switches[0]) {
            return days + (ONE_DAY_DAYS * CalculateJulianDayOffset(days)) + offset;
        }
        uint32_t i = switches.size() - 1;
        while (i > 0 && days < switches[i]) {
            i -= 1;
        }
        return days + diffs[i];
    }

    int32_t JulianGregorianRebaseDays::CalculateJulianDayOffset(int32_t days)
    {
        int32_t dayOffset = 2;
        int32_t switchDays = switches[1];
        int32_t daysOffset = switchDays - days - JULIAN_100YEAR_DAYS;
        if (daysOffset <= 0) {
            return dayOffset;
        }
        int32_t step = daysOffset / JULIAN_100YEAR_DAYS - (daysOffset % JULIAN_100YEAR_DAYS == 0 ? 1 : 0);
        dayOffset += step - (step / 4);
        return dayOffset;
    }
}
