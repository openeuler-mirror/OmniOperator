/*
* Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// This file is generated. Do not modify it manually. To re-generate it, run:
//
//  ./velox/type/tz/gen_timezone_database.py -f /tmp/zone-index.properties \
//       > velox/type/tz/TimeZoneDatabase.cpp
//
// The zone-index.properties file should be the same one used in Presto,
// Available here :
// https://github.com/prestodb/presto/blob/master/presto-common/src/main/resources/com/facebook/presto/common/type/zone-index.properties.
// @generated

#include <string>
#include <unordered_map>
#include <vector>

namespace omniruntime::tz {

    const std::vector<std::pair<int16_t, std::string>>& getTimeZoneEntries() {
        static auto* tzDB = new std::vector<std::pair<int16_t, std::string>>([] {
          // Work around clang compiler bug causing multi-hour compilation
          // with -fsanitize=fuzzer
          // https://github.com/llvm/llvm-project/issues/75666
          return std::vector<std::pair<int16_t, std::string>>{
              {1980, "Asia/Shanghai"},
          };
        }());
        return *tzDB;
    }

} // namespace omniruntime::tz
