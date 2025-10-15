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

#pragma once

#include <utility>
#include <vector>
#include <cstdint>
#include <memory>

#include <type/data_type.h>

namespace omniruntime::connector::hive {

class HivePartitionFunction {
public:
    ~HivePartitionFunction() = default;

private:
  // Precompute single value hive hash for a constant partition key.
    void precompute(const vec::BaseVector& value, size_t column_index_t);

    void hashTyped(
        bool /* mix */,
        std::vector<uint32_t>& /* hashes */,
        size_t /* poolIndex */) {}

    const std::vector<type::column_index_t> keyChannels_;

    // Pools of reusable memory.
    std::vector<std::unique_ptr<std::vector<uint32_t>>> hashesPool_;
    std::vector<uint32_t> precomputedHashes_;
};

} // namespace omniruntime::connector::hive
