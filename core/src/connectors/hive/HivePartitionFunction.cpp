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

#include <cstdint>

namespace omniruntime::connector::hive {

namespace {
    void mergeHash(bool mix, uint32_t oneHash, uint32_t &aggregateHash)
    {
        aggregateHash = mix ? aggregateHash * 31 + oneHash : oneHash;
    }

    int32_t hashInt64(int64_t value)
    {
        return ((*reinterpret_cast<uint64_t *>(&value)) >> 32) ^ value;
    }

#if defined(__has_feature)
#if __has_feature(__address_sanitizer__)
    __attribute__((no_sanitize("integer")))
#endif
#endif
    } // namespace
} // namespace omniruntime::connector::hive
