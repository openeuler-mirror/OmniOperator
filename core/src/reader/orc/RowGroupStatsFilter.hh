/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

// Row-group pruning driven by the ScanSpec filter tree rather than by the ORC SearchArgument.
//
// A dynamic filter reaches the scan by being installed on the ScanSpec after the SearchArgument
// has already been built, so SargsApplier can never see it. Without this the only thing a dynamic
// filter can do is reject rows one at a time after they have been fully decoded, which is why it
// shows no measurable gain. Comparing it against the per-row-group min/max in the ORC index lets
// it skip decoding entire row groups, and skip the data IO for stripes it eliminates completely.

#ifndef OMNI_READER_ORC_ROW_GROUP_STATS_FILTER_HH
#define OMNI_READER_ORC_ROW_GROUP_STATS_FILTER_HH

#include <cstdint>
#include <vector>

#include "orc/Type.hh"
#include "codegen/ScanSpec.h"

namespace orc::proto {
class ColumnStatistics;
}

namespace omniruntime::reader {

// A projected leaf column whose ORC statistics we know how to compare against a pushed filter.
// The ScanSpec node is kept instead of the Filter itself: dynamic filter pushdown can replace
// spec->filter() while a split is being read, and columns that carry no filter at construction
// time may acquire one later.
struct StatsPrunableColumn {
    uint64_t columnId = 0;
    ::orc::TypeKind kind = ::orc::INT;
    const codegen::ScanSpec *spec = nullptr;
};

// Collects the leaf columns of 'selectedRoot' that carry a ScanSpec node and have a type whose
// statistics can be tested. Returns the number of columns collected.
size_t CollectStatsPrunableColumns(const ::orc::Type &selectedRoot, const codegen::ScanSpec *rootSpec,
                                   std::vector<StatsPrunableColumn> &out);

// False means no row covered by 'stats' can pass the column's filter, so the row group or stripe
// is safe to skip. Absent, unsupported or self-inconsistent statistics always return true.
bool StatsMayContainMatch(const StatsPrunableColumn &column, const ::orc::proto::ColumnStatistics &stats);

} // namespace omniruntime::reader

#endif // OMNI_READER_ORC_ROW_GROUP_STATS_FILTER_HH
