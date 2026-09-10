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

#include "RowGroupStatsFilter.hh"

#include <algorithm>
#include <cctype>
#include <string>

#include "orc/ColumnReader.hh"
#include "reader/common/Filter.h"

namespace omniruntime::reader {

namespace {

bool EqualsIgnoreCase(const std::string &lhs, const std::string &rhs)
{
    if (lhs.size() != rhs.size()) {
        return false;
    }
    for (size_t i = 0; i < lhs.size(); ++i) {
        if (std::tolower(static_cast<unsigned char>(lhs[i])) != std::tolower(static_cast<unsigned char>(rhs[i]))) {
            return false;
        }
    }
    return true;
}

bool KindHasTestableStats(::orc::TypeKind kind)
{
    switch (kind) {
        case ::orc::BOOLEAN:
        case ::orc::BYTE:
        case ::orc::SHORT:
        case ::orc::INT:
        case ::orc::LONG:
        case ::orc::DATE:
        case ::orc::FLOAT:
        case ::orc::DOUBLE:
            return true;
        default:
            // DECIMAL and TIMESTAMP statistics need scale/unit reconstruction, and string bounds
            // are writer-version dependent; static predicates on those columns are already pruned
            // through the SearchArgument path.
            return false;
    }
}

bool TestIntegerStats(const common::Filter &filter, const ::orc::proto::ColumnStatistics &stats, bool hasNull)
{
    if (stats.has_intstatistics()) {
        const auto &s = stats.intstatistics();
        if (s.has_minimum() && s.has_maximum()) {
            return filter.testInt64Range(s.minimum(), s.maximum(), hasNull);
        }
    }
    return true;
}

bool TestDateStats(const common::Filter &filter, const ::orc::proto::ColumnStatistics &stats, bool hasNull)
{
    if (stats.has_datestatistics()) {
        const auto &s = stats.datestatistics();
        if (s.has_minimum() && s.has_maximum()) {
            return filter.testInt64Range(s.minimum(), s.maximum(), hasNull);
        }
    }
    // Some writers record days for DATE columns under intStatistics.
    return TestIntegerStats(filter, stats, hasNull);
}

bool TestDoubleStats(const common::Filter &filter, const ::orc::proto::ColumnStatistics &stats, bool hasNull)
{
    if (stats.has_doublestatistics()) {
        const auto &s = stats.doublestatistics();
        if (s.has_minimum() && s.has_maximum()) {
            return filter.testDoubleRange(s.minimum(), s.maximum(), hasNull);
        }
    }
    return true;
}

// testInt64Range, testDoubleRange and testBytesRange all default to "keep the range", so a filter
// tested against a domain it does not implement simply stops pruning. testBool defaults to false
// instead, which would prune groups that do contain matches, so boolean statistics may only be
// tested by the filter kinds that genuinely answer for booleans.
bool AnswersForBool(common::FilterKind kind)
{
    switch (kind) {
        case common::FilterKind::kBoolValue:
        case common::FilterKind::kAlwaysTrue:
        case common::FilterKind::kAlwaysFalse:
        case common::FilterKind::kIsNull:
        case common::FilterKind::kIsNotNull:
            return true;
        default:
            return false;
    }
}

bool TestBooleanStats(const common::Filter &filter, const ::orc::proto::ColumnStatistics &stats, bool hasNull)
{
    if (!AnswersForBool(filter.kind())) {
        return true;
    }
    if (!stats.has_bucketstatistics() || stats.bucketstatistics().count_size() == 0 ||
        !stats.has_numberofvalues()) {
        return true;
    }
    const uint64_t trueCount = stats.bucketstatistics().count(0);
    const uint64_t total = stats.numberofvalues();
    if (trueCount > total) {
        return true; // inconsistent statistics: never prune on them
    }
    if (trueCount > 0 && filter.testBool(true)) {
        return true;
    }
    if (total - trueCount > 0 && filter.testBool(false)) {
        return true;
    }
    return hasNull && filter.testNull();
}

} // namespace

size_t CollectStatsPrunableColumns(const ::orc::Type &selectedRoot, const codegen::ScanSpec *rootSpec,
                                   std::vector<StatsPrunableColumn> &out)
{
    out.clear();
    if (rootSpec == nullptr) {
        return 0;
    }
    // Pairing is by field name rather than by ordinal. The selective path guarantees that the
    // ScanSpec children line up with the selected subtypes, but the legacy path reads through
    // fileRowType_, and testing one column's filter against another column's statistics would
    // silently drop rows. An unmatched name simply means no pruning for that column.
    const auto &specChildren = rootSpec->children();
    const uint64_t n = selectedRoot.getSubtypeCount();
    for (uint64_t i = 0; i < n; ++i) {
        const ::orc::Type *childType = selectedRoot.getSubtype(i);
        if (childType == nullptr || !KindHasTestableStats(childType->getKind())) {
            continue;
        }
        const std::string &orcName = selectedRoot.getFieldName(i);
        const codegen::ScanSpec *childSpec = nullptr;
        for (const auto &candidate : specChildren) {
            if (candidate != nullptr && EqualsIgnoreCase(candidate->fieldName(), orcName)) {
                childSpec = candidate.get();
                break;
            }
        }
        if (childSpec == nullptr) {
            continue;
        }
        out.push_back(StatsPrunableColumn{childType->getColumnId(), childType->getKind(), childSpec});
    }
    return out.size();
}

bool StatsMayContainMatch(const StatsPrunableColumn &column, const ::orc::proto::ColumnStatistics &stats)
{
    if (column.spec == nullptr) {
        return true;
    }
    const common::Filter *filter = column.spec->filter();
    if (filter == nullptr) {
        return true;
    }
    // A missing hasNull flag has to be read as "might contain nulls".
    const bool hasNull = !stats.has_hasnull() || stats.hasnull();
    if (stats.has_numberofvalues() && stats.numberofvalues() == 0) {
        return filter->testNull();
    }

    switch (column.kind) {
        case ::orc::BOOLEAN:
            return TestBooleanStats(*filter, stats, hasNull);
        case ::orc::BYTE:
        case ::orc::SHORT:
        case ::orc::INT:
        case ::orc::LONG:
            return TestIntegerStats(*filter, stats, hasNull);
        case ::orc::DATE:
            return TestDateStats(*filter, stats, hasNull);
        case ::orc::FLOAT:
        case ::orc::DOUBLE:
            return TestDoubleStats(*filter, stats, hasNull);
        default:
            return true;
    }
}

} // namespace omniruntime::reader
