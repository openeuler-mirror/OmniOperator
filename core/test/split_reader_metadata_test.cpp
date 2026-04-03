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

#include "connectors/hive/HiveConnectorSplit.h"
#include "connectors/hive/SplitReaderMetadata.h"
#include "gtest/gtest.h"
#include "type/data_type.h"

namespace omniruntime::connector::hive::test {

namespace {

std::shared_ptr<HiveConnectorSplit> makeSplit(std::unordered_map<std::string, std::string> infoCols = {})
{
    HiveConnectorSplitBuilder builder("/tmp/x");
    builder.connectorId("test-connector")
        .fileFormat(omniruntime::codegen::FileFormat::PARQUET)
        .start(0)
        .length(100);
    for (const auto &kv : infoCols) {
        builder.infoColumn(kv.first, kv.second);
    }
    return builder.build();
}

} // namespace

TEST(SplitReaderMetadataTest, splitMarksGlutenOmniHudiDatasource_nullptr)
{
    std::shared_ptr<const HiveConnectorSplit> nullSplit;
    EXPECT_FALSE(splitMarksGlutenOmniHudiDatasource(nullSplit));
}

TEST(SplitReaderMetadataTest, splitMarksGlutenOmniHudiDatasource_true)
{
    auto s = makeSplit({{kGlutenOmniInternalHudiDatasourceKey, "true"}});
    EXPECT_TRUE(splitMarksGlutenOmniHudiDatasource(s));
}

TEST(SplitReaderMetadataTest, splitMarksGlutenOmniHudiDatasource_missing_key)
{
    EXPECT_FALSE(splitMarksGlutenOmniHudiDatasource(makeSplit({})));
}

TEST(SplitReaderMetadataTest, splitMarksGlutenOmniHudiDatasource_value_must_be_true)
{
    auto s = makeSplit({{kGlutenOmniInternalHudiDatasourceKey, "1"}});
    EXPECT_FALSE(splitMarksGlutenOmniHudiDatasource(s));
}

TEST(SplitReaderMetadataTest, hoodie_prefix_only_when_hudi_scan_flag_true)
{
    EXPECT_FALSE(columnMaterializedFromSplitInfo("_hoodie_commit_time", nullptr, false));
    EXPECT_TRUE(columnMaterializedFromSplitInfo("_hoodie_commit_time", nullptr, true));
}

TEST(SplitReaderMetadataTest, synthesized_column_materialized_from_split_info)
{
    const auto dt = type::LongDataType::Instance();
    auto syn = std::make_shared<HiveColumnHandle>(
        "syn_col", HiveColumnHandle::ColumnType::kSynthesized, dt, dt);
    std::unordered_map<std::string, std::shared_ptr<HiveColumnHandle>> handles;
    handles["syn_col"] = syn;

    EXPECT_TRUE(columnMaterializedFromSplitInfo("syn_col", &handles, false));

    auto reg = std::make_shared<HiveColumnHandle>(
        "reg_col", HiveColumnHandle::ColumnType::kRegular, dt, dt);
    handles.clear();
    handles["reg_col"] = reg;
    EXPECT_FALSE(columnMaterializedFromSplitInfo("reg_col", &handles, false));
}

TEST(SplitReaderMetadataTest, info_handles_null_skips_synthesized_lookup)
{
    EXPECT_FALSE(columnMaterializedFromSplitInfo("syn_col", nullptr, false));
}

} // namespace omniruntime::connector::hive::test
