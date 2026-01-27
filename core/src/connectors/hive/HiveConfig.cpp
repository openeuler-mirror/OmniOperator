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

#include "HiveConfig.h"
#include "util/debug.h"

#include <boost/algorithm/string.hpp>

namespace omniruntime {
namespace connector {
namespace hive {

namespace {

HiveConfig::InsertExistingPartitionsBehavior stringToInsertExistingPartitionsBehavior(const std::string& strValue)
{
    auto upperValue = boost::algorithm::to_upper_copy(strValue);
    if (upperValue == "ERROR") {
      return HiveConfig::InsertExistingPartitionsBehavior::kError;
    }
    if (upperValue == "OVERWRITE") {
      return HiveConfig::InsertExistingPartitionsBehavior::kOverwrite;
    }
}

} // namespace

HiveConfig::InsertExistingPartitionsBehavior
HiveConfig::insertExistingPartitionsBehavior(
        const config::ConfigBase* session) const
{
    return stringToInsertExistingPartitionsBehavior(session->Get<std::string>(
        kInsertExistingPartitionsBehaviorSession,
        config_->Get<std::string>(kInsertExistingPartitionsBehavior, "ERROR")));
}

uint32_t HiveConfig::maxPartitionsPerWriters(
        const config::ConfigBase* session) const
{
    return session->Get<uint32_t>(
        kMaxPartitionsPerWritersSession,
        config_->Get<uint32_t>(kMaxPartitionsPerWriters, 128));
}

bool HiveConfig::immutablePartitions() const
{
    return config_->Get<bool>(kImmutablePartitions, false);
}

std::string HiveConfig::gcsEndpoint() const
{
    return config_->Get<std::string>(kGcsEndpoint, std::string(""));
}

std::string HiveConfig::gcsCredentialsPath() const
{
    return config_->Get<std::string>(kGcsCredentialsPath, std::string(""));
}

std::optional<int> HiveConfig::gcsMaxRetryCount() const
{
    return static_cast<std::optional<int>>(config_->Get<int>(kGcsMaxRetryCount));
}

std::optional<std::string> HiveConfig::gcsMaxRetryTime() const
{
    return static_cast<std::optional<std::string>>(
        config_->Get<std::string>(kGcsMaxRetryTime));
}

bool HiveConfig::isOrcUseColumnNames(const config::ConfigBase* session) const
{
    return session->Get<bool>(
        kOrcUseColumnNamesSession, config_->Get<bool>(kOrcUseColumnNames, false));
}

bool HiveConfig::isParquetUseColumnNames(
        const config::ConfigBase* session) const
{
    return session->Get<bool>(
        kParquetUseColumnNamesSession,
        config_->Get<bool>(kParquetUseColumnNames, false));
}

bool HiveConfig::isFileColumnNamesReadAsLowerCase(
        const config::ConfigBase* session) const
{
    return session->Get<bool>(
        kFileColumnNamesReadAsLowerCaseSession,
        config_->Get<bool>(kFileColumnNamesReadAsLowerCase, false));
}

bool HiveConfig::isPartitionPathAsLowerCase(
        const config::ConfigBase* session) const
{
    return session->Get<bool>(kPartitionPathAsLowerCaseSession, true);
}

bool HiveConfig::allowNullPartitionKeys(
        const config::ConfigBase* session) const
{
    return session->Get<bool>(
        kAllowNullPartitionKeysSession,
        config_->Get<bool>(kAllowNullPartitionKeys, true));
}

bool HiveConfig::ignoreMissingFiles(const config::ConfigBase* session) const
{
    return session->Get<bool>(kIgnoreMissingFilesSession, false);
}

int64_t HiveConfig::maxCoalescedBytes(const config::ConfigBase* session) const
{
    return session->Get<int64_t>(
        kMaxCoalescedBytesSession,
        config_->Get<int64_t>(kMaxCoalescedBytes, 128 << 20)); // 128MB
}


int32_t HiveConfig::prefetchRowGroups() const
{
    return config_->Get<int32_t>(kPrefetchRowGroups, 1);
}

int32_t HiveConfig::loadQuantum(const config::ConfigBase* session) const
{
    return session->Get<int32_t>(
        kLoadQuantumSession, config_->Get<int32_t>(kLoadQuantum, 8 << 20));
}

int32_t HiveConfig::numCacheFileHandles() const
{
    return config_->Get<int32_t>(kNumCacheFileHandles, 20'000);
}

uint64_t HiveConfig::fileHandleExpirationDurationMs() const
{
    return config_->Get<uint64_t>(kFileHandleExpirationDurationMs, 0);
}

bool HiveConfig::isFileHandleCacheEnabled() const
{
    return config_->Get<bool>(kEnableFileHandleCache, true);
}

std::string HiveConfig::writeFileCreateConfig() const
{
    return config_->Get<std::string>(kWriteFileCreateConfig, "");
}

uint32_t HiveConfig::sortWriterMaxOutputRows(
        const config::ConfigBase* session) const
{
    return session->Get<uint32_t>(
        kSortWriterMaxOutputRowsSession,
        config_->Get<uint32_t>(kSortWriterMaxOutputRows, 1024));
}


uint64_t HiveConfig::sortWriterFinishTimeSliceLimitMs(
        const config::ConfigBase* session) const
{
    return session->Get<uint64_t>(
        kSortWriterFinishTimeSliceLimitMsSession,
        config_->Get<uint64_t>(kSortWriterFinishTimeSliceLimitMs, 5'000));
}

uint64_t HiveConfig::footerEstimatedSize() const
{
    return config_->Get<uint64_t>(kFooterEstimatedSize, 256UL << 10);
}

uint64_t HiveConfig::filePreloadThreshold() const
{
    return config_->Get<uint64_t>(kFilePreloadThreshold, 8UL << 20);
}

uint8_t HiveConfig::readTimestampUnit(const config::ConfigBase* session) const
{
    const auto unit = session->Get<uint8_t>(
        kReadTimestampUnitSession,
        config_->Get<uint8_t>(kReadTimestampUnit, 3 /*milli*/));
    return unit;
}

bool HiveConfig::readTimestampPartitionValueAsLocalTime(
        const config::ConfigBase* session) const
{
    return session->Get<bool>(
        kReadTimestampPartitionValueAsLocalTimeSession,
        config_->Get<bool>(kReadTimestampPartitionValueAsLocalTime, true));
}

bool HiveConfig::readStatsBasedFilterReorderDisabled(
        const config::ConfigBase* session) const
{
    return session->Get<bool>(
        kReadStatsBasedFilterReorderDisabledSession,
        config_->Get<bool>(kReadStatsBasedFilterReorderDisabled, false));
}

std::string HiveConfig::hiveLocalDataPath() const
{
    return config_->Get<std::string>(kLocalDataPath, "");
}

std::string HiveConfig::hiveLocalFileFormat() const
{
    return config_->Get<std::string>(kLocalFileFormat, "");
}

} } } // namespace omniruntime::connector::hive
