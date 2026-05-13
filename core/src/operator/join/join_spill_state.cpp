/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Description: first-stage subpartitioned join spill state.
 */
#include "join_spill_state.h"

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <iostream>
#include <sys/stat.h>
#include <utility>

#include "operator/spill/spiller.h"
#include "util/compiler_util.h"
#include "util/global_log.h"
#include "util/omni_exception.h"
#include "vector/vector_helper.h"

namespace omniruntime::op {
namespace {

std::atomic<uint64_t> gJoinSpillSessionId{0};

bool MkdirIfNeeded(const std::string &path)
{
    if (path.empty()) {
        return false;
    }
    if (mkdir(path.c_str(), 0750) == 0) {
        return true;
    }
    return errno == EEXIST;
}

uint64_t EstimateSpillVectorBatchSize(vec::VectorBatch *batch)
{
    if (batch == nullptr || batch->GetRowCount() <= 0) {
        return 0;
    }
    // SpillWriter only needs this value to maintain fileLength when no write buffer is used.
    // Use a conservative upper bound; SpillReader stops by totalRowCount for uncompressed files.
    return std::max<uint64_t>(4096, static_cast<uint64_t>(batch->GetRowCount()) *
        static_cast<uint64_t>(std::max(1, batch->GetVectorCount())) * 4096);
}

} // namespace

JoinSpillState::JoinSpillState(const std::string &spillDir, const type::DataTypes &buildTypes,
    const type::DataTypes &probeTypes, uint32_t numSubPartitions)
    : buildTypes_(buildTypes),
      probeTypes_(probeTypes),
      spillDir_(spillDir),
      numSubPartitions_(std::max(1U, numSubPartitions)),
      buildFiles_(numSubPartitions_)
{}

void JoinSpillState::EnsureSessionDirLocked()
{
    if (!sessionDir_.empty()) {
        return;
    }
    MkdirIfNeeded(spillDir_);
    const auto sessionId = gJoinSpillSessionId.fetch_add(1, std::memory_order_relaxed);
    sessionDir_ = spillDir_ + "/join-spill-" + std::to_string(sessionId);
    MkdirIfNeeded(sessionDir_);
    if (UNLIKELY(IsDebugEnable())) {
        std::cout << "[OmniRuntime][JoinSpill] session_dir=" << sessionDir_ << "\n";
        std::cout.flush();
    }
}

/// run: round n of spill,each round usually is one batch that its rowCount is  greater than joinMaxSpillRunRows_
/// one Join Task has one join_spill_state with multi Runs (multi batches input)
uint64_t JoinSpillState::NextRunId()
{
    std::lock_guard<std::mutex> lock(mutex_);
    return nextRunId_++;
}

std::string JoinSpillState::MakeSubPartitionDirLocked(const char *side, uint64_t runId, uint32_t subPartition)
{
    EnsureSessionDirLocked();
    const std::string sideDir = sessionDir_ + "/" + side;
    MkdirIfNeeded(sideDir);
    const std::string runDir = sideDir + "/run" + std::to_string(runId);
    MkdirIfNeeded(runDir);
    const std::string subPartitionDir = runDir + "/sp" + std::to_string(subPartition);
    MkdirIfNeeded(subPartitionDir);
    return subPartitionDir;
}

std::string JoinSpillState::ProbeSubPartitionDir(uint64_t runId, uint32_t subPartition)
{
    std::lock_guard<std::mutex> lock(mutex_);
    return MakeSubPartitionDirLocked("probe", runId, subPartition);
}

/// Spill one sub-partition's build batch under run{runId}/sp{subPartition}.
void JoinSpillState::SpillBuildSubPartition(uint64_t runId, uint32_t subPartition, vec::VectorBatch *batch)
{
    if (batch == nullptr || batch->GetRowCount() <= 0) {
        return;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    const std::string path = MakeSubPartitionDirLocked("build", runId, subPartition);
    SpillWriter writer(buildTypes_, path);
    auto status = writer.WriteVecBatch(batch, EstimateSpillVectorBatchSize(batch));
    if (status != ErrorCode::SUCCESS) {
        throw omniruntime::exception::OmniException(
            "JOIN_SPILL_WRITE_FAILED", "Failed to spill build side input.");
    }
    status = writer.Close();
    if (status != ErrorCode::SUCCESS) {
        throw omniruntime::exception::OmniException(
            "JOIN_SPILL_WRITE_FAILED", "Failed to close build side spill writer.");
    }
    auto file = writer.GetSpillFileInfo();
    buildFiles_[subPartition].emplace_back(file);
    buildSpilledRows_ += static_cast<uint64_t>(batch->GetRowCount());
    if (UNLIKELY(IsDebugEnable())) {
        std::cout << "[OmniRuntime][JoinSpill][build] write runId=" << runId << " subPartition=" << subPartition
                  << " rows=" << batch->GetRowCount() << " path=" << file.filePath << " fileLength="
                  << file.fileLength << "\n";
        std::cout.flush();
    }
}

void JoinSpillState::ReplayBuildInput(HashTableVariants *hashTables)
{
    if (hashTables == nullptr) {
        return;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    std::visit(
        [&](auto &&arg) {
            for (uint32_t subPartition = 0; subPartition < arg.GetHashTableCount(); ++subPartition) {
                for (const auto &file : buildFiles_[subPartition]) {
                    SpillReader reader(buildTypes_, file.filePath, file.fileLength, file.totalRowCount, false);
                    uint64_t rows = 0;
                    bool isEnd = false;
                    while (!isEnd) {
                        std::unique_ptr<vec::VectorBatch> batch = nullptr;
                        auto status = reader.ReadVecBatch(batch, isEnd);
                        if (status != ErrorCode::SUCCESS) {
                            throw omniruntime::exception::OmniException(
                                "JOIN_SPILL_READ_FAILED", "Failed to replay build side spill file.");
                        }
                        if (isEnd) {
                            break;
                        }
                        rows += static_cast<uint64_t>(batch->GetRowCount());
                        arg.AddVecBatch(static_cast<int32_t>(subPartition), batch.release());
                    }
                    if (UNLIKELY(IsDebugEnable())) {
                        std::cout << "[OmniRuntime][JoinSpill][build] read subPartition=" << subPartition
                                  << " rows=" << rows << " path=" << file.filePath << "\n";
                        std::cout.flush();
                    }
                }
                buildFiles_[subPartition].clear();
            }
        },
        *hashTables);
}

} // namespace omniruntime::op
