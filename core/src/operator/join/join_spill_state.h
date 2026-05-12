/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Description: first-stage subpartitioned join spill state.
 */
#ifndef OMNI_RUNTIME_JOIN_SPILL_STATE_H
#define OMNI_RUNTIME_JOIN_SPILL_STATE_H

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "join_hash_table_variants.h"
#include "join_sub_partitioner.h"
#include "operator/spill/spill_merger.h"
#include "type/data_types.h"
#include "vector/vector_batch.h"

namespace omniruntime::op {

class JoinSpillState {
public:
    JoinSpillState(const std::string &spillDir, const type::DataTypes &buildTypes,
        const type::DataTypes &probeTypes, uint32_t numSubPartitions);

    uint64_t NextRunId();

    void SpillBuildSubPartition(uint64_t runId, uint32_t subPartition, vec::VectorBatch *batch);

    void ReplayBuildInput(HashTableVariants *hashTables);

    std::string ProbeSubPartitionDir(uint64_t runId, uint32_t subPartition);

    uint64_t BuildSpilledRows() const
    {
        return buildSpilledRows_;
    }

private:
    void EnsureSessionDirLocked();
    std::string MakeSubPartitionDirLocked(const char *side, uint64_t runId, uint32_t subPartition);

    type::DataTypes buildTypes_;
    type::DataTypes probeTypes_;
    std::string spillDir_;
    std::string sessionDir_;
    uint32_t numSubPartitions_ = 1;

    mutable std::mutex mutex_;
    /// Per sub-partition index: spill files produced for the build side (replay order preserved per slot).
    std::vector<std::vector<SpillFileInfo>> buildFiles_;
    uint64_t nextRunId_ = 0;
    uint64_t buildSpilledRows_ = 0;
};

} // namespace omniruntime::op

#endif // OMNI_RUNTIME_JOIN_SPILL_STATE_H
