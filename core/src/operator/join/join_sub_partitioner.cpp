/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 */
#include "join_sub_partitioner.h"

#include <string>

#include "operator/hash_util.h"
#include "type/data_type.h"
#include "util/omni_exception.h"
#include "vector/large_string_container.h"
#include "vector/vector.h"

namespace omniruntime::op {

/// get all config of spill partitions
JoinSubPartitionConfig JoinSubPartitionConfig::FromQueryConfig(
    const omniruntime::config::QueryConfig &queryConfig)
{
    JoinSubPartitionConfig cfg;
    cfg.numSubPartitions = 1U << queryConfig.spillNumPartitionBits();
    cfg.startPartitionBit = queryConfig.spillStartPartitionBit();
    cfg.activeSubPartition = 0;
    return cfg;
}

/// get patitionId by hash, from 0 to numSubPartitions
uint32_t JoinSubPartitioner::SubPartitionFromHash(int64_t hash) const
{
    if (config_.numSubPartitions <= 1U) {
        return 0;
    }
    const uint32_t mask = config_.numSubPartitions - 1U;
    return static_cast<uint32_t>((static_cast<uint64_t>(hash) >> config_.startPartitionBit) & mask);
}

namespace {

int64_t HashLongColumn(omniruntime::vec::BaseVector *col, int32_t row)
{
    auto *v = dynamic_cast<omniruntime::vec::Vector<int64_t> *>(col);
    if (v == nullptr) {
        throw omniruntime::exception::OmniException(
            "UNSUPPORTED_ERROR", "JoinSubPartitioner: LONG key column has unexpected vector type.");
    }
    return HashUtil::HashValue(v->GetValue(row));
}

int64_t HashVarcharColumn(omniruntime::vec::BaseVector *col, int32_t row)
{
    using LC = omniruntime::vec::LargeStringContainer<std::string_view>;
    auto *v = dynamic_cast<omniruntime::vec::Vector<LC> *>(col);
    if (v == nullptr) {
        throw omniruntime::exception::OmniException(
            "UNSUPPORTED_ERROR", "JoinSubPartitioner: VARCHAR key column has unexpected vector type.");
    }
    auto value = v->GetValue(row);
    auto *data = reinterpret_cast<int8_t *>(const_cast<char *>(value.data()));
    return HashUtil::HashValue(data, static_cast<int32_t>(value.size()));
}

int64_t HashKeyColumn(omniruntime::vec::BaseVector *col, int32_t row, int32_t typeId)
{
    switch (typeId) {
        case omniruntime::type::OMNI_LONG:
            return HashLongColumn(col, row);
        case omniruntime::type::OMNI_VARCHAR:
            return HashVarcharColumn(col, row);
        default:
            throw omniruntime::exception::OmniException("UNSUPPORTED_ERROR",
                "JoinSubPartitioner: unsupported join key type id " + std::to_string(typeId)
                    + " (only LONG and VARCHAR are supported in this iteration).");
    }
}

} // namespace

/// input VectorBatch, output vector<partitionId: uint32_t>, mark the partitionId of every row in current batch.
/// activeSubPartition is fixed to partition 0
std::vector<uint32_t> JoinSubPartitioner::PartitionRowsByKeyColumns(omniruntime::vec::VectorBatch *batch,
    const omniruntime::type::DataTypes &rowTypes, const std::vector<int32_t> &keyColIndices) const
{
    if (batch == nullptr) {
        return {};
    }
    const int32_t rows = batch->GetRowCount();
    std::vector<uint32_t> partitions(static_cast<size_t>(rows), config_.activeSubPartition);
    if (rows <= 0) {
        return partitions;
    }
    for (int32_t row = 0; row < rows; ++row) {
        int64_t combinedHash = 0;
        bool firstKey = true;
        bool anyNull = false;
        for (auto keyCol : keyColIndices) {
            auto *col = batch->Get(keyCol);
            if (col->IsNull(row)) {
                anyNull = true;
                break;
            }
            const int64_t keyHash = HashKeyColumn(col, row, rowTypes.GetIds()[keyCol]);
            combinedHash = firstKey ? keyHash : HashUtil::CombineHash(combinedHash, keyHash);
            firstKey = false;
        }

        // if row is null, put it in the active subpartition 0 for avoid to spill it.
        partitions[static_cast<size_t>(row)] = anyNull ? config_.activeSubPartition : SubPartitionFromHash(combinedHash);
    }
    return partitions;
}

} // namespace omniruntime::op
