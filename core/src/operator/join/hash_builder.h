/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2024. All rights reserved.
 * @Description: hash builder implementations
 */
#ifndef __HASH_BUILDER_H__
#define __HASH_BUILDER_H__

#include <memory>

#include "plannode/planNode.h"
#include "operator/operator_factory.h"
#include "operator/operator.h"
#include "join_hash_table_variants.h"
#include "common_join.h"
#include "join_sub_partitioner.h"

namespace omniruntime {
namespace op {
class JoinSpillState;

class HashBuilderOperatorFactory : public OperatorFactory {
public:
    HashBuilderOperatorFactory(JoinType joinType, const DataTypes &buildTypes, const int32_t *buildHashCols,
        int32_t buildHashColsCount, int32_t operatorCount);
    HashBuilderOperatorFactory(JoinType joinType, BuildSide buildSide, const DataTypes &buildTypes,
        const int32_t *buildHashCols, int32_t buildHashColsCount, int32_t operatorCount);
    ~HashBuilderOperatorFactory()
    {
        delete hashTablesVariants;
    }
    static HashBuilderOperatorFactory *CreateHashBuilderOperatorFactory(JoinType joinType, const DataTypes &buildTypes,
        const int32_t *buildHashCols, int32_t buildHashColsCount, int32_t operatorCount);
    static HashBuilderOperatorFactory *CreateHashBuilderOperatorFactory(JoinType joinType, BuildSide buildSide,
        const DataTypes &buildTypes, const int32_t *buildHashCols, int32_t buildHashColsCount, int32_t operatorCount);
    /// when Join run without spill, we only need one HashBuilderOperator
    /// But when Join run with spill, we need muti HashBuilderOperator that every join sub partition o
    static HashBuilderOperatorFactory *CreateHashBuilderOperatorFactory(
        std::shared_ptr<const HashJoinNode> planNode, int32_t operatorCount = 1);
    omniruntime::op::Operator *CreateOperator() override;
    void SetJoinSpillSubPartitionPolicy(bool joinSpillEnabled, uint64_t maxSpillRunRows,
        JoinSubPartitionConfig joinSubPartCfg);
    void SetJoinSpillState(std::shared_ptr<JoinSpillState> joinSpillState);

    std::shared_ptr<JoinSpillState> GetJoinSpillState() const
    {
        return joinSpillState_;
    }

    HashTableVariants *GetHashTablesVariants()
    {
        return hashTablesVariants;
    }

private:
    DataTypes buildTypes;
    std::vector<int32_t> buildHashCols;
    HashTableVariants *hashTablesVariants;
    std::atomic<int32_t> operatorIndex;
    bool joinSpillEnabled_ = false;
    uint64_t joinMaxSpillRunRows_ = 0;
    JoinSubPartitionConfig joinSubPartCfg_;
    std::shared_ptr<JoinSpillState> joinSpillState_;

    template <class RowRefListType>
    HashTableVariants *InitVariant(int32_t buildHashColsCount, int32_t operatorCount, JoinType joinType,
        BuildSide buildSide = OMNI_BUILD_UNKNOWN, bool isMultiCols = false);
};

class HashBuilderOperator : public Operator {
public:
    HashBuilderOperator(const DataTypes &buildTypes, HashTableVariants *hashTables, int32_t partitionIndex,
        bool joinSpillEnabled, uint64_t joinMaxSpillRunRows, JoinSubPartitionConfig joinSubPartCfg,
        std::vector<int32_t> buildHashCols, JoinSpillState *joinSpillState);

    ~HashBuilderOperator() = default;

    int32_t AddInput(omniruntime::vec::VectorBatch *vecBatch) override;

    int32_t GetOutput(omniruntime::vec::VectorBatch **outputVecBatch) override;

    OmniStatus Close() override;

    // for test
    DataTypes &GetBuildTypes()
    {
        return buildTypes;
    }

    HashTableImplementationType GetHashTableType()
    {
        return std::visit(
            [&](auto &&arg) -> HashTableImplementationType { return arg.GetHashTableTypes(partitionIndex); },
            *hashTablesVariants);
    }

    uint32_t GetHashTableSize()
    {
        return std::visit([&](auto &&arg) -> uint32_t { return arg.GetHashTableSize(); }, *hashTablesVariants);
    }

    uint32_t GetHashTableCount()
    {
        return std::visit([&](auto &&arg) -> uint32_t { return arg.GetHashTableCount(); }, *hashTablesVariants);
    }

private:
    /// True when join spill sub-partition layout applies (computed once in ctor). Spill/replay still require
    /// \c joinSpillState_ and (for spill) row count >= \c joinMaxSpillRunRows_ at the call site.
    bool UseJoinSubPartitioning() const;
    bool AddSubPartitionedInput(omniruntime::vec::VectorBatch *vecBatch);

    DataTypes buildTypes;
    int32_t partitionIndex;
    HashTableVariants *hashTablesVariants;
    bool joinSpillEnabled_ = false;
    uint64_t joinMaxSpillRunRows_ = 0;
    uint64_t cumulativeBuildRows_ = 0;
    bool joinSpillBoundaryLogged_ = false;
    bool joinSubPartitionLogged_ = false;
    JoinSubPartitionConfig joinSubPartCfg_;
    /// Cached at construction; matches the former \c UseJoinSubPartitioning() predicate (no spill-state / batch).
    const bool useJoinSubPartitioning_;
    std::vector<int32_t> buildHashCols_;
    JoinSpillState *joinSpillState_ = nullptr;
};

int32_t GetTypeLength(int buildHashColsCount, DataTypes& buildTypes, std::vector<int32_t>& buildHashCols);
} // end of op
} // end of omniruntime
#endif
