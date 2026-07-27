
#ifndef __CASCADING_LOOKUP_JOIN_H__
#define __CASCADING_LOOKUP_JOIN_H__

#include <cstdint>
#include <memory>
#include <vector>

#include "plannode/planNode.h"
#include "operator/operator.h"
#include "operator/operator_factory.h"
#include "type/data_types.h"
#include "type/data_type.h"
#include "operator/join/hash_builder_expr.h"
#include "operator/join/hash_builder.h"
#include "operator/join/lookup_join.h"
#include "operator/join/common_join.h"

namespace omniruntime {
namespace op {

class HashBuilderWithExprOperatorFactory;

/**
 * Compact address of a row stored in one build-side VectorBatch.
 *
 * The old implementation encoded this information into uint64_t and then
 * copied a std::vector<uint64_t> into every output MatchChain.  For the
 * cascading join we keep the two components explicitly and store all result
 * addresses in one flat, pre-reserved vector.
 */
struct CascadingBuildAddress {
    uint32_t rowIdx;
    uint32_t vecBatchIdx;
};

/**
 * Output builder specialized for CascadingLookupJoin.
 *
 * Storage layout:
 *   probePositions[row]
 *   buildRowPositions[level][row]
 *   buildBatchIndices[level][row]
 *
 * This removes the per-output-row std::vector allocation and also avoids the
 * strided [row][level] address scan during final materialization.
 */
class CascadingJoinOutputBuilder {
public:
    CascadingJoinOutputBuilder(int32_t maxRowCount, int32_t cascadeLevels);
    ~CascadingJoinOutputBuilder() = default;

    void SetBuildOutputTypes(const std::vector<type::DataTypes>& types)
    {
        buildOutputTypesList = types;
    }

    void AppendMatchChain(
        int32_t probePosition,
        const std::vector<CascadingBuildAddress>& addresses);

    void BuildFinalOutput(
        omniruntime::vec::BaseVector** probeColumns,
        std::vector<std::vector<omniruntime::vec::BaseVector***>>& allBuildColumns,
        std::vector<int32_t>& probeOutputCols,
        std::vector<std::vector<int32_t>>& buildOutputColsList,
        omniruntime::vec::VectorBatch** outputVecBatch);

    bool IsFull() const
    {
        return static_cast<int32_t>(probePositions.size()) >= maxRowCount;
    }

    bool IsEmpty() const
    {
        return probePositions.empty();
    }

    void Clear()
    {
        // clear() keeps vector capacity, so following output batches reuse the
        // same allocations.
        probePositions.clear();
        for (int32_t level = 0; level < cascadeLevels; ++level) {
            buildRowPositions[static_cast<size_t>(level)].clear();
            buildBatchIndices[static_cast<size_t>(level)].clear();
            firstBatchIndices[static_cast<size_t>(level)] = 0;
            singleBatchFlags[static_cast<size_t>(level)] = true;
        }
    }

private:
    CascadingBuildAddress GetBuildAddress(int32_t row, int32_t level) const
    {
        const size_t levelIndex = static_cast<size_t>(level);
        const size_t rowIndex = static_cast<size_t>(row);
        return CascadingBuildAddress{
            static_cast<uint32_t>(buildRowPositions[levelIndex][rowIndex]),
            buildBatchIndices[levelIndex][rowIndex]};
    }

    void AppendBuildAddress(int32_t level, const CascadingBuildAddress& address);

    template <typename T>
    omniruntime::vec::BaseVector* ConstructBuildColumn(
        std::vector<omniruntime::vec::BaseVector***>& levelColumns,
        int32_t outputCol,
        int32_t level,
        int32_t numRows);

    omniruntime::vec::BaseVector* ConstructBuildVarcharColumn(
        std::vector<omniruntime::vec::BaseVector***>& levelColumns,
        int32_t outputCol,
        int32_t level,
        int32_t numRows);

    int32_t maxRowCount;
    int32_t cascadeLevels;

    // One contiguous vector for probe row indices.
    std::vector<int32_t> probePositions;

    // Structure-of-arrays layout. Build output is consumed one level at a
    // time, so keeping each level contiguous avoids strided reads through an
    // interleaved [row][level] address array.
    std::vector<std::vector<int32_t>> buildRowPositions;
    std::vector<std::vector<uint32_t>> buildBatchIndices;

    // Maintained incrementally while appending matches. BuildFinalOutput can
    // therefore take the single-build-VectorBatch fast path in O(1), instead
    // of rescanning every address merely to discover that all batch ids match.
    std::vector<uint32_t> firstBatchIndices;
    std::vector<bool> singleBatchFlags;

    std::vector<type::DataTypes> buildOutputTypesList;
};

class CascadingLookupJoinOperatorFactory : public OperatorFactory {
public:
    CascadingLookupJoinOperatorFactory(
        std::vector<type::DataTypes>& probeTypesList,
        std::vector<std::vector<int32_t>>& probeHashColsList,
        std::vector<std::vector<int32_t>>& probeHashColTypesList,
        std::vector<std::vector<int32_t>>& buildOutputColsList,
        std::vector<type::DataTypes>& buildOutputTypesList,
        std::vector<HashTableVariants*>& hashTablesList,
        int32_t outputRowSize);

    ~CascadingLookupJoinOperatorFactory() override = default;

    static CascadingLookupJoinOperatorFactory* CreateCascadingLookupJoinOperatorFactory(
        std::vector<std::shared_ptr<const HashJoinNode>>& joinNodes,
        std::vector<HashBuilderWithExprOperatorFactory*>& hashBuilderFactories,
        const config::QueryConfig& queryConfig);

    static CascadingLookupJoinOperatorFactory* CreateCascadingLookupJoinOperatorFactory(
        std::shared_ptr<const CascadingJoinNode> cascadingNode,
        std::vector<HashBuilderWithExprOperatorFactory*>& hashBuilderFactories,
        const config::QueryConfig& queryConfig);

    Operator* CreateOperator() override;

private:
    std::vector<type::DataTypes> probeTypesList;
    std::vector<std::vector<int32_t>> probeHashColsList;
    std::vector<std::vector<int32_t>> probeHashColTypesList;
    std::vector<std::vector<int32_t>> buildOutputColsList;
    std::vector<type::DataTypes> buildOutputTypesList;
    std::vector<HashTableVariants*> hashTablesList;
    int32_t outputRowSize;
    int32_t cascadeLevels;
};

/**
 * One match retained between two cascading levels.
 *
 * This is deliberately metadata-only: no A+B, A+B+C ... intermediate
 * VectorBatch is materialized. rootProbePosition is carried through every
 * level because the current CascadingJoinNode contract uses root-A probe keys
 * at every level; address stores the matched build row used for lineage/final
 * materialization.
 */
struct CascadingFrontierEntry {
    int32_t rootProbePosition;
    CascadingBuildAddress address;
    uint32_t parentIndex;
};

class CascadingLookupJoinOperator : public Operator {
public:
    CascadingLookupJoinOperator(
        std::vector<type::DataTypes>& probeTypesList,
        std::vector<std::vector<int32_t>>& probeHashColsList,
        std::vector<std::vector<int32_t>>& probeHashColTypesList,
        std::vector<std::vector<int32_t>>& buildOutputColsList,
        std::vector<type::DataTypes>& buildOutputTypesList,
        std::vector<HashTableVariants*>& hashTablesList,
        int32_t outputRowSize);

    ~CascadingLookupJoinOperator() override;

    int32_t AddInput(omniruntime::vec::VectorBatch* vecBatch) override;
    int32_t GetOutput(omniruntime::vec::VectorBatch** outputVecBatch) override;
    OmniStatus Close() override;
    BlockingReason IsBlocked(ContinueFuture* future) override;

private:
    static constexpr uint32_t INVALID_PARENT = static_cast<uint32_t>(-1);

    void InitFirst();
    void PrepareCurrentProbe();
    void PopulateLevel0ProbeNulls();
    void PrepareLevel0Serializers();
    void PrepareProbeCapabilities();

    VectorSerializerIgnoreNull SelectSerializer(
        omniruntime::vec::BaseVector* vector) const;

    // Bind one level's hash-key columns to the current root A VectorBatch.
    // sourceVecBatchIdx is retained only for compatibility with the existing
    // frontier grouping loop; root-probe execution does not use it as a source.
    bool PrepareLevelProbeGroup(
        int32_t level,
        uint32_t sourceVecBatchIdx,
        std::vector<omniruntime::vec::BaseVector*>& probeColumns,
        std::vector<VectorSerializerIgnoreNull>& serializers);

    void BuildAllFrontiers();
    void EmitFinalFrontier();
    void ResetFrontierState();
    void ProcessProbe();

    // N-level direct-final path: levels [0, N-2] materialize compact
    // frontiers, while the last level probes directly into outputBuilder.
    void ProbeFinalLevelDirect();
    bool AppendDirectFinalMatch(
        int32_t finalLevel,
        uint32_t parentIndex,
        const CascadingBuildAddress& finalAddress);

    template <typename TableType>
    void ProbeFinalLevelDirectWithTable(
        int32_t finalLevel,
        TableType& table,
        const std::vector<CascadingFrontierEntry>& inputFrontier);
    void PrintTimingStats();

    template <bool HAS_NULLS, typename TableType>
    void ProbeLevel0WithTable(TableType& table);

    template <typename TableType>
    void ProbeFrontierLevelWithTable(
        int32_t level,
        TableType& table,
        const std::vector<CascadingFrontierEntry>& inputFrontier,
        std::vector<CascadingFrontierEntry>& outputFrontier);

    void BuildGroupedParentIndices(
        const std::vector<CascadingFrontierEntry>& inputFrontier);

    std::vector<HashTableVariants*> hashTablesList;
    std::vector<std::vector<int32_t>> probeHashColsList;
    std::vector<std::vector<int32_t>> probeHashColTypesList;
    std::vector<std::vector<int32_t>> buildOutputColsList;
    std::vector<type::DataTypes> probeTypesList;
    std::vector<type::DataTypes> buildOutputTypesList;
    std::vector<int32_t> probeOutputCols;

    std::unique_ptr<CascadingJoinOutputBuilder> outputBuilder;
    omniruntime::vec::VectorBatch* curInputBatch = nullptr;
    int32_t curProbePosition = 0;
    int32_t cascadeLevels;
    int32_t outputRowSize;

    omniruntime::vec::BaseVector** probeHashColumns = nullptr;

    // Level-0 input metadata prepared once per A VectorBatch.
    std::vector<int8_t> curProbeNulls;
    bool level0HasAnyNull = false;
    bool level0ProbeSimdCapable = false;

    // One serializer vector and one BaseVector* buffer per level. Under the
    // current root-probe contract every level is bound once to the current A
    // VectorBatch; frontier levels use rootProbePosition to select rows.
    std::vector<std::vector<VectorSerializerIgnoreNull>> levelProbeSerializers;
    std::vector<std::vector<omniruntime::vec::BaseVector*>> levelProbeColumnBuffers;

    std::vector<std::unique_ptr<ExecutionContext>> levelExecutionContexts;

    std::vector<std::vector<omniruntime::vec::BaseVector***>> levelBuildColumns;
    std::vector<uint32_t> levelPartitionMasks;
    std::vector<bool> levelIsSingleHT;

    // Level-at-a-time state:
    //   frontiers[0] = all A->B matches for the current A VectorBatch
    //   frontiers[1] = all (A->B)->C matches
    //   ...
    // For cascadeLevels >= 2 only [0, cascadeLevels-2] are materialized.
    // The final level is direct-emitted with resumable state below.
    //
    // Each entry contains only row addresses + lineage.  No full intermediate
    // VectorBatch is created.
    std::vector<std::vector<CascadingFrontierEntry>> frontiers;
    bool frontiersBuiltForCurrentInput = false;
    size_t finalFrontierEmitIndex = 0;

    // Resumable N-level final probe state.  For cascadeLevels >= 2 the last
    // level is never materialized into frontiers.back().  Instead we remember
    // the current parent entry and the ordinal within its hash-match chain so
    // GetOutput() can stop exactly when outputBuilder fills and resume later.
    size_t finalProbeParentIndex = 0;
    uint32_t finalProbeMatchOffset = 0;
    bool finalProbeComplete = false;

    // Reused when reconstructing one final lineage into the output builder.
    // It is sized once to cascadeLevels; there is no recursive push/pop.
    std::vector<CascadingBuildAddress> reusableMatchChain;

    // O(n) grouping scratch.  BuildGroupedParentIndices groups a frontier by
    // source vecBatchIdx so all rows from the same physical VectorBatch are
    // probed together at the next level.
    std::vector<uint32_t> groupedParentIndices;
    std::vector<uint32_t> batchCounts;
    std::vector<uint32_t> batchOffsets;
    std::vector<uint32_t> batchWriteOffsets;

    bool firstVecBatch = true;
};

} // end of op
} // end of omniruntime
#endif
