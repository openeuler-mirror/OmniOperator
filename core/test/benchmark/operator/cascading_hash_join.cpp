
#include <algorithm>
#include <cstdint>
#include <random>
#include "common/common.h"
#include "operator/join/lookup_join.h"
#include "operator/join/cascading_lookup_join.h"
#include "operator/join/join_hash_table_variants.h"
#include "two_step_join.h"
#include "common/vector_util.h"

using namespace benchmark;
using namespace omniruntime::op;
using namespace omniruntime::vec;
using namespace omniruntime::type;

namespace om_benchmark {

class CascadingJoinFixture : public BaseOperatorFixture {
protected:
    int32_t rowsPerPage = 10240;
    std::string prefix;

    // ─────────────────────────────────────────────────────────────────────────
    // Parameters
    // ─────────────────────────────────────────────────────────────────────────

    // Controlled partial-hit workload:
    //   Level 0 (A -> B): MatchRate controls the hit ratio; default 80%.
    //   Level 1 (matched key -> C): C owns the lower 50% of B's key domain,
    //   therefore roughly 50% of level-0 hits continue to a C hit.
    // With the defaults the expected cardinalities are approximately:
    //   level0 hit  = 102.4M * 0.80 = 81.92M
    //   level1 hit  = 81.92M * 0.50 = 40.96M
    //   final rows  = 40.96M
    OMNI_BENCHMARK_PARAM(int32_t, ProbeRows, 102400000);
    OMNI_BENCHMARK_PARAM(int32_t, BuildRows, 1024000);
    OMNI_BENCHMARK_PARAM(double, MatchRate, 0.80);
    OMNI_BENCHMARK_PARAM(bool, IsDictionaryBlocks, false);

    // Scenario hooks. The default values preserve the original benchmark exactly.
    // Case subclasses override only these two values; all join/data-generation
    // logic below remains unchanged.
    virtual double getScenarioMatchRate(const State& state) const
    {
        return MatchRate(state);
    }

    virtual int32_t getScenarioLevel1HitPercent(const State&) const
    {
        return 50;
    }

    int32_t getLevel1BuildRows(
        int32_t buildRows,
        int32_t level1HitPercent) const
    {
        const int32_t rows =
            static_cast<int32_t>((static_cast<int64_t>(buildRows) *
                                  level1HitPercent) / 100);
        // Preserve the original behavior: C always contains at least one row.
        return std::max(1, rows);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Table schemas: A (probe), B (build1), C (build2)
    // ─────────────────────────────────────────────────────────────────────────

    // Table A: [id, value_a]
    std::vector<DataTypePtr> probeTypesA = { LongType(), IntType() };

    // Table B: [id, value_b]
    std::vector<DataTypePtr> buildTypesB = { LongType(), IntType() };

    // Table C: [id, value_c]
    std::vector<DataTypePtr> buildTypesC = { LongType(), IntType() };

    // ─────────────────────────────────────────────────────────────────────────
    // Helper: Create build data
    // ─────────────────────────────────────────────────────────────────────────

    std::vector<VectorBatch*> createBuildData(
        const std::vector<DataTypePtr>& buildTypes,
        int32_t rows,
        int32_t maxValue,
        bool useDictionary)
    {
        std::vector<VectorBatch*> batches;
        std::vector<std::vector<int32_t>> columnValues(buildTypes.size());

        std::map<int32_t, int32_t> initialValueOffsets = {
            { OMNI_LONG, 30 }, { OMNI_INT, 40 }
        };

        int remainingRows = rows;

        while (remainingRows > 0) {
            int newRows = std::min(remainingRows, rowsPerPage);

            for (size_t i = 0; i < buildTypes.size(); i++) {
                DataTypeId type = buildTypes[i]->GetId();
                std::vector<int32_t> values(newRows);

                for (int j = 0; j < newRows; j++) {
                    values[j] = (rows - remainingRows + initialValueOffsets[type] + j) % maxValue;
                }
                columnValues[i] = values;
            }

            if (useDictionary) {
                batches.push_back(CreateVectorBatch(OMNI_DICTIONARY, buildTypes, prefix, columnValues, newRows));
            } else {
                batches.push_back(CreateVectorBatch(OMNI_FLAT, buildTypes, prefix, columnValues, newRows));
            }

            remainingRows -= newRows;

            for (auto& cols : columnValues) {
                cols.clear();
            }
        }

        return batches;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Helper: Create probe data (table A)
    // ─────────────────────────────────────────────────────────────────────────

    std::vector<VectorBatch*> createProbeData(
        int32_t rows,
        int32_t maxValue,
        double matchRate,
        bool useDict)
    {
        std::vector<VectorBatch*> batches;
        std::vector<std::vector<int32_t>> columnValues(probeTypesA.size());

        // MatchRate controls only level-0 A -> B hit probability.
        // Use the key value rather than logicalRow for the decision so every
        // full pass over the B key domain has the same deterministic hit set.
        const double effectiveMatchRate =
            std::max(0.0, std::min(1.0, matchRate));
        constexpr int32_t kRatioScale = 100;
        const int32_t matchThreshold = static_cast<int32_t>(
            effectiveMatchRate * static_cast<double>(kRatioScale));

        int32_t remainingRows = rows;
        int64_t logicalRow = 0;

        while (remainingRows > 0) {
            const int32_t newRows = std::min(remainingRows, rowsPerPage);
            for (auto& values : columnValues) {
                values.resize(static_cast<size_t>(newRows));
            }

            for (int32_t i = 0; i < newRows; ++i, ++logicalRow) {
                // Build B/C use (logical + 30) % maxValue for their LONG key,
                // so use exactly the same key domain here.
                const int32_t matchingId =
                    static_cast<int32_t>((logicalRow + 30) % maxValue);

                const bool shouldMatch =
                    ((matchingId % kRatioScale) < matchThreshold);

                // Build keys are non-negative [0, maxValue). Negative values
                // are therefore guaranteed misses.
                columnValues[0][static_cast<size_t>(i)] =
                    shouldMatch ? matchingId : -(matchingId + 1);

                // A.value_a is not a join key. Keep it deterministic and local.
                columnValues[1][static_cast<size_t>(i)] =
                    static_cast<int32_t>((logicalRow + 40) % maxValue);
            }

            if (useDict) {
                batches.push_back(CreateVectorBatch(
                    OMNI_DICTIONARY,
                    probeTypesA,
                    prefix,
                    columnValues,
                    newRows));
            } else {
                batches.push_back(CreateVectorBatch(
                    OMNI_FLAT,
                    probeTypesA,
                    prefix,
                    columnValues,
                    newRows));
            }

            remainingRows -= newRows;
        }

        return batches;
    }
};

//
// Benchmark 1: Cascading JOIN (one-pass)
// A JOIN B JOIN C without intermediate results
//

class CascadingLookupJoin : public CascadingJoinFixture {
protected:
    HashBuilderOperatorFactory* hashBuilderFactoryB = nullptr;
    HashBuilderOperatorFactory* hashBuilderFactoryC = nullptr;
    omniruntime::op::Operator* hashBuilderOpB = nullptr;
    omniruntime::op::Operator* hashBuilderOpC = nullptr;

    omniruntime::op::OperatorFactory* createOperatorFactory(const State& state) override
    {
        int32_t probeRows = ProbeRows(state);
        int32_t buildRows = BuildRows(state);
        double matchRate = getScenarioMatchRate(state);
        bool useDict = IsDictionaryBlocks(state);

        int32_t maxKey = buildRows;

        // ─────────────────────────────────────────────────────────────────────
        // Build HashTable for B
        // ─────────────────────────────────────────────────────────────────────
        auto buildDataB = createBuildData(buildTypesB, buildRows, maxKey, useDict);

        std::vector<int32_t> buildHashColsB = {0};
        hashBuilderFactoryB = new HashBuilderOperatorFactory(
            JoinType::OMNI_JOIN_TYPE_INNER,
            DataTypes(buildTypesB),
            buildHashColsB.data(),
            static_cast<int32_t>(buildHashColsB.size()),
            1);

        hashBuilderOpB = hashBuilderFactoryB->CreateOperator();
        for (auto* batch : buildDataB) {
            hashBuilderOpB->AddInput(batch);
        }
        VectorBatch* outputB = nullptr;
        hashBuilderOpB->GetOutput(&outputB);
        if (outputB) VectorHelper::FreeVecBatch(outputB);

        // ─────────────────────────────────────────────────────────────────────
        // Build HashTable for C
        // ─────────────────────────────────────────────────────────────────────
        const int32_t buildRowsC = getLevel1BuildRows(
            buildRows, getScenarioLevel1HitPercent(state));
        // C contains the configured lower percentage of B's key domain.
        // The default remains 50%, exactly matching the original benchmark.
        // A level-0 hit reaches level 1 and hits C only when its key is in
        // [0, buildRowsC).
        auto buildDataC = createBuildData(
            buildTypesC, buildRowsC, buildRowsC, useDict);

        std::vector<int32_t> buildHashColsC = {0};
        hashBuilderFactoryC = new HashBuilderOperatorFactory(
            JoinType::OMNI_JOIN_TYPE_INNER,
            DataTypes(buildTypesC),
            buildHashColsC.data(),
            static_cast<int32_t>(buildHashColsC.size()),
            1);

        hashBuilderOpC = hashBuilderFactoryC->CreateOperator();
        for (auto* batch : buildDataC) {
            hashBuilderOpC->AddInput(batch);
        }
        VectorBatch* outputC = nullptr;
        hashBuilderOpC->GetOutput(&outputC);
        if (outputC) VectorHelper::FreeVecBatch(outputC);

        // ─────────────────────────────────────────────────────────────────────
        // Create CascadingLookupJoinOperatorFactory
        // A probes B; each matched B.id immediately probes C.
        // ─────────────────────────────────────────────────────────────────────
        std::vector<DataTypes> probeTypesList;
        probeTypesList.push_back(DataTypes(probeTypesA));  // Level 0: A 表类型
        probeTypesList.push_back(DataTypes(buildTypesB));  // Level 1: B 表类型

        std::vector<std::vector<int32_t>> probeHashColsList;
        probeHashColsList.push_back({0});  // Level 0: 用 A.id 探测 B
        probeHashColsList.push_back({0});  // Level 1: 用 B.id 探测 C

        std::vector<std::vector<int32_t>> probeHashColTypesList;
        probeHashColTypesList.push_back({OMNI_LONG});  // Level 0: A.id 的类型
        probeHashColTypesList.push_back({OMNI_LONG});  // Level 1: B.id 的类型

        std::vector<std::vector<int32_t>> buildOutputColsList;
        buildOutputColsList.push_back({1});  // Output B.value_b
        buildOutputColsList.push_back({1});  // Output C.value_c

        std::vector<DataTypes> buildOutputTypesList;
        buildOutputTypesList.push_back(DataTypes({IntType()}));
        buildOutputTypesList.push_back(DataTypes({IntType()}));

        std::vector<HashTableVariants*> hashTablesList;
        hashTablesList.push_back(hashBuilderFactoryB->GetHashTablesVariants());
        hashTablesList.push_back(hashBuilderFactoryC->GetHashTablesVariants());

        int32_t outputRowSize = 8 + 4 + 4 + 4;  // A.id + A.value_a + B.value_b + C.value_c

        return new CascadingLookupJoinOperatorFactory(
            probeTypesList,
            probeHashColsList,
            probeHashColTypesList,
            buildOutputColsList,
            buildOutputTypesList,
            hashTablesList,
            outputRowSize);
    }

    std::vector<VectorBatchSupplier> createVecBatch(const State& state) override
    {
        int32_t probeRows = ProbeRows(state);
        int32_t buildRows = BuildRows(state);
        double matchRate = getScenarioMatchRate(state);
        bool useDict = IsDictionaryBlocks(state);

        int32_t maxKey = buildRows;
        auto batches = createProbeData(probeRows, maxKey, matchRate, useDict);

        return VectorBatchToVectorBatchSupplier(batches);
    }

    BaseFixtureGetOutputStrategy GetOutputStrategy() override
    {
        return AFTER_EACH_INPUT_FINISHED;
    }

    void TearDown(State& state) override
    {
        BaseOperatorFixture::TearDown(state);

        if (hashBuilderOpB) {
            omniruntime::op::Operator::DeleteOperator(hashBuilderOpB);
            hashBuilderOpB = nullptr;
        }
        if (hashBuilderOpC) {
            omniruntime::op::Operator::DeleteOperator(hashBuilderOpC);
            hashBuilderOpC = nullptr;
        }
        if (hashBuilderFactoryB) {
            delete hashBuilderFactoryB;
            hashBuilderFactoryB = nullptr;
        }
        if (hashBuilderFactoryC) {
            delete hashBuilderFactoryC;
            hashBuilderFactoryC = nullptr;
        }
    }
};

//
// Benchmark 2: Traditional two-step JOIN
// Step 1: A JOIN B → intermediate result (A⊕B)
// Step 2: (A⊕B) JOIN C → final result
//

class TraditionalTwoStepJoin : public CascadingJoinFixture {
protected:
    HashBuilderOperatorFactory* hashBuilderFactoryB = nullptr;
    HashBuilderOperatorFactory* hashBuilderFactoryC = nullptr;
    omniruntime::op::Operator* hashBuilderOpB = nullptr;
    omniruntime::op::Operator* hashBuilderOpC = nullptr;

    LookupJoinOperatorFactory* lookupJoinFactoryAB = nullptr;
    LookupJoinOperatorFactory* lookupJoinFactoryABC = nullptr;
    TwoStepJoinOperatorFactory* twoStepJoinFactory = nullptr;

    omniruntime::op::OperatorFactory* createOperatorFactory(const State& state) override
    {
        int32_t buildRows = BuildRows(state);
        double matchRate = getScenarioMatchRate(state);
        bool useDict = IsDictionaryBlocks(state);

        int32_t maxKey = buildRows;

        // ─────────────────────────────────────────────────────────────────────
        // Build HashTable for B
        // ─────────────────────────────────────────────────────────────────────
        auto buildDataB = createBuildData(buildTypesB, buildRows, maxKey, useDict);

        std::vector<int32_t> buildHashColsB = {0};
        hashBuilderFactoryB = new HashBuilderOperatorFactory(
            JoinType::OMNI_JOIN_TYPE_INNER,
            DataTypes(buildTypesB),
            buildHashColsB.data(),
            static_cast<int32_t>(buildHashColsB.size()),
            1);

        hashBuilderOpB = hashBuilderFactoryB->CreateOperator();
        for (auto* batch : buildDataB) {
            hashBuilderOpB->AddInput(batch);
        }
        VectorBatch* outputB = nullptr;
        hashBuilderOpB->GetOutput(&outputB);
        if (outputB) VectorHelper::FreeVecBatch(outputB);

        // ─────────────────────────────────────────────────────────────────────
        // Build HashTable for C
        // ─────────────────────────────────────────────────────────────────────
        const int32_t buildRowsC = getLevel1BuildRows(
            buildRows, getScenarioLevel1HitPercent(state));
        // C contains the configured lower percentage of B's key domain.
        // The default remains 50%, exactly matching the original benchmark.
        // A level-0 hit reaches level 1 and hits C only when its key is in
        // [0, buildRowsC).
        auto buildDataC = createBuildData(
            buildTypesC, buildRowsC, buildRowsC, useDict);

        std::vector<int32_t> buildHashColsC = {0};
        hashBuilderFactoryC = new HashBuilderOperatorFactory(
            JoinType::OMNI_JOIN_TYPE_INNER,
            DataTypes(buildTypesC),
            buildHashColsC.data(),
            static_cast<int32_t>(buildHashColsC.size()),
            1);

        hashBuilderOpC = hashBuilderFactoryC->CreateOperator();
        for (auto* batch : buildDataC) {
            hashBuilderOpC->AddInput(batch);
        }
        VectorBatch* outputC = nullptr;
        hashBuilderOpC->GetOutput(&outputC);
        if (outputC) VectorHelper::FreeVecBatch(outputC);

        // ─────────────────────────────────────────────────────────────────────
        // Step 1: Create first LookupJoin for A JOIN B
        // Output: A.id, A.value_a, B.value_b (3 columns)
        // ─────────────────────────────────────────────────────────────────────
        std::vector<int32_t> probeOutputColsAB = {0, 1};  // Output A.id, A.value_a
        std::vector<int32_t> probeHashColsAB = {0};        // A.id as join key
        std::vector<int32_t> buildOutputColsB = {1};       // Output B.value_b

        DataTypes buildOutputTypesB({IntType()});

        lookupJoinFactoryAB = new LookupJoinOperatorFactory(
            DataTypes(probeTypesA),                        // Probe: A (id, value_a)
            probeOutputColsAB.data(),
            static_cast<int32_t>(probeOutputColsAB.size()),
            probeHashColsAB.data(),
            static_cast<int32_t>(probeHashColsAB.size()),
            buildOutputColsB.data(),
            static_cast<int32_t>(buildOutputColsB.size()),
            buildOutputTypesB,
            hashBuilderFactoryB->GetHashTablesVariants(),
            nullptr,
            false,
            new OverflowConfig());

        // ─────────────────────────────────────────────────────────────────────
        // Step 2: Create second LookupJoin for (A⊕B) JOIN C
        // Input: A⊕B (id, value_a, value_b)
        // Output: A.id, A.value_a, B.value_b, C.value_c (4 columns)
        // ─────────────────────────────────────────────────────────────────────
        std::vector<DataTypePtr> intermediateTypes = {LongType(), IntType(), IntType()};  // A.id, A.value_a, B.value_b

        std::vector<int32_t> probeOutputColsABC = {0, 1, 2};  // Output A.id, A.value_a, B.value_b
        std::vector<int32_t> probeHashColsABC = {0};          // A.id (which is same as B.id after join)
        std::vector<int32_t> buildOutputColsC = {1};          // Output C.value_c

        DataTypes buildOutputTypesC({IntType()});

        lookupJoinFactoryABC = new LookupJoinOperatorFactory(
            DataTypes(intermediateTypes),                   // Probe: A⊕B (id, value_a, value_b)
            probeOutputColsABC.data(),
            static_cast<int32_t>(probeOutputColsABC.size()),
            probeHashColsABC.data(),
            static_cast<int32_t>(probeHashColsABC.size()),
            buildOutputColsC.data(),
            static_cast<int32_t>(buildOutputColsC.size()),
            buildOutputTypesC,
            hashBuilderFactoryC->GetHashTablesVariants(),
            nullptr,
            false,
            new OverflowConfig());

        // ─────────────────────────────────────────────────────────────────────
        // Create two-step join operator factory
        // ─────────────────────────────────────────────────────────────────────
        twoStepJoinFactory =
            new TwoStepJoinOperatorFactory(
                lookupJoinFactoryAB,
                lookupJoinFactoryABC);
        return twoStepJoinFactory;
    }

    std::vector<VectorBatchSupplier> createVecBatch(const State& state) override
    {
        int32_t probeRows = ProbeRows(state);
        int32_t buildRows = BuildRows(state);
        double matchRate = getScenarioMatchRate(state);
        bool useDict = IsDictionaryBlocks(state);

        int32_t maxKey = buildRows;
        auto batches = createProbeData(probeRows, maxKey, matchRate, useDict);

        return VectorBatchToVectorBatchSupplier(batches);
    }

    BaseFixtureGetOutputStrategy GetOutputStrategy() override
    {
        return AFTER_EACH_INPUT_FINISHED;
    }

    void TearDown(State& state) override
    {
        // Print before BaseOperatorFixture::TearDown(). The base fixture may
        // destroy the runtime operator/factory.
        if (twoStepJoinFactory) {
        }

        BaseOperatorFixture::TearDown(state);
        twoStepJoinFactory = nullptr;

        if (hashBuilderOpB) {
            omniruntime::op::Operator::DeleteOperator(hashBuilderOpB);
            hashBuilderOpB = nullptr;
        }
        if (hashBuilderOpC) {
            omniruntime::op::Operator::DeleteOperator(hashBuilderOpC);
            hashBuilderOpC = nullptr;
        }
        if (hashBuilderFactoryB) {
            delete hashBuilderFactoryB;
            hashBuilderFactoryB = nullptr;
        }
        if (hashBuilderFactoryC) {
            delete hashBuilderFactoryC;
            hashBuilderFactoryC = nullptr;
        }
        if (lookupJoinFactoryAB) {
            delete lookupJoinFactoryAB;
            lookupJoinFactoryAB = nullptr;
        }
        if (lookupJoinFactoryABC) {
            delete lookupJoinFactoryABC;
            lookupJoinFactoryABC = nullptr;
        }
    }
};

//
// Additional benchmark scenarios.
//
// IMPORTANT: these classes override configuration values only. They do NOT
// override createOperatorFactory(), createVecBatch(), GetOutputStrategy(), or
// any join/data-generation implementation.
//
// Case 4 (80% -> 50%) is intentionally identical to the original default
// workload and can be used as a regression baseline.
//
// Case 7 requests 0% at level 1. The original benchmark intentionally clamps
// C to at least one build row via std::max(1, rows), so this is a near-zero
// workload rather than mathematically exact zero. This preserves the original
// data-generation semantics instead of introducing a special-case path.
//

#define DEFINE_JOIN_SCENARIO_CASE(ID, LEVEL0_RATE, LEVEL1_PERCENT)                  \
class CascadingLookupJoinCase##ID : public CascadingLookupJoin {                   \
protected:                                                                          \
    double getScenarioMatchRate(const State&) const override                       \
    {                                                                               \
        return LEVEL0_RATE;                                                         \
    }                                                                               \
    int32_t getScenarioLevel1HitPercent(const State&) const override                \
    {                                                                               \
        return LEVEL1_PERCENT;                                                      \
    }                                                                               \
};                                                                                  \
class TraditionalTwoStepJoinCase##ID : public TraditionalTwoStepJoin {             \
protected:                                                                          \
    double getScenarioMatchRate(const State&) const override                       \
    {                                                                               \
        return LEVEL0_RATE;                                                         \
    }                                                                               \
    int32_t getScenarioLevel1HitPercent(const State&) const override                \
    {                                                                               \
        return LEVEL1_PERCENT;                                                      \
    }                                                                               \
};

// Case 1:   0% ->  50%
DEFINE_JOIN_SCENARIO_CASE(1, 0.00,  50)
// Case 2:  10% ->  50%
DEFINE_JOIN_SCENARIO_CASE(2, 0.10,  50)
// Case 3:  50% ->  50%
DEFINE_JOIN_SCENARIO_CASE(3, 0.50,  50)
// Case 4:  80% ->  50%  (same as original default)
DEFINE_JOIN_SCENARIO_CASE(4, 0.80,  50)
// Case 5: 100% ->  50%
DEFINE_JOIN_SCENARIO_CASE(5, 1.00,  50)
// Case 6:  80% ->  10%
DEFINE_JOIN_SCENARIO_CASE(6, 0.80,  10)
// Case 7: 100% -> ~0%   (original min-one-row C semantics preserved)
DEFINE_JOIN_SCENARIO_CASE(7, 1.00,   0)
// Case 8: 100% -> 100%
DEFINE_JOIN_SCENARIO_CASE(8, 1.00, 100)

#undef DEFINE_JOIN_SCENARIO_CASE

//
// Register benchmarks
//

// Keep the two original entries unchanged.
OMNI_BENCHMARK_DECLARE_OPERATOR_DEFAULT(CascadingLookupJoin);
OMNI_BENCHMARK_DECLARE_OPERATOR_DEFAULT(TraditionalTwoStepJoin);

// Add 8 paired scenarios. Every pair uses exactly the same original execution
// logic and differs only in the two configured hit ratios above.
OMNI_BENCHMARK_DECLARE_OPERATOR_DEFAULT(CascadingLookupJoinCase1);
OMNI_BENCHMARK_DECLARE_OPERATOR_DEFAULT(TraditionalTwoStepJoinCase1);
OMNI_BENCHMARK_DECLARE_OPERATOR_DEFAULT(CascadingLookupJoinCase2);
OMNI_BENCHMARK_DECLARE_OPERATOR_DEFAULT(TraditionalTwoStepJoinCase2);
OMNI_BENCHMARK_DECLARE_OPERATOR_DEFAULT(CascadingLookupJoinCase3);
OMNI_BENCHMARK_DECLARE_OPERATOR_DEFAULT(TraditionalTwoStepJoinCase3);
OMNI_BENCHMARK_DECLARE_OPERATOR_DEFAULT(CascadingLookupJoinCase4);
OMNI_BENCHMARK_DECLARE_OPERATOR_DEFAULT(TraditionalTwoStepJoinCase4);
OMNI_BENCHMARK_DECLARE_OPERATOR_DEFAULT(CascadingLookupJoinCase5);
OMNI_BENCHMARK_DECLARE_OPERATOR_DEFAULT(TraditionalTwoStepJoinCase5);
OMNI_BENCHMARK_DECLARE_OPERATOR_DEFAULT(CascadingLookupJoinCase6);
OMNI_BENCHMARK_DECLARE_OPERATOR_DEFAULT(TraditionalTwoStepJoinCase6);
OMNI_BENCHMARK_DECLARE_OPERATOR_DEFAULT(CascadingLookupJoinCase7);
OMNI_BENCHMARK_DECLARE_OPERATOR_DEFAULT(TraditionalTwoStepJoinCase7);
OMNI_BENCHMARK_DECLARE_OPERATOR_DEFAULT(CascadingLookupJoinCase8);
OMNI_BENCHMARK_DECLARE_OPERATOR_DEFAULT(TraditionalTwoStepJoinCase8);


} // namespace om_benchmark