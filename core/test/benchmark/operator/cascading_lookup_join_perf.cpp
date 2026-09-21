#include <algorithm>
#include <cstdint>
#include <map>
#include <string>
#include <vector>

#include "common/common.h"
#include "common/vector_util.h"
#include "operator/join/cascading_lookup_join.h"
#include "operator/join/hash_builder.h"
#include "operator/join/join_hash_table_variants.h"

using namespace benchmark;
using namespace omniruntime::op;
using namespace omniruntime::vec;
using namespace omniruntime::type;

namespace om_benchmark {

/**
 * Dedicated CascadingLookupJoin performance benchmark.
 *
 * Workload:
 *   A JOIN B JOIN C
 *
 * Default:
 *   ProbeRows          = 102,400,000
 *   BuildRows(B)       = 1,024,000
 *   Level0 A->B hit    = 80%
 *   Level1 B->C hit    = 50%
 *   IsDictionaryBlocks = false
 *
 * Expected cardinalities:
 *   A->B matches       ~= 81,920,000
 *   B->C matches       ~= 40,960,000
 *   final rows         ~= 40,960,000
 *
 * Only CascadingLookupJoin is registered in this file.
 */
class CascadingLookupJoinPerf : public BaseOperatorFixture {
protected:
    static constexpr int32_t kRowsPerPage = 10240;
    static constexpr int32_t kLevel1HitPercent = 50;

    std::string prefix;

    OMNI_BENCHMARK_PARAM(int32_t, ProbeRows, 102400000);
    OMNI_BENCHMARK_PARAM(int32_t, BuildRows, 1024000);
    OMNI_BENCHMARK_PARAM(double, MatchRate, 0.80);
    OMNI_BENCHMARK_PARAM(bool, IsDictionaryBlocks, false);

    // A: [id, value_a]
    std::vector<DataTypePtr> probeTypesA = {
        LongType(),
        IntType()
    };

    // B: [id, value_b]
    std::vector<DataTypePtr> buildTypesB = {
        LongType(),
        IntType()
    };

    // C: [id, value_c]
    std::vector<DataTypePtr> buildTypesC = {
        LongType(),
        IntType()
    };

    HashBuilderOperatorFactory* hashBuilderFactoryB = nullptr;
    HashBuilderOperatorFactory* hashBuilderFactoryC = nullptr;

    omniruntime::op::Operator* hashBuilderOpB = nullptr;
    omniruntime::op::Operator* hashBuilderOpC = nullptr;

    int32_t getLevel1BuildRows(int32_t buildRows) const
    {
        const int32_t rows = static_cast<int32_t>(
            (static_cast<int64_t>(buildRows) *
             kLevel1HitPercent) /
            100);

        return std::max(1, rows);
    }

    std::vector<VectorBatch*> createBuildData(
        const std::vector<DataTypePtr>& buildTypes,
        int32_t rows,
        int32_t maxValue,
        bool useDictionary)
    {
        std::vector<VectorBatch*> batches;
        std::vector<std::vector<int32_t>>
            columnValues(buildTypes.size());

        std::map<int32_t, int32_t> initialValueOffsets = {
            {OMNI_LONG, 30},
            {OMNI_INT, 40}
        };

        int32_t remainingRows = rows;

        while (remainingRows > 0) {
            const int32_t newRows =
                std::min(remainingRows, kRowsPerPage);

            for (size_t col = 0;
                 col < buildTypes.size();
                 ++col) {
                const DataTypeId type =
                    buildTypes[col]->GetId();

                std::vector<int32_t> values(
                    static_cast<size_t>(newRows));

                for (int32_t row = 0;
                     row < newRows;
                     ++row) {
                    values[static_cast<size_t>(row)] =
                        (rows -
                         remainingRows +
                         initialValueOffsets[type] +
                         row) %
                        maxValue;
                }

                columnValues[col] = std::move(values);
            }

            if (useDictionary) {
                batches.push_back(
                    CreateVectorBatch(
                        OMNI_DICTIONARY,
                        buildTypes,
                        prefix,
                        columnValues,
                        newRows));
            } else {
                batches.push_back(
                    CreateVectorBatch(
                        OMNI_FLAT,
                        buildTypes,
                        prefix,
                        columnValues,
                        newRows));
            }

            remainingRows -= newRows;

            for (auto& column : columnValues) {
                column.clear();
            }
        }

        return batches;
    }

    std::vector<VectorBatch*> createProbeData(
        int32_t rows,
        int32_t maxValue,
        double matchRate,
        bool useDictionary)
    {
        std::vector<VectorBatch*> batches;
        std::vector<std::vector<int32_t>>
            columnValues(probeTypesA.size());

        const double effectiveMatchRate =
            std::max(
                0.0,
                std::min(1.0, matchRate));

        constexpr int32_t kRatioScale = 100;

        const int32_t matchThreshold =
            static_cast<int32_t>(
                effectiveMatchRate *
                static_cast<double>(kRatioScale));

        int32_t remainingRows = rows;
        int64_t logicalRow = 0;

        while (remainingRows > 0) {
            const int32_t newRows =
                std::min(
                    remainingRows,
                    kRowsPerPage);

            for (auto& values : columnValues) {
                values.resize(
                    static_cast<size_t>(newRows));
            }

            for (int32_t row = 0;
                 row < newRows;
                 ++row, ++logicalRow) {
                const int32_t matchingId =
                    static_cast<int32_t>(
                        (logicalRow + 30) %
                        maxValue);

                const bool shouldMatch =
                    (matchingId % kRatioScale) <
                    matchThreshold;

                // Build keys are non-negative.
                // A negative id is therefore a guaranteed miss.
                columnValues[0][
                    static_cast<size_t>(row)] =
                    shouldMatch
                        ? matchingId
                        : -(matchingId + 1);

                // A.value_a
                columnValues[1][
                    static_cast<size_t>(row)] =
                    static_cast<int32_t>(
                        (logicalRow + 40) %
                        maxValue);
            }

            if (useDictionary) {
                batches.push_back(
                    CreateVectorBatch(
                        OMNI_DICTIONARY,
                        probeTypesA,
                        prefix,
                        columnValues,
                        newRows));
            } else {
                batches.push_back(
                    CreateVectorBatch(
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

    void buildHashTableB(
        int32_t buildRows,
        bool useDictionary)
    {
        const int32_t maxKey = buildRows;

        auto buildDataB =
            createBuildData(
                buildTypesB,
                buildRows,
                maxKey,
                useDictionary);

        std::vector<int32_t> buildHashColsB = {0};

        hashBuilderFactoryB =
            new HashBuilderOperatorFactory(
                JoinType::OMNI_JOIN_TYPE_INNER,
                DataTypes(buildTypesB),
                buildHashColsB.data(),
                static_cast<int32_t>(
                    buildHashColsB.size()),
                1);

        hashBuilderOpB =
            hashBuilderFactoryB->CreateOperator();

        for (auto* batch : buildDataB) {
            hashBuilderOpB->AddInput(batch);
        }

        VectorBatch* outputB = nullptr;
        hashBuilderOpB->GetOutput(&outputB);

        if (outputB != nullptr) {
            VectorHelper::FreeVecBatch(outputB);
        }
    }

    void buildHashTableC(
        int32_t buildRows,
        bool useDictionary)
    {
        const int32_t buildRowsC =
            getLevel1BuildRows(buildRows);

        auto buildDataC =
            createBuildData(
                buildTypesC,
                buildRowsC,
                buildRowsC,
                useDictionary);

        std::vector<int32_t> buildHashColsC = {0};

        hashBuilderFactoryC =
            new HashBuilderOperatorFactory(
                JoinType::OMNI_JOIN_TYPE_INNER,
                DataTypes(buildTypesC),
                buildHashColsC.data(),
                static_cast<int32_t>(
                    buildHashColsC.size()),
                1);

        hashBuilderOpC =
            hashBuilderFactoryC->CreateOperator();

        for (auto* batch : buildDataC) {
            hashBuilderOpC->AddInput(batch);
        }

        VectorBatch* outputC = nullptr;
        hashBuilderOpC->GetOutput(&outputC);

        if (outputC != nullptr) {
            VectorHelper::FreeVecBatch(outputC);
        }
    }

    omniruntime::op::OperatorFactory*
    createOperatorFactory(
        const State& state) override
    {
        const int32_t buildRows =
            BuildRows(state);

        const bool useDictionary =
            IsDictionaryBlocks(state);

        // ------------------------------------------------------------
        // Build side B/C is prepared once by fixture setup.
        // ------------------------------------------------------------
        buildHashTableB(
            buildRows,
            useDictionary);

        buildHashTableC(
            buildRows,
            useDictionary);

        // ------------------------------------------------------------
        // Cascading:
        //   Level 0: A.id probes B.id
        //   Level 1: matched B.id probes C.id
        // ------------------------------------------------------------
        std::vector<DataTypes> probeTypesList = {
            DataTypes(probeTypesA),
            DataTypes(buildTypesB)
        };

        std::vector<std::vector<int32_t>>
            probeHashColsList = {
                {0},
                {0}
            };

        std::vector<std::vector<int32_t>>
            probeHashColTypesList = {
                {OMNI_LONG},
                {OMNI_LONG}
            };

        std::vector<std::vector<int32_t>>
            buildOutputColsList = {
                {1},
                {1}
            };

        std::vector<DataTypes>
            buildOutputTypesList = {
                DataTypes({IntType()}),
                DataTypes({IntType()})
            };

        std::vector<HashTableVariants*>
            hashTablesList = {
                hashBuilderFactoryB
                    ->GetHashTablesVariants(),
                hashBuilderFactoryC
                    ->GetHashTablesVariants()
            };

        // Output:
        //   A.id       : 8 bytes
        //   A.value_a  : 4 bytes
        //   B.value_b  : 4 bytes
        //   C.value_c  : 4 bytes
        constexpr int32_t outputRowSize =
            8 + 4 + 4 + 4;

        return new
            CascadingLookupJoinOperatorFactory(
                probeTypesList,
                probeHashColsList,
                probeHashColTypesList,
                buildOutputColsList,
                buildOutputTypesList,
                hashTablesList,
                outputRowSize);
    }

    std::vector<VectorBatchSupplier>
    createVecBatch(
        const State& state) override
    {
        const int32_t probeRows =
            ProbeRows(state);

        const int32_t buildRows =
            BuildRows(state);

        const double matchRate =
            MatchRate(state);

        const bool useDictionary =
            IsDictionaryBlocks(state);

        auto batches =
            createProbeData(
                probeRows,
                buildRows,
                matchRate,
                useDictionary);

        return
            VectorBatchToVectorBatchSupplier(
                batches);
    }

    BaseFixtureGetOutputStrategy
    GetOutputStrategy() override
    {
        // For every input A VectorBatch:
        // AddInput -> fully drain GetOutput -> next input batch.
        return AFTER_EACH_INPUT_FINISHED;
    }

    void TearDown(
        State& state) override
    {
        // Let BaseOperatorFixture release the benchmark operator/factory first.
        BaseOperatorFixture::TearDown(state);

        if (hashBuilderOpB != nullptr) {
            omniruntime::op::Operator::
                DeleteOperator(hashBuilderOpB);
            hashBuilderOpB = nullptr;
        }

        if (hashBuilderOpC != nullptr) {
            omniruntime::op::Operator::
                DeleteOperator(hashBuilderOpC);
            hashBuilderOpC = nullptr;
        }

        if (hashBuilderFactoryB != nullptr) {
            delete hashBuilderFactoryB;
            hashBuilderFactoryB = nullptr;
        }

        if (hashBuilderFactoryC != nullptr) {
            delete hashBuilderFactoryC;
            hashBuilderFactoryC = nullptr;
        }
    }
};

// Only this benchmark is registered.
OMNI_BENCHMARK_DECLARE_OPERATOR_DEFAULT(
    CascadingLookupJoinPerf);

} // namespace om_benchmark
