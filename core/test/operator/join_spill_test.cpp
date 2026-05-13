/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Description: join spill v1 tests.
 */
#include <sys/stat.h>

#include <memory>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "operator/join/hash_builder.h"
#include "operator/join/join_spill_state.h"
#include "operator/join/lookup_join.h"
#include "type/data_type.h"
#include "type/data_types.h"
#include "util/native_log.h"
#include "util/test_util.h"
#include "vector/large_string_container.h"
#include "vector/vector.h"
#include "vector/vector_batch.h"
#include "vector/vector_helper.h"

using namespace omniruntime::op;
using namespace omniruntime::type;
using namespace omniruntime::vec;

namespace {

using VarcharVector = Vector<LargeStringContainer<std::string_view>>;

JoinSubPartitionConfig TestSubPartitionConfig()
{
    JoinSubPartitionConfig cfg;
    cfg.numSubPartitions = 4;
    cfg.startPartitionBit = 0;
    cfg.activeSubPartition = 0;
    return cfg;
}

VectorBatch *MakeLongBatch(const std::vector<int64_t> &keys, const std::vector<int64_t> &values)
{
    auto *batch = new VectorBatch(static_cast<int32_t>(keys.size()));
    auto *keyVec = new Vector<int64_t>(static_cast<int32_t>(keys.size()));
    auto *valueVec = new Vector<int64_t>(static_cast<int32_t>(values.size()));
    for (size_t i = 0; i < keys.size(); ++i) {
        keyVec->SetValue(static_cast<int32_t>(i), keys[i]);
        valueVec->SetValue(static_cast<int32_t>(i), values[i]);
    }
    batch->Append(keyVec);
    batch->Append(valueVec);
    return batch;
}

VectorBatch *MakeVarcharBatch(const std::vector<std::string> &keys, const std::vector<std::string> &values)
{
    auto *batch = new VectorBatch(static_cast<int32_t>(keys.size()));
    auto *keyVec = new VarcharVector(static_cast<int32_t>(keys.size()));
    auto *valueVec = new VarcharVector(static_cast<int32_t>(values.size()));
    for (size_t i = 0; i < keys.size(); ++i) {
        keyVec->SetValue(static_cast<int32_t>(i), std::string_view(keys[i]));
        valueVec->SetValue(static_cast<int32_t>(i), std::string_view(values[i]));
    }
    batch->Append(keyVec);
    batch->Append(valueVec);
    return batch;
}

int32_t DrainLookupJoin(LookupJoinOperator *lookupJoinOperator)
{
    int32_t outputRows = 0;
    while (lookupJoinOperator->GetStatus() != OMNI_STATUS_FINISHED) {
        VectorBatch *output = nullptr;
        lookupJoinOperator->GetOutput(&output);
        if (output != nullptr) {
            outputRows += output->GetRowCount();
            VectorHelper::FreeVecBatch(output);
        }
    }
    return outputRows;
}

std::shared_ptr<JoinSpillState> CreateState(const std::string &spillPath, const DataTypes &buildTypes,
    const DataTypes &probeTypes, const JoinSubPartitionConfig &cfg)
{
    return std::make_shared<JoinSpillState>(spillPath, buildTypes, probeTypes, cfg.numSubPartitions);
}

size_t CountSubstr(const std::string &text, const std::string &needle)
{
    size_t count = 0;
    size_t pos = text.find(needle);
    while (pos != std::string::npos) {
        ++count;
        pos = text.find(needle, pos + needle.size());
    }
    return count;
}

class DebugLogGuard {
public:
    DebugLogGuard() : oldDebugEnable_(g_isDebugEnable)
    {
        g_isDebugEnable = true;
    }

    ~DebugLogGuard()
    {
        g_isDebugEnable = oldDebugEnable_;
    }

private:
    bool oldDebugEnable_;
};

} // namespace

TEST(JoinSpillTest, InnerJoinLongKeyValueBuildAndProbeReplay)
{
    DebugLogGuard debugLogGuard;
    auto cfg = TestSubPartitionConfig();
    const uint64_t maxSpillRunRows = 1;
    const std::string spillPath = TestUtil::GenerateSpillPath();
    mkdir(spillPath.c_str(), 0750);

    DataTypes rowTypes(std::vector<DataTypePtr>({ LongType(), LongType() }));
    int32_t hashCols[] = {0};
    auto *hashBuilderFactory =
        HashBuilderOperatorFactory::CreateHashBuilderOperatorFactory(OMNI_JOIN_TYPE_INNER, rowTypes, hashCols, 1, 4);
    auto state = CreateState(spillPath, rowTypes, rowTypes, cfg);
    hashBuilderFactory->SetJoinSpillSubPartitionPolicy(true, maxSpillRunRows, cfg);
    hashBuilderFactory->SetJoinSpillState(state);

    auto *hashBuilder = dynamic_cast<HashBuilderOperator *>(hashBuilderFactory->CreateOperator());
    testing::internal::CaptureStdout();
    hashBuilder->AddInput(MakeLongBatch({1, 2, 3, 4}, {10, 20, 30, 40}));
    VectorBatch *hashOutput = nullptr;
    hashBuilder->GetOutput(&hashOutput);
    const auto buildLog = testing::internal::GetCapturedStdout();

    int32_t probeOutputCols[] = {0, 1};
    int32_t buildOutputCols[] = {0, 1};
    auto *lookupFactory = LookupJoinOperatorFactory::CreateLookupJoinOperatorFactory(rowTypes, probeOutputCols, 2,
        hashCols, 1, buildOutputCols, 2, rowTypes, reinterpret_cast<int64_t>(hashBuilderFactory), nullptr, false,
        nullptr);
    lookupFactory->SetJoinSpillPolicy(true, maxSpillRunRows, cfg);
    lookupFactory->SetJoinSpillState(state);
    auto *lookupJoin = dynamic_cast<LookupJoinOperator *>(lookupFactory->CreateOperator());

    testing::internal::CaptureStdout();
    lookupJoin->AddInput(MakeLongBatch({2, 4}, {200, 400}));
    const int32_t outputRows = DrainLookupJoin(lookupJoin);
    const auto probeLog = testing::internal::GetCapturedStdout();

    EXPECT_EQ(outputRows, 2);
    EXPECT_GT(state->BuildSpilledRows(), 0);
    EXPECT_NE(buildLog.find("[OmniRuntime][JoinSpill][build] write runId="), std::string::npos);
    EXPECT_NE(buildLog.find("[OmniRuntime][JoinSpill][build] read subPartition="), std::string::npos);
    EXPECT_NE(probeLog.find("[OmniRuntime][JoinSpill][probe] write runId="), std::string::npos);
    EXPECT_NE(probeLog.find("[OmniRuntime][JoinSpill][probe] read runId="), std::string::npos);

    omniruntime::op::Operator::DeleteOperator(lookupJoin);
    omniruntime::op::Operator::DeleteOperator(hashBuilder);
    delete lookupFactory;
    delete hashBuilderFactory;
}

TEST(JoinSpillTest, InnerJoinVarcharKeyValueBuildAndProbeReplay)
{
    DebugLogGuard debugLogGuard;
    auto cfg = TestSubPartitionConfig();
    const uint64_t maxSpillRunRows = 1;
    const std::string spillPath = TestUtil::GenerateSpillPath();
    mkdir(spillPath.c_str(), 0750);

    DataTypes rowTypes(std::vector<DataTypePtr>({ VarcharType(8), VarcharType(8) }));
    int32_t hashCols[] = {0};
    auto *hashBuilderFactory =
        HashBuilderOperatorFactory::CreateHashBuilderOperatorFactory(OMNI_JOIN_TYPE_INNER, rowTypes, hashCols, 1, 4);
    auto state = CreateState(spillPath, rowTypes, rowTypes, cfg);
    hashBuilderFactory->SetJoinSpillSubPartitionPolicy(true, maxSpillRunRows, cfg);
    hashBuilderFactory->SetJoinSpillState(state);

    auto *hashBuilder = dynamic_cast<HashBuilderOperator *>(hashBuilderFactory->CreateOperator());
    testing::internal::CaptureStdout();
    hashBuilder->AddInput(MakeVarcharBatch({"aa", "bb", "cc"}, {"ba", "bb", "bc"}));
    VectorBatch *hashOutput = nullptr;
    hashBuilder->GetOutput(&hashOutput);
    const auto buildLog = testing::internal::GetCapturedStdout();

    int32_t probeOutputCols[] = {0, 1};
    int32_t buildOutputCols[] = {0, 1};
    auto *lookupFactory = LookupJoinOperatorFactory::CreateLookupJoinOperatorFactory(rowTypes, probeOutputCols, 2,
        hashCols, 1, buildOutputCols, 2, rowTypes, reinterpret_cast<int64_t>(hashBuilderFactory), nullptr, false,
        nullptr);
    lookupFactory->SetJoinSpillPolicy(true, maxSpillRunRows, cfg);
    lookupFactory->SetJoinSpillState(state);
    auto *lookupJoin = dynamic_cast<LookupJoinOperator *>(lookupFactory->CreateOperator());

    testing::internal::CaptureStdout();
    lookupJoin->AddInput(MakeVarcharBatch({"bb", "cc"}, {"pb", "pc"}));
    const int32_t outputRows = DrainLookupJoin(lookupJoin);
    const auto probeLog = testing::internal::GetCapturedStdout();

    EXPECT_EQ(outputRows, 2);
    EXPECT_GT(state->BuildSpilledRows(), 0);
    EXPECT_NE(buildLog.find("[OmniRuntime][JoinSpill][build] write runId="), std::string::npos);
    EXPECT_NE(buildLog.find("[OmniRuntime][JoinSpill][build] read subPartition="), std::string::npos);
    EXPECT_NE(probeLog.find("[OmniRuntime][JoinSpill][probe] write runId="), std::string::npos);
    EXPECT_NE(probeLog.find("[OmniRuntime][JoinSpill][probe] read runId="), std::string::npos);

    omniruntime::op::Operator::DeleteOperator(lookupJoin);
    omniruntime::op::Operator::DeleteOperator(hashBuilder);
    delete lookupFactory;
    delete hashBuilderFactory;
}

TEST(JoinSpillTest, SmallBatchBelowThresholdDoesNotSpill)
{
    auto cfg = TestSubPartitionConfig();
    const uint64_t maxSpillRunRows = 10;
    const std::string spillPath = TestUtil::GenerateSpillPath();
    mkdir(spillPath.c_str(), 0750);

    DataTypes rowTypes(std::vector<DataTypePtr>({ LongType(), LongType() }));
    int32_t hashCols[] = {0};
    auto *hashBuilderFactory =
        HashBuilderOperatorFactory::CreateHashBuilderOperatorFactory(OMNI_JOIN_TYPE_INNER, rowTypes, hashCols, 1, 4);
    auto state = CreateState(spillPath, rowTypes, rowTypes, cfg);
    hashBuilderFactory->SetJoinSpillSubPartitionPolicy(true, maxSpillRunRows, cfg);
    hashBuilderFactory->SetJoinSpillState(state);

    auto *hashBuilder = dynamic_cast<HashBuilderOperator *>(hashBuilderFactory->CreateOperator());
    testing::internal::CaptureStdout();
    hashBuilder->AddInput(MakeLongBatch({1, 2, 3, 4}, {10, 20, 30, 40}));
    VectorBatch *hashOutput = nullptr;
    hashBuilder->GetOutput(&hashOutput);
    const auto buildLog = testing::internal::GetCapturedStdout();

    int32_t probeOutputCols[] = {0, 1};
    int32_t buildOutputCols[] = {0, 1};
    auto *lookupFactory = LookupJoinOperatorFactory::CreateLookupJoinOperatorFactory(rowTypes, probeOutputCols, 2,
        hashCols, 1, buildOutputCols, 2, rowTypes, reinterpret_cast<int64_t>(hashBuilderFactory), nullptr, false,
        nullptr);
    lookupFactory->SetJoinSpillPolicy(true, maxSpillRunRows, cfg);
    lookupFactory->SetJoinSpillState(state);
    auto *lookupJoin = dynamic_cast<LookupJoinOperator *>(lookupFactory->CreateOperator());

    testing::internal::CaptureStdout();
    lookupJoin->AddInput(MakeLongBatch({2, 4}, {200, 400}));
    const int32_t outputRows = DrainLookupJoin(lookupJoin);
    const auto probeLog = testing::internal::GetCapturedStdout();

    EXPECT_EQ(outputRows, 2);
    EXPECT_EQ(state->BuildSpilledRows(), 0);
    EXPECT_EQ(buildLog.find("[OmniRuntime][JoinSpill][build] write"), std::string::npos);
    EXPECT_EQ(buildLog.find("[OmniRuntime][JoinSpill][build] read"), std::string::npos);
    EXPECT_EQ(probeLog.find("[OmniRuntime][JoinSpill][probe] write"), std::string::npos);
    EXPECT_EQ(probeLog.find("[OmniRuntime][JoinSpill][probe] read"), std::string::npos);

    omniruntime::op::Operator::DeleteOperator(lookupJoin);
    omniruntime::op::Operator::DeleteOperator(hashBuilder);
    delete lookupFactory;
    delete hashBuilderFactory;
}

TEST(JoinSpillTest, BuildSpillUsesIndependentRunIdsForMultipleBatches)
{
    DebugLogGuard debugLogGuard;
    auto cfg = TestSubPartitionConfig();
    const uint64_t maxSpillRunRows = 2;
    const std::string spillPath = TestUtil::GenerateSpillPath();
    mkdir(spillPath.c_str(), 0750);

    DataTypes rowTypes(std::vector<DataTypePtr>({ LongType(), LongType() }));
    int32_t hashCols[] = {0};
    auto *hashBuilderFactory =
        HashBuilderOperatorFactory::CreateHashBuilderOperatorFactory(OMNI_JOIN_TYPE_INNER, rowTypes, hashCols, 1, 4);
    auto state = CreateState(spillPath, rowTypes, rowTypes, cfg);
    hashBuilderFactory->SetJoinSpillSubPartitionPolicy(true, maxSpillRunRows, cfg);
    hashBuilderFactory->SetJoinSpillState(state);

    auto *hashBuilder = dynamic_cast<HashBuilderOperator *>(hashBuilderFactory->CreateOperator());
    testing::internal::CaptureStdout();
    hashBuilder->AddInput(MakeLongBatch({1, 2, 3, 4}, {10, 20, 30, 40}));
    hashBuilder->AddInput(MakeLongBatch({5, 6, 7, 8}, {50, 60, 70, 80}));
    VectorBatch *hashOutput = nullptr;
    hashBuilder->GetOutput(&hashOutput);
    const auto buildLog = testing::internal::GetCapturedStdout();

    EXPECT_GT(state->BuildSpilledRows(), 0);
    EXPECT_GE(CountSubstr(buildLog, "[OmniRuntime][JoinSpill][build] write runId="), 2);
    EXPECT_NE(buildLog.find("runId=0"), std::string::npos);
    EXPECT_NE(buildLog.find("runId=1"), std::string::npos);
    EXPECT_NE(buildLog.find("[OmniRuntime][JoinSpill][build] read subPartition="), std::string::npos);

    omniruntime::op::Operator::DeleteOperator(hashBuilder);
    delete hashBuilderFactory;
}
