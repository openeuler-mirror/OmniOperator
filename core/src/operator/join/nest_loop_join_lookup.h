/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights
 * reserved.
 * @Description: lookup join implementations
 */
#ifndef __NEST_LOOP_JOIN_LOOKUP_H__
#define __NEST_LOOP_JOIN_LOOKUP_H__

#include <memory>
#include "common_join.h"
#include "hash_builder.h"
#include "operator/filter/filter_and_project.h"
#include "operator/operator.h"
#include "operator/operator_factory.h"
#include "type/data_type.h"
#include "type/data_types.h"
#include "nest_loop_join_builder.h"

namespace omniruntime {
namespace op {
class NestLoopJoinLookupOperatorFactory : public OperatorFactory {
public:
    static NestLoopJoinLookupOperatorFactory *CreateNestLoopJoinLookupOperatorFactory(JoinType &joinTypePtr,
        DataTypes &probeTypesPtr, int32_t *probeOutputColsPtr, int32_t probeOutputColsCount, Expr *filterExpr,
        int64_t buildOperatorFactoryAddr, OverflowConfig *overflowConfig);

    static NestLoopJoinLookupOperatorFactory *CreateNestLoopJoinLookupOperatorFactory(
        std::shared_ptr<const NestedLoopJoinNode> planNode,
        NestedLoopJoinBuildOperatorFactory* builderOperatorFactory,
        const config::QueryConfig &queryConfig);

    NestLoopJoinLookupOperatorFactory(JoinType joinType, DataTypes probeTypes, int32_t *probeOutputCols,
        int32_t probeOutputColsCount, Filter *filter, DataTypes joinedTypes, int64_t buildOpFactoryAddr,
        OverflowConfig *overflowConfig);

    ~NestLoopJoinLookupOperatorFactory() override;

    omniruntime::op::Operator *CreateOperator() override;

private:
    DataTypes probeTypes; // all types for probe
    JoinType joinType;
    std::vector<int32_t> probeOutputCols; // output columns for probe
    Filter *filter;
    DataTypes joinedTypes;
    int64_t buildOpFactoryAddr;
};

class NestLoopJoinLookupOperator : public Operator {
public:
    NestLoopJoinLookupOperator(JoinType joinType, std::vector<int32_t> &probeOutputCols, Filter *filter,
        DataTypes probeTypes, VectorBatch *buildVectorBatch, DataTypes buildTypes,
        std::vector<int32_t> &buildOutputCols, DataTypes joinedTypes);

    ~NestLoopJoinLookupOperator() override;

    int32_t AddInput(omniruntime::vec::VectorBatch *vecBatch) override;

    int32_t GetOutput(omniruntime::vec::VectorBatch **outputVecBatch) override;

    OmniStatus Close() override;

private:
    void BuildOutput(VectorBatch **outputVecBatch);

    void InitJoinedVectorBatch();

    void UpdateJoinedVectorBatch(int32_t probeRow);

    int32_t GetNumSelectedRows();

    void ProbeInnerJoin(int32_t probeRowCount, int32_t resultOutputRows);

    void ProbeOuterJoin(int32_t probeRowCount, int32_t resultOutputRows);

    void ProbeCrossJoin(int32_t probeRowCount, int32_t buildRowCount, int32_t resultOutputRows);

    void PrepareCurrentProbe();

    VectorBatch *buildVectorBatch;
    omniruntime::vec::VectorBatch *curInputBatch = nullptr;
    std::vector<int32_t> &buildOutputCols;
    JoinType joinType;
    std::vector<int32_t> &probeOutputCols;
    Filter *filter;
    int32_t curProbePosition;
    int32_t maxRowCount;
    std::vector<int32_t> probeResultOutputRows;
    std::vector<int32_t> buildResultOutputRows;
    std::vector<int32_t> buildNullPosition;
    std::unique_ptr<VectorBatch> joinedVectorBatch;
    DataTypes joinedTypes;
    bool firstVecBatch = true;
    int32_t *selectedRows;
};
} // namespace op
} // namespace omniruntime
#endif
