/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * @Description: lookup outer join implementations
 */
#ifndef __LOOKUP_OUTER_JOIN_H__
#define __LOOKUP_OUTER_JOIN_H__

#include <memory>
#include "join_hash_table.h"
#include "operator/operator.h"
#include "operator/operator_factory.h"
#include "type/data_types.h"
#include "type/data_type.h"
#include "hash_builder.h"
#include "common_join.h"

namespace omniruntime {
namespace op {
class LookupOuterJoinOperatorFactory : public OperatorFactory {
public:
    LookupOuterJoinOperatorFactory(const type::DataTypes &probeTypes,
        int32_t *probeOutputCols, int32_t probeOutputColsCount, int32_t *probeHashCols, int32_t probeHashColsCount,
        int32_t *buildOutputCols, const type::DataTypes &buildOutputTypes, JoinHashTables *hashTables);
    ~LookupOuterJoinOperatorFactory() override;
    static LookupOuterJoinOperatorFactory *CreateLookupOuterJoinOperatorFactory(const type::DataTypes &probeTypes,
        int32_t *probeOutputCols, int32_t probeOutputColsCount, int32_t *probeHashCols, int32_t probeHashColsCount,
        int32_t *buildOutputCols, const type::DataTypes &buildOutputTypes, int64_t hashBuilderFactoryAddr);
    Operator *CreateOperator() override;

private:
    DataTypes buildOutputTypes;
    std::vector<int32_t> buildOutputCols;
    DataTypes probeTypes;                 // all types for probe
    std::vector<int32_t> probeOutputCols;        // output columns for probe
    std::vector<int32_t> probeHashCols;          // join columns for probe
    std::vector<int32_t> probeHashColTypes;
    JoinHashTables *hashTables;
};

class LookupOuterPositionIterator;

class LookupOuterJoinOperator : public Operator {
public:
    LookupOuterJoinOperator(DataTypes &probeOutputTypes, std::vector<int32_t> &probeOutputCols,
        std::vector<int32_t> &probeHashCols, std::vector<int32_t> &probeHashColTypes,
        std::vector<int32_t> &buildOutputCols, const type::DataTypes &buildOutputTypes, JoinHashTables *hashTables);
    ~LookupOuterJoinOperator() override;
    int32_t AddInput(omniruntime::vec::VectorBatch *vecBatch) override;
    int32_t GetOutput(std::vector<omniruntime::vec::VectorBatch *> &outputPages) override;
    void AppendToNext(VectorBatch *vectorBatch, const int32_t *buildOutputIds,
        int32_t buildOutputColsCount, int32_t probeOutputColsCount, int32_t destRowIndex);

private:
    void BuildVecBatch(VectorBatch * vectorBatch);

    DataTypes probeOutputTypes;
    std::vector<int32_t> probeOutputCols;
    std::vector<int32_t> buildOutputCols;
    DataTypes buildOutputTypes;
    JoinHashTables *hashTables;
    LookupOuterPositionIterator *iterator;
    int32_t outputRowSize;
};

class LookupOuterPositionIterator {
public:
    LookupOuterPositionIterator(JoinHashTables *hashTables);
    void NextUnVisitedAddress(uint32_t &hashTableIndex, uint64_t &address);
    void Reset();

private:
    uint32_t currentHashTable;
    uint32_t currentPosition;
    JoinHashTables *joinHashTables;
};
} // end of op
} // end of omniruntime
#endif
