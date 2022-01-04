/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */
#ifndef __DISTINCT_LIMIT_H__
#define __DISTINCT_LIMIT_H__

#include <vector>
#include <unordered_map>
#include <memory>
#include "vector/vector_types.h"
#include "operator/operator.h"
#include "operator/operator_factory.h"
#include "operator/status.h"
#include "operator/hash_util.h"
#include "operator/aggregation/aggregator/aggregator.h"

namespace omniruntime {
namespace op {
class DistinctLimitOperatorFactory : public OperatorFactory {
public:
    DistinctLimitOperatorFactory(const vec::VecTypes &sourceTypes, const int32_t *distinctCols,
        int32_t distinctColsCount, int32_t hashCol, int64_t limit);

    ~DistinctLimitOperatorFactory() override;

    static DistinctLimitOperatorFactory *CreateDistinctLimitOperatorFactory(const vec::VecTypes &sourceTypes,
        const int32_t *distinctCols, int32_t distinctColsCount, int32_t hashCol, int64_t limit);

    Operator *CreateOperator() override;

private:
    const vec::VecTypes *sourceTypes;
    int32_t *distinctCols;
    int32_t distinctColsCount;
    int32_t hashCol;
    int64_t limit;
};

typedef struct {
    uint64_t hashValue; // hash value of the row
    int32_t slotIndex;  // index when hash conflict
} DistinctRowInfo;


using DuplicateValueFunc = void (*)(AggregateState &distinctSlot, Vector *inputVector, uint32_t rowIndex,
    ExecutionContext *context);
using GenerateHashFunc = void (*)(Vector *vector, const uint32_t rowCount, const int32_t *rowArray,
                                          uint64_t *combinedHash);
using GenerateHashFuncVect = void (*)(Vector *vector, const uint32_t start, const uint32_t rowCount,
    uint64_t *combinedHash);
using CheckEqualFunc = void (*)(Vector *vector, const uint32_t offset, AggregateState &slot, bool &isSame);
using FillOutputFunc = void (*)(VectorBatch *resultBatch, std::vector<AggregateState> &rowVector, int32_t rowIndex,
    int32_t colIndex);

using DistinctLimitFuncSet = struct {
    VecTypeId typeId;
    DuplicateValueFunc duplicateValueFunc;
    GenerateHashFunc generateHashFunc;
    GenerateHashFuncVect generateHashFuncVect;
    CheckEqualFunc checkEqualFunc;
    FillOutputFunc fillOutputFunc;
};

class DistinctLimitOperator : public Operator {
public:
    static const int32_t INVALID_DISTINCT_COL_ID = -1;
public:
    DistinctLimitOperator(const vec::VecTypes *sourceTypes, int32_t *distinctCols, int32_t distinctColsCount,
        int32_t hashCol, int64_t limit);

    ~DistinctLimitOperator() override;

    int32_t AddInput(omniruntime::vec::VectorBatch *vecBatch) override;

    int32_t GetOutput(std::vector<omniruntime::vec::VectorBatch *> &outputPages) override;

    OmniStatus Close() override;

private:
    void fillDistinctedTuple(Vector **inputVectors, int rowIndex, std::vector<AggregateState> &tuple);

    void InLoop(omniruntime::vec::VectorBatch *vecBatch, uint64_t *combineHashVal);

    void BuildOutputTypes();

    void releaseRowInfo(std::vector<DistinctRowInfo *> &rowInfo);

private:
    std::unique_ptr<ExecutionContext> executionContext;
    std::unordered_map<uint64_t, std::vector<std::vector<AggregateState>>, HashUtil>
        distinctedTable;                            // hashValue=>record vector with distinct
    std::vector<DistinctRowInfo *> distinctRowInfo; // info(hash value and conflict index) of all distinct records
    const vec::VecTypes *sourceTypes;
    vec::VecTypes *outTypes;
    int32_t *distinctCols;
    int32_t distinctColsCount;
    int32_t hashCol;
    int32_t *outCols; // include distinct cols, hash cols
    int32_t outColsCount;
    int64_t remainingLimit;
    int64_t limit;
};
}
}

#endif // __DISTINCT_LIMIT_H__
