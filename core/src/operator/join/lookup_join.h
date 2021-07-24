/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2021. All rights reserved.
 */
#ifndef __LOOKUP_JOIN_H__
#define __LOOKUP_JOIN_H__

#include <memory>
#include "join_hash_table.h"
#include "../operator_factory.h"
#include "../operator.h"

namespace omniruntime {
namespace op {
class LookupJoinOperatorFactory : public OperatorFactory {
public:
    LookupJoinOperatorFactory(int32_t *probeTypes, int32_t probeTypesCount, int32_t *probeOutputCols,
        int32_t probeOutputColsCount, int32_t *probeHashCols, int32_t probeHashColsCount, int32_t *buildOutputCols,
        int32_t *buildOutputTypes, int32_t buildOutputColsCount, JoinHashTables *hashTables);
    ~LookupJoinOperatorFactory() override;
    static LookupJoinOperatorFactory *CreateLookupJoinOperatorFactory(int32_t *probeTypes, int32_t probeTypesCount,
        int32_t *probeOutputCols, int32_t probeOutputColsCount, int32_t *probeHashCols, int32_t probeHashColsCount,
        int32_t *buildOutputCols, int32_t *buildOutputTypes, int32_t buildOutputColsCount,
        int64_t hashBuilderFactoryAddr);
    Operator *CreateOperator() override;

private:
    std::vector<int32_t> probeTypes;      // all types for probe
    std::vector<int32_t> probeOutputCols; // output columns for probe
    std::vector<int32_t> probeHashCols;   // join columns for probe
    std::vector<int32_t> probeHashColTypes;
    std::vector<int32_t> buildOutputCols;  // output columns for build
    std::vector<int32_t> buildOutputTypes; // output column types for build
    JoinHashTables *hashTables;
    int32_t rowSize;
};

class JoinProbe;
class LookupJoinOutputBuilder;

class LookupJoinOperator : public Operator {
public:
    LookupJoinOperator(std::vector<int32_t> &probeTypes, std::vector<int32_t> &probeOutputCols,
        std::vector<int32_t> &probeHashCols, std::vector<int32_t> &probeHashColTypes,
        std::vector<int32_t> &buildOutputCols, std::vector<int32_t> &buildOutputTypes, JoinHashTables *hashTables,
        int32_t outputRowSize);
    ~LookupJoinOperator() override;
    int32_t AddInput(omniruntime::vec::VectorBatch *data) override;
    int32_t GetOutput(std::vector<omniruntime::vec::VectorBatch *> &outputPages) override;
    int32_t *GetSourceTypes() override;

private:
    void ProcessProbe();
    bool JoinCurrentPosition();
    bool AdvanceProbePosition();
    int64_t GetNextJoinPosition(int64_t currentJoinPosition, int32_t probePosition) const;

    std::vector<int32_t> probeTypes;
    std::vector<int32_t> probeOutputCols;
    std::vector<int32_t> probeHashCols;
    std::vector<int32_t> probeHashColTypes;
    std::vector<int32_t> buildOutputCols;
    std::vector<int32_t> buildOutputTypes;
    JoinHashTables *hashTables;
    std::unique_ptr<JoinProbe> joinProbe;
    int32_t partitionedJoinPosition; // the addressIndex combined partition for build, it is encoded by ((addressIndex
    // << shiftSize) | partition)
    std::unique_ptr<LookupJoinOutputBuilder> outputBuilder;
};

class JoinProbe {
public:
    JoinProbe(omniruntime::vec::VectorBatch *input, int32_t allColsCount, int32_t *hashCols, int32_t *hashColTypes,
        int32_t hashColsCount);
    ~JoinProbe();
    int32_t GetPosition() const
    {
        return position;
    }
    omniruntime::vec::Vector **GetProbeAllColumns() const
    {
        return probeAllColumns;
    }
    bool AdvanceNextPosition();
    int64_t GetCurrentJoinPosition(const JoinHashTables *hashTables) const;

private:
    bool CurrentRowContainsNull() const;

    omniruntime::vec::Vector **probeAllColumns;
    int32_t probeAllColsCount;
    int32_t positionCount;
    omniruntime::vec::Vector **probeHashColumns; // Vector *[join column count]
    int32_t *probeHashColTypes;
    int32_t probeHashColsCount;
    int32_t position;
};

class LookupJoinOutputBuilder {
public:
    LookupJoinOutputBuilder(int32_t *probeTypes, int32_t *probeOutputCols, int32_t probeOutputColsCount,
        int32_t *buildOutputCols, int32_t *buildOutputTypes, int32_t buildOutputColsCount, int32_t outputRowSize);
    ~LookupJoinOutputBuilder() {}
    void AppendRow(int32_t probePosition, int64_t partitionedJoinPosition);
    void BuildOutput(const JoinProbe *joinProbe, const JoinHashTables *hashTables,
        std::vector<omniruntime::vec::VectorBatch *> &outputTables);

private:
    int32_t *probeTypes;
    int32_t *probeOutputCols;
    int32_t probeOutputColsCount;
    int32_t *buildOutputCols;
    int32_t *buildOutputTypes;
    int32_t buildOutputColsCount;
    int32_t outputRowSize;
    std::vector<int32_t> probeIndex;
    std::vector<int64_t> buildIndex;
    bool isSequentialProbeIndices;
};
} // end of op
} // end of omniruntime
#endif
