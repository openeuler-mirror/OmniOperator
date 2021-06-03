#ifndef __LOOKUP_JOIN_H__
#define __LOOKUP_JOIN_H__

#include "../operator_factory.h"
#include "../operator.h"
#include "join_hash_table.h"

using namespace omni;

class LookupJoinOperatorFactory : public OperatorFactory
{
public:
    LookupJoinOperatorFactory(
        int32_t *probeTypes,
        int32_t probeTypesCount,
        int32_t *probeOutputCols,
        int32_t probeOutputColsCount,
        int32_t *probeHashCols,
        int32_t probeHashColsCount,
        int32_t *buildOutputCols,
        int32_t *buildOutputTypes,
        int32_t buildOutputColsCount,
        JoinHashTables *hashTables);
    ~LookupJoinOperatorFactory();
    static LookupJoinOperatorFactory *createLookupJoinOperatorFactory(
        int32_t *probeTypes,
        int32_t probeTypesCount,
        int32_t *probeOutputCols,
        int32_t probeOutputColsCount,
        int32_t *probeHashCols,
        int32_t probeHashColsCount,
        int32_t *buildOutputCols,
        int32_t *buildOutputTypes,
        int32_t buildOutputColsCount,
        int64_t hashBuilderFactoryAddr);
    omni::Operator *createOperator();
private:
    int32_t *probeTypes;      // all types for probe
    int32_t probeTypesCount;
    int32_t *probeOutputCols; // output columns for probe
    int32_t probeOutputColsCount;
    int32_t *probeHashCols;   // join columns for probe
    int32_t probeHashColsCount;
    int32_t *buildOutputCols; // output columns for build
    int32_t *buildOutputTypes; // output column types for build
    int32_t buildOutputColsCount;
    JoinHashTables *hashTables;
};

class JoinProbe;
class LookupJoinOutputBuilder;

class LookupJoinOperator : public omni::Operator
{
public:
    LookupJoinOperator(
        int32_t *probeTypes,
        int32_t probeTypesCount,
        int32_t *probeOutputCols,
        int32_t probeOutputColsCount,
        int32_t *probeHashCols,
        int32_t probeHashColsCount,
        int32_t *buildOutputCols,
        int32_t *buildOutputTypes,
        int32_t buildOutputColsCount,
        JoinHashTables *hashTables);
    ~LookupJoinOperator();
    int32_t addInput(Table* data, int32_t rowCount) override;
    int32_t addInput(Table **datas, int32_t *rowCounts, int32_t pageCount) override;
    int32_t getOutput(std::vector<Table *>& outputTables) override;
    int32_t *getSourceTypes() override;
private:
    void processProbe();
    bool joinCurrentPosition();
    bool advanceProbePosition();
    int64_t getNextJoinPosition(int64_t currentJoinPosition, int32_t probePosition);

    int32_t *probeTypes;
    int32_t probeTypesCount;
    int32_t *probeOutputCols;
    int32_t probeOutputColsCount;
    int32_t *probeHashCols;
    int32_t probeHashColsCount;
    int32_t *buildOutputCols;
    int32_t *buildOutputTypes;
    int32_t buildOutputColsCount;
    JoinHashTables *hashTables;
    JoinProbe *joinProbe;
    Table *outputTable;
    int32_t partitionedJoinPosition; //the addressIndex combined partition for build, it is encoded by ((addressIndex << shiftSize) | partition)
    LookupJoinOutputBuilder *outputBuilder;
};

class JoinProbe
{
public:
    JoinProbe(Table *input, int32_t *hashCols, int32_t hashColsCount, int32_t positionCount);
    ~JoinProbe();
    int32_t getPosition()
    {
        return position;
    }
    Column **getProbeAllColumns()
    {
        return probeAllColumns;
    }
    bool advanceNextPosition();
    int64_t getCurrentJoinPosition(JoinHashTables *hashTables);

private:
    bool currentRowContainsNull();

    Column **probeAllColumns;
    int32_t probeAllColsCount;
    int32_t positionCount;
    Column **probeHashColumns; // Column *[join column count]
    int32_t probeHashColsCount;
    int32_t position;
};

class LookupJoinOutputBuilder
{
public:
    LookupJoinOutputBuilder(int32_t *probeOutputCols, int32_t probeOutputColsCount, int32_t *buildOutputCols, int32_t *buildOutputTypes, int32_t buildOutputColsCount);
    ~LookupJoinOutputBuilder() {}
    void appendRow(int32_t probePosition, int64_t partitionedJoinPosition);
    Table *buildOutput(JoinProbe *joinProbe, JoinHashTables *hashTables);

private:
    int32_t *probeOutputCols;
    int32_t probeOutputColsCount;
    int32_t *buildOutputCols;
    int32_t *buildOutputTypes;
    int32_t buildOutputColsCount;
    std::vector<int32_t> probeIndex;
    std::vector<int64_t> buildIndex;
};

#endif
