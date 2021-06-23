#ifndef __HASH_BUILDER_H__
#define __HASH_BUILDER_H__

#include "../operator_factory.h"
#include "../operator.h"
#include "../pages_index.h"
#include "join_hash_table.h"
#include <atomic>

namespace omniruntime {
namespace op {

class HashBuilderOperatorFactory : public OperatorFactory
{
public:
    HashBuilderOperatorFactory(
        int32_t *buildTypes,
        int32_t buildTypesCount,
        int32_t *buildOutputCols,
        int32_t buildOutputColsCount,
        int32_t *buildHashCols,
        int32_t buildHashColsCount,
        int32_t operatorCount);
    ~HashBuilderOperatorFactory();
    static HashBuilderOperatorFactory *createHashBuilderOperatorFactory(
        int32_t *buildTypes,
        int32_t buildTypesCount,
        int32_t *buildOutputCols,
        int32_t buildOutputColsCount,
        int32_t *buildHashCols,
        int32_t buildHashColsCount,
        int32_t operatorCount);
    omniruntime::op::Operator *createOperator();
    JoinHashTables *getHashTables()
    {
        return hashTables;
    }
private:
    int32_t *buildTypes;                // all types for build
    int32_t buildTypesCount;
    int32_t *buildOutputCols;           // output columns for build
    int32_t buildOutputColsCount;
    int32_t *buildHashCols;             // join columns for build
    int32_t buildHashColsCount;
    JoinHashTables *hashTables;
    std::atomic<int32_t> operatorIndex;
};

class HashBuilderOperator : public Operator
{
public:
    HashBuilderOperator(
        int32_t *buildTypes,
        int32_t buildTypesCount,
        int32_t *buildOutputCols,
        int32_t buildOutputColsCount,
        int32_t *buildHashCols,
        int32_t buildHashColsCount,
        JoinHashTables *hashTables,
        int32_t partitionIndex);
    ~HashBuilderOperator();
    int32_t addInput(VectorBatch *vecBatch) override;
    int32_t getOutput(std::vector<VectorBatch *>& outputPages) override;
    int32_t *getSourceTypes() override;
private:
    int32_t *buildTypes;
    int32_t buildTypesCount;
    int32_t *buildHashCols;
    int32_t buildHashColsCount;
    PagesIndex *pagesIndex;
    int32_t partitionIndex;
    JoinHashTables *hashTables;
    vector<VectorBatch *> inputVecBatches;
};
} // end of op
} // end of omniruntime
#endif
