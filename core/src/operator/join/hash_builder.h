#ifndef __HASH_BUILDER_H__
#define __HASH_BUILDER_H__

#include "../native_base.h"
#include "../pages_index.h"
#include "join_hash_table.h"
#include <atomic>

class NativeOmniHashBuilderOperatorFactory : public NativeOmniOperatorFactory
{
public:
    NativeOmniHashBuilderOperatorFactory(
        int32_t *buildTypes,
        int32_t buildTypesCount,
        int32_t *buildOutputCols,
        int32_t buildOutputColsCount,
        int32_t *buildHashCols,
        int32_t buildHashColsCount,
        int32_t operatorCount);
    ~NativeOmniHashBuilderOperatorFactory();
    static NativeOmniHashBuilderOperatorFactory *createNativeOmniHashBuilderOperatorFactory(
        int32_t *buildTypes,
        int32_t buildTypesCount,
        int32_t *buildOutputCols,
        int32_t buildOutputColsCount,
        int32_t *buildHashCols,
        int32_t buildHashColsCount,
        int32_t operatorCount);
    NativeOmniOperator *createOmniOperator();
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

class NativeOmniHashBuilderOperator : public NativeOmniOperator
{
public:
    NativeOmniHashBuilderOperator(
        int32_t *buildTypes,
        int32_t buildTypesCount,
        int32_t *buildOutputCols,
        int32_t buildOutputColsCount,
        int32_t *buildHashCols,
        int32_t buildHashColsCount,
        JoinHashTables *hashTables,
        int32_t partitionIndex);
    ~NativeOmniHashBuilderOperator();
    int32_t addInput(Table* data, int32_t rowCount) override
    {
        return 0;
    }
    int32_t addInput(Table **datas, int32_t *rowCounts, int32_t pageCount) override;
    int32_t getOutput(std::vector<Table *>& outputTables) override;
    int32_t *getSourceTypes() override;
private:
    int32_t *buildTypes;
    int32_t buildTypesCount;
    int32_t *buildOutputCols;
    int32_t buildOutputColsCount;
    int32_t *buildHashCols;
    int32_t buildHashColsCount;
    PagesIndex *pagesIndex;
    int32_t partitionIndex;
    JoinHashTables *hashTables;
};

#endif
