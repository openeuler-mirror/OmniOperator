/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2021. All rights reserved.
 */
#ifndef __HASH_BUILDER_H__
#define __HASH_BUILDER_H__

#include <memory>
#include "../operator_factory.h"
#include "../operator.h"
#include "../pages_index.h"
#include "join_hash_table.h"

namespace omniruntime {
namespace op {
class HashBuilderOperatorFactory : public OperatorFactory {
public:
    HashBuilderOperatorFactory(const int32_t *buildTypes, int32_t buildTypesCount, const int32_t *buildOutputCols,
        int32_t buildOutputColsCount, const int32_t *buildHashCols, int32_t buildHashColsCount, int32_t operatorCount);
    int32_t Init();
    ~HashBuilderOperatorFactory() override;
    static HashBuilderOperatorFactory *CreateHashBuilderOperatorFactory(const int32_t *buildTypes,
        int32_t buildTypesCount, const int32_t *buildOutputCols, int32_t buildOutputColsCount,
        const int32_t *buildHashCols, int32_t buildHashColsCount, int32_t operatorCount);
    omniruntime::op::Operator *CreateOperator() override;
    JoinHashTables *GetHashTables() const
    {
        return hashTables;
    }

private:
    std::vector<int32_t> buildTypes;
    std::vector<int32_t> buildOutputCols;
    std::vector<int32_t> buildHashCols;
    JoinHashTables *hashTables;
    int32_t hashTableCount;
    std::atomic<int32_t> operatorIndex;
};

class HashBuilderOperator : public Operator {
public:
    HashBuilderOperator(std::vector<int32_t> &buildTypes, const std::vector<int32_t> &buildOutputCols,
        std::vector<int32_t> &buildHashCols, JoinHashTables *hashTables, int32_t partitionIndex,
        std::unique_ptr<PagesIndex> &pagesIndex);
    ~HashBuilderOperator() override;
    int32_t AddInput(omniruntime::vec::VectorBatch *vecBatch) override;
    int32_t GetOutput(std::vector<omniruntime::vec::VectorBatch *> &outputPages) override;
    int32_t *GetSourceTypes() override;
    void Close() override;

private:
    std::vector<int32_t> buildTypes;
    std::vector<int32_t> buildHashCols;
    std::unique_ptr<PagesIndex> pagesIndex;
    int32_t partitionIndex;
    JoinHashTables *hashTables;
    std::vector<omniruntime::vec::VectorBatch *> inputVecBatches;
};
} // end of op
} // end of omniruntime
#endif
