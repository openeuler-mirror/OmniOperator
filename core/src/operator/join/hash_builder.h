/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: hash builder implementations
 */
#ifndef __HASH_BUILDER_H__
#define __HASH_BUILDER_H__

#include <memory>
#include "operator/operator_factory.h"
#include "operator/operator.h"
#include "operator/pages_index.h"
#include "join_hash_table.h"

namespace omniruntime {
namespace op {
class HashBuilderOperatorFactory : public OperatorFactory {
public:
    HashBuilderOperatorFactory(const DataTypes &buildTypes, const int32_t *buildHashCols, int32_t buildHashColsCount,
        std::string &filterExpr, int32_t operatorCount);
    int32_t Init();
    ~HashBuilderOperatorFactory() override;
    static HashBuilderOperatorFactory *CreateHashBuilderOperatorFactory(const DataTypes &dataTypes,
        const int32_t *buildHashCols, int32_t buildHashColsCount, std::string &filterExpr, int32_t operatorCount);
    omniruntime::op::Operator *CreateOperator() override;
    JoinHashTables *GetHashTables() const
    {
        return hashTables;
    }

private:
    DataTypes buildTypes;
    std::vector<int32_t> buildHashCols;
    JoinHashTables *hashTables;
    int32_t hashTableCount;
    std::atomic<int32_t> operatorIndex;
};

class HashBuilderOperator : public Operator {
public:
    HashBuilderOperator(const DataTypes &buildTypes, std::vector<int32_t> &buildHashCols, JoinHashTables *hashTables,
        int32_t partitionIndex, std::unique_ptr<PagesIndex> &pagesIndex);

    ~HashBuilderOperator() override;

    int32_t AddInput(omniruntime::vec::VectorBatch *vecBatch) override;

    int32_t GetOutput(omniruntime::vec::VectorBatch **outputVecBatch) override;

    OmniStatus Close() override;

    // for test
    DataTypes &GetBuildTypes()
    {
        return buildTypes;
    }

private:
    DataTypes buildTypes;
    std::vector<int32_t> buildHashCols;
    JoinHashTables *hashTables;
    int32_t partitionIndex;
    std::unique_ptr<PagesIndex> pagesIndex;
};
} // end of op
} // end of omniruntime
#endif
