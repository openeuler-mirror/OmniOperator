/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2023. All rights reserved.
 * @Description: hash builder implementations
 */
#ifndef __HASH_BUILDER_H__
#define __HASH_BUILDER_H__

#include <memory>

#include "operator/operator_factory.h"
#include "operator/operator.h"
#include "join_hash_table_variants.h"
#include "common_join.h"

namespace omniruntime {
namespace op {
class HashBuilderOperatorFactory : public OperatorFactory {
public:
    HashBuilderOperatorFactory(JoinType joinType, const DataTypes &buildTypes, const int32_t *buildHashCols,
        int32_t buildHashColsCount, int32_t operatorCount);
    ~HashBuilderOperatorFactory()
    {
        delete hashTablesVariants;
    }
    static HashBuilderOperatorFactory *CreateHashBuilderOperatorFactory(JoinType joinType, const DataTypes &buildTypes,
        const int32_t *buildHashCols, int32_t buildHashColsCount, int32_t operatorCount);
    omniruntime::op::Operator *CreateOperator() override;

    HashTableVariants *GetHashTablesVariants()
    {
        return hashTablesVariants;
    }

private:
    DataTypes buildTypes;
    std::vector<int32_t> buildHashCols;
    HashTableVariants *hashTablesVariants;
    std::atomic<int32_t> operatorIndex;

    template <class RowRefListType>
    HashTableVariants *InitVariant(int32_t buildHashColsCount, int32_t operatorCount, JoinType joinType);
};

class HashBuilderOperator : public Operator {
public:
    HashBuilderOperator(const DataTypes &buildTypes, HashTableVariants *hashTables, int32_t partitionIndex);

    ~HashBuilderOperator() = default;

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
    int32_t partitionIndex;
    HashTableVariants *hashTablesVariants;
};
} // end of op
} // end of omniruntime
#endif
