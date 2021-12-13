/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: hash builder implementations
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
    HashBuilderOperatorFactory(const vec::VecTypes &buildTypes, const int32_t *buildHashCols,
        int32_t buildHashColsCount, std::string &filterExpr, int32_t operatorCount);
    int32_t Init();
    ~HashBuilderOperatorFactory() override;
    static HashBuilderOperatorFactory *CreateHashBuilderOperatorFactory(const vec::VecTypes &vecTypes,
        const int32_t *buildHashCols, int32_t buildHashColsCount, std::string &filterExpr, int32_t operatorCount);
    omniruntime::op::Operator *CreateOperator() override;
    JoinHashTables *GetHashTables() const
    {
        return hashTables;
    }

private:
    std::unique_ptr<vec::VecTypes> buildTypes;
    std::vector<int32_t> buildHashCols;
    JoinHashTables *hashTables;
    int32_t hashTableCount;
    std::atomic<int32_t> operatorIndex;
};

class HashBuilderOperator : public Operator {
public:
    HashBuilderOperator(const vec::VecTypes &buildTypes, std::vector<int32_t> &buildHashCols,
        JoinHashTables *hashTables, int32_t partitionIndex, std::unique_ptr<PagesIndex> &pagesIndex);
    ~HashBuilderOperator() override;
    int32_t AddInput(omniruntime::vec::VectorBatch *vecBatch) override;
    int32_t GetOutput(std::vector<omniruntime::vec::VectorBatch *> &outputPages) override;

private:
    const vec::VecTypes &buildTypes;
    std::vector<int32_t> buildHashCols;
    std::unique_ptr<PagesIndex> pagesIndex;
    int32_t partitionIndex;
    JoinHashTables *hashTables;
    std::vector<omniruntime::vec::VectorBatch *> inputVecBatches;
};
} // end of op
} // end of omniruntime
#endif
