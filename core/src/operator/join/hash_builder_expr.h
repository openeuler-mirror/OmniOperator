/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: sort implementations
 */

#ifndef __HASH_BUILDER_EXPR_H__
#define __HASH_BUILDER_EXPR_H__

#include <cstdint>
#include "operator/operator_factory.h"
#include "type/data_types.h"
#include "operator/projection/projection.h"
#include "operator/join/hash_builder.h"

namespace omniruntime {
namespace op {
class HashBuilderWithExprOperatorFactory : public OperatorFactory {
public:
    static HashBuilderWithExprOperatorFactory *CreateHashBuilderWithExprOperatorFactory(
        const type::DataTypes &buildTypes, const std::vector<omniruntime::expressions::Expr *> &buildHashKeys,
        int32_t buildHashKeysCount, std::string &filter, int32_t hashTableCount, OverflowConfig *overflowConfig);

    HashBuilderWithExprOperatorFactory(const DataTypes &buildTypes,
        const std::vector<omniruntime::expressions::Expr *> &buildHashKeys, int32_t buildHashKeysCount,
        std::string &filter, int32_t hashTableCount, OverflowConfig *overflowConfig);

    ~HashBuilderWithExprOperatorFactory() override;

    omniruntime::op::Operator *CreateOperator() override;

    HashBuilderOperatorFactory *GetHashBuilderOperatorFactory()
    {
        return operatorFactory;
    }

private:
    std::unique_ptr<DataTypes> buildTypes;
    std::vector<int32_t> buildHashCols;
    std::vector<std::unique_ptr<Projection>> projections;
    std::vector<ProjFunc> projectFuncs;
    HashBuilderOperatorFactory *operatorFactory;
};

class HashBuilderWithExprOperator : public Operator {
public:
    HashBuilderWithExprOperator(const DataTypes &buildTypes, const std::vector<int32_t> &buildHashCols,
        const std::vector<ProjFunc> &projectFuncs, HashBuilderOperator *hashBuilderOperator);

    ~HashBuilderWithExprOperator() override;

    int32_t AddInput(omniruntime::vec::VectorBatch *vecBatch) override;

    int32_t GetOutput(omniruntime::vec::VectorBatch **outputVecBatch) override;

    OmniStatus Close() override;

private:
    const DataTypes buildTypes;
    std::vector<int32_t> buildHashCols;
    std::vector<ProjFunc> projectFuncs;
    HashBuilderOperator *hashBuilderOperator;
};
}
}


#endif // __HASH_BUILDER_EXPR_H__
