/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: sort implementations
 */

#ifndef __HASH_BUILDER_EXPR_H__
#define __HASH_BUILDER_EXPR_H__

#include "../operator_factory.h"
#include "../../type/data_types.h"
#include "../projection/projection.h"
#include "../join/hash_builder.h"

namespace omniruntime {
namespace op {
class HashBuilderWithExprOperatorFactory : public OperatorFactory {
public:
    static HashBuilderWithExprOperatorFactory *CreateHashBuilderWithExprOperatorFactory(
        const type::DataTypes &buildTypes, const std::vector<omniruntime::expressions::Expr *> &buildHashKeys,
        int32_t buildHashKeysCount, std::string &filter, int32_t hashTableCount);

    HashBuilderWithExprOperatorFactory(const type::DataTypes &buildTypes,
        const std::vector<omniruntime::expressions::Expr *> &buildHashKeys, int32_t buildHashKeysCount,
        std::string &filter, int32_t hashTableCount);

    ~HashBuilderWithExprOperatorFactory() override;

    omniruntime::op::Operator *CreateOperator() override;

    HashBuilderOperatorFactory *GetHashBuilderOperatorFactory()
    {
        return operatorFactory;
    }

private:
    std::unique_ptr<type::DataTypes> buildTypes;
    std::vector<int32_t> buildHashCols;
    std::vector<std::unique_ptr<RowProjection>> rowProjections;
    std::vector<RowProjFunc> projectFuncs;
    HashBuilderOperatorFactory *operatorFactory;
};

class HashBuilderWithExprOperator : public Operator {
public:
    HashBuilderWithExprOperator(const type::DataTypes &buildTypes, const std::vector<int32_t> &buildHashCols,
        const std::vector<RowProjFunc> &projectFuncs, HashBuilderOperator *HashBuilderOperator);

    ~HashBuilderWithExprOperator() override;

    int32_t AddInput(omniruntime::vec::VectorBatch *vecBatch) override;

    int32_t GetOutput(std::vector<omniruntime::vec::VectorBatch *> &outputPages) override;

    OmniStatus Close() override;

private:
    const omniruntime::type::DataTypes &buildTypes;
    std::vector<int32_t> buildHashCols;
    std::vector<RowProjFunc> projectFuncs;
    HashBuilderOperator *hashBuilderOperator;
};
}
}


#endif // __HASH_BUILDER_EXPR_H__
