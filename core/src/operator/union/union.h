/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */
#ifndef __UNION_H__
#define __UNION_H__

#include <vector>
#include <memory>
#include "operator/operator.h"
#include "operator/operator_factory.h"
#include "type/data_type_serializer.h"

namespace omniruntime {
namespace op {
class UnionOperatorFactory : public OperatorFactory {
public:
    UnionOperatorFactory(const type::DataTypes &sourceTypes, int32_t sourceTypesCount, bool isDistinct);

    ~UnionOperatorFactory() override;

    static UnionOperatorFactory *CreateUnionOperatorFactory(const type::DataTypes &sourceTypesField,
        int32_t sourceTypesCountField, bool distinct);

    Operator *CreateOperator() override;

private:
    const type::DataTypes sourceTypes;
    int32_t sourceTypesCount;
    bool isDistinct;
};

class UnionOperator : public Operator {
public:
    UnionOperator(const type::DataTypes &sourceTypes, int32_t sourceTypesCount, bool isDistinct);

    ~UnionOperator() override;

    int32_t AddInput(omniruntime::vec::VectorBatch *vecBatch) override;

    int32_t GetOutput(std::vector<omniruntime::vec::VectorBatch *> &outputPages) override;

    OmniStatus Close() override;

private:
    const type::DataTypes &sourceTypes;
    int32_t sourceTypesCount;
    bool isDistinct;
    std::vector<vec::VectorBatch *> inputVecBatches;
};
}
}

#endif // __UNION_H__
