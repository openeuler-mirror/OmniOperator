/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */
#ifndef __UNION_H__
#define __UNION_H__

#include <vector>
#include <memory>
#include "../operator.h"
#include "../operator_factory.h"
#include "../../vector/vector_type_serializer.h"

using namespace std;
namespace omniruntime {
namespace op {
class UnionOperatorFactory : public OperatorFactory {
public:
    UnionOperatorFactory(const vec::VecTypes &sourceTypes, int32_t sourceTypesCount, bool isDistinct);

    ~UnionOperatorFactory() override;

    static UnionOperatorFactory *CreateUnionOperatorFactory(const vec::VecTypes &sourceTypes, int32_t sourceTypesCount,
        bool isDistinct);

    Operator *CreateOperator() override;

private:
    const vec::VecTypes sourceTypes;
    int32_t sourceTypesCount;
    bool isDistinct;
};

class UnionOperator : public Operator {
public:
    UnionOperator(const vec::VecTypes &sourceTypes, int32_t sourceTypesCount, bool isDistinct);

    ~UnionOperator() override;

    int32_t AddInput(omniruntime::vec::VectorBatch *vecBatch) override;

    int32_t GetOutput(std::vector<omniruntime::vec::VectorBatch *> &outputPages) override;

private:
    const vec::VecTypes &sourceTypes;
    int32_t sourceTypesCount;
    bool isDistinct;
    vector<vec::VectorBatch *> inputVecBatches;
};
}
}

#endif // __UNION_H__
