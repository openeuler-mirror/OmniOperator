/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */
#ifndef __LIMIT_COUNT_OPERATOR_H__
#define __LIMIT_COUNT_OPERATOR_H__

#include <vector>
#include <memory>
#include "../operator.h"
#include "../operator_factory.h"
#include "../../vector/vector_type_serializer.h"

using namespace std;
namespace omniruntime {
namespace op {
class LimitOperatorFactory : public OperatorFactory {
public:
    LimitOperatorFactory(int64_t limit);

    ~LimitOperatorFactory() override;

    static LimitOperatorFactory *CreateLimitOperatorFactory(int64_t limit);

    Operator *CreateOperator() override;

private:
    int64_t limit;
};

class LimitOperator : public Operator {
public:
    LimitOperator(int64_t limit);

    ~LimitOperator() override;

    int32_t AddInput(omniruntime::vec::VectorBatch *vecBatch) override;

    int32_t GetOutput(std::vector<omniruntime::vec::VectorBatch *> &outputPages) override;

    OmniStatus Close() override;

private:
    int64_t remainingLimit;
    vec::VectorBatch *inputVecBatch;
};
}
}

#endif // __LIMIT_COUNT_OPERATOR_H__
