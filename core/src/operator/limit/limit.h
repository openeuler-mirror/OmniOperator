/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */
#ifndef __LIMIT_COUNT_OPERATOR_H__
#define __LIMIT_COUNT_OPERATOR_H__

#include <vector>
#include <memory>
#include "operator/operator.h"
#include "operator/operator_factory.h"
#include "type/data_type_serializer.h"

namespace omniruntime {
namespace op {
class LimitOperatorFactory : public OperatorFactory {
public:
    explicit LimitOperatorFactory(int64_t limit);

    ~LimitOperatorFactory() override;

    static LimitOperatorFactory *CreateLimitOperatorFactory(int64_t limitNum);

    Operator *CreateOperator() override;

private:
    int64_t limit;
};

class LimitOperator : public Operator {
public:
    explicit LimitOperator(int64_t limit);

    ~LimitOperator() override;

    int32_t AddInput(omniruntime::vec::VectorBatch *vecBatch) override;

    int32_t GetOutput(omniruntime::vec::VectorBatch **resultVecBatch) override;

    OmniStatus Close() override;

private:
    int64_t remainingLimit;
    std::unique_ptr<vec::VectorBatch> outputVecBatch;
};
}
}

#endif // __LIMIT_COUNT_OPERATOR_H__
