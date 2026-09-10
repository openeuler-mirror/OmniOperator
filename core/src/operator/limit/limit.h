/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */
#ifndef __LIMIT_COUNT_OPERATOR_H__
#define __LIMIT_COUNT_OPERATOR_H__

#include <vector>
#include <memory>
#include "plannode/planNode.h"
#include "operator/operator.h"
#include "operator/operator_factory.h"
#include "type/data_type_serializer.h"

namespace omniruntime {
namespace op {
class LimitOperatorFactory : public OperatorFactory {
public:
    LimitOperatorFactory(int32_t limit, int32_t offset, int32_t columnCount = 0);

    ~LimitOperatorFactory() override;

    static LimitOperatorFactory *CreateLimitOperatorFactory(int32_t limitNum, int32_t offsetNum);

    static LimitOperatorFactory *CreateLimitOperatorFactory(std::shared_ptr<const LimitNode> planNode);

    Operator *CreateOperator() override;

private:
    int32_t limit;
    int32_t offset;
    int32_t columnCount;
};

class LimitOperator : public Operator {
public:
    LimitOperator(int32_t limit, int32_t offset, int32_t columnCount = 0);

    ~LimitOperator() override;

    int32_t AddInput(omniruntime::vec::VectorBatch *vecBatch) override;

    int32_t GetOutput(omniruntime::vec::VectorBatch **resultVecBatch) override;

    OmniStatus Close() override;

    /// Limit can reach isFinished() before noMoreInput (when remainingLimit <= 0).
    bool isEarlyFinish() const override
    {
        return true;
    }

private:
    int32_t remainingLimit;
    int32_t remainingOffset;
    vec::VectorBatch *outputVecBatch;
};
}
}

#endif // __LIMIT_COUNT_OPERATOR_H__
