/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2023. All rights reserved.
 * Description: Inner supported aggregators header
 */
#include "typed_aggregator.h"
namespace omniruntime {
namespace op {
TypedAggregator::TypedAggregator(const FunctionType aggregateType, const DataTypes &inputTypes,
    const DataTypes &outputTypes, const std::vector<int32_t> &channels, const bool inputRaw, const bool outputPartial,
    const bool isOverflowAsNull)
    : Aggregator(aggregateType, inputTypes, outputTypes, channels, inputRaw, outputPartial, isOverflowAsNull)
{}

BaseVector* TypedAggregator::GetVector(VectorBatch *vectorBatch, const int32_t rowOffset, const int32_t rowCount,
    std::shared_ptr<NullsHelper> *nullMap)
{
    std::vector<BaseVector*> baseVectors;
    auto nullsBuffer = std::make_shared<NullsBuffer>(rowCount, nullptr, 0);

    std::vector<BaseVector*> vectors;
    for (size_t i = 0; i < channels.size(); ++i) {
        vectors.emplace_back(vectorBatch->Get(channels[i]));
    }

    for (size_t i = 0; i < rowCount; ++i) {
        bool isNull = std::any_of(vectors.begin(), vectors.end(), [rowOffset, i](BaseVector* baseVector) {
            return baseVector->IsNull(rowOffset + i);
        });
        nullsBuffer->SetNull(i, isNull);
    }

    auto nullsHelper = nullsBuffer->HasNull() ? std::make_shared<NullsHelper>(nullsBuffer) : nullptr;
    *nullMap = nullsHelper;

    // this is only one vector which is used in partial phase to count intemediate result of agg function
    // which means the index of channels is 0
    auto channel = channels[0];
    auto vector = vectorBatch->Get(channel);
    return vector;
}


bool TypedAggregator::CheckTypes(const std::string &aggName, const DataTypes &inputTypes, const DataTypes &outputTypes,
    const DataTypeId inId, const DataTypeId outId)
{
    if (inputTypes.GetSize() <= 0 || outputTypes.GetSize() <= 0) {
        LogError("Error in %s aggregator: Input or output DataTypes is empty", aggName.c_str());
        return false;
    }

    if (!CheckType(inputTypes.GetType(0)->GetId(), inId)) {
        LogError("Error in %s aggregator: Expecting %s input type. Got %s", aggName.c_str(),
            TypeUtil::TypeToStringLog(inId).c_str(), TypeUtil::TypeToStringLog(inputTypes.GetType(0)->GetId()).c_str());
        return false;
    }
    if (!CheckType(outputTypes.GetType(0)->GetId(), outId)) {
        LogError("Error in %s aggregator: Expecting %s input type. Got %s", aggName.c_str(),
            TypeUtil::TypeToStringLog(outId).c_str(),
            TypeUtil::TypeToStringLog(outputTypes.GetType(0)->GetId()).c_str());
        return false;
    }

    return true;
}
}
}