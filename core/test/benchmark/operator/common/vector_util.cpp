/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#include "vector_util.h"
#include "operator/aggregation/group_aggregation.h"

using namespace omniruntime::op;

namespace om_benchmark {

VectorBatch *CreateSequenceVectorBatch(const std::vector<DataTypePtr> &types, int length)
{
    auto *vectorBatch = new VectorBatch(length);
    for (const auto &type : types) {
        auto *vector = VectorHelper::CreateVector(OMNI_FLAT, type->GetId(), length).release();
        for (int index = 0; index < length; ++index) {
            switch (type->GetId()) {
                case OMNI_INT:
                case OMNI_DATE32:
                case OMNI_LONG:
                case OMNI_DOUBLE:
                case OMNI_DECIMAL64: {
                    auto val = index;
                    VectorHelper::SetValue(vector, index, &val, type->GetId());
                } break;
                case OMNI_VARCHAR:
                case OMNI_CHAR: {
                    auto val = std::to_string(index);
                    VectorHelper::SetValue(vector, index, &val, type->GetId());
                } break;
                case OMNI_DECIMAL128: {
                    Decimal128 val = Decimal128(0, index);
                    VectorHelper::SetValue(vector, index, &val, type->GetId());
                } break;
                default:
                    LogError("No such data type %d", type->GetId());
                    break;
            }
        }
        vectorBatch->Append(vector);
    }
    return vectorBatch;
}

VectorBatch *CreateSequenceVectorBatchWithDictionaryVector(const std::vector<DataTypePtr> &types, int length)
{
    auto *vectorBatch = new VectorBatch(length);
    int ratio = 5;
    for (const auto &type : types) {
        auto *inner = VectorHelper::CreateVector(OMNI_FLAT, type->GetId(), length / ratio).release();
        for (int index = 0; index < length / ratio; ++index) {
            switch (type->GetId()) {
                case OMNI_INT:
                case OMNI_DATE32:
                case OMNI_LONG:
                case OMNI_DOUBLE:
                case OMNI_DECIMAL64: {
                    auto val = index;
                    VectorHelper::SetValue(inner, index, &val, type->GetId());
                } break;
                case OMNI_VARCHAR:
                case OMNI_CHAR: {
                    auto val = std::to_string(index);
                    VectorHelper::SetValue(inner, index, &val, type->GetId());
                } break;
                case OMNI_DECIMAL128: {
                    Decimal128 val = Decimal128(0, index);
                    VectorHelper::SetValue(inner, index, &val, type->GetId());
                } break;
                default:
                    LogError("No such data type %d", type->GetId());
                    break;
            }
        }
        std::vector<int32_t> ids(length);
        for (int k = 0; k < length; ++k) {
            ids[k] = (k % ratio);
        }
        auto vector = VectorHelper::CreateDictionaryVector(ids.data(), (int32_t)ids.size(), inner, type->GetId());
        delete inner;
        vectorBatch->Append(vector.release());
    }
    return vectorBatch;
}

VectorBatch *CreateVectorBatch(uint32_t encoding, const std::vector<DataTypePtr> &types, std::string &prefix,
    const std::vector<std::vector<int32_t>> &values,int rowCount)
{
    auto *vectorBatch = new VectorBatch(rowCount);
    for (int i = 0; i < (int32_t)types.size(); ++i) {
        auto *vector = VectorHelper::CreateVector(OMNI_FLAT, types[i]->GetId(), (int32_t)values[i].size()).release();
        for (int index = 0; index < (int32_t)values[i].size(); ++index) {
            switch (types[i]->GetId()) {
                case OMNI_INT:
                case OMNI_DATE32:
                case OMNI_LONG:
                case OMNI_DECIMAL64:
                case OMNI_DOUBLE:
                case OMNI_BOOLEAN: {
                    auto val = values[i][index];
                    VectorHelper::SetValue(vector, index, &val, types[i]->GetId());
                } break;
                case OMNI_VARCHAR:
                case OMNI_CHAR: {
                    auto val = prefix + std::to_string(values[i][index]);
                    VectorHelper::SetValue(vector, index, &val, types[i]->GetId());
                } break;
                case OMNI_DECIMAL128: {
                    auto val = Decimal128(0, values[i][index]);
                    VectorHelper::SetValue(vector, index, &val, types[i]->GetId());
                } break;
                default:
                    LogError("No such data type %d", types[i]->GetId());
                    break;
            }
        }
        if (encoding == OMNI_DICTIONARY) {
            std::vector<int32_t> ids((int32_t)values[i].size());
            for (int j = 0; j < (int32_t)values[i].size(); ++j) {
                ids[j] = j;
            }
            vectorBatch->Append(
                VectorHelper::CreateDictionaryVector(ids.data(), (int32_t)ids.size(), vector, types[i]->GetId())
                                       .release());
            delete vector;
            continue;
        }
        vectorBatch->Append(vector);
    }

    return vectorBatch;
}

std::vector<VectorBatchSupplier> VectorBatchToVectorBatchSupplier(std::vector<VectorBatch*> vectorBatches){
    auto suppliers = std::vector<VectorBatchSupplier>(vectorBatches.size());

    for (int i = 0; i < (int32_t)suppliers.size(); ++i) {
        auto vectorBatch = vectorBatches[i];
        suppliers[i] = [vectorBatch](){ return vectorBatch;};
    }
    return suppliers;
}
}
