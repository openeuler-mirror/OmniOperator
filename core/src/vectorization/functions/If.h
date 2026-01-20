/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: visitor class for expressions
 */

#pragma once
#include "vectorization/VectorFunction.h"
#include "vector/array_vector.h"
#include "type/data_operations.h"
#include "util/debug.h"
#include <vector>
#include <string_view>

namespace omniruntime::vectorization {
using namespace omniruntime::type;
using namespace omniruntime::vec;
using namespace omniruntime::op;

class IfFunction : public VectorFunction {
public:
    explicit IfFunction() {}

    void Apply(std::stack<BaseVector *> &args, const DataTypePtr &outputType, BaseVector *&result,
        ExecutionContext *context) const override
    {
        auto falseVec = args.top();
        args.pop();
        auto trueVec = args.top();
        args.pop();
        auto condVec = args.top();
        args.pop();
        auto *boolVec = static_cast<Vector<bool> *>(condVec);
        auto size = boolVec->GetSize();
        result = VectorHelper::CreateFlatVector(outputType->GetId(), size);
        for (int32_t row = 0; row < size; ++row) {
            BaseVector* temp = nullptr;
            if (boolVec->IsNull(row)) {
                temp = falseVec;
            } else {
                temp = boolVec->GetValue(row) ? trueVec : falseVec;
            }
            if (temp->IsNull(row)) {
                result->SetNull(row);
                continue;
            }
            switch (outputType->GetId()) {
                case OMNI_BOOLEAN :
                {
                    auto res = static_cast<Vector<bool> *>(temp)->GetValue(row);
                    VectorHelper::SetValue(result, row, &res);
                    break;
                }
                case OMNI_BYTE :
                {
                    auto res = static_cast<Vector<int8_t> *>(temp)->GetValue(row);
                    VectorHelper::SetValue(result, row, &res);
                    break;
                }
                case OMNI_SHORT :
                {
                    auto res = static_cast<Vector<int16_t> *>(temp)->GetValue(row);
                    VectorHelper::SetValue(result, row, &res);
                    break;
                }
                case OMNI_INT :
                case OMNI_DATE32 :
                {
                    auto res = static_cast<Vector<int32_t> *>(temp)->GetValue(row);
                    VectorHelper::SetValue(result, row, &res);
                    break;
                }
                case OMNI_LONG :
                case OMNI_DATE64 :
                case OMNI_TIMESTAMP :
                case OMNI_DECIMAL64 :
                {
                    auto res = static_cast<Vector<int64_t> *>(temp)->GetValue(row);
                    VectorHelper::SetValue(result, row, &res);
                    break;
                }
                case OMNI_FLOAT :
                {
                    auto res = static_cast<Vector<float> *>(temp)->GetValue(row);
                    VectorHelper::SetValue(result, row, &res);
                    break;
                }
                case OMNI_DOUBLE :
                {
                    auto res = static_cast<Vector<double> *>(temp)->GetValue(row);
                    VectorHelper::SetValue(result, row, &res);
                    break;
                }
                case OMNI_VARCHAR :
                {
                    auto res = static_cast<Vector<LargeStringContainer<std::string_view>> *>(temp)->GetValue(row);
                    VectorHelper::SetValue(result, row, &res);
                    break;
                }
                case OMNI_DECIMAL128 :
                {
                    auto res = static_cast<Vector<Decimal128> *>(temp)->GetValue(row);
                    VectorHelper::SetValue(result, row, &res);
                    break;
                }
                case OMNI_ARRAY :
                {
                    auto res = static_cast<Vector<BaseVector*> *>(temp)->GetValue(row);
                    VectorHelper::SetValue(result, row, &res);
                    break;
                }
                default :
                    OMNI_THROW("If expr Error", "Unsupported output type Id:" + outputType->GetId());
            }
        }
    }
};
}
