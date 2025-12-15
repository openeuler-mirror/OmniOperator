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

class CastFunction : public VectorFunction {
public:
    explicit CastFunction() {}

    void Apply(std::stack<BaseVector *> &args, const DataTypePtr &outputType, BaseVector *&result,
               ExecutionContext *context) const override
    {
        auto inputArg = args.top();
        args.pop();
        if (outputType->GetId() == OMNI_INT) {
            castStringToInter<int32_t>(inputArg, result, outputType);
        } else {
            castStringToInter<int64_t>(inputArg, result, outputType);
        }

    }

    template<typename T>
    void castStringToInter(BaseVector *inputArg, BaseVector *&result, const DataTypePtr &outputType) const {
        auto size = inputArg->GetSize();

        result = VectorHelper::CreateFlatVector(outputType->GetId(), size);

        for (int32_t row = 0; row < size; ++row) {
            if (inputArg->IsNull(row)) {
                result->SetNull(row);
                continue;
            }
            T res;
            std::string_view inputStr;
            if (inputArg->GetEncoding() == OMNI_DICTIONARY) {
                inputStr = static_cast<Vector<DictionaryContainer<std::string_view, LargeStringContainer>> *>(inputArg)->GetValue(row);
            } else {
                inputStr = static_cast<Vector<LargeStringContainer<std::string_view>> *>(inputArg)->GetValue(row);
            }
            type::Status status = ConvertStringToInteger<T>(res, inputStr.data(), static_cast<int>(inputStr.size()));
            //*isNull = status != Status::CONVERT_SUCCESS;
            if (status == type::Status::CONVERT_SUCCESS) {
                static_cast<Vector<T>*>(result)->SetValue(row, res);
            } else {
                result->SetNull(row);
            }
        }
    }
};
}