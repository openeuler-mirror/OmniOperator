/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: Concat function implementation
 */

#include "ConcatFunction.h"
#include "vector/vector.h"
#include "vector/vector_helper.h"
#include "type/string_Impl.h"
#include <algorithm>
#include <cstring>

namespace omniruntime::vectorization {
using namespace omniruntime::type;
using namespace omniruntime::vec;

void ConcatFunction::Apply(std::stack<BaseVector *> &args, const DataTypePtr &outputType,
    BaseVector *&result, ExecutionContext *context) const
{
    if (args.size() < 2) {
        OMNI_THROW("Concat function Error", "Concat requires at least 2 arguments");
    }

    // Pop exactly 2 arguments from stack (as Gluten's UnfoldConcatStringFunc always creates
    // nested 2-argument concat calls for multi-argument concat)
    // In ExprEval, arguments are pushed in order: first arg (left) pushed first, second arg (right) pushed last
    // So stack order: top = arg2 (right), next = arg1 (left)
    // For concat(str1, str2), we want to concatenate str1 + str2
    std::vector<BaseVector *> argVectors;

    // Pop arg2 (right argument) - top of stack
    BaseVector *arg2 = args.top();
    args.pop();

    // Pop arg1 (left argument) - next on stack
    BaseVector *arg1 = args.top();
    args.pop();

    // Order: arg1 first, then arg2 for correct concatenation order (left + right)
    argVectors.push_back(arg1);
    argVectors.push_back(arg2);

    ApplyConcat(argVectors, result, outputType, context);
}

void ConcatFunction::ApplyConcat(const std::vector<BaseVector *> &argVectors, BaseVector *&result,
    const DataTypePtr &outputType, ExecutionContext *context) const
{
    // Get size from context (handles constant vectors correctly)
    int32_t size = context->GetResultRowSize();
    result = VectorHelper::CreateFlatVector(outputType->GetId(), size);

    // Process each row
    for (int32_t row = 0; row < size; ++row) {
        // Check if any argument is NULL
        // For ConstVector, null flag is stored at index 0
        bool hasNull = false;
        for (auto *argVec : argVectors) {
            int32_t nullCheckIdx = (argVec->GetEncoding() == OMNI_ENCODING_CONST) ? 0 : row;
            if (argVec->IsNull(nullCheckIdx)) {
                hasNull = true;
                break;
            }
        }

        if (hasNull) {
            result->SetNull(row);
            continue;
        }

        // Concatenate all string values
        std::string concatenated;
        for (auto *argVec : argVectors) {
            std::string_view str = GetStringValueFromVector(argVec, row);
            concatenated.append(str.data(), str.size());
        }

        // Set the concatenated result
        std::string_view resultView(concatenated);
        SetStringValueToVector(result, row, const_cast<std::string_view &>(resultView));
    }
}

std::string_view ConcatFunction::GetStringValueFromVector(BaseVector *vec, int32_t row) const
{
    Encoding encoding = vec->GetEncoding();

    if (encoding == OMNI_ENCODING_CONST) {
        auto *constVec = static_cast<ConstVector<std::string_view> *>(vec);
        return constVec->GetConstValue();
    } else if (encoding == OMNI_FLAT) {
        auto *flatVec = static_cast<Vector<LargeStringContainer<std::string_view>> *>(vec);
        return flatVec->GetValue(row);
    } else if (encoding == OMNI_DICTIONARY) {
        auto *dictVec = static_cast<Vector<DictionaryContainer<std::string_view, LargeStringContainer>> *>(vec);
        return dictVec->GetValue(row);
    } else {
        OMNI_THROW("Concat function Error", "Unsupported encoding type for string");
    }
}

void ConcatFunction::SetStringValueToVector(BaseVector *vec, int32_t row, std::string_view &value) const
{
    auto *resultVec = static_cast<Vector<LargeStringContainer<std::string_view>> *>(vec);
    resultVec->SetValue(row, value);
}

} // namespace omniruntime::vectorization
