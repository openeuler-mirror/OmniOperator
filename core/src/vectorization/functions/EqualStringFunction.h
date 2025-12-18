/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: visitor class for expressions
 */

#include "Comparisons.h"
#include "vectorization/SelectivityVector.h"

namespace omniruntime::vectorization {
using namespace omniruntime::vec;

class EqualStringFunction final : public VectorFunction {

    bool SupportsFlatNoNullsFastPath() const override
    {
        return true;
    }

    void Apply(std::stack<BaseVector *> &args, const DataTypePtr &outputType, BaseVector *&result,
               op::ExecutionContext *context) const override
    {
        auto rightArg = args.top();
        args.pop();
        auto leftArg = args.top();
        args.pop();
        auto rowSize = context->GetResultRowSize();
        auto *flatResult = reinterpret_cast<Vector<bool> *>(VectorHelper::CreateFlatVector(OMNI_BOOLEAN, rowSize));
        auto rows = std::make_shared<SelectivityVector>(rowSize);
        if (leftArg->GetEncoding() == OMNI_FLAT && rightArg->GetEncoding() == OMNI_FLAT) {
            // Fast path for (flat, flat).
            rows->applyToSelected([&](vector_size_t i) {
                flatResult->SetValue(i, static_cast<Vector<LargeStringContainer<std::string_view>> *>(rightArg)->GetValue(i) ==
                                        static_cast<Vector<LargeStringContainer<std::string_view>> *>(leftArg)->GetValue(i));
            });
        } else if (leftArg->GetEncoding() == OMNI_ENCODING_CONST && rightArg->GetEncoding() == OMNI_FLAT) {
            // Fast path for (const, flat).
            auto constant = reinterpret_cast<ConstVector<std::string> *>(leftArg)->GetConstValue();
            rows->applyToSelected([&](vector_size_t i) {
                flatResult->SetValue(i, constant == static_cast<Vector<LargeStringContainer<std::string_view>> *>(rightArg)->GetValue(i));
            });
        } else if (leftArg->GetEncoding() == OMNI_FLAT && rightArg->GetEncoding() == OMNI_ENCODING_CONST) {
            // Fast path for (flat, const).
            auto constant = reinterpret_cast<ConstVector<std::string> *>(rightArg)->GetConstValue();
            rows->applyToSelected([&](vector_size_t i) {
                flatResult->SetValue(i, constant == static_cast<Vector<LargeStringContainer<std::string_view>> *>(leftArg)->GetValue(i));
            });
        } else {
            // Path if one or more arguments are encoded.
            OMNI_THROW("ComparisonFunction Error:", "Not support decoded vector");
        }
        result = flatResult;
        delete rightArg;
        delete leftArg;
    }
};
}