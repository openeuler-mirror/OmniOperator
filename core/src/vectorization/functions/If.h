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
            auto falseVec  = args.top(); args.pop();
            auto trueVec   = args.top(); args.pop();
            auto condVec   = args.top(); args.pop();

            if (condVec->GetTypeId() != OMNI_BOOLEAN) {
                OMNI_THROW("If expr Error", "condVec must be a BooleanVector");
            }

            auto *stringTrueVector = static_cast<ConstVector<std::string> *>(trueVec);
            auto *boolVec = static_cast<Vector<bool> *>(condVec);

            auto size = boolVec->GetSize();

            result = VectorHelper::CreateFlatVector(outputType->GetId(), size);

            for (int32_t row = 0; row < size; ++row) {
                if (condVec->IsNull(row)) {
                    result->SetNull(row);
                    continue;
                }
                auto cond = boolVec->GetValue(row);
                if (cond) {
                    if (trueVec->GetEncoding() == OMNI_ENCODING_CONST) {
                        auto res =  static_cast<ConstVector<std::string> *>(trueVec)->GetConstValue();
                        std::string_view sv = res;
                        static_cast<Vector<LargeStringContainer<std::string_view>> *>(result)->SetValue(row,sv);
                    } else if (trueVec->GetEncoding() == OMNI_FLAT) {
                        auto res = static_cast<Vector<LargeStringContainer<std::string_view>> *>(trueVec)->GetValue(row);
                        static_cast<Vector<LargeStringContainer<std::string_view>> *>(result)->SetValue(row,res);
                    } else {
                        auto res = static_cast<Vector<DictionaryContainer<std::string_view, LargeStringContainer>> *>(trueVec)->GetValue(row);
                        static_cast<Vector<LargeStringContainer<std::string_view>> *>(result)->SetValue(row,res);
                    }
                } else {
                    if (falseVec->GetEncoding() == OMNI_ENCODING_CONST) {
                        auto res =  static_cast<ConstVector<std::string> *>(falseVec)->GetConstValue();
                        std::string_view sv = res;
                        static_cast<Vector<LargeStringContainer<std::string_view>> *>(result)->SetValue(row,sv);
                    } else if (falseVec->GetEncoding() == OMNI_FLAT) {
                        auto res = static_cast<Vector<LargeStringContainer<std::string_view>> *>(falseVec)->GetValue(row);
                        static_cast<Vector<LargeStringContainer<std::string_view>> *>(result)->SetValue(row,res);
                    } else {
                        auto res = static_cast<Vector<DictionaryContainer<std::string_view, LargeStringContainer>> *>(falseVec)->GetValue(row);
                        static_cast<Vector<LargeStringContainer<std::string_view>> *>(result)->SetValue(row,res);
                    }
                }
            }
        }
    };
}