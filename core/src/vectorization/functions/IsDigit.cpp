/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Description: IS_DIGIT function implementation
 *
 * is_digit(string) -> boolean. See String.h for full semantics.
 */

#include "String.h"

#include <memory>

namespace omniruntime::vectorization {

using namespace omniruntime::type;
using namespace omniruntime::vec;
using namespace omniruntime::op;

void IsDigitFunction::Apply(std::stack<BaseVector *> &args, const DataTypePtr &outputType,
    BaseVector *&result, ExecutionContext *context) const
{
    if (args.empty()) {
        OMNI_THROW("IsDigit function Error:", "Expected 1 argument");
    }

    auto *inputArg = args.top();
    args.pop();
    int32_t rowSize = inputArg->GetSize();

    result = VectorHelper::CreateFlatVector(outputType->GetId(), rowSize);
    auto *resultVec = static_cast<Vector<bool> *>(result);

    for (int32_t row = 0; row < rowSize; ++row) {
        // NULL input -> FALSE (Flink semantics: not NULL propagation).
        // The result type is BOOLEAN NOT NULL, so every row is explicitly non-NULL.
        bool value = false;
        if (!inputArg->IsNull(row)) {
            // Get string value based on encoding
            std::string_view str;
            switch (inputArg->GetEncoding()) {
                case OMNI_FLAT: {
                    auto *stringVector = static_cast<Vector<LargeStringContainer<std::string_view>> *>(inputArg);
                    str = stringVector->GetValue(row);
                    break;
                }
                case OMNI_DICTIONARY: {
                    auto *dictVector =
                        static_cast<Vector<DictionaryContainer<std::string_view, LargeStringContainer>> *>(inputArg);
                    str = dictVector->GetValue(row);
                    break;
                }
                case OMNI_ENCODING_CONST: {
                    auto *constVector = static_cast<ConstVector<std::string_view> *>(inputArg);
                    str = constVector->GetConstValue();
                    break;
                }
                default:
                    OMNI_THROW("IsDigit function Error:", "Unsupported encoding type");
            }

            // Check if all characters are digits
            value = !str.empty();
            for (char c : str) {
                if (c < '0' || c > '9') {
                    value = false;
                    break;
                }
            }
        }

        resultVec->SetValue(row, value);
        resultVec->SetNotNull(row);
    }

    delete inputArg;
}

} // namespace omniruntime::vectorization
