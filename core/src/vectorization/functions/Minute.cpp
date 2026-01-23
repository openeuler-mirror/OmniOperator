/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: Minute function implementation
 */

#include "Minute.h"
#include "vector/vector.h"
#include "../VectorFunction.h"
#include "vectorization/SelectivityVector.h"
#include "type/date32.h"
#include "type/Timestamp.h"
#include "vector/vector_helper.h"
#include <ctime>

namespace omniruntime::vectorization {
using namespace omniruntime::vec;
using namespace omniruntime::type;

namespace {
class MinuteFunction : public VectorFunction {
public:
    void Apply(std::stack<BaseVector *> &args, const DataTypePtr &outputType, BaseVector *&result,
        op::ExecutionContext *context) const override
    {
        if (args.empty()) {
            return;
        }
        
        const auto inputArg = args.top();
        args.pop();
        
        const auto size = inputArg->GetSize();
        
        // Create result vector if it doesn't exist
        if (result == nullptr) {
            result = VectorHelper::CreateFlatVector(outputType->GetId(), size);
        }
        
        auto *resultVector = reinterpret_cast<Vector<int32_t> *>(result);
        auto *resultRaw = unsafe::UnsafeVector::GetRawValues(resultVector);
        
        // Get input type
        const auto inputTypeId = inputArg->GetTypeId();
        
        if (inputTypeId == OMNI_TIMESTAMP) {
            // Extract minute from timestamp
            auto *inputVector = reinterpret_cast<Vector<int64_t> *>(inputArg);
            const auto *inputRaw = unsafe::UnsafeVector::GetRawValues(inputVector);
            const auto *inputNulls = reinterpret_cast<uint64_t *>(unsafe::UnsafeBaseVector::GetNulls(inputArg));
            
            SelectivityVector rows(size);
            rows.setFromBitsNegate(inputNulls, size);
            
            rows.applyToSelected([&](vector_size_t i) {
                if (!inputArg->IsNull(i)) {
                    // Convert timestamp (microseconds) to seconds
                    int64_t microseconds = inputRaw[i];
                    int64_t seconds = microseconds / 1000000;
                    
                    // Extract minute using Timestamp::epochToCalendarUtc (static method)
                    std::tm tmValue;
                    if (Timestamp::epochToCalendarUtc(seconds, tmValue)) {
                        resultRaw[i] = static_cast<int32_t>(tmValue.tm_min);
                        result->SetNotNull(i);
                    } else {
                        // If conversion fails, set to null
                        result->SetNull(i);
                    }
                } else {
                    result->SetNull(i);
                }
            });
        } else if (inputTypeId == OMNI_DATE32) {
            // For DATE32, minute is always 0 (date has no time component)
            const auto *inputNulls = reinterpret_cast<uint64_t *>(unsafe::UnsafeBaseVector::GetNulls(inputArg));
            
            SelectivityVector rows(size);
            rows.setFromBitsNegate(inputNulls, size);
            
            rows.applyToSelected([&](vector_size_t i) {
                if (!inputArg->IsNull(i)) {
                    resultRaw[i] = 0;
                    result->SetNotNull(i);
                } else {
                    result->SetNull(i);
                }
            });
        }
    }
};
} // namespace

void RegisterMinuteFunction(const std::string &name)
{
    // Minute takes TIMESTAMP or DATE32 and returns INT (minute of hour)
    VectorFunction::RegisterVectorFunction(name, {OMNI_TIMESTAMP}, OMNI_INT,
        std::make_shared<MinuteFunction>());
    VectorFunction::RegisterVectorFunction(name, {OMNI_DATE32}, OMNI_INT,
        std::make_shared<MinuteFunction>());
}
}
