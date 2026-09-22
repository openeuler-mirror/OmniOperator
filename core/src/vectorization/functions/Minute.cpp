/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: Minute function implementation
 */

#include "Minute.h"
#include "vector/vector.h"
#include "../VectorFunction.h"
#include "vectorization/SelectivityVector.h"
#include "type/Timestamp.h"
#include "vector/vector_helper.h"
#include "util/bit_util.h"
#include "util/TimeUtils.h"
#include "util/config/QueryConfig.h"
#include "type/tz/TimeZoneMap.h"
#include <ctime>
#include <cstring>
#include "libboundscheck/include/securec.h"

namespace omniruntime::vectorization {
using namespace omniruntime::vec;
using namespace omniruntime::type;

namespace {

const tz::TimeZone *getTimeZoneFromConfig(const config::QueryConfig &config)
{
    const auto sessionTzName = config.SessionTimezone();
    if (!sessionTzName.empty()) {
        return tz::locateZone(sessionTzName);
    }
    return nullptr;
}

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
        
        const tz::TimeZone *sessionTz = getTimeZoneFromConfig(context->queryConfig());
        
        // TIMESTAMP is represented as OMNI_LONG (int64_t) at runtime
        if (inputTypeId == OMNI_TIMESTAMP || inputTypeId == OMNI_LONG) {
            // Extract minute from timestamp
            auto *inputVector = reinterpret_cast<Vector<int64_t> *>(inputArg);
            const auto *inputRaw = unsafe::UnsafeVector::GetRawValues(inputVector);
            const auto *inputNulls = reinterpret_cast<uint64_t *>(unsafe::UnsafeBaseVector::GetNulls(inputArg));
            
            // Copy NULL bits from input to result (so NULL rows are already set to NULL)
            auto *resultNulls = reinterpret_cast<uint64_t *>(unsafe::UnsafeBaseVector::GetNulls(result));
            auto nullsSize = BitUtil::Nbytes(size);
            memcpy(resultNulls, inputNulls, nullsSize);
            
            // Process only non-NULL rows using SelectivityVector
            SelectivityVector rows(size);
            rows.setFromBitsNegate(inputNulls, size);
            
            rows.applyToSelected([&](vector_size_t i) {
                // The minute of hour is read off the local clock: a zone offset
                // is not necessarily a whole number of hours, and may not even
                // be a whole number of minutes for historical LMT offsets
                // (Asia/Shanghai is +08:05:43 before 1901).
                Timestamp ts = Timestamp::fromMicros(inputRaw[i]);
                int64_t seconds = util::GetSeconds(ts, sessionTz);
                
                std::tm tmValue;
                if (Timestamp::epochToCalendarUtc(seconds, tmValue)) {
                    resultRaw[i] = static_cast<int32_t>(tmValue.tm_min);
                    result->SetNotNull(i);
                } else {
                    // If conversion fails, set to null
                    result->SetNull(i);
                }
            });
        }
        delete inputArg;
    }
};
} // namespace

void RegisterMinuteFunction(const std::string &name)
{
    // Minute takes TIMESTAMP and returns INT (minute of hour)
    VectorFunction::RegisterVectorFunction(name, {OMNI_TIMESTAMP}, OMNI_INT,
        std::make_shared<MinuteFunction>());
}
}
