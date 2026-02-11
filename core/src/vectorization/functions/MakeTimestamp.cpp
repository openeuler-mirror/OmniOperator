/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Description: MakeTimestamp function implementation - make_timestamp(...) -> TIMESTAMP (micros)
 */

#include "MakeTimestamp.h"
#include "vector/vector.h"
#include "../VectorFunction.h"
#include "vectorization/SelectivityVector.h"
#include "type/date32.h"
#include "type/Timestamp.h"
#include "vector/vector_helper.h"
#include "util/bit_util.h"
#include "util/config/QueryConfig.h"
#include "type/tz/TimeZoneMap.h"
#include <chrono>
#include <cmath>
#include <cstring>
#include <limits>
#include "libboundscheck/include/securec.h"

namespace omniruntime::vectorization {
using namespace omniruntime::vec;
using namespace omniruntime::type;

namespace {
static constexpr int64_t kMicrosPerSec = 1'000'000LL;
static constexpr int64_t kSecsPerDay = 86400LL;

const tz::TimeZone *getTimeZoneFromConfig(const config::QueryConfig &config)
{
    if (config.AdjustTimestampToTimezone()) {
        const auto sessionTzName = config.SessionTimezone();
        if (!sessionTzName.empty()) {
            return tz::locateZone(sessionTzName);
        }
    }
    return nullptr;
}

// From (hour, minute, second as double) compute micros since midnight. Returns true if valid.
// Validates hour/minute/second together: 24 only allows 24:00:00; 60 minute only allows xx:60:00; 60 second = leap second.
inline bool fromTime(int32_t hour, int32_t minute, double second, int64_t &microsOut)
{
    if (hour < 0 || hour > 24) {
        return false;
    }
    // 24:00:00 is end-of-day; 24:01:00, 24:00:01 etc. are invalid
    if (hour == 24 && (minute != 0 || second > 1e-9)) {
        return false;
    }
    if (minute < 0 || minute > 60) {
        return false;
    }
    // xx:60:00 is valid (next hour); xx:60:01 etc. are invalid
    if (minute == 60 && second > 1e-9) {
        return false;
    }
    if (std::isnan(second) || std::isinf(second) || second < 0) {
        return false;
    }
    double secs = second;
    if (secs > 60.0 || (secs == 60.0 && (second - 60.0) > 1e-9)) {
        return false;
    }
    int64_t micros = static_cast<int64_t>(std::round(second * kMicrosPerSec));
    if (micros < 0 || micros > 60 * kMicrosPerSec) {
        return false;
    }
    int64_t total = hour * 3600LL * kMicrosPerSec + minute * 60LL * kMicrosPerSec + micros;
    microsOut = total;
    return true;
}
} // namespace

namespace {
class MakeTimestampFunction : public VectorFunction {
public:
    void Apply(std::stack<BaseVector *> &args, const DataTypePtr &outputType, BaseVector *&result,
        op::ExecutionContext *context) const override
    {
        if (args.size() < 6) {
            OMNI_THROW("MakeTimestampFunction Error",
                "make_timestamp requires 6 arguments: year, month, day, hour, min, sec");
        }
        // Create result first (using size from stack top) so result does not reuse an arg's memory
        const auto size = args.top()->GetSize();
        if (result == nullptr) {
            result = VectorHelper::CreateFlatVector(outputType->GetId(), size);
        }

        // Args are pushed in call order (year, month, day, hour, min, sec), so stack top = sec; pop order = sec, min, hour, day, month, year
        BaseVector *secVec = args.top();
        args.pop();
        BaseVector *minVec = args.top();
        args.pop();
        BaseVector *hourVec = args.top();
        args.pop();
        BaseVector *dayVec = args.top();
        args.pop();
        BaseVector *monthVec = args.top();
        args.pop();
        BaseVector *yearVec = args.top();
        args.pop();

        const DataTypeId secTypeId = secVec->GetTypeId();
        const bool secIsDecimal = (secTypeId == OMNI_DECIMAL64);

        auto *resultRaw = unsafe::UnsafeVector::GetRawValues(reinterpret_cast<Vector<int64_t> *>(result));
        auto *yearRaw = unsafe::UnsafeVector::GetRawValues(reinterpret_cast<Vector<int32_t> *>(yearVec));
        auto *monthRaw = unsafe::UnsafeVector::GetRawValues(reinterpret_cast<Vector<int32_t> *>(monthVec));
        auto *dayRaw = unsafe::UnsafeVector::GetRawValues(reinterpret_cast<Vector<int32_t> *>(dayVec));
        auto *hourRaw = unsafe::UnsafeVector::GetRawValues(reinterpret_cast<Vector<int32_t> *>(hourVec));
        auto *minRaw = unsafe::UnsafeVector::GetRawValues(reinterpret_cast<Vector<int32_t> *>(minVec));
        const int64_t *secRawDecimal = secIsDecimal
            ? unsafe::UnsafeVector::GetRawValues(reinterpret_cast<Vector<int64_t> *>(secVec))
            : nullptr;
        const double *secRawDouble = secIsDecimal
            ? nullptr
            : unsafe::UnsafeVector::GetRawValues(reinterpret_cast<Vector<double> *>(secVec));

        const auto *yearNulls = reinterpret_cast<uint64_t *>(unsafe::UnsafeBaseVector::GetNulls(yearVec));
        auto *resultNulls = reinterpret_cast<uint64_t *>(unsafe::UnsafeBaseVector::GetNulls(result));
        auto nullsSize = BitUtil::Nbytes(size);
        memcpy(resultNulls, yearNulls, nullsSize);

        // Session timezone: treat (year,month,day,hour,min,sec) as local time and convert to UTC.
        auto sessionTz = getTimeZoneFromConfig(context->queryConfig());

        for (vector_size_t i = 0; i < size; ++i) {
            if (result->IsNull(i)) {
                continue;
            }
            if (monthVec->IsNull(i) || dayVec->IsNull(i) || hourVec->IsNull(i) || minVec->IsNull(i) || secVec->IsNull(i)) {
                result->SetNull(i);
                continue;
            }
            int32_t year = yearRaw[i];
            int32_t month = monthRaw[i];
            int32_t day = dayRaw[i];
            int32_t hour = hourRaw[i];
            int32_t minute = minRaw[i];
            double second;
            if (secIsDecimal) {
                // Spark sends seconds as Decimal(scale=6), raw value = seconds * 1e6
                second = static_cast<double>(secRawDecimal[i]) / static_cast<double>(kMicrosPerSec);
            } else {
                second = secRawDouble[i];
            }

            int64_t daysSinceEpoch = 0;
            if (!Date32::DaysSinceEpochFromDate(year, month, day, daysSinceEpoch)) {
                result->SetNull(i);
                continue;
            }
            int64_t microsSinceMidnight = 0;
            if (!fromTime(hour, minute, second, microsSinceMidnight)) {
                result->SetNull(i);
                continue;
            }
            int64_t microsSinceEpoch = daysSinceEpoch * kSecsPerDay * kMicrosPerSec + microsSinceMidnight;
            if (sessionTz != nullptr) {
                // Treat (year,month,day,hour,min,sec) as local time; convert to UTC micros.
                const int64_t localSec = daysSinceEpoch * kSecsPerDay + microsSinceMidnight / kMicrosPerSec;
                const int64_t microsFraction = microsSinceMidnight % kMicrosPerSec;
                try {
                    const auto sysSec = sessionTz->to_sys(std::chrono::seconds(localSec));
                    resultRaw[i] = static_cast<int64_t>(sysSec.count()) * kMicrosPerSec + microsFraction;
                } catch (...) {
                    result->SetNull(i);
                    continue;
                }
            } else {
                resultRaw[i] = microsSinceEpoch;
            }
            result->SetNotNull(i);
        }

        delete secVec;
        delete minVec;
        delete hourVec;
        delete dayVec;
        delete monthVec;
        delete yearVec;
    }
};
} // namespace

void RegisterMakeTimestampFunction(const std::string &name)
{
    auto impl = std::make_shared<MakeTimestampFunction>();
    VectorFunction::RegisterVectorFunction(name,
        {OMNI_INT, OMNI_INT, OMNI_INT, OMNI_INT, OMNI_INT, OMNI_DOUBLE}, OMNI_TIMESTAMP,
        impl);
    // Spark sends seconds as Decimal(scale=6), so register DECIMAL64 for 6th argument
    VectorFunction::RegisterVectorFunction(name,
        {OMNI_INT, OMNI_INT, OMNI_INT, OMNI_INT, OMNI_INT, OMNI_DECIMAL64}, OMNI_TIMESTAMP,
        impl);
}
}
