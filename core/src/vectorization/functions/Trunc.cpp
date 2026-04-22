/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: Trunc function implementation
 */

#include "Trunc.h"
#include "vector/vector.h"
#include "../VectorFunction.h"
#include "vectorization/SelectivityVector.h"
#include "type/date32.h"
#include "type/Timestamp.h"
#include "type/TimestampConversion.h"
#include "type/tz/TimeZoneMap.h"
#include "util/config/QueryConfig.h"
#include "util/type_util.h"
#include "vector/vector_helper.h"
#include "util/bit_util.h"
#include <ctime>
#include <cstring>
#include <string>
#include <string_view>
#include "libboundscheck/include/securec.h"

namespace omniruntime::vectorization {
using namespace omniruntime::vec;
using namespace omniruntime::type;
using namespace omniruntime::tz;

namespace {
class TruncFunction : public VectorFunction {
public:
    void Apply(std::stack<BaseVector *> &args, const DataTypePtr &outputType, BaseVector *&result,
        op::ExecutionContext *context) const override
    {
        if (args.size() < 2) {
            return;
        }
        
        // Stack top = format (VARCHAR), then date / timestamp column
        const auto formatArg = args.top();
        args.pop();
        const auto dateArg = args.top();
        args.pop();
        
        const auto size = dateArg->GetSize();
        
        if (result == nullptr) {
            result = VectorHelper::CreateFlatVector(outputType->GetId(), size);
        }
        
        auto *resultVector = reinterpret_cast<Vector<int32_t> *>(result);
        auto *resultRaw = unsafe::UnsafeVector::GetRawValues(resultVector);
        
        const auto dateTypeId = dateArg->GetTypeId();
        const TimeZone *sessionTz = nullptr;
        if (context != nullptr && context->queryConfig().AdjustTimestampToTimezone()) {
            const std::string tzName = context->queryConfig().SessionTimezone();
            if (!tzName.empty()) {
                sessionTz = locateZone(tzName);
            }
        }
        
        if (dateTypeId == OMNI_DATE32 || dateTypeId == OMNI_INT) {
            // Extract date values
            auto *dateVector = reinterpret_cast<Vector<int32_t> *>(dateArg);
            const auto *dateRaw = unsafe::UnsafeVector::GetRawValues(dateVector);
            const auto *dateNulls = reinterpret_cast<uint64_t *>(unsafe::UnsafeBaseVector::GetNulls(dateArg));
            
            // Get format vector (may be null if const)
            Vector<LargeStringContainer<std::string_view>> *formatVector = nullptr;
            if (formatArg->GetEncoding() != OMNI_ENCODING_CONST) {
                formatVector = dynamic_cast<Vector<LargeStringContainer<std::string_view>> *>(formatArg);
            }
            const auto *formatNulls = reinterpret_cast<uint64_t *>(unsafe::UnsafeBaseVector::GetNulls(formatArg));
            
            // Copy NULL bits from date input to result (so NULL rows are already set to NULL)
            auto *resultNulls = reinterpret_cast<uint64_t *>(unsafe::UnsafeBaseVector::GetNulls(result));
            auto nullsSize = BitUtil::Nbytes(size);
            memcpy(resultNulls, dateNulls, nullsSize);
            
            // Process only non-NULL rows using SelectivityVector
            SelectivityVector rows(size);
            rows.setFromBitsNegate(dateNulls, size);
            
            // Check if format is constant
            bool formatIsConst = (formatArg->GetEncoding() == OMNI_ENCODING_CONST);
            std::string constFormat;
            DateTruncMode constLevel = DateTruncMode::TRUNC_INVALID;
            
            if (formatIsConst) {
                auto *constFormatVec = reinterpret_cast<ConstVector<std::string_view> *>(formatArg);
                std::string_view formatView = constFormatVec->GetConstValue();
                constFormat = std::string(formatView);
                constLevel = Date32::ParseTruncLevel(constFormat);
            }
            
            rows.applyToSelected([&](vector_size_t i) {
                // Check if format is NULL (for non-const format)
                if (!formatIsConst) {
                    if (formatNulls && BitUtil::IsBitSet(formatNulls, i)) {
                        result->SetNull(i);
                        return;
                    }
                }
                
                // Get format string
                std::string formatStr;
                DateTruncMode level;
                
                if (formatIsConst) {
                    level = constLevel;
                } else {
                    std::string_view formatView = formatVector->GetValue(i);
                    formatStr = std::string(formatView);
                    level = Date32::ParseTruncLevel(formatStr);
                }
                
                // Check if format is invalid
                if (level == DateTruncMode::TRUNC_INVALID) {
                    result->SetNull(i);
                    return;
                }
                
                // Check if level is valid for date truncation (must be >= WEEK)
                if (level < DateTruncMode::TRUNC_TO_WEEK) {
                    result->SetNull(i);
                    return;
                }
                
                // Perform truncation
                int32_t daysSinceEpoch = dateRaw[i];
                int32_t truncatedDays = 0;
                
                if (Date32::TruncDate(daysSinceEpoch, level, truncatedDays) == CONVERT_SUCCESS) {
                    resultRaw[i] = truncatedDays;
                    result->SetNotNull(i);
                } else {
                    // If truncation fails, set to null
                    result->SetNull(i);
                }
            });
        } else if (dateTypeId == OMNI_TIMESTAMP || dateTypeId == OMNI_LONG) {
            auto *tsVector = reinterpret_cast<Vector<int64_t> *>(dateArg);
            const auto *tsRaw = unsafe::UnsafeVector::GetRawValues(tsVector);
            const auto *tsNulls = reinterpret_cast<uint64_t *>(unsafe::UnsafeBaseVector::GetNulls(dateArg));
            Vector<LargeStringContainer<std::string_view>> *formatVector = nullptr;
            if (formatArg->GetEncoding() != OMNI_ENCODING_CONST) {
                formatVector = dynamic_cast<Vector<LargeStringContainer<std::string_view>> *>(formatArg);
            }
            const auto *formatNulls = reinterpret_cast<uint64_t *>(unsafe::UnsafeBaseVector::GetNulls(formatArg));
            auto *resultNulls = reinterpret_cast<uint64_t *>(unsafe::UnsafeBaseVector::GetNulls(result));
            const auto nullsSize = BitUtil::Nbytes(size);
            memcpy(resultNulls, tsNulls, nullsSize);
            SelectivityVector rows(size);
            rows.setFromBitsNegate(tsNulls, size);
            const bool formatIsConst = (formatArg->GetEncoding() == OMNI_ENCODING_CONST);
            std::string constFormat;
            DateTruncMode constLevel = DateTruncMode::TRUNC_INVALID;
            if (formatIsConst) {
                auto *constFormatVec = reinterpret_cast<ConstVector<std::string_view> *>(formatArg);
                constFormat = std::string(constFormatVec->GetConstValue());
                constLevel = Date32::ParseTruncLevel(constFormat);
            }
            rows.applyToSelected([&](vector_size_t i) {
                if (!formatIsConst) {
                    if (formatNulls && BitUtil::IsBitSet(formatNulls, i)) {
                        result->SetNull(i);
                        return;
                    }
                }
                DateTruncMode level;
                if (formatIsConst) {
                    level = constLevel;
                } else {
                    std::string_view formatView = formatVector->GetValue(i);
                    level = Date32::ParseTruncLevel(std::string(formatView));
                }
                if (level == DateTruncMode::TRUNC_INVALID || level < DateTruncMode::TRUNC_TO_WEEK) {
                    result->SetNull(i);
                    return;
                }
                int64_t micros = tsRaw[i];
                int32_t daysSinceEpoch =
                    type::util::toDate(Timestamp::fromMicros(micros), sessionTz);
                int32_t truncatedDays = 0;
                if (Date32::TruncDate(daysSinceEpoch, level, truncatedDays) == CONVERT_SUCCESS) {
                    resultRaw[i] = truncatedDays;
                    result->SetNotNull(i);
                } else {
                    result->SetNull(i);
                }
            });
        } else {
            delete formatArg;
            delete dateArg;
            OMNI_THROW("Trunc function Error", "Unsupported input for trunc: " + TypeUtil::TypeToString(dateTypeId));
        }
        delete formatArg;
        delete dateArg;
    }
    
private:
    // Helper: Get string value from vector with different encodings
    std::string_view GetStringValueFromVector(BaseVector *vec, int32_t row) const {
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
            return std::string_view();
        }
    }
};
} // namespace

void RegisterTruncFunction(const std::string &name)
{
    auto tr = std::make_shared<TruncFunction>();
    VectorFunction::RegisterVectorFunction(name, {OMNI_DATE32, OMNI_VARCHAR}, OMNI_DATE32, tr);
    VectorFunction::RegisterVectorFunction(name, {OMNI_INT, OMNI_VARCHAR}, OMNI_DATE32, tr);
    VectorFunction::RegisterVectorFunction(name, {OMNI_TIMESTAMP, OMNI_VARCHAR}, OMNI_DATE32, tr);
    VectorFunction::RegisterVectorFunction(name, {OMNI_LONG, OMNI_VARCHAR}, OMNI_DATE32, tr);
}
}
