/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: Shared implementation for date arithmetic functions (date_add and date_sub)
 */

#include "DateArithmetic.h"
#include "vector/vector.h"
#include "../VectorFunction.h"
#include "vectorization/SelectivityVector.h"
#include "type/date32.h"
#include "vector/vector_helper.h"
#include "util/bit_util.h"
#include <limits>
#include "libboundscheck/include/securec.h"

namespace omniruntime::vectorization {
using namespace omniruntime::vec;
using namespace omniruntime::type;

// Shared implementation namespace for date arithmetic functions
namespace date_arithmetic_impl {
// Helper function to perform date arithmetic (add or subtract days)
// Returns true on success, false on overflow
template <typename TNumDays>
bool ComputeDateArithmetic(int32_t daysSinceEpoch, TNumDays numDays, int32_t &result, bool isSubtract) {
    // Convert numDays to int64_t to handle all integer types safely
    int64_t numDays64 = static_cast<int64_t>(numDays);
    int64_t daysSinceEpoch64 = static_cast<int64_t>(daysSinceEpoch);
    int64_t result64 = 0;
    
    // Perform addition or subtraction based on isSubtract flag
    if (isSubtract) {
        // date_sub: subtract numDays
        if (__builtin_sub_overflow(daysSinceEpoch64, numDays64, &result64)) {
            return false; // Overflow detected
        }
    } else {
        // date_add: add numDays
        if (__builtin_add_overflow(daysSinceEpoch64, numDays64, &result64)) {
            return false; // Overflow detected
        }
    }
    
    // Check if result fits in int32_t
    if (result64 < std::numeric_limits<int32_t>::min() || 
        result64 > std::numeric_limits<int32_t>::max()) {
        return false;
    }
    
    result = static_cast<int32_t>(result64);
    return true;
}

// Template class for DateArithmetic function supporting different integer types for numDays
// isSubtract: true for date_sub, false for date_add
template <typename TNumDays>
class DateArithmeticFunctionImpl : public VectorFunction {
private:
    bool isSubtract_;
    const char* functionName_;
    
public:
    DateArithmeticFunctionImpl(bool isSubtract, const char* functionName) 
        : isSubtract_(isSubtract), functionName_(functionName) {}
    
public:
    void Apply(std::stack<BaseVector *> &args, const DataTypePtr &outputType, BaseVector *&result,
        op::ExecutionContext *context) const override
    {
        if (args.size() < 2) {
            return;
        }
        
        // Extract arguments from stack: numDays (TNumDays), date (DATE32)
        const auto numDaysArg = args.top();
        args.pop();
        const auto dateArg = args.top();
        args.pop();
        
        const auto size = dateArg->GetSize();
        
        // Create result vector if it doesn't exist
        if (result == nullptr) {
            result = VectorHelper::CreateFlatVector(outputType->GetId(), size);
        }
        
        auto *resultVector = reinterpret_cast<Vector<int32_t> *>(result);
        auto *resultRaw = unsafe::UnsafeVector::GetRawValues(resultVector);
        
        // Get input type
        const auto dateTypeId = dateArg->GetTypeId();
        
        if (dateTypeId == OMNI_DATE32 || dateTypeId == OMNI_INT) {
            // Extract date values
            auto *dateVector = reinterpret_cast<Vector<int32_t> *>(dateArg);
            const auto *dateRaw = unsafe::UnsafeVector::GetRawValues(dateVector);
            const auto *dateNulls = reinterpret_cast<uint64_t *>(unsafe::UnsafeBaseVector::GetNulls(dateArg));
            
            // Check if numDays is constant
            bool numDaysIsConst = (numDaysArg->GetEncoding() == OMNI_ENCODING_CONST);
            TNumDays constNumDays = 0;
            const TNumDays *numDaysRaw = nullptr;
            const uint64_t *numDaysNulls = nullptr;
            
            if (numDaysIsConst) {
                // Handle constant numDays
                auto *constNumDaysVec = reinterpret_cast<ConstVector<TNumDays> *>(numDaysArg);
                constNumDays = constNumDaysVec->GetConstValue();
                // For const vector, check if it's null
                if (numDaysArg->IsNull(0)) {
                    // If constant is NULL, set all results to NULL
                    auto *resultNulls = unsafe::UnsafeBaseVector::GetNulls(result);
                    auto nullsSize = BitUtil::Nbytes(size);
                    auto result_code = memset_s(resultNulls, nullsSize, 0xFF, nullsSize);
                    if (result_code != EOK) {
                        OMNI_THROW("DateArithmetic error:", "Failed to set null bits, error code: {}", result_code);
                    }
                    return;
                }
            } else {
                // Handle non-const numDays
                auto *numDaysVector = reinterpret_cast<Vector<TNumDays> *>(numDaysArg);
                numDaysRaw = unsafe::UnsafeVector::GetRawValues(numDaysVector);
                numDaysNulls = reinterpret_cast<uint64_t *>(unsafe::UnsafeBaseVector::GetNulls(numDaysArg));
            }
            
            // Copy NULL bits from date input to result (so NULL rows are already set to NULL)
            auto *resultNulls = reinterpret_cast<uint64_t *>(unsafe::UnsafeBaseVector::GetNulls(result));
            auto nullsSize = BitUtil::Nbytes(size);
            memcpy_s(resultNulls, nullsSize, dateNulls, nullsSize);
            
            // Process only non-NULL rows using SelectivityVector
            SelectivityVector rows(size);
            rows.setFromBitsNegate(dateNulls, size);
            
            rows.applyToSelected([&](vector_size_t i) {
                // Check if numDays is NULL (for non-const case)
                if (!numDaysIsConst) {
                    if (numDaysNulls && BitUtil::IsBitSet(numDaysNulls, i)) {
                        result->SetNull(i);
                        return;
                    }
                }
                
                // Perform date arithmetic operation
                int32_t daysSinceEpoch = dateRaw[i];
                TNumDays numDays = numDaysIsConst ? constNumDays : numDaysRaw[i];
                int32_t resultDays = 0;
                
                if (ComputeDateArithmetic(daysSinceEpoch, numDays, resultDays, isSubtract_)) {
                    resultRaw[i] = resultDays;
                    result->SetNotNull(i);
                } else {
                    // If operation fails (overflow), set to null
                    result->SetNull(i);
                }
            });
        }
    }
};
} // namespace date_arithmetic_impl

// Helper function to register date arithmetic functions (date_add or date_sub)
void RegisterDateArithmeticFunction(const std::string &name, bool isSubtract) {
    using namespace date_arithmetic_impl;
    const char* functionName = isSubtract ? "date_sub" : "date_add";
    // DateArithmetic takes DATE32 and BYTE/SHORT/INT(numDays) and returns DATE32
    // Support BYTE (int8_t, equivalent to TINYINT)
    VectorFunction::RegisterVectorFunction(name, {OMNI_DATE32, OMNI_BYTE}, OMNI_DATE32,
        std::make_shared<DateArithmeticFunctionImpl<int8_t>>(isSubtract, functionName));
    VectorFunction::RegisterVectorFunction(name, {OMNI_INT, OMNI_BYTE}, OMNI_DATE32,
        std::make_shared<DateArithmeticFunctionImpl<int8_t>>(isSubtract, functionName));
    
    // Support SHORT (int16_t, equivalent to SMALLINT)
    VectorFunction::RegisterVectorFunction(name, {OMNI_DATE32, OMNI_SHORT}, OMNI_DATE32,
        std::make_shared<DateArithmeticFunctionImpl<int16_t>>(isSubtract, functionName));
    VectorFunction::RegisterVectorFunction(name, {OMNI_INT, OMNI_SHORT}, OMNI_DATE32,
        std::make_shared<DateArithmeticFunctionImpl<int16_t>>(isSubtract, functionName));
    
    // Support INT (int32_t)
    VectorFunction::RegisterVectorFunction(name, {OMNI_DATE32, OMNI_INT}, OMNI_DATE32,
        std::make_shared<DateArithmeticFunctionImpl<int32_t>>(isSubtract, functionName));
    VectorFunction::RegisterVectorFunction(name, {OMNI_INT, OMNI_INT}, OMNI_DATE32,
        std::make_shared<DateArithmeticFunctionImpl<int32_t>>(isSubtract, functionName));
}
}
