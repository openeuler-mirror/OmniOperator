/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: Least and Greatest function implementation for vectorized comparison expressions
 */

#include "LeastGreatest.h"
#include "vector/vector.h"
#include <limits>
#include <cstring>
#include <algorithm>
#include <cmath>
#include <iostream>

namespace omniruntime::vectorization {
    using namespace omniruntime::type;
    using namespace omniruntime::vec;

// Helper function for comparing values with NaN handling (Spark SQL semantics)
// In Spark, NaN is considered greater than any other value
template<CompareMode Mode, typename T>
static bool shouldReplace(const T &newVal, const T &currentVal) {
    if constexpr (std::is_floating_point_v<T>) {
        // NaN handling: NaN is considered greater than any other value
        bool newIsNan = std::isnan(newVal);
        bool currentIsNan = std::isnan(currentVal);
        
        if (Mode == CompareMode::GREATEST) {
            // For greatest: NaN > everything, so if new is NaN, replace
            if (newIsNan) return true;
            if (currentIsNan) return false;
            return newVal > currentVal;
        } else {
            // For least: NaN > everything, so NaN is never the least
            if (newIsNan) return false;
            if (currentIsNan) return true;
            return newVal < currentVal;
        }
    } else {
        if constexpr (Mode == CompareMode::GREATEST) {
            return newVal > currentVal;
        } else {
            return newVal < currentVal;
        }
    }
}

template<CompareMode Mode>
void LeastGreatestFunction<Mode>::Apply(std::stack<BaseVector *> &args, const DataTypePtr &outputType, 
                            BaseVector *&result, ExecutionContext *context) const {
    const char* funcName = (Mode == CompareMode::GREATEST) ? "Greatest" : "Least";
    // Collect all arguments from stack (they are in reverse order)
    std::vector<BaseVector *> argVectors;
    argVectors.push_back(args.top());
    args.pop();
    argVectors.push_back(args.top());
    args.pop();
    std::reverse(argVectors.begin(), argVectors.end());
    DispatchCompare(argVectors, outputType, result);
}

template<CompareMode Mode>
void LeastGreatestFunction<Mode>::DispatchCompare(const std::vector<BaseVector *> &argVectors, 
                                        const DataTypePtr &outputType, BaseVector *&result) const {
    DataTypeId outputTypeId = outputType->GetId();
    
    if (TypeUtil::IsStringType(outputTypeId)) {
        CompareString(argVectors, result, outputType);
    } else if (outputTypeId == OMNI_BOOLEAN) {
        CompareBoolean(argVectors, result, outputType);
    } else {
        // Numeric types
        switch (outputTypeId) {
            case OMNI_BYTE:
                CompareNumeric<int8_t>(argVectors, result, outputType);
                break;
            case OMNI_SHORT:
                CompareNumeric<int16_t>(argVectors, result, outputType);
                break;
            case OMNI_INT:
            case OMNI_DATE32:
                CompareNumeric<int32_t>(argVectors, result, outputType);
                break;
            case OMNI_LONG:
            case OMNI_TIMESTAMP:
            case OMNI_DECIMAL64:
                CompareNumeric<int64_t>(argVectors, result, outputType);
                break;
            case OMNI_FLOAT:
                CompareNumeric<float>(argVectors, result, outputType);
                break;
            case OMNI_DOUBLE:
                CompareNumeric<double>(argVectors, result, outputType);
                break;
            case OMNI_DECIMAL128:
                CompareNumeric<int128_t>(argVectors, result, outputType);
                break;
            case OMNI_VARBINARY:
            case OMNI_VARCHAR:
            case OMNI_CHAR:
                CompareString(argVectors, result, outputType);
                break;
            default:
                OMNI_THROW("LeastGreatest function Error", 
                        "Unsupported output type: " + TypeUtil::TypeToString(outputTypeId));
        }
    }
}

template<CompareMode Mode>
template<typename T>
void LeastGreatestFunction<Mode>::CompareNumeric(const std::vector<BaseVector *> &argVectors, 
                                    BaseVector *&result, const DataTypePtr &outputType) const {
    auto size = argVectors[0]->GetSize();
    result = VectorHelper::CreateFlatVector(outputType->GetId(), size);
    
    for (int32_t row = 0; row < size; ++row) {
        bool hasNonNull = false;
        T bestValue{};
        
        for (size_t argIdx = 0; argIdx < argVectors.size(); ++argIdx) {
            if (!argVectors[argIdx]->IsNull(row)) {
                T value = GetValueFromVector<T>(argVectors[argIdx], row);
                if (!hasNonNull) {
                    bestValue = value;
                    hasNonNull = true;
                } else if (shouldReplace<Mode, T>(value, bestValue)) {
                    bestValue = value;
                }
            }
        }
        
        if (hasNonNull) {
            SetValueToVector(result, row, bestValue);
        } else {
            // All arguments are NULL
            result->SetNull(row);
        }
    }
}

template<CompareMode Mode>
void LeastGreatestFunction<Mode>::CompareString(const std::vector<BaseVector *> &argVectors, 
                                    BaseVector *&result, const DataTypePtr &outputType) const {
    auto size = argVectors[0]->GetSize();
    result = VectorHelper::CreateFlatVector(outputType->GetId(), size);
    
    for (int32_t row = 0; row < size; ++row) {
        bool hasNonNull = false;
        std::string_view bestValue;
        
        for (size_t argIdx = 0; argIdx < argVectors.size(); ++argIdx) {
            if (!argVectors[argIdx]->IsNull(row)) {
                std::string_view value = GetStringValueFromVector(argVectors[argIdx], row);
                if (!hasNonNull) {
                    bestValue = value;
                    hasNonNull = true;
                } else {
                    bool replace = (Mode == CompareMode::GREATEST) ? (value > bestValue) : (value < bestValue);
                    if (replace) {
                        bestValue = value;
                    }
                }
            }
        }
        
        if (hasNonNull) {
            SetStringValueToVector(result, row, const_cast<std::string_view &>(bestValue));
        } else {
            // All arguments are NULL
            result->SetNull(row);
        }
    }
}

template<CompareMode Mode>
void LeastGreatestFunction<Mode>::CompareBoolean(const std::vector<BaseVector *> &argVectors, 
                                    BaseVector *&result, const DataTypePtr &outputType) const {
    auto size = argVectors[0]->GetSize();
    result = VectorHelper::CreateFlatVector(outputType->GetId(), size);
    
    for (int32_t row = 0; row < size; ++row) {
        bool hasNonNull = false;
        bool bestValue = false;
        
        for (size_t argIdx = 0; argIdx < argVectors.size(); ++argIdx) {
            if (!argVectors[argIdx]->IsNull(row)) {
                bool value = GetValueFromVector<bool>(argVectors[argIdx], row);
                if (!hasNonNull) {
                    bestValue = value;
                    hasNonNull = true;
                } else {
                    // For greatest: true > false, so replace if value is true and current is false
                    // For least: false < true, so replace if value is false and current is true
                    bool replace = (Mode == CompareMode::GREATEST) ? 
                                   (value && !bestValue) : (!value && bestValue);
                    if (replace) {
                        bestValue = value;
                    }
                }
            }
        }
        
        if (hasNonNull) {
            SetValueToVector(result, row, bestValue);
        } else {
            // All arguments are NULL
            result->SetNull(row);
        }
    }
}

template<CompareMode Mode>
template<typename T>
T LeastGreatestFunction<Mode>::GetValueFromVector(BaseVector *vec, int32_t row) const {
    Encoding encoding = vec->GetEncoding();
    
    if (encoding == OMNI_ENCODING_CONST) {
        auto *constVec = static_cast<ConstVector<T> *>(vec);
        return constVec->GetConstValue();
    } else if (encoding == OMNI_FLAT) {
        auto *flatVec = static_cast<Vector<T> *>(vec);
        return flatVec->GetValue(row);
    } else if (encoding == OMNI_DICTIONARY) {
        auto *dictVec = static_cast<Vector<DictionaryContainer<T>> *>(vec);
        return dictVec->GetValue(row);
    } else {
        OMNI_THROW("LeastGreatest function Error", "Unsupported encoding type");
    }
}

template<CompareMode Mode>
std::string_view LeastGreatestFunction<Mode>::GetStringValueFromVector(BaseVector *vec, int32_t row) const {
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
        OMNI_THROW("LeastGreatest function Error", "Unsupported encoding type for string");
    }
}

template<CompareMode Mode>
template<typename T>
void LeastGreatestFunction<Mode>::SetValueToVector(BaseVector *vec, int32_t row, const T &value) const {
    auto *resultVec = static_cast<Vector<T> *>(vec);
    resultVec->SetValue(row, value);
}

template<CompareMode Mode>
void LeastGreatestFunction<Mode>::SetStringValueToVector(BaseVector *vec, int32_t row, 
                                            std::string_view &value) const {
    auto *resultVec = static_cast<Vector<LargeStringContainer<std::string_view>> *>(vec);
    resultVec->SetValue(row, value);
}

// Factory functions
std::shared_ptr<VectorFunction> makeGreatest(const std::string &name, 
    const std::vector<DataTypeId> &inputArgs, const config::QueryConfig &) {
    return std::make_shared<GreatestFunction>();
}

std::shared_ptr<VectorFunction> makeLeast(const std::string &name, 
    const std::vector<DataTypeId> &inputArgs, const config::QueryConfig &) {
    return std::make_shared<LeastFunction>();
}

// Helper function to generate signatures for both least and greatest
static std::vector<std::shared_ptr<codegen::FunctionSignature>> GenerateSignatures(const std::string &funcName) {
    std::vector<std::shared_ptr<codegen::FunctionSignature>> signatures;
    
    std::vector<DataTypeId> supportedTypes = {
        OMNI_BOOLEAN,
        OMNI_BYTE,
        OMNI_SHORT,
        OMNI_INT,
        OMNI_LONG,
        OMNI_FLOAT,
        OMNI_DOUBLE,
        OMNI_VARCHAR,
        OMNI_CHAR,
        OMNI_DATE32,
        OMNI_TIMESTAMP,
        OMNI_DECIMAL64,
        OMNI_DECIMAL128,
        OMNI_VARBINARY
    };
    
    for (const auto &typeId : supportedTypes) {
        signatures.emplace_back(
            codegen::FunctionSignature::Variadic(funcName, typeId, typeId, 2));
    }
    
    return signatures;
}

std::vector<std::shared_ptr<codegen::FunctionSignature>> GreatestSignatures() {
    return GenerateSignatures("Greatest");
}

std::vector<std::shared_ptr<codegen::FunctionSignature>> LeastSignatures() {
    return GenerateSignatures("Least");
}

// Explicit template instantiations for GREATEST
template class LeastGreatestFunction<CompareMode::GREATEST>;
template void LeastGreatestFunction<CompareMode::GREATEST>::CompareNumeric<int8_t>(const std::vector<BaseVector *> &, BaseVector *&, const DataTypePtr &) const;
template void LeastGreatestFunction<CompareMode::GREATEST>::CompareNumeric<int16_t>(const std::vector<BaseVector *> &, BaseVector *&, const DataTypePtr &) const;
template void LeastGreatestFunction<CompareMode::GREATEST>::CompareNumeric<int32_t>(const std::vector<BaseVector *> &, BaseVector *&, const DataTypePtr &) const;
template void LeastGreatestFunction<CompareMode::GREATEST>::CompareNumeric<int64_t>(const std::vector<BaseVector *> &, BaseVector *&, const DataTypePtr &) const;
template void LeastGreatestFunction<CompareMode::GREATEST>::CompareNumeric<float>(const std::vector<BaseVector *> &, BaseVector *&, const DataTypePtr &) const;
template void LeastGreatestFunction<CompareMode::GREATEST>::CompareNumeric<double>(const std::vector<BaseVector *> &, BaseVector *&, const DataTypePtr &) const;
template void LeastGreatestFunction<CompareMode::GREATEST>::CompareNumeric<int128_t>(const std::vector<BaseVector *> &, BaseVector *&, const DataTypePtr &) const;

template int8_t LeastGreatestFunction<CompareMode::GREATEST>::GetValueFromVector<int8_t>(BaseVector *, int32_t) const;
template int16_t LeastGreatestFunction<CompareMode::GREATEST>::GetValueFromVector<int16_t>(BaseVector *, int32_t) const;
template int32_t LeastGreatestFunction<CompareMode::GREATEST>::GetValueFromVector<int32_t>(BaseVector *, int32_t) const;
template int64_t LeastGreatestFunction<CompareMode::GREATEST>::GetValueFromVector<int64_t>(BaseVector *, int32_t) const;
template float LeastGreatestFunction<CompareMode::GREATEST>::GetValueFromVector<float>(BaseVector *, int32_t) const;
template double LeastGreatestFunction<CompareMode::GREATEST>::GetValueFromVector<double>(BaseVector *, int32_t) const;
template int128_t LeastGreatestFunction<CompareMode::GREATEST>::GetValueFromVector<int128_t>(BaseVector *, int32_t) const;
template bool LeastGreatestFunction<CompareMode::GREATEST>::GetValueFromVector<bool>(BaseVector *, int32_t) const;

template void LeastGreatestFunction<CompareMode::GREATEST>::SetValueToVector<int8_t>(BaseVector *, int32_t, const int8_t &) const;
template void LeastGreatestFunction<CompareMode::GREATEST>::SetValueToVector<int16_t>(BaseVector *, int32_t, const int16_t &) const;
template void LeastGreatestFunction<CompareMode::GREATEST>::SetValueToVector<int32_t>(BaseVector *, int32_t, const int32_t &) const;
template void LeastGreatestFunction<CompareMode::GREATEST>::SetValueToVector<int64_t>(BaseVector *, int32_t, const int64_t &) const;
template void LeastGreatestFunction<CompareMode::GREATEST>::SetValueToVector<float>(BaseVector *, int32_t, const float &) const;
template void LeastGreatestFunction<CompareMode::GREATEST>::SetValueToVector<double>(BaseVector *, int32_t, const double &) const;
template void LeastGreatestFunction<CompareMode::GREATEST>::SetValueToVector<int128_t>(BaseVector *, int32_t, const int128_t &) const;
template void LeastGreatestFunction<CompareMode::GREATEST>::SetValueToVector<bool>(BaseVector *, int32_t, const bool &) const;

// Explicit template instantiations for LEAST
template class LeastGreatestFunction<CompareMode::LEAST>;
template void LeastGreatestFunction<CompareMode::LEAST>::CompareNumeric<int8_t>(const std::vector<BaseVector *> &, BaseVector *&, const DataTypePtr &) const;
template void LeastGreatestFunction<CompareMode::LEAST>::CompareNumeric<int16_t>(const std::vector<BaseVector *> &, BaseVector *&, const DataTypePtr &) const;
template void LeastGreatestFunction<CompareMode::LEAST>::CompareNumeric<int32_t>(const std::vector<BaseVector *> &, BaseVector *&, const DataTypePtr &) const;
template void LeastGreatestFunction<CompareMode::LEAST>::CompareNumeric<int64_t>(const std::vector<BaseVector *> &, BaseVector *&, const DataTypePtr &) const;
template void LeastGreatestFunction<CompareMode::LEAST>::CompareNumeric<float>(const std::vector<BaseVector *> &, BaseVector *&, const DataTypePtr &) const;
template void LeastGreatestFunction<CompareMode::LEAST>::CompareNumeric<double>(const std::vector<BaseVector *> &, BaseVector *&, const DataTypePtr &) const;
template void LeastGreatestFunction<CompareMode::LEAST>::CompareNumeric<int128_t>(const std::vector<BaseVector *> &, BaseVector *&, const DataTypePtr &) const;

template int8_t LeastGreatestFunction<CompareMode::LEAST>::GetValueFromVector<int8_t>(BaseVector *, int32_t) const;
template int16_t LeastGreatestFunction<CompareMode::LEAST>::GetValueFromVector<int16_t>(BaseVector *, int32_t) const;
template int32_t LeastGreatestFunction<CompareMode::LEAST>::GetValueFromVector<int32_t>(BaseVector *, int32_t) const;
template int64_t LeastGreatestFunction<CompareMode::LEAST>::GetValueFromVector<int64_t>(BaseVector *, int32_t) const;
template float LeastGreatestFunction<CompareMode::LEAST>::GetValueFromVector<float>(BaseVector *, int32_t) const;
template double LeastGreatestFunction<CompareMode::LEAST>::GetValueFromVector<double>(BaseVector *, int32_t) const;
template int128_t LeastGreatestFunction<CompareMode::LEAST>::GetValueFromVector<int128_t>(BaseVector *, int32_t) const;
template bool LeastGreatestFunction<CompareMode::LEAST>::GetValueFromVector<bool>(BaseVector *, int32_t) const;

template void LeastGreatestFunction<CompareMode::LEAST>::SetValueToVector<int8_t>(BaseVector *, int32_t, const int8_t &) const;
template void LeastGreatestFunction<CompareMode::LEAST>::SetValueToVector<int16_t>(BaseVector *, int32_t, const int16_t &) const;
template void LeastGreatestFunction<CompareMode::LEAST>::SetValueToVector<int32_t>(BaseVector *, int32_t, const int32_t &) const;
template void LeastGreatestFunction<CompareMode::LEAST>::SetValueToVector<int64_t>(BaseVector *, int32_t, const int64_t &) const;
template void LeastGreatestFunction<CompareMode::LEAST>::SetValueToVector<float>(BaseVector *, int32_t, const float &) const;
template void LeastGreatestFunction<CompareMode::LEAST>::SetValueToVector<double>(BaseVector *, int32_t, const double &) const;
template void LeastGreatestFunction<CompareMode::LEAST>::SetValueToVector<int128_t>(BaseVector *, int32_t, const int128_t &) const;
template void LeastGreatestFunction<CompareMode::LEAST>::SetValueToVector<bool>(BaseVector *, int32_t, const bool &) const;

}
