/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: Greatest function implementation for vectorized comparison expressions
 */

#include "Greatest.h"
#include "vector/vector.h"
#include <limits>
#include <cstring>
#include <algorithm>
#include <cmath>
#include <iostream>

namespace omniruntime::vectorization {
    using namespace omniruntime::type;
    using namespace omniruntime::vec;

void GreatestFunction::Apply(std::stack<BaseVector *> &args, const DataTypePtr &outputType, 
                            BaseVector *&result, ExecutionContext *context) const {
    // Debug log to confirm expression is being executed
    std::cout << "[DEBUG] GreatestFunction::Apply - Entering greatest expression" << std::endl;
    
    if (args.empty()) {
        OMNI_THROW("Greatest function Error", "No input arguments");
    }
    
    // Collect all arguments from stack (they are in reverse order)
    std::vector<BaseVector *> argVectors;
    while (!args.empty()) {
        argVectors.push_back(args.top());
        args.pop();
    }
    
    // Reverse to get correct order (first argument first)
    std::reverse(argVectors.begin(), argVectors.end());
    
    if (argVectors.size() < 2) {
        OMNI_THROW("Greatest function Error", "Greatest requires at least 2 arguments");
    }
    
    // Check that all arguments have the same size
    int32_t size = argVectors[0]->GetSize();
    for (size_t i = 1; i < argVectors.size(); ++i) {
        if (argVectors[i]->GetSize() != size) {
            OMNI_THROW("Greatest function Error", "All arguments must have the same size");
        }
    }
    
    std::cout << "[DEBUG] GreatestFunction::Apply - Processing " << argVectors.size() 
              << " arguments with " << size << " rows" << std::endl;
    
    DispatchGreatest(argVectors, outputType, result);
    
    std::cout << "[DEBUG] GreatestFunction::Apply - Completed successfully" << std::endl;
}

void GreatestFunction::DispatchGreatest(const std::vector<BaseVector *> &argVectors, 
                                        const DataTypePtr &outputType, BaseVector *&result) const {
    DataTypeId outputTypeId = outputType->GetId();
    
    if (TypeUtil::IsStringType(outputTypeId)) {
        GreatestString(argVectors, result, outputType);
    } else if (outputTypeId == OMNI_BOOLEAN) {
        GreatestBoolean(argVectors, result, outputType);
    } else {
        // Numeric types
        switch (outputTypeId) {
            case OMNI_BYTE:
                GreatestNumeric<int8_t>(argVectors, result, outputType);
                break;
            case OMNI_SHORT:
                GreatestNumeric<int16_t>(argVectors, result, outputType);
                break;
            case OMNI_INT:
            case OMNI_DATE32:
                GreatestNumeric<int32_t>(argVectors, result, outputType);
                break;
            case OMNI_LONG:
            case OMNI_TIMESTAMP:
            case OMNI_DECIMAL64:
                GreatestNumeric<int64_t>(argVectors, result, outputType);
                break;
            case OMNI_FLOAT:
                GreatestNumeric<float>(argVectors, result, outputType);
                break;
            case OMNI_DOUBLE:
                GreatestNumeric<double>(argVectors, result, outputType);
                break;
            case OMNI_DECIMAL128:
                GreatestNumeric<int128_t>(argVectors, result, outputType);
                break;
            case OMNI_VARBINARY:
                GreatestString(argVectors, result, outputType);
                break;
            default:
                OMNI_THROW("Greatest function Error", 
                        "Unsupported output type: " + TypeUtil::TypeToString(outputTypeId));
        }
    }
}

// Helper function for comparing values with NaN handling (Spark SQL semantics)
// In Spark, NaN is considered greater than any other value
template<typename T>
static bool isGreater(const T &a, const T &b) {
    if constexpr (std::is_floating_point_v<T>) {
        // NaN is greater than everything (Spark SQL semantics)
        if (std::isnan(a)) {
            return true;
        }
        if (std::isnan(b)) {
            return false;
        }
    }
    return a > b;
}

template<typename T>
void GreatestFunction::GreatestNumeric(const std::vector<BaseVector *> &argVectors, 
                                       BaseVector *&result, const DataTypePtr &outputType) const {
    auto size = argVectors[0]->GetSize();
    result = VectorHelper::CreateFlatVector(outputType->GetId(), size);
    
    for (int32_t row = 0; row < size; ++row) {
        bool hasNonNull = false;
        T greatestValue{};
        
        for (size_t argIdx = 0; argIdx < argVectors.size(); ++argIdx) {
            if (!argVectors[argIdx]->IsNull(row)) {
                T value = GetValueFromVector<T>(argVectors[argIdx], row);
                if (!hasNonNull) {
                    greatestValue = value;
                    hasNonNull = true;
                } else if (isGreater(value, greatestValue)) {
                    greatestValue = value;
                }
            }
        }
        
        if (hasNonNull) {
            SetValueToVector(result, row, greatestValue);
        } else {
            // All arguments are NULL
            result->SetNull(row);
        }
    }
}

void GreatestFunction::GreatestString(const std::vector<BaseVector *> &argVectors, 
                                     BaseVector *&result, const DataTypePtr &outputType) const {
    auto size = argVectors[0]->GetSize();
    result = VectorHelper::CreateFlatVector(outputType->GetId(), size);
    
    for (int32_t row = 0; row < size; ++row) {
        bool hasNonNull = false;
        std::string_view greatestValue;
        
        for (size_t argIdx = 0; argIdx < argVectors.size(); ++argIdx) {
            if (!argVectors[argIdx]->IsNull(row)) {
                std::string_view value = GetStringValueFromVector(argVectors[argIdx], row);
                if (!hasNonNull) {
                    greatestValue = value;
                    hasNonNull = true;
                } else if (value > greatestValue) {
                    greatestValue = value;
                }
            }
        }
        
        if (hasNonNull) {
            SetStringValueToVector(result, row, const_cast<std::string_view &>(greatestValue));
        } else {
            // All arguments are NULL
            result->SetNull(row);
        }
    }
}

void GreatestFunction::GreatestBoolean(const std::vector<BaseVector *> &argVectors, 
                                      BaseVector *&result, const DataTypePtr &outputType) const {
    auto size = argVectors[0]->GetSize();
    result = VectorHelper::CreateFlatVector(outputType->GetId(), size);
    
    for (int32_t row = 0; row < size; ++row) {
        bool hasNonNull = false;
        bool greatestValue = false;
        
        for (size_t argIdx = 0; argIdx < argVectors.size(); ++argIdx) {
            if (!argVectors[argIdx]->IsNull(row)) {
                bool value = GetValueFromVector<bool>(argVectors[argIdx], row);
                if (!hasNonNull) {
                    greatestValue = value;
                    hasNonNull = true;
                } else if (value && !greatestValue) {
                    // true > false
                    greatestValue = value;
                }
            }
        }
        
        if (hasNonNull) {
            SetValueToVector(result, row, greatestValue);
        } else {
            // All arguments are NULL
            result->SetNull(row);
        }
    }
}

template<typename T>
T GreatestFunction::GetValueFromVector(BaseVector *vec, int32_t row) const {
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
        OMNI_THROW("Greatest function Error", "Unsupported encoding type");
    }
}

std::string_view GreatestFunction::GetStringValueFromVector(BaseVector *vec, int32_t row) const {
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
        OMNI_THROW("Greatest function Error", "Unsupported encoding type for string");
    }
}

template<typename T>
void GreatestFunction::SetValueToVector(BaseVector *vec, int32_t row, const T &value) const {
    auto *resultVec = static_cast<Vector<T> *>(vec);
    resultVec->SetValue(row, value);
}

void GreatestFunction::SetStringValueToVector(BaseVector *vec, int32_t row, 
                                             std::string_view &value) const {
    auto *resultVec = static_cast<Vector<LargeStringContainer<std::string_view>> *>(vec);
    resultVec->SetValue(row, value);
}

// Factory function for creating GreatestFunction
std::shared_ptr<VectorFunction> makeGreatest(const std::string &name, 
    const std::vector<DataTypeId> &inputArgs, const config::QueryConfig &) {
    return std::make_shared<GreatestFunction>();
}

// Generate function signatures for all supported types
std::vector<std::shared_ptr<codegen::FunctionSignature>> GreatestSignatures() {
    std::vector<std::shared_ptr<codegen::FunctionSignature>> signatures;
    
    // Supported types: BOOLEAN, BYTE, SHORT, INT, LONG, FLOAT, DOUBLE, VARCHAR, DATE, TIMESTAMP, DECIMAL64, DECIMAL128, VARBINARY
    std::vector<DataTypeId> supportedTypes = {
        OMNI_BOOLEAN,
        OMNI_BYTE,
        OMNI_SHORT,
        OMNI_INT,
        OMNI_LONG,
        OMNI_FLOAT,
        OMNI_DOUBLE,
        OMNI_VARCHAR,
        OMNI_DATE32,
        OMNI_TIMESTAMP,
        OMNI_DECIMAL64,
        OMNI_DECIMAL128,
        OMNI_VARBINARY
    };
    
    // Register for 2, 3, and 4 arguments (variable arity)
    for (const auto &typeId : supportedTypes) {
        // 2 arguments
        signatures.emplace_back(
            codegen::FunctionSignatureBuilder()
                .FuncName("Greatest")
                .ReturnType(typeId)
                .ArgumentType(typeId)
                .ArgumentType(typeId)
                .Build());
        
        // 3 arguments
        signatures.emplace_back(
            codegen::FunctionSignatureBuilder()
                .FuncName("Greatest")
                .ReturnType(typeId)
                .ArgumentType(typeId)
                .ArgumentType(typeId)
                .ArgumentType(typeId)
                .Build());
        
        // 4 arguments
        signatures.emplace_back(
            codegen::FunctionSignatureBuilder()
                .FuncName("Greatest")
                .ReturnType(typeId)
                .ArgumentType(typeId)
                .ArgumentType(typeId)
                .ArgumentType(typeId)
                .ArgumentType(typeId)
                .Build());
    }
    
    return signatures;
}

// Explicit template instantiations
template void GreatestFunction::GreatestNumeric<int8_t>(const std::vector<BaseVector *> &, BaseVector *&, const DataTypePtr &) const;
template void GreatestFunction::GreatestNumeric<int16_t>(const std::vector<BaseVector *> &, BaseVector *&, const DataTypePtr &) const;
template void GreatestFunction::GreatestNumeric<int32_t>(const std::vector<BaseVector *> &, BaseVector *&, const DataTypePtr &) const;
template void GreatestFunction::GreatestNumeric<int64_t>(const std::vector<BaseVector *> &, BaseVector *&, const DataTypePtr &) const;
template void GreatestFunction::GreatestNumeric<float>(const std::vector<BaseVector *> &, BaseVector *&, const DataTypePtr &) const;
template void GreatestFunction::GreatestNumeric<double>(const std::vector<BaseVector *> &, BaseVector *&, const DataTypePtr &) const;
template void GreatestFunction::GreatestNumeric<int128_t>(const std::vector<BaseVector *> &, BaseVector *&, const DataTypePtr &) const;

template int8_t GreatestFunction::GetValueFromVector<int8_t>(BaseVector *, int32_t) const;
template int16_t GreatestFunction::GetValueFromVector<int16_t>(BaseVector *, int32_t) const;
template int32_t GreatestFunction::GetValueFromVector<int32_t>(BaseVector *, int32_t) const;
template int64_t GreatestFunction::GetValueFromVector<int64_t>(BaseVector *, int32_t) const;
template float GreatestFunction::GetValueFromVector<float>(BaseVector *, int32_t) const;
template double GreatestFunction::GetValueFromVector<double>(BaseVector *, int32_t) const;
template int128_t GreatestFunction::GetValueFromVector<int128_t>(BaseVector *, int32_t) const;
template bool GreatestFunction::GetValueFromVector<bool>(BaseVector *, int32_t) const;

template void GreatestFunction::SetValueToVector<int8_t>(BaseVector *, int32_t, const int8_t &) const;
template void GreatestFunction::SetValueToVector<int16_t>(BaseVector *, int32_t, const int16_t &) const;
template void GreatestFunction::SetValueToVector<int32_t>(BaseVector *, int32_t, const int32_t &) const;
template void GreatestFunction::SetValueToVector<int64_t>(BaseVector *, int32_t, const int64_t &) const;
template void GreatestFunction::SetValueToVector<float>(BaseVector *, int32_t, const float &) const;
template void GreatestFunction::SetValueToVector<double>(BaseVector *, int32_t, const double &) const;
template void GreatestFunction::SetValueToVector<int128_t>(BaseVector *, int32_t, const int128_t &) const;
template void GreatestFunction::SetValueToVector<bool>(BaseVector *, int32_t, const bool &) const;

}
