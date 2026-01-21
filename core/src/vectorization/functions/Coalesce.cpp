/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: Coalesce function implementation for vectorized conditional expressions
 */

#include "Coalesce.h"
#include "vector/vector.h"
#include <limits>
#include <cstring>
#include <algorithm>

namespace omniruntime::vectorization {
    using namespace omniruntime::type;
    using namespace omniruntime::vec;

void CoalesceFunction::Apply(std::stack<BaseVector *> &args, const DataTypePtr &outputType, 
                            BaseVector *&result, ExecutionContext *context) const {
    if (args.empty()) {
        OMNI_THROW("Coalesce function Error", "No input arguments");
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
        OMNI_THROW("Coalesce function Error", "Coalesce requires at least 2 arguments");
    }
    
    // Check that all arguments have the same size
    int32_t size = argVectors[0]->GetSize();
    for (size_t i = 1; i < argVectors.size(); ++i) {
        if (argVectors[i]->GetSize() != size) {
            OMNI_THROW("Coalesce function Error", "All arguments must have the same size");
        }
    }
    
    DispatchCoalesce(argVectors, outputType, result);
}

void CoalesceFunction::DispatchCoalesce(const std::vector<BaseVector *> &argVectors, 
                                        const DataTypePtr &outputType, BaseVector *&result) const {
    DataTypeId outputTypeId = outputType->GetId();
    
    if (TypeUtil::IsStringType(outputTypeId)) {
        CoalesceString(argVectors, result, outputType);
    } else if (outputTypeId == OMNI_BOOLEAN) {
        CoalesceBoolean(argVectors, result, outputType);
    } else {
        // Numeric types
        switch (outputTypeId) {
            case OMNI_BYTE:
                CoalesceNumeric<int8_t>(argVectors, result, outputType);
                break;
            case OMNI_SHORT:
                CoalesceNumeric<int16_t>(argVectors, result, outputType);
                break;
            case OMNI_INT:
            case OMNI_DATE32:
                CoalesceNumeric<int32_t>(argVectors, result, outputType);
                break;
            case OMNI_LONG:
            case OMNI_TIMESTAMP:
            case OMNI_DECIMAL64:
                CoalesceNumeric<int64_t>(argVectors, result, outputType);
                break;
            case OMNI_FLOAT:
                CoalesceNumeric<float>(argVectors, result, outputType);
                break;
            case OMNI_DOUBLE:
                CoalesceNumeric<double>(argVectors, result, outputType);
                break;
            default:
                OMNI_THROW("Coalesce function Error", 
                        "Unsupported output type: " + TypeUtil::TypeToString(outputTypeId));
        }
    }
}

template<typename T>
void CoalesceFunction::CoalesceNumeric(const std::vector<BaseVector *> &argVectors, 
                                       BaseVector *&result, const DataTypePtr &outputType) const {
    auto size = argVectors[0]->GetSize();
    result = VectorHelper::CreateFlatVector(outputType->GetId(), size);
    
    for (int32_t row = 0; row < size; ++row) {
        bool found = false;
        for (size_t argIdx = 0; argIdx < argVectors.size(); ++argIdx) {
            if (!argVectors[argIdx]->IsNull(row)) {
                T value = GetValueFromVector<T>(argVectors[argIdx], row);
                SetValueToVector(result, row, value);
                found = true;
                break;
            }
        }
        if (!found) {
            // All arguments are NULL
            result->SetNull(row);
        }
    }
}

void CoalesceFunction::CoalesceString(const std::vector<BaseVector *> &argVectors, 
                                     BaseVector *&result, const DataTypePtr &outputType) const {
    auto size = argVectors[0]->GetSize();
    result = VectorHelper::CreateFlatVector(outputType->GetId(), size);
    
    for (int32_t row = 0; row < size; ++row) {
        bool found = false;
        for (size_t argIdx = 0; argIdx < argVectors.size(); ++argIdx) {
            if (!argVectors[argIdx]->IsNull(row)) {
                std::string_view value = GetStringValueFromVector(argVectors[argIdx], row);
                SetStringValueToVector(result, row, const_cast<std::string_view &>(value));
                found = true;
                break;
            }
        }
        if (!found) {
            // All arguments are NULL
            result->SetNull(row);
        }
    }
}

void CoalesceFunction::CoalesceBoolean(const std::vector<BaseVector *> &argVectors, 
                                      BaseVector *&result, const DataTypePtr &outputType) const {
    auto size = argVectors[0]->GetSize();
    result = VectorHelper::CreateFlatVector(outputType->GetId(), size);
    
    for (int32_t row = 0; row < size; ++row) {
        bool found = false;
        for (size_t argIdx = 0; argIdx < argVectors.size(); ++argIdx) {
            if (!argVectors[argIdx]->IsNull(row)) {
                bool value = GetValueFromVector<bool>(argVectors[argIdx], row);
                SetValueToVector(result, row, value);
                found = true;
                break;
            }
        }
        if (!found) {
            // All arguments are NULL
            result->SetNull(row);
        }
    }
}

template<typename T>
T CoalesceFunction::GetValueFromVector(BaseVector *vec, int32_t row) const {
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
        OMNI_THROW("Coalesce function Error", "Unsupported encoding type");
    }
}

std::string_view CoalesceFunction::GetStringValueFromVector(BaseVector *vec, int32_t row) const {
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
        OMNI_THROW("Coalesce function Error", "Unsupported encoding type for string");
    }
}

template<typename T>
void CoalesceFunction::SetValueToVector(BaseVector *vec, int32_t row, const T &value) const {
    auto *resultVec = static_cast<Vector<T> *>(vec);
    resultVec->SetValue(row, value);
}

void CoalesceFunction::SetStringValueToVector(BaseVector *vec, int32_t row, 
                                             std::string_view &value) const {
    auto *resultVec = static_cast<Vector<LargeStringContainer<std::string_view>> *>(vec);
    resultVec->SetValue(row, value);
}

// Explicit template instantiations
template void CoalesceFunction::CoalesceNumeric<int8_t>(const std::vector<BaseVector *> &, BaseVector *&, const DataTypePtr &) const;
template void CoalesceFunction::CoalesceNumeric<int16_t>(const std::vector<BaseVector *> &, BaseVector *&, const DataTypePtr &) const;
template void CoalesceFunction::CoalesceNumeric<int32_t>(const std::vector<BaseVector *> &, BaseVector *&, const DataTypePtr &) const;
template void CoalesceFunction::CoalesceNumeric<int64_t>(const std::vector<BaseVector *> &, BaseVector *&, const DataTypePtr &) const;
template void CoalesceFunction::CoalesceNumeric<float>(const std::vector<BaseVector *> &, BaseVector *&, const DataTypePtr &) const;
template void CoalesceFunction::CoalesceNumeric<double>(const std::vector<BaseVector *> &, BaseVector *&, const DataTypePtr &) const;

template int8_t CoalesceFunction::GetValueFromVector<int8_t>(BaseVector *, int32_t) const;
template int16_t CoalesceFunction::GetValueFromVector<int16_t>(BaseVector *, int32_t) const;
template int32_t CoalesceFunction::GetValueFromVector<int32_t>(BaseVector *, int32_t) const;
template int64_t CoalesceFunction::GetValueFromVector<int64_t>(BaseVector *, int32_t) const;
template float CoalesceFunction::GetValueFromVector<float>(BaseVector *, int32_t) const;
template double CoalesceFunction::GetValueFromVector<double>(BaseVector *, int32_t) const;
template bool CoalesceFunction::GetValueFromVector<bool>(BaseVector *, int32_t) const;

template void CoalesceFunction::SetValueToVector<int8_t>(BaseVector *, int32_t, const int8_t &) const;
template void CoalesceFunction::SetValueToVector<int16_t>(BaseVector *, int32_t, const int16_t &) const;
template void CoalesceFunction::SetValueToVector<int32_t>(BaseVector *, int32_t, const int32_t &) const;
template void CoalesceFunction::SetValueToVector<int64_t>(BaseVector *, int32_t, const int64_t &) const;
template void CoalesceFunction::SetValueToVector<float>(BaseVector *, int32_t, const float &) const;
template void CoalesceFunction::SetValueToVector<double>(BaseVector *, int32_t, const double &) const;
template void CoalesceFunction::SetValueToVector<bool>(BaseVector *, int32_t, const bool &) const;

}
