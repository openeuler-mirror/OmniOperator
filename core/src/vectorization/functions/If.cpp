/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: If function implementation for vectorized conditional expressions
 */

#include "If.h"
#include "vector/vector.h"
#include <limits>
#include <cstring>

namespace omniruntime::vectorization {
    using namespace omniruntime::type;
    using namespace omniruntime::vec;

void IfFunction::Apply(std::stack<BaseVector *> &args, const DataTypePtr &outputType, 
                      BaseVector *&result, ExecutionContext *context) const {
    if (args.size() < 3) {
        OMNI_THROW("If function Error", "If requires 3 arguments: condition, true_value, false_value");
    }
    
    auto falseVec = args.top();
    args.pop();
    auto trueVec = args.top();
    args.pop();
    auto condVec = args.top();
    args.pop();
    
    if (condVec->GetTypeId() != OMNI_BOOLEAN) {
        OMNI_THROW("If function Error", "First argument (condition) must be a BooleanVector");
    }
    
    // Check that all vectors have the same size
    int32_t size = condVec->GetSize();
    if (trueVec->GetSize() != size || falseVec->GetSize() != size) {
        OMNI_THROW("If function Error", "All arguments must have the same size");
    }
    
    DispatchIf(condVec, trueVec, falseVec, outputType, result);
}

void IfFunction::DispatchIf(BaseVector *condVec, BaseVector *trueVec, BaseVector *falseVec,
                           const DataTypePtr &outputType, BaseVector *&result) const {
    DataTypeId outputTypeId = outputType->GetId();
    
    if (TypeUtil::IsStringType(outputTypeId)) {
        IfString(condVec, trueVec, falseVec, result, outputType);
    } else if (outputTypeId == OMNI_BOOLEAN) {
        IfBoolean(condVec, trueVec, falseVec, result, outputType);
    } else {
        // Numeric types
        switch (outputTypeId) {
            case OMNI_BYTE:
                IfNumeric<int8_t>(condVec, trueVec, falseVec, result, outputType);
                break;
            case OMNI_SHORT:
                IfNumeric<int16_t>(condVec, trueVec, falseVec, result, outputType);
                break;
            case OMNI_INT:
            case OMNI_DATE32:
                IfNumeric<int32_t>(condVec, trueVec, falseVec, result, outputType);
                break;
            case OMNI_LONG:
            case OMNI_TIMESTAMP:
            case OMNI_DECIMAL64:
                IfNumeric<int64_t>(condVec, trueVec, falseVec, result, outputType);
                break;
            case OMNI_FLOAT:
                IfNumeric<float>(condVec, trueVec, falseVec, result, outputType);
                break;
            case OMNI_DOUBLE:
                IfNumeric<double>(condVec, trueVec, falseVec, result, outputType);
                break;
            default:
                OMNI_THROW("If function Error", 
                        "Unsupported output type: " + TypeUtil::TypeToString(outputTypeId));
        }
    }
}

template<typename T>
void IfFunction::IfNumeric(BaseVector *condVec, BaseVector *trueVec, BaseVector *falseVec,
                           BaseVector *&result, const DataTypePtr &outputType) const {
    auto size = condVec->GetSize();
    result = VectorHelper::CreateFlatVector(outputType->GetId(), size);
    
    auto *boolVec = static_cast<Vector<bool> *>(condVec);
    
    for (int32_t row = 0; row < size; ++row) {
        if (condVec->IsNull(row)) {
            result->SetNull(row);
            continue;
        }
        
        bool cond = boolVec->GetValue(row);
        
        if (cond) {
            if (trueVec->IsNull(row)) {
                result->SetNull(row);
            } else {
                T value = GetValueFromVector<T>(trueVec, row);
                SetValueToVector(result, row, value);
            }
        } else {
            if (falseVec->IsNull(row)) {
                result->SetNull(row);
            } else {
                T value = GetValueFromVector<T>(falseVec, row);
                SetValueToVector(result, row, value);
            }
        }
    }
}

void IfFunction::IfString(BaseVector *condVec, BaseVector *trueVec, BaseVector *falseVec,
                         BaseVector *&result, const DataTypePtr &outputType) const {
    auto size = condVec->GetSize();
    result = VectorHelper::CreateFlatVector(outputType->GetId(), size);
    
    auto *boolVec = static_cast<Vector<bool> *>(condVec);
    
    for (int32_t row = 0; row < size; ++row) {
        if (condVec->IsNull(row)) {
            result->SetNull(row);
            continue;
        }
        
        bool cond = boolVec->GetValue(row);
        
        if (cond) {
            if (trueVec->IsNull(row)) {
                result->SetNull(row);
            } else {
                std::string_view value = GetStringValueFromVector(trueVec, row);
                SetStringValueToVector(result, row, const_cast<std::string_view &>(value));
            }
        } else {
            if (falseVec->IsNull(row)) {
                result->SetNull(row);
            } else {
                std::string_view value = GetStringValueFromVector(falseVec, row);
                SetStringValueToVector(result, row, const_cast<std::string_view &>(value));
            }
        }
    }
}

void IfFunction::IfBoolean(BaseVector *condVec, BaseVector *trueVec, BaseVector *falseVec,
                          BaseVector *&result, const DataTypePtr &outputType) const {
    auto size = condVec->GetSize();
    result = VectorHelper::CreateFlatVector(outputType->GetId(), size);
    
    auto *boolVec = static_cast<Vector<bool> *>(condVec);
    
    for (int32_t row = 0; row < size; ++row) {
        if (condVec->IsNull(row)) {
            result->SetNull(row);
            continue;
        }
        
        bool cond = boolVec->GetValue(row);
        
        if (cond) {
            if (trueVec->IsNull(row)) {
                result->SetNull(row);
            } else {
                bool value = GetValueFromVector<bool>(trueVec, row);
                SetValueToVector(result, row, value);
            }
        } else {
            if (falseVec->IsNull(row)) {
                result->SetNull(row);
            } else {
                bool value = GetValueFromVector<bool>(falseVec, row);
                SetValueToVector(result, row, value);
            }
        }
    }
}

template<typename T>
T IfFunction::GetValueFromVector(BaseVector *vec, int32_t row) const {
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
        OMNI_THROW("If function Error", "Unsupported encoding type");
    }
}

std::string_view IfFunction::GetStringValueFromVector(BaseVector *vec, int32_t row) const {
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
        OMNI_THROW("If function Error", "Unsupported encoding type for string");
    }
}

template<typename T>
void IfFunction::SetValueToVector(BaseVector *vec, int32_t row, const T &value) const {
    auto *resultVec = static_cast<Vector<T> *>(vec);
    resultVec->SetValue(row, value);
}

void IfFunction::SetStringValueToVector(BaseVector *vec, int32_t row, 
                                        std::string_view &value) const {
    auto *resultVec = static_cast<Vector<LargeStringContainer<std::string_view>> *>(vec);
    resultVec->SetValue(row, value);
}

// Explicit template instantiations
template void IfFunction::IfNumeric<int8_t>(BaseVector *, BaseVector *, BaseVector *, BaseVector *&, const DataTypePtr &) const;
template void IfFunction::IfNumeric<int16_t>(BaseVector *, BaseVector *, BaseVector *, BaseVector *&, const DataTypePtr &) const;
template void IfFunction::IfNumeric<int32_t>(BaseVector *, BaseVector *, BaseVector *, BaseVector *&, const DataTypePtr &) const;
template void IfFunction::IfNumeric<int64_t>(BaseVector *, BaseVector *, BaseVector *, BaseVector *&, const DataTypePtr &) const;
template void IfFunction::IfNumeric<float>(BaseVector *, BaseVector *, BaseVector *, BaseVector *&, const DataTypePtr &) const;
template void IfFunction::IfNumeric<double>(BaseVector *, BaseVector *, BaseVector *, BaseVector *&, const DataTypePtr &) const;

template int8_t IfFunction::GetValueFromVector<int8_t>(BaseVector *, int32_t) const;
template int16_t IfFunction::GetValueFromVector<int16_t>(BaseVector *, int32_t) const;
template int32_t IfFunction::GetValueFromVector<int32_t>(BaseVector *, int32_t) const;
template int64_t IfFunction::GetValueFromVector<int64_t>(BaseVector *, int32_t) const;
template float IfFunction::GetValueFromVector<float>(BaseVector *, int32_t) const;
template double IfFunction::GetValueFromVector<double>(BaseVector *, int32_t) const;
template bool IfFunction::GetValueFromVector<bool>(BaseVector *, int32_t) const;

template void IfFunction::SetValueToVector<int8_t>(BaseVector *, int32_t, const int8_t &) const;
template void IfFunction::SetValueToVector<int16_t>(BaseVector *, int32_t, const int16_t &) const;
template void IfFunction::SetValueToVector<int32_t>(BaseVector *, int32_t, const int32_t &) const;
template void IfFunction::SetValueToVector<int64_t>(BaseVector *, int32_t, const int64_t &) const;
template void IfFunction::SetValueToVector<float>(BaseVector *, int32_t, const float &) const;
template void IfFunction::SetValueToVector<double>(BaseVector *, int32_t, const double &) const;
template void IfFunction::SetValueToVector<bool>(BaseVector *, int32_t, const bool &) const;

}
