/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: Cast function implementation for vectorized type conversion
 */

#include "Cast.h"
#include "vector/vector.h"
#include <limits>
#include <cstring>

namespace omniruntime::vectorization {
    using namespace omniruntime::type;
    using namespace omniruntime::vec;

void CastFunction::Apply(std::stack<BaseVector *> &args, const DataTypePtr &outputType, 
                        BaseVector *&result, ExecutionContext *context) const {
    if (args.empty()) {
        OMNI_THROW("Cast function Error", "No input arguments");
    }
    
    auto inputArg = args.top();
    args.pop();
    
    DataTypeId inputTypeId = GetInputTypeId(inputArg);
    DataTypeId outputTypeId = outputType->GetId();
    
    // Same type, just copy
    if (inputTypeId == outputTypeId) {
        result = inputArg;
        return;
    }
    
    DispatchCast(inputArg, inputTypeId, outputType, result, context);

    // Clean up input argument if it's not being used as the result
    // (same type cast or string-to-string sets result = inputArg)
    if (inputArg != result) {
        delete inputArg;
    }
}

bool CastFunction::IsNullAt(BaseVector *vec, int32_t row) const {
    // For ConstVector, null flag is stored at index 0
    int32_t nullCheckIdx = (vec->GetEncoding() == OMNI_ENCODING_CONST) ? 0 : row;
    return vec->IsNull(nullCheckIdx);
}

DataTypeId CastFunction::GetInputTypeId(BaseVector *input) const {
    return input->GetTypeId();
}

void CastFunction::DispatchCast(BaseVector *input, DataTypeId inputTypeId,
                                const DataTypePtr &outputType, BaseVector *&result,
                                ExecutionContext *context) const {
    DataTypeId outputTypeId = outputType->GetId();

    // Handle string output types
    if (TypeUtil::IsStringType(outputTypeId)) {
        CastToString(input, inputTypeId, result, outputType, context);
        return;
    }

    // Handle boolean output type
    if (outputTypeId == OMNI_BOOLEAN) {
        CastToBoolean(input, inputTypeId, result, outputType, context);
        return;
    }

    // Handle string input types
    if (TypeUtil::IsStringType(inputTypeId)) {
        switch (outputTypeId) {
            case OMNI_BOOLEAN:
                CastToBoolean(input, inputTypeId, result, outputType, context);
                break;
            case OMNI_BYTE:
                CastStringToNumeric<int8_t>(input, result, outputType, context);
                break;
            case OMNI_SHORT:
                CastStringToNumeric<int16_t>(input, result, outputType, context);
                break;
            case OMNI_INT:
            case OMNI_DATE32:
                CastStringToNumeric<int32_t>(input, result, outputType, context);
                break;
            case OMNI_LONG:
            case OMNI_TIMESTAMP:
            case OMNI_DECIMAL64:
                CastStringToNumeric<int64_t>(input, result, outputType, context);
                break;
            case OMNI_FLOAT:
                CastStringToNumeric<float>(input, result, outputType, context);
                break;
            case OMNI_DOUBLE:
                CastStringToNumeric<double>(input, result, outputType, context);
                break;
            default:
                OMNI_THROW("Cast function Error",
                        "Unsupported cast from string to " + TypeUtil::TypeToString(outputTypeId));
        }
        return;
    }

    // Handle boolean input type
    if (inputTypeId == OMNI_BOOLEAN) {
        CastFromBoolean(input, result, outputType, context);
        return;
    }

    // Handle numeric to numeric conversions
    switch (outputTypeId) {
        case OMNI_BYTE:
            switch (inputTypeId) {
                case OMNI_SHORT:
                    CastNumericToNumeric<int16_t, int8_t>(input, result, outputType, context);
                    break;
                case OMNI_INT:
                case OMNI_DATE32:
                    CastNumericToNumeric<int32_t, int8_t>(input, result, outputType, context);
                    break;
                case OMNI_LONG:
                case OMNI_TIMESTAMP:
                case OMNI_DECIMAL64:
                    CastNumericToNumeric<int64_t, int8_t>(input, result, outputType, context);
                    break;
                case OMNI_FLOAT:
                    CastNumericToNumeric<float, int8_t>(input, result, outputType, context);
                    break;
                case OMNI_DOUBLE:
                    CastNumericToNumeric<double, int8_t>(input, result, outputType, context);
                    break;
                default:
                    OMNI_THROW("Cast function Error",
                            "Unsupported cast to BYTE from " + TypeUtil::TypeToString(inputTypeId));
            }
            break;

        case OMNI_SHORT:
            switch (inputTypeId) {
                case OMNI_BYTE:
                    CastNumericToNumeric<int8_t, int16_t>(input, result, outputType, context);
                    break;
                case OMNI_INT:
                case OMNI_DATE32:
                    CastNumericToNumeric<int32_t, int16_t>(input, result, outputType, context);
                    break;
                case OMNI_LONG:
                case OMNI_TIMESTAMP:
                case OMNI_DECIMAL64:
                    CastNumericToNumeric<int64_t, int16_t>(input, result, outputType, context);
                    break;
                case OMNI_FLOAT:
                    CastNumericToNumeric<float, int16_t>(input, result, outputType, context);
                    break;
                case OMNI_DOUBLE:
                    CastNumericToNumeric<double, int16_t>(input, result, outputType, context);
                    break;
                default:
                    OMNI_THROW("Cast function Error",
                            "Unsupported cast to SHORT from " + TypeUtil::TypeToString(inputTypeId));
            }
            break;

        case OMNI_INT:
        case OMNI_DATE32:
            switch (inputTypeId) {
                case OMNI_BYTE:
                    CastNumericToNumeric<int8_t, int32_t>(input, result, outputType, context);
                    break;
                case OMNI_SHORT:
                    CastNumericToNumeric<int16_t, int32_t>(input, result, outputType, context);
                    break;
                case OMNI_LONG:
                case OMNI_TIMESTAMP:
                case OMNI_DECIMAL64:
                    CastNumericToNumeric<int64_t, int32_t>(input, result, outputType, context);
                    break;
                case OMNI_FLOAT:
                    CastNumericToNumeric<float, int32_t>(input, result, outputType, context);
                    break;
                case OMNI_DOUBLE:
                    CastNumericToNumeric<double, int32_t>(input, result, outputType, context);
                    break;
                default:
                    OMNI_THROW("Cast function Error",
                            "Unsupported cast to INT from " + TypeUtil::TypeToString(inputTypeId));
            }
            break;

        case OMNI_LONG:
        case OMNI_TIMESTAMP:
        case OMNI_DECIMAL64:
            switch (inputTypeId) {
                case OMNI_BYTE:
                    CastNumericToNumeric<int8_t, int64_t>(input, result, outputType, context);
                    break;
                case OMNI_SHORT:
                    CastNumericToNumeric<int16_t, int64_t>(input, result, outputType, context);
                    break;
                case OMNI_INT:
                case OMNI_DATE32:
                    CastNumericToNumeric<int32_t, int64_t>(input, result, outputType, context);
                    break;
                case OMNI_FLOAT:
                    CastNumericToNumeric<float, int64_t>(input, result, outputType, context);
                    break;
                case OMNI_DOUBLE:
                    CastNumericToNumeric<double, int64_t>(input, result, outputType, context);
                    break;
                default:
                    OMNI_THROW("Cast function Error",
                            "Unsupported cast to LONG from " + TypeUtil::TypeToString(inputTypeId));
            }
            break;

        case OMNI_FLOAT:
            switch (inputTypeId) {
                case OMNI_BYTE:
                    CastNumericToNumeric<int8_t, float>(input, result, outputType, context);
                    break;
                case OMNI_SHORT:
                    CastNumericToNumeric<int16_t, float>(input, result, outputType, context);
                    break;
                case OMNI_INT:
                case OMNI_DATE32:
                    CastNumericToNumeric<int32_t, float>(input, result, outputType, context);
                    break;
                case OMNI_LONG:
                case OMNI_TIMESTAMP:
                case OMNI_DECIMAL64:
                    CastNumericToNumeric<int64_t, float>(input, result, outputType, context);
                    break;
                case OMNI_DOUBLE:
                    CastNumericToNumeric<double, float>(input, result, outputType, context);
                    break;
                default:
                    OMNI_THROW("Cast function Error",
                            "Unsupported cast to FLOAT from " + TypeUtil::TypeToString(inputTypeId));
            }
            break;

        case OMNI_DOUBLE:
            switch (inputTypeId) {
                case OMNI_BYTE:
                    CastNumericToNumeric<int8_t, double>(input, result, outputType, context);
                    break;
                case OMNI_SHORT:
                    CastNumericToNumeric<int16_t, double>(input, result, outputType, context);
                    break;
                case OMNI_INT:
                case OMNI_DATE32:
                    CastNumericToNumeric<int32_t, double>(input, result, outputType, context);
                    break;
                case OMNI_LONG:
                case OMNI_TIMESTAMP:
                case OMNI_DECIMAL64:
                    CastNumericToNumeric<int64_t, double>(input, result, outputType, context);
                    break;
                case OMNI_FLOAT:
                    CastNumericToNumeric<float, double>(input, result, outputType, context);
                    break;
                default:
                    OMNI_THROW("Cast function Error",
                            "Unsupported cast to DOUBLE from " + TypeUtil::TypeToString(inputTypeId));
            }
            break;

        default:
            OMNI_THROW("Cast function Error",
                    "Unsupported output type: " + TypeUtil::TypeToString(outputTypeId));
    }
}

template<typename T>
void CastFunction::CastStringToNumeric(BaseVector *input, BaseVector *&result,
                                    const DataTypePtr &outputType,
                                    ExecutionContext *context) const {
    // Use context to get row size (handles ConstVector correctly)
    auto size = context->GetResultRowSize();
    result = VectorHelper::CreateFlatVector(outputType->GetId(), size);

    for (int32_t row = 0; row < size; ++row) {
        if (IsNullAt(input, row)) {
            result->SetNull(row);
            continue;
        }

        std::string_view inputStr = GetStringValueFromVector(input, row);

        if constexpr (std::is_same_v<T, float> || std::is_same_v<T, double>) {
            // Floating point conversion
            double doubleRes;
            type::Status status = ConvertStringToDouble(doubleRes, inputStr.data(),
                                                    static_cast<int32_t>(inputStr.size()));
            if (status == type::Status::CONVERT_SUCCESS) {
                T res;
                if constexpr (std::is_same_v<T, float>) {
                    res = static_cast<float>(doubleRes);
                } else {
                    res = doubleRes;
                }
                SetValueToVector(result, row, res);
            } else {
                result->SetNull(row);
            }
        } else {
            // Integer conversion
            T res;
            type::Status status = ConvertStringToInteger<T>(res, inputStr.data(),
                                                        static_cast<int>(inputStr.size()));
            if (status == type::Status::CONVERT_SUCCESS) {
                SetValueToVector(result, row, res);
            } else {
                result->SetNull(row);
            }
        }
    }
}

template<typename T>
void CastFunction::CastNumericToString(BaseVector *input, BaseVector *&result,
                                    const DataTypePtr &outputType,
                                    ExecutionContext *context) const {
    // Use context to get row size (handles ConstVector correctly)
    auto size = context->GetResultRowSize();
    result = VectorHelper::CreateFlatVector(outputType->GetId(), size);

    for (int32_t row = 0; row < size; ++row) {
        if (IsNullAt(input, row)) {
            result->SetNull(row);
            continue;
        }

        T value = GetValueFromVector<T>(input, row);
        std::string strValue = NumericToString(value);
        std::string_view strView(strValue);
        SetStringValueToVector(result, row, strView);
    }
}

template<typename TInput, typename TOutput>
void CastFunction::CastNumericToNumeric(BaseVector *input, BaseVector *&result,
                                    const DataTypePtr &outputType,
                                    ExecutionContext *context) const {
    // Use context to get row size (handles ConstVector correctly)
    auto size = context->GetResultRowSize();
    result = VectorHelper::CreateFlatVector(outputType->GetId(), size);

    for (int32_t row = 0; row < size; ++row) {
        if (IsNullAt(input, row)) {
            result->SetNull(row);
            continue;
        }

        TInput inputValue = GetValueFromVector<TInput>(input, row);
        TOutput outputValue = static_cast<TOutput>(inputValue);

        // Check for overflow/underflow for integer types
        if constexpr (std::is_integral_v<TInput> && std::is_integral_v<TOutput>) {
            if (inputValue < std::numeric_limits<TOutput>::min() ||
                inputValue > std::numeric_limits<TOutput>::max()) {
                result->SetNull(row);
                continue;
            }
        }

        SetValueToVector(result, row, outputValue);
    }
}

void CastFunction::CastToBoolean(BaseVector *input, DataTypeId inputTypeId,
                                BaseVector *&result, const DataTypePtr &outputType,
                                ExecutionContext *context) const {
    // Use context to get row size (handles ConstVector correctly)
    auto size = context->GetResultRowSize();
    result = VectorHelper::CreateFlatVector(outputType->GetId(), size);

    for (int32_t row = 0; row < size; ++row) {
        if (IsNullAt(input, row)) {
            result->SetNull(row);
            continue;
        }

        bool boolValue = false;

        if (TypeUtil::IsStringType(inputTypeId)) {
            std::string_view strValue = GetStringValueFromVector(input, row);
            // Convert string to boolean: "true", "1" -> true, others -> false
            std::string lowerStr;
            lowerStr.reserve(strValue.size());
            for (char c : strValue) {
                lowerStr += static_cast<char>(std::tolower(c));
            }
            boolValue = (lowerStr == "true" || lowerStr == "1" || lowerStr == "t");
        } else {
            switch (inputTypeId) {
                case OMNI_BYTE:
                    boolValue = GetValueFromVector<int8_t>(input, row) != 0;
                    break;
                case OMNI_SHORT:
                    boolValue = GetValueFromVector<int16_t>(input, row) != 0;
                    break;
                case OMNI_INT:
                case OMNI_DATE32:
                    boolValue = GetValueFromVector<int32_t>(input, row) != 0;
                    break;
                case OMNI_LONG:
                case OMNI_TIMESTAMP:
                case OMNI_DECIMAL64:
                    boolValue = GetValueFromVector<int64_t>(input, row) != 0;
                    break;
                case OMNI_FLOAT:
                    boolValue = std::fabs(GetValueFromVector<float>(input, row)) > 1e-6f;
                    break;
                case OMNI_DOUBLE:
                    boolValue = std::fabs(GetValueFromVector<double>(input, row)) > 1e-9;
                    break;
                default:
                    result->SetNull(row);
                    continue;
            }
        }

        SetValueToVector(result, row, boolValue);
    }
}

void CastFunction::CastFromBoolean(BaseVector *input, BaseVector *&result,
                                const DataTypePtr &outputType,
                                ExecutionContext *context) const {
    DataTypeId outputTypeId = outputType->GetId();
    // Use context to get row size (handles ConstVector correctly)
    auto size = context->GetResultRowSize();
    result = VectorHelper::CreateFlatVector(outputTypeId, size);

    for (int32_t row = 0; row < size; ++row) {
        if (IsNullAt(input, row)) {
            result->SetNull(row);
            continue;
        }

        bool boolValue = GetValueFromVector<bool>(input, row);

        switch (outputTypeId) {
            case OMNI_BYTE:
                SetValueToVector(result, row, static_cast<int8_t>(boolValue ? 1 : 0));
                break;
            case OMNI_SHORT:
                SetValueToVector(result, row, static_cast<int16_t>(boolValue ? 1 : 0));
                break;
            case OMNI_INT:
                SetValueToVector(result, row, static_cast<int32_t>(boolValue ? 1 : 0));
                break;
            case OMNI_LONG:
                SetValueToVector(result, row, static_cast<int64_t>(boolValue ? 1 : 0));
                break;
            case OMNI_FLOAT:
                SetValueToVector(result, row, static_cast<float>(boolValue ? 1.0f : 0.0f));
                break;
            case OMNI_DOUBLE:
                SetValueToVector(result, row, static_cast<double>(boolValue ? 1.0 : 0.0));
                break;
            default:
                result->SetNull(row);
                break;
        }
    }
}

void CastFunction::CastToString(BaseVector *input, DataTypeId inputTypeId,
                                BaseVector *&result, const DataTypePtr &outputType,
                                ExecutionContext *context) const {
    switch (inputTypeId) {
        case OMNI_BOOLEAN:
            CastNumericToString<bool>(input, result, outputType, context);
            break;
        case OMNI_BYTE:
            CastNumericToString<int8_t>(input, result, outputType, context);
            break;
        case OMNI_SHORT:
            CastNumericToString<int16_t>(input, result, outputType, context);
            break;
        case OMNI_INT:
        case OMNI_DATE32:
            CastNumericToString<int32_t>(input, result, outputType, context);
            break;
        case OMNI_LONG:
        case OMNI_TIMESTAMP:
        case OMNI_DECIMAL64:
            CastNumericToString<int64_t>(input, result, outputType, context);
            break;
        case OMNI_FLOAT:
            CastNumericToString<float>(input, result, outputType, context);
            break;
        case OMNI_DOUBLE:
            CastNumericToString<double>(input, result, outputType, context);
            break;
        case OMNI_VARCHAR:
        case OMNI_CHAR:
        case OMNI_VARBINARY:
            // String to string, just copy
            result = input;
            break;
        default:
            OMNI_THROW("Cast function Error",
                    "Unsupported cast to string from " + TypeUtil::TypeToString(inputTypeId));
    }
}

template<typename T>
T CastFunction::GetValueFromVector(BaseVector *vec, int32_t row) const {
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
        OMNI_THROW("Cast function Error", "Unsupported encoding type");
    }
}

std::string_view CastFunction::GetStringValueFromVector(BaseVector *vec, int32_t row) const {
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
        OMNI_THROW("Cast function Error", "Unsupported encoding type for string");
    }
}

template<typename T>
void CastFunction::SetValueToVector(BaseVector *vec, int32_t row, const T &value) const {
    auto *resultVec = static_cast<Vector<T> *>(vec);
    resultVec->SetValue(row, value);
}

void CastFunction::SetStringValueToVector(BaseVector *vec, int32_t row,
                                        std::string_view &value) const {
    auto *resultVec = static_cast<Vector<LargeStringContainer<std::string_view>> *>(vec);
    resultVec->SetValue(row, value);
}

template<typename T>
std::string CastFunction::NumericToString(T value) const {
    if constexpr (std::is_same_v<T, bool>) {
        return value ? "true" : "false";
    } else if constexpr (std::is_integral_v<T>) {
        return std::to_string(value);
    } else if constexpr (std::is_floating_point_v<T>) {
        std::ostringstream oss;
        oss << std::setprecision(std::numeric_limits<T>::max_digits10) << value;
        return oss.str();
    } else {
        return std::to_string(value);
    }
}

// Explicit template instantiations
template void CastFunction::CastStringToNumeric<int8_t>(BaseVector *, BaseVector *&, const DataTypePtr &, ExecutionContext *) const;
template void CastFunction::CastStringToNumeric<int16_t>(BaseVector *, BaseVector *&, const DataTypePtr &, ExecutionContext *) const;
template void CastFunction::CastStringToNumeric<int32_t>(BaseVector *, BaseVector *&, const DataTypePtr &, ExecutionContext *) const;
template void CastFunction::CastStringToNumeric<int64_t>(BaseVector *, BaseVector *&, const DataTypePtr &, ExecutionContext *) const;
template void CastFunction::CastStringToNumeric<float>(BaseVector *, BaseVector *&, const DataTypePtr &, ExecutionContext *) const;
template void CastFunction::CastStringToNumeric<double>(BaseVector *, BaseVector *&, const DataTypePtr &, ExecutionContext *) const;

template void CastFunction::CastNumericToString<bool>(BaseVector *, BaseVector *&, const DataTypePtr &, ExecutionContext *) const;
template void CastFunction::CastNumericToString<int8_t>(BaseVector *, BaseVector *&, const DataTypePtr &, ExecutionContext *) const;
template void CastFunction::CastNumericToString<int16_t>(BaseVector *, BaseVector *&, const DataTypePtr &, ExecutionContext *) const;
template void CastFunction::CastNumericToString<int32_t>(BaseVector *, BaseVector *&, const DataTypePtr &, ExecutionContext *) const;
template void CastFunction::CastNumericToString<int64_t>(BaseVector *, BaseVector *&, const DataTypePtr &, ExecutionContext *) const;
template void CastFunction::CastNumericToString<float>(BaseVector *, BaseVector *&, const DataTypePtr &, ExecutionContext *) const;
template void CastFunction::CastNumericToString<double>(BaseVector *, BaseVector *&, const DataTypePtr &, ExecutionContext *) const;

template void CastFunction::CastNumericToNumeric<int8_t, int16_t>(BaseVector *, BaseVector *&, const DataTypePtr &, ExecutionContext *) const;
template void CastFunction::CastNumericToNumeric<int8_t, int32_t>(BaseVector *, BaseVector *&, const DataTypePtr &, ExecutionContext *) const;
template void CastFunction::CastNumericToNumeric<int8_t, int64_t>(BaseVector *, BaseVector *&, const DataTypePtr &, ExecutionContext *) const;
template void CastFunction::CastNumericToNumeric<int8_t, float>(BaseVector *, BaseVector *&, const DataTypePtr &, ExecutionContext *) const;
template void CastFunction::CastNumericToNumeric<int8_t, double>(BaseVector *, BaseVector *&, const DataTypePtr &, ExecutionContext *) const;

template void CastFunction::CastNumericToNumeric<int16_t, int8_t>(BaseVector *, BaseVector *&, const DataTypePtr &, ExecutionContext *) const;
template void CastFunction::CastNumericToNumeric<int16_t, int32_t>(BaseVector *, BaseVector *&, const DataTypePtr &, ExecutionContext *) const;
template void CastFunction::CastNumericToNumeric<int16_t, int64_t>(BaseVector *, BaseVector *&, const DataTypePtr &, ExecutionContext *) const;
template void CastFunction::CastNumericToNumeric<int16_t, float>(BaseVector *, BaseVector *&, const DataTypePtr &, ExecutionContext *) const;
template void CastFunction::CastNumericToNumeric<int16_t, double>(BaseVector *, BaseVector *&, const DataTypePtr &, ExecutionContext *) const;

template void CastFunction::CastNumericToNumeric<int32_t, int8_t>(BaseVector *, BaseVector *&, const DataTypePtr &, ExecutionContext *) const;
template void CastFunction::CastNumericToNumeric<int32_t, int16_t>(BaseVector *, BaseVector *&, const DataTypePtr &, ExecutionContext *) const;
template void CastFunction::CastNumericToNumeric<int32_t, int64_t>(BaseVector *, BaseVector *&, const DataTypePtr &, ExecutionContext *) const;
template void CastFunction::CastNumericToNumeric<int32_t, float>(BaseVector *, BaseVector *&, const DataTypePtr &, ExecutionContext *) const;
template void CastFunction::CastNumericToNumeric<int32_t, double>(BaseVector *, BaseVector *&, const DataTypePtr &, ExecutionContext *) const;

template void CastFunction::CastNumericToNumeric<int64_t, int8_t>(BaseVector *, BaseVector *&, const DataTypePtr &, ExecutionContext *) const;
template void CastFunction::CastNumericToNumeric<int64_t, int16_t>(BaseVector *, BaseVector *&, const DataTypePtr &, ExecutionContext *) const;
template void CastFunction::CastNumericToNumeric<int64_t, int32_t>(BaseVector *, BaseVector *&, const DataTypePtr &, ExecutionContext *) const;
template void CastFunction::CastNumericToNumeric<int64_t, float>(BaseVector *, BaseVector *&, const DataTypePtr &, ExecutionContext *) const;
template void CastFunction::CastNumericToNumeric<int64_t, double>(BaseVector *, BaseVector *&, const DataTypePtr &, ExecutionContext *) const;

template void CastFunction::CastNumericToNumeric<float, int8_t>(BaseVector *, BaseVector *&, const DataTypePtr &, ExecutionContext *) const;
template void CastFunction::CastNumericToNumeric<float, int16_t>(BaseVector *, BaseVector *&, const DataTypePtr &, ExecutionContext *) const;
template void CastFunction::CastNumericToNumeric<float, int32_t>(BaseVector *, BaseVector *&, const DataTypePtr &, ExecutionContext *) const;
template void CastFunction::CastNumericToNumeric<float, int64_t>(BaseVector *, BaseVector *&, const DataTypePtr &, ExecutionContext *) const;
template void CastFunction::CastNumericToNumeric<float, double>(BaseVector *, BaseVector *&, const DataTypePtr &, ExecutionContext *) const;

template void CastFunction::CastNumericToNumeric<double, int8_t>(BaseVector *, BaseVector *&, const DataTypePtr &, ExecutionContext *) const;
template void CastFunction::CastNumericToNumeric<double, int16_t>(BaseVector *, BaseVector *&, const DataTypePtr &, ExecutionContext *) const;
template void CastFunction::CastNumericToNumeric<double, int32_t>(BaseVector *, BaseVector *&, const DataTypePtr &, ExecutionContext *) const;
template void CastFunction::CastNumericToNumeric<double, int64_t>(BaseVector *, BaseVector *&, const DataTypePtr &, ExecutionContext *) const;
template void CastFunction::CastNumericToNumeric<double, float>(BaseVector *, BaseVector *&, const DataTypePtr &, ExecutionContext *) const;

template int8_t CastFunction::GetValueFromVector<int8_t>(BaseVector *, int32_t) const;
template int16_t CastFunction::GetValueFromVector<int16_t>(BaseVector *, int32_t) const;
template int32_t CastFunction::GetValueFromVector<int32_t>(BaseVector *, int32_t) const;
template int64_t CastFunction::GetValueFromVector<int64_t>(BaseVector *, int32_t) const;
template float CastFunction::GetValueFromVector<float>(BaseVector *, int32_t) const;
template double CastFunction::GetValueFromVector<double>(BaseVector *, int32_t) const;
template bool CastFunction::GetValueFromVector<bool>(BaseVector *, int32_t) const;

template void CastFunction::SetValueToVector<int8_t>(BaseVector *, int32_t, const int8_t &) const;
template void CastFunction::SetValueToVector<int16_t>(BaseVector *, int32_t, const int16_t &) const;
template void CastFunction::SetValueToVector<int32_t>(BaseVector *, int32_t, const int32_t &) const;
template void CastFunction::SetValueToVector<int64_t>(BaseVector *, int32_t, const int64_t &) const;
template void CastFunction::SetValueToVector<float>(BaseVector *, int32_t, const float &) const;
template void CastFunction::SetValueToVector<double>(BaseVector *, int32_t, const double &) const;
template void CastFunction::SetValueToVector<bool>(BaseVector *, int32_t, const bool &) const;

template std::string CastFunction::NumericToString<bool>(bool) const;
template std::string CastFunction::NumericToString<int8_t>(int8_t) const;
template std::string CastFunction::NumericToString<int16_t>(int16_t) const;
template std::string CastFunction::NumericToString<int32_t>(int32_t) const;
template std::string CastFunction::NumericToString<int64_t>(int64_t) const;
template std::string CastFunction::NumericToString<float>(float) const;
template std::string CastFunction::NumericToString<double>(double) const;

}
