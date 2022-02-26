/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: JNI Vector Operations Header
 */
#ifndef OMNI_RUNTIME_TYPE_INFER_H
#define OMNI_RUNTIME_TYPE_INFER_H

#include <stdint.h>
#include "type/data_type.h"
#include "debug.h"

class TypeUtil {
public:
    static int32_t GetVarByteSize(uint32_t type)
    {
        switch (type) {
            case omniruntime::type::OMNI_INT: {
                return sizeof(int32_t);
            }
            case omniruntime::type::OMNI_LONG: {
                return sizeof(int64_t);
            }
            case omniruntime::type::OMNI_DOUBLE: {
                return sizeof(double);
            }
            default:
                break;
        }
        return 0;
    }

    // Helper function for debugging DataType
    static std::string TypeToString(omniruntime::type::DataTypeId id);

    static bool IsStringType(omniruntime::type::DataTypeId id);

    static bool IsDecimalType(omniruntime::type::DataTypeId type);
};

std::unique_ptr<omniruntime::type::DataType> IntType();

std::unique_ptr<omniruntime::type::DataType> Date32Type();

std::unique_ptr<omniruntime::type::DataType> LongType();

std::unique_ptr<omniruntime::type::DataType> DoubleType();

std::unique_ptr<omniruntime::type::DataType> BooleanType();

std::unique_ptr<omniruntime::type::DataType> VarcharType();

std::unique_ptr<omniruntime::type::DataType> VarcharType(int32_t width);

std::unique_ptr<omniruntime::type::DataType> CharType(int32_t width);

std::unique_ptr<omniruntime::type::DataType> Decimal64Type(int32_t precision, int32_t scale);

std::unique_ptr<omniruntime::type::DataType> Decimal128Type(int32_t precision, int32_t scale);
#endif // OMNI_RUNTIME_TYPE_INFER_H