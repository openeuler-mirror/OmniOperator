/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: JNI Vector Operations Header
 */
#ifndef OMNI_RUNTIME_TYPE_INFER_H
#define OMNI_RUNTIME_TYPE_INFER_H

#include <stdint.h>
#include "../vector/vector_type.h"

class TypeUtil {
public:
    static int32_t GetVarByteSize(uint32_t type)
    {
        switch (type) {
            case omniruntime::vec::OMNI_VEC_TYPE_INT: {
                return sizeof(int32_t);
            }
            case omniruntime::vec::OMNI_VEC_TYPE_LONG: {
                return sizeof(int64_t);
            }
            case omniruntime::vec::OMNI_VEC_TYPE_DOUBLE: {
                return sizeof(double);
            }
            default:
                break;
        }
        return 0;
    }

    // Helper function for debugging DataType
    static std::string TypeToString(omniruntime::vec::VecTypeId id);

    static bool IsStringType(omniruntime::vec::VecTypeId id);

    static bool IsDecimalType(omniruntime::vec::VecTypeId type);
};

std::unique_ptr<omniruntime::vec::VecType> IntType();

std::unique_ptr<omniruntime::vec::VecType> Date32Type();

std::unique_ptr<omniruntime::vec::VecType> LongType();

std::unique_ptr<omniruntime::vec::VecType> DoubleType();

std::unique_ptr<omniruntime::vec::VecType> BooleanType();

std::unique_ptr<omniruntime::vec::VecType> VarcharType();

std::unique_ptr<omniruntime::vec::VecType> VarcharType(int32_t width);

std::unique_ptr<omniruntime::vec::VecType> CharType(int32_t width);

std::unique_ptr<omniruntime::vec::VecType> Decimal64Type(int32_t precision, int32_t scale);

std::unique_ptr<omniruntime::vec::VecType> Decimal128Type(int32_t precision, int32_t scale);
#endif // OMNI_RUNTIME_TYPE_INFER_H