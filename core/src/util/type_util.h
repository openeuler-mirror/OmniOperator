/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: JNI Vector Operations Header
 */
#ifndef OMNI_RUNTIME_TYPE_INFER_H
#define OMNI_RUNTIME_TYPE_INFER_H

#include <cstdint>
#include "type/data_types.h"
#include "debug.h"

class TypeUtil {
public:
    static int32_t GetVarByteSize(uint32_t type)
    {
        switch (type) {
            case omniruntime::type::OMNI_INT: {
                return sizeof(int32_t);
            }
            case omniruntime::type::OMNI_SHORT: {
                return sizeof(int16_t);
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

    // used in info/error logs
    static std::string TypeToStringLog(omniruntime::type::DataTypeId id);

    static bool IsStringType(omniruntime::type::DataTypeId id);

    static bool IsDecimalType(omniruntime::type::DataTypeId type);
};

namespace omniruntime {
namespace type {
std::shared_ptr<omniruntime::type::DataType> InvalidType();

std::shared_ptr<omniruntime::type::DataType> NoneType();

std::shared_ptr<omniruntime::type::DataType> IntType();

std::shared_ptr<omniruntime::type::DataType> ShortType();

std::shared_ptr<omniruntime::type::DataType> Date32Type();

std::shared_ptr<omniruntime::type::DataType> Date64Type();

std::shared_ptr<omniruntime::type::DataType> Date32Type(omniruntime::type::DateUnit dateUnit);

std::shared_ptr<omniruntime::type::DataType> Date64Type(omniruntime::type::DateUnit dateUnit);

std::shared_ptr<omniruntime::type::DataType> Time32Type();

std::shared_ptr<omniruntime::type::DataType> Time64Type();

std::shared_ptr<omniruntime::type::DataType> Time32Type(omniruntime::type::TimeUnit timeUnit);

std::shared_ptr<omniruntime::type::DataType> Time64Type(omniruntime::type::TimeUnit timeUnit);

std::shared_ptr<omniruntime::type::DataType> LongType();

std::shared_ptr<omniruntime::type::DataType> TimestampType();

std::shared_ptr<omniruntime::type::DataType> DoubleType();

std::shared_ptr<omniruntime::type::DataType> BooleanType();

std::shared_ptr<omniruntime::type::DataType> VarcharType();

std::shared_ptr<omniruntime::type::DataType> CharType();

std::shared_ptr<omniruntime::type::DataType> VarcharType(int32_t width);

std::shared_ptr<omniruntime::type::DataType> CharType(int32_t width);

std::shared_ptr<omniruntime::type::DataType> Decimal64Type();

std::shared_ptr<omniruntime::type::DataType> Decimal128Type();

std::shared_ptr<omniruntime::type::DataType> Decimal64Type(int32_t precision, int32_t scale);

std::shared_ptr<omniruntime::type::DataType> Decimal128Type(int32_t precision, int32_t scale);

std::shared_ptr<omniruntime::type::DataType> ContainerType();

std::shared_ptr<omniruntime::type::ContainerDataType> ContainerType(
    std::vector<omniruntime::type::DataTypePtr> &fieldTypes);

std::shared_ptr<omniruntime::type::ContainerDataType> ContainerType(
    std::vector<omniruntime::type::DataTypePtr> &&fieldTypes);

template <typename...> struct CheckTypesContainsDecimal128 {
    static constexpr bool value = false;
};

template <typename RawType> struct CheckTypesContainsDecimal128<RawType> {
    static constexpr bool value = std::is_same_v<RawType, omniruntime::type::Decimal128>;
};

template <typename oneType, typename... RemainTypes> struct CheckTypesContainsDecimal128<oneType, RemainTypes...> {
    static constexpr bool value =
        CheckTypesContainsDecimal128<oneType>::value || CheckTypesContainsDecimal128<RemainTypes...>::value;
};
}
}

#endif // OMNI_RUNTIME_TYPE_INFER_H