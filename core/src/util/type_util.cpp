/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Inner supported aggregators source file
 */
#include "type_util.h"

using namespace omniruntime::type;

bool TypeUtil::IsStringType(omniruntime::type::DataTypeId id)
{
    return id == OMNI_CHAR || id == OMNI_VARCHAR;
}

bool TypeUtil::IsDecimalType(omniruntime::type::DataTypeId type)
{
    return type == OMNI_DECIMAL128 || type == OMNI_DECIMAL64;
}

std::string TypeUtil::TypeToString(omniruntime::type::DataTypeId id)
{
    switch (id) {
        case OMNI_BOOLEAN:
            return "bool";
        case OMNI_DOUBLE:
            return "double";
        case OMNI_DATE32:
        case OMNI_INT:
            return "int32";
        case OMNI_LONG:
            return "int64";
        case OMNI_VARCHAR:
            return "string";
        case OMNI_CHAR:
            return "char";
        case OMNI_DECIMAL64:
            return "decimal64";
        case OMNI_DECIMAL128:
            return "decimal128";
        case OMNI_NONE:
            return "void";
        case OMNI_INVALID:
            return "invalid";
        default:
            return "";
    }
}

std::shared_ptr<DataType> NoneType()
{
    return NoneDataType::Instance();
}

std::shared_ptr<DataType> IntType()
{
    return IntDataType::Instance();
}

std::shared_ptr<DataType> Date32Type()
{
    return Date32DataType::Instance();
}

std::shared_ptr<DataType> LongType()
{
    return LongDataType::Instance();
}

std::shared_ptr<DataType> DoubleType()
{
    return DoubleDataType::Instance();
}

std::shared_ptr<DataType> BooleanType()
{
    return BooleanDataType::Instance();
}

std::shared_ptr<DataType> VarcharType()
{
    return std::make_shared<VarcharDataType>(INT_MAX);
}

std::shared_ptr<DataType> VarcharType(int32_t width)
{
    return std::make_shared<VarcharDataType>(width);
}

std::shared_ptr<DataType> CharType(int32_t width)
{
    return std::make_shared<CharDataType>(width);
}

std::shared_ptr<DataType> Decimal64Type(int32_t precision, int32_t scale)
{
    return std::make_shared<Decimal64DataType>(precision, scale);
}

std::shared_ptr<DataType> Decimal128Type(int32_t precision, int32_t scale)
{
    return std::make_shared<Decimal128DataType>(precision, scale);
}