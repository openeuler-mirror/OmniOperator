/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Inner supported aggregators source file
 */
#include "type_util.h"

using namespace omniruntime::vec;

bool TypeUtil::IsStringType(omniruntime::vec::VecTypeId id)
{
    return id == OMNI_VEC_TYPE_CHAR || id == OMNI_VEC_TYPE_VARCHAR;
}

bool TypeUtil::IsDecimalType(omniruntime::vec::VecTypeId type)
{
    return type == OMNI_VEC_TYPE_DECIMAL128 || type == OMNI_VEC_TYPE_DECIMAL64;
}

std::string TypeUtil::TypeToString(omniruntime::vec::VecTypeId id)
{
    switch (id) {
        case OMNI_VEC_TYPE_BOOLEAN:
            return "bool";
        case OMNI_VEC_TYPE_DOUBLE:
            return "double";
        case OMNI_VEC_TYPE_DATE32:
        case OMNI_VEC_TYPE_INT:
            return "int32";
        case OMNI_VEC_TYPE_LONG:
            return "int64";
        case OMNI_VEC_TYPE_VARCHAR:
            return "string";
        case OMNI_VEC_TYPE_CHAR:
            return "char";
        case OMNI_VEC_TYPE_DECIMAL64:
            return "decimal64";
        case OMNI_VEC_TYPE_DECIMAL128:
            return "decimal128";
        case OMNI_VEC_TYPE_NONE:
            return "void";
        case OMNI_VEC_TYPE_INVALID:
            return "invalid";
        default:
            return "";
    }
}

std::unique_ptr<VecType> IntType()
{
    return std::make_unique<VecType>(OMNI_VEC_TYPE_INT);
}

std::unique_ptr<VecType> Date32Type()
{
    return std::make_unique<VecType>(OMNI_VEC_TYPE_DATE32);
}

std::unique_ptr<VecType> LongType()
{
    return std::make_unique<VecType>(OMNI_VEC_TYPE_LONG);
}

std::unique_ptr<VecType> DoubleType()
{
    return std::make_unique<VecType>(OMNI_VEC_TYPE_DOUBLE);
}

std::unique_ptr<VecType> BooleanType()
{
    return std::make_unique<VecType>(OMNI_VEC_TYPE_BOOLEAN);
}

std::unique_ptr<VecType> VarcharType()
{
    return std::make_unique<VarcharVecType>(INT_MAX);
}

std::unique_ptr<VecType> VarcharType(int32_t width)
{
    return std::make_unique<VarcharVecType>(width);
}

std::unique_ptr<VecType> CharType(int32_t width)
{
    return std::make_unique<CharVecType>(width);
}

std::unique_ptr<VecType> Decimal64Type(int32_t precision, int32_t scale)
{
    return std::make_unique<Decimal64VecType>(precision, scale);
}

std::unique_ptr<VecType> Decimal128Type(int32_t precision, int32_t scale)
{
    return std::make_unique<Decimal128VecType>(precision, scale);
}