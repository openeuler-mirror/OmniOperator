/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Inner supported aggregators source file
 */
#include "type_util.h"

const std::map<std::string, omniruntime::vec::VecTypeId> TypeUtil::stringDataTypeMap = {
    {"INT32", OMNI_VEC_TYPE_INT},
    {"INT64", OMNI_VEC_TYPE_LONG},
    {"DOUBLE", OMNI_VEC_TYPE_DOUBLE},
    {"BOOL", OMNI_VEC_TYPE_BOOLEAN},
    {"STRING", OMNI_VEC_TYPE_VARCHAR},
    {"DECIMAL128", OMNI_VEC_TYPE_DECIMAL128},
    {"DECIMAL64", OMNI_VEC_TYPE_DECIMAL64}
};

omniruntime::vec::VecTypeId TypeUtil::StringToType(std::string dt)
{
    // Strip spaces
    dt.erase(remove(dt.begin(), dt.end(), ' '), dt.end());
    if (stringDataTypeMap.count(dt)) {
        return OMNI_VEC_TYPE_INVALID;
    } else {
        return stringDataTypeMap.find(dt)->second;
    }
}

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
        case OMNI_VEC_TYPE_BOOLEAN: return "bool";
        case OMNI_VEC_TYPE_DOUBLE: return "double";
        case OMNI_VEC_TYPE_INT: return "int32";
        case OMNI_VEC_TYPE_LONG: return "int64";
        case OMNI_VEC_TYPE_VARCHAR: return "string";
        case OMNI_VEC_TYPE_CHAR: return "char";
        case OMNI_VEC_TYPE_DECIMAL64: return "decimal64";
        case OMNI_VEC_TYPE_DECIMAL128: return "decimal128";
        case OMNI_VEC_TYPE_NONE: return "void";
        case OMNI_VEC_TYPE_INVALID: return "invalid";
        default: return "";
    }
}
