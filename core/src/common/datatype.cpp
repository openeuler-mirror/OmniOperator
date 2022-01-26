/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: DataType enum and helper functions
 */
#include "datatype.h"
#include <string>
#include <algorithm>
#include "../vector/vector_type.h"

namespace omniruntime {
namespace expressions {
// Helper function to get DataType from jint type
// Find types in core/src/types/vector_type.h
DataType ColTypeTrans(int32_t colType)
{
    switch (colType) {
        case TYPE_INT32D:
        case TYPE_DATE32D:
            return DataType::INT32D;
        case TYPE_INT64D:
        case TYPE_DECIMAL64D:
            return DataType::INT64D;
        case TYPE_DECIMAL128D:
            return DataType::DECIMAL128D;
        case TYPE_DOUBLED:
            return DataType::DOUBLED;
        case TYPE_BOOLD:
            return DataType::BOOLD;
            // Should be short datatype (INT16D)
        case TYPE_INT32D_2ND:
            return DataType::INT32D;
        case TYPE_STRINGD:
            return DataType::VARCHARD;
        case TYPE_CHAR:
            return DataType::CHARD;
        default:
            return DataType::INVALIDDATAD;
    }
}

std::string DataTypeString(DataType dt)
{
    switch (dt) {
        case DataType::BOOLD: return "bool";
        case DataType::DOUBLED: return "double";
        case DataType::INT32D: return "int32";
        case DataType::INT64D: return "int64";
        case DataType::CHARD: return "char";
        case DataType::VARCHARD: return "string";
        case DataType::DECIMAL64D: return "decimal64";
        case DataType::DECIMAL128D: return "decimal128";
        case DataType::INT32PTRD: return "int32ptr";
        case DataType::INT8PTRD: return "int8ptr";
        case DataType::VOIDD: return "void";
        case DataType::INVALIDDATAD: return "invalid";
        default: return "";
    }
}

bool IsStringDataType(DataType type)
{
    return type == CHARD || type == VARCHARD;
}

bool IsDecimalDataType(DataType type)
{
    return type == DECIMAL128D || type == DECIMAL64D;
}

// Helper function to get DataType from a string representing the type
DataType StringToDataType(std::string dt)
{
    // Strip spaces
    dt.erase(remove(dt.begin(), dt.end(), ' '), dt.end());
    if (STRING_DATA_TYPE_MAP.count(dt)) {
        return INVALIDDATAD;
    } else {
        return STRING_DATA_TYPE_MAP.find(dt)->second;
    }
}

// Helper function to get DataType from the enum ordinal value of the type
DataType OrdinalToDataType(const int32_t& dt)
{
    if (dt < INT32_MAX) {
        if (dt == omniruntime::vec::OMNI_VEC_TYPE_DATE32) {
            return DataType::INT32D;
        }
        if (dt == DECIMAL64D) {
            return INT64D;
        }
        if (omniruntime::vec::OMNI_VEC_TYPE_SHORT == dt || (omniruntime::vec::OMNI_VEC_TYPE_DATE64 <= dt &&
            omniruntime::vec::OMNI_VEC_TYPE_INTERVAL_DAY_TIME >= dt)) {
            LogWarn("Unsupported return type: %u", static_cast<omniruntime::vec::VecTypeId>(dt));
        }
        return static_cast<DataType>(dt);
    }
    std::cout << "Unsupported return type: " << dt << std::endl;
    return INVALIDDATAD;
}
}
}