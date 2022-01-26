/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: DataType enum and helper functions
 */
#ifndef OMNI_RUNTIME_DATATYPE_H
#define OMNI_RUNTIME_DATATYPE_H
#include <string>
#include <map>

namespace omniruntime {
namespace expressions {

enum DataType {
    UNKNOWND = 0,
    INT32D = 1,
    INT64D = 2,
    DOUBLED = 3,
    BOOLD = 4,
    DECIMAL64D = 6,
    DECIMAL128D = 7,
    VARCHARD = 15,
    CHARD = 16,
    INT32PTRD,
    INT8PTRD,
    VOIDD,
    INVALIDDATAD
};

// jni type number
const int TYPE_INT32D = 1;
const int TYPE_INT64D = 2;
const int TYPE_DOUBLED = 3;
const int TYPE_BOOLD = 4;
const int TYPE_INT32D_2ND = 5;
const int TYPE_DECIMAL64D = 6;
const int TYPE_DECIMAL128D = 7;
const int TYPE_DATE32D = 8;
const int TYPE_DATE64D = 9;
const int TYPE_TIME32D = 10;
const int TYPE_TIME64D = 11;
const int TYPE_TIMESTAMPD = 12;
const int TYPE_STRINGD = 15;
const int TYPE_CHAR = 16;

const std::map<std::string, DataType> STRING_DATA_TYPE_MAP = {
    {"INT32", INT32D},
    {"INT64", INT64D},
    {"DOUBLE", DOUBLED},
    {"BOOL", BOOLD},
    {"STRING", VARCHARD},
    {"DECIMAL128", DECIMAL128D}
};

// Helper function to get DataType from a string representing the type
DataType StringToDataType(std::string dt);
bool IsStringDataType(DataType type);
bool IsDecimalDataType(DataType type);
// Helper function for debugging DataType
std::string DataTypeString(DataType dt);
// Helper function to translate from jni type number to DataType
DataType ColTypeTrans(int32_t colType);

DataType OrdinalToDataType(const int32_t& dt);

}
}

#endif // OMNI_RUNTIME_DATATYPE_H
