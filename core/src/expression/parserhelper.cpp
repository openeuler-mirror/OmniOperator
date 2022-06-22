/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */
#include "parserhelper.h"

using namespace std;
using namespace omniruntime::expressions;
using namespace omniruntime::type;

constexpr int32_t INT_DEFAULT_VALUE = 0;
constexpr int64_t LONG_DEFAULT_VALUE = 0L;
constexpr double DOUBLE_DEFAULT_VALUE = 0.000;
constexpr bool BOOL_DEFAULT_VALUE = true;
constexpr char CHAR_DEFAULT_VALUE[] = "NULL";
constexpr char DECIMAL128_DEFAULT_VALUE[] = "0";

omniruntime::expressions::LiteralExpr *ParserHelper::GetDefaultValueForType(DataTypeId destTypeId, int32_t precision,
    int32_t scale)
{
    if (TypeUtil::IsDecimalType(destTypeId)) {
        switch (destTypeId) {
            case OMNI_DECIMAL64: {
                DataTypeRawPtr destType = new Decimal64DataType(precision, scale);
                return new LiteralExpr(LONG_DEFAULT_VALUE, std::move(destType));
            }
            case OMNI_DECIMAL128:
            default: {
                DataTypeRawPtr destType = new Decimal128DataType(precision, scale);
                return new LiteralExpr(new string(DECIMAL128_DEFAULT_VALUE), std::move(destType));
            }
        }
    } else {
        DataTypeRawPtr destType = new DataType(destTypeId);
        switch (destTypeId) {
            case OMNI_INT:
            case OMNI_DATE32:
                return new LiteralExpr(INT_DEFAULT_VALUE, std::move(destType));
            case OMNI_LONG:
                return new LiteralExpr(LONG_DEFAULT_VALUE, std::move(destType));
            case OMNI_DOUBLE:
                return new LiteralExpr(DOUBLE_DEFAULT_VALUE, std::move(destType));
            case OMNI_BOOLEAN:
                return new LiteralExpr(BOOL_DEFAULT_VALUE, std::move(destType));
            case OMNI_CHAR:
            case OMNI_VARCHAR:
                return new LiteralExpr(new string(CHAR_DEFAULT_VALUE), std::move(destType));
            case OMNI_NONE:
                return new LiteralExpr(INT_DEFAULT_VALUE, std::move(destType));
            default:
                return nullptr;
        }
    }
}

DataTypeRawPtr ParserHelper::GetReturnDataType(nlohmann::json jsonExpr)
{
    DataTypeId typeId = static_cast<DataTypeId>(jsonExpr["returnType"].get<int32_t>());
    int32_t precision = 0;
    int32_t scale = 0;
    uint32_t width = 0;
    switch (typeId) {
        case OMNI_BOOLEAN:
            return new BooleanDataType();
        case OMNI_INT:
            return new IntDataType();
        case OMNI_DATE32:
            return new Date32DataType();
        case OMNI_LONG:
            return new LongDataType();
        case OMNI_DOUBLE:
            return new DoubleDataType();
        case OMNI_DECIMAL64:
            precision = jsonExpr["precision"].get<int32_t>();
            scale = jsonExpr["scale"].get<int32_t>();
            return new Decimal64DataType(precision, scale);
        case OMNI_DECIMAL128:
            precision = jsonExpr["precision"].get<int32_t>();
            scale = jsonExpr["scale"].get<int32_t>();
            return new Decimal128DataType(precision, scale);
        case OMNI_VARCHAR:
            width = jsonExpr["width"].get<uint32_t>();
            return new VarcharDataType(width);
        case OMNI_CHAR:
            width = jsonExpr["width"].get<uint32_t>();
            return new CharDataType(width);
        case OMNI_NONE:
            return new NoneDataType();
        default:
            LogError("Unsupported data type %d ", typeId);
            return nullptr;
    }
}
