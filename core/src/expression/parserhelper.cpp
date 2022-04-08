/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */
#include "parserhelper.h"

using namespace std;
using namespace omniruntime::expressions;
using namespace omniruntime::type;

namespace omniruntime {
namespace expressions {
constexpr int32_t INT_DEFAULT_VALUE = 0;
constexpr int64_t LONG_DEFAULT_VALUE = 0L;
constexpr double DOUBLE_DEFAULT_VALUE = 0.000;
constexpr bool BOOL_DEFAULT_VALUE = true;
constexpr char CHAR_DEFAULT_VALUE[] = "NULL";
constexpr char DECIMAL128_DEFAULT_VALUE[] = "0";

omniruntime::expressions::LiteralExpr *ParserHelper::GetDefaultValueForType(DataTypeId destTypeId)
{
    DataTypePtr destType = make_unique<DataType>(destTypeId);
    switch (destTypeId) {
        case OMNI_INT:
        case OMNI_DATE32:
            return new LiteralExpr(INT_DEFAULT_VALUE, std::move(destType));
        case OMNI_LONG:
        case OMNI_DECIMAL64:
            return new LiteralExpr(LONG_DEFAULT_VALUE, std::move(destType));
        case OMNI_DOUBLE:
            return new LiteralExpr(DOUBLE_DEFAULT_VALUE, std::move(destType));
        case OMNI_BOOLEAN:
            return new LiteralExpr(BOOL_DEFAULT_VALUE, std::move(destType));
        case OMNI_CHAR:
        case OMNI_VARCHAR:
            return new LiteralExpr(new string(CHAR_DEFAULT_VALUE), std::move(destType));
        case OMNI_DECIMAL128:
            return new LiteralExpr(new string(DECIMAL128_DEFAULT_VALUE), std::move(destType));
        case OMNI_NONE:
            return new LiteralExpr(INT_DEFAULT_VALUE, std::move(destType));
        default:
            return nullptr;
    }
}

DataTypePtr ParserHelper::GetReturnDataType(nlohmann::json jsonExpr)
{
    DataTypeId typeId = static_cast<DataTypeId>(jsonExpr["returnType"].get<int32_t>());
    int precision = 0;
    int scale = 0;
    int width = 0;
    switch (typeId) {
        case OMNI_BOOLEAN:
            return make_unique<BooleanDataType>();
        case OMNI_INT:
            return make_unique<IntDataType>();
        case OMNI_DATE32:
            return make_unique<DataType>(OMNI_DATE32);
        case OMNI_LONG:
            return make_unique<LongDataType>();
        case OMNI_DOUBLE:
            return make_unique<DoubleDataType>();
        case OMNI_DECIMAL64:
            precision = jsonExpr["precision"].get<int32_t>();
            scale = jsonExpr["scale"].get<int32_t>();
            return make_unique<Decimal64DataType>(precision, scale);
        case OMNI_DECIMAL128:
            precision = jsonExpr["precision"].get<int32_t>();
            scale = jsonExpr["scale"].get<int32_t>();
            return make_unique<Decimal128DataType>(precision, scale);
        case OMNI_VARCHAR:
            width = jsonExpr["width"].get<int32_t>();
            return make_unique<VarcharDataType>(width);
        case OMNI_CHAR:
            width = jsonExpr["width"].get<int32_t>();
            return make_unique<CharDataType>(width);
        case OMNI_NONE:
            return make_unique<DataType>();
        default:
            LLVM_DEBUG_LOG("Unsupported data type  %d", typeId);
            LogError("Unsupported data type %d ", typeId);
            return nullptr;
    }
}
}
}