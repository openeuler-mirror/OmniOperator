/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */
#include "parserhelper.h"

using namespace std;
using namespace omniruntime::expressions;
using namespace omniruntime::vec;

constexpr int32_t INT_DEFAULT_VALUE = 0;
constexpr int64_t LONG_DEFAULT_VALUE = 0L;
constexpr double DOUBLE_DEFAULT_VALUE = 0.000;
constexpr bool BOOL_DEFAULT_VALUE = true;
constexpr char *CHAR_DEFAULT_VALUE = "NULL";
constexpr int64_t DECIMAL128_DEFAULT_VALUE = 0L;

omniruntime::expressions::LiteralExpr *ParserHelper::GetDefaultValueForType(VecTypeId destTypeId)
{
    VecTypePtr destType = make_unique<VecType>(destTypeId);
    switch (destTypeId) {
        case OMNI_VEC_TYPE_INT:
        case OMNI_VEC_TYPE_DATE32:
            return std::make_unique<LiteralExpr>(INT_DEFAULT_VALUE, std::move(destType)).release();
        case OMNI_VEC_TYPE_LONG:
        case OMNI_VEC_TYPE_DECIMAL64:
            return std::make_unique<LiteralExpr>(LONG_DEFAULT_VALUE, std::move(destType)).release();
        case OMNI_VEC_TYPE_DOUBLE:
            return std::make_unique<LiteralExpr>(DOUBLE_DEFAULT_VALUE, std::move(destType)).release();
        case OMNI_VEC_TYPE_BOOLEAN:
            return std::make_unique<LiteralExpr>(BOOL_DEFAULT_VALUE, std::move(destType)).release();
        case OMNI_VEC_TYPE_CHAR:
        case OMNI_VEC_TYPE_VARCHAR:
            return std::make_unique<LiteralExpr>(
                    make_unique<string>(CHAR_DEFAULT_VALUE).release(), std::move(destType)).release();
        case OMNI_VEC_TYPE_DECIMAL128:
            return std::make_unique<LiteralExpr>(DECIMAL128_DEFAULT_VALUE, std::move(destType)).release();
        case OMNI_VEC_TYPE_NONE:
            return std::make_unique<LiteralExpr>(INT_DEFAULT_VALUE, std::move(destType)).release();
        default:
            return nullptr;
    }
}