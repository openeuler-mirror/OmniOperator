/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */
#include "parserhelper.h"

using namespace std;
using namespace omniruntime::expressions;
using namespace omniruntime::vec;

omniruntime::expressions::LiteralExpr *ParserHelper::GetDefaultValueForType(VecTypeId destTypeId)
{
    VecTypePtr destType = make_unique<VecType>(destTypeId);
    switch (destTypeId) {
        case OMNI_VEC_TYPE_INT:
        case OMNI_VEC_TYPE_DATE32:
            return std::make_unique<LiteralExpr>(0, std::move(destType)).release();
        case OMNI_VEC_TYPE_LONG:
            return std::make_unique<LiteralExpr>(0L, std::move(destType)).release();
        case OMNI_VEC_TYPE_DOUBLE:
            return std::make_unique<LiteralExpr>(0.000, std::move(destType)).release();
        case OMNI_VEC_TYPE_BOOLEAN:
            return std::make_unique<LiteralExpr>(true, std::move(destType)).release();
        case OMNI_VEC_TYPE_CHAR:
        case OMNI_VEC_TYPE_VARCHAR:
            return std::make_unique<LiteralExpr>(make_unique<string>("NULL").release(), std::move(destType)).release();
        case OMNI_VEC_TYPE_DECIMAL128:
            return std::make_unique<LiteralExpr>(0L, std::move(destType)).release();
        case OMNI_VEC_TYPE_NONE:
            return std::make_unique<LiteralExpr>(0, std::move(destType)).release();
        default:
            return nullptr;
    }
}