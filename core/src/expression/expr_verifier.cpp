//
// Created by root on 5/13/22.
//
#include "expr_verifier.h"

using namespace omniruntime::expressions;
using namespace omniruntime::type;

bool ExprVerifier::VisitExpr(const Expr &e)
{
    e.Accept(*this);
    return this->supportedFlag;
}

bool ExprVerifier::AreInvalidDataTypes(DataTypeId type1, DataTypeId type2)
{
    return type1 != type2 && !(TypeUtil::IsStringType(type1) && TypeUtil::IsStringType(type2));
}

void ExprVerifier::Visit(const LiteralExpr &literalExpr)
{
    switch (literalExpr.GetReturnTypeId()) {
        case OMNI_INT:
        case OMNI_DATE32:
        case OMNI_LONG:
        case OMNI_DOUBLE:
        case OMNI_CHAR:
        case OMNI_VARCHAR:
        case OMNI_BOOLEAN:
        case OMNI_DECIMAL64:
        case OMNI_DECIMAL128:
            this->supportedFlag = true;
        default:
            this->supportedFlag = false;
    }
}

void ExprVerifier::Visit(const FieldExpr &fieldExpr)
{
    switch (fieldExpr.GetReturnTypeId()) {
        case OMNI_INT:
        case OMNI_DATE32:
        case OMNI_LONG:
        case OMNI_DOUBLE:
        case OMNI_CHAR:
        case OMNI_VARCHAR:
        case OMNI_BOOLEAN:
        case OMNI_DECIMAL64:
        case OMNI_DECIMAL128:
            this->supportedFlag = true;
            break;
        default:
            this->supportedFlag = false;
            break;
    }
}

void ExprVerifier::Visit(const UnaryExpr &unaryExpr)
{
    if (!VisitExpr(*(unaryExpr.exp))) {
        this->supportedFlag = false;
        return;
    }
    switch (unaryExpr.op) {
        case omniruntime::expressions::Operator::NOT:
            this->supportedFlag = true;
            break;
        default:
            this->supportedFlag = false;
            break;
    }
}

void ExprVerifier::Visit(const BinaryExpr &binaryExpr)
{
    if (binaryExpr.left->GetReturnTypeId() == DataTypeId::OMNI_DECIMAL64 &&
        binaryExpr.right->GetReturnTypeId() == DataTypeId::OMNI_DECIMAL64 &&
        binaryExpr.GetReturnTypeId() == DataTypeId::OMNI_DECIMAL128) {
        this->supportedFlag = false;
        return;
    }
    if (!VisitExpr(*(binaryExpr.left))) {
        this->supportedFlag = false;
        return;
    }
    if (!VisitExpr(*(binaryExpr.right))) {
        this->supportedFlag = false;
        return;
    }

    if (binaryExpr.op == omniruntime::expressions::Operator::AND ||
        binaryExpr.op == omniruntime::expressions::Operator::OR) {
        this->supportedFlag = (binaryExpr.left->GetReturnTypeId() == binaryExpr.right->GetReturnTypeId() &&
            binaryExpr.left->GetReturnTypeId() == DataTypeId::OMNI_BOOLEAN);
        return;
    }

    if (binaryExpr.left->GetReturnTypeId() == OMNI_INT || binaryExpr.left->GetReturnTypeId() == OMNI_LONG ||
        binaryExpr.left->GetReturnTypeId() == OMNI_DATE32 || binaryExpr.left->GetReturnTypeId() == OMNI_DOUBLE) {
        this->supportedFlag = true;
        return;
    } else if (TypeUtil::IsStringType(binaryExpr.left->GetReturnTypeId())) {
        switch (binaryExpr.op) {
            case omniruntime::expressions::Operator::LT:
            case omniruntime::expressions::Operator::GT:
            case omniruntime::expressions::Operator::LTE:
            case omniruntime::expressions::Operator::GTE:
            case omniruntime::expressions::Operator::EQ:
            case omniruntime::expressions::Operator::NEQ:
                this->supportedFlag = true;
                break;
            default:
                this->supportedFlag = false;
                break;
        }
        return;
    } else if (binaryExpr.left->GetReturnTypeId() == OMNI_DECIMAL64) {
        // hard code p, s
    } else if (binaryExpr.left->GetReturnTypeId() == OMNI_DECIMAL64) {
        // hard code p, s
    }
    this->supportedFlag = false;
}

void ExprVerifier::Visit(const InExpr &inExpr)
{
    Expr *toCompare = inExpr.arguments[0];
    switch (toCompare->GetReturnTypeId()) {
        case OMNI_INT:
        case OMNI_DATE32:
        case OMNI_LONG:
        case OMNI_DOUBLE:
        case OMNI_CHAR:
        case OMNI_VARCHAR:
        case OMNI_DECIMAL64:
        case OMNI_DECIMAL128:
            break;
        default:
            this->supportedFlag = false;
            return;
    }

    if (!VisitExpr(*toCompare)) {
        this->supportedFlag = false;
        return;
    }
    for (size_t i = 1; i < inExpr.arguments.size(); i++) {
        if (AreInvalidDataTypes(toCompare->GetReturnTypeId(), inExpr.arguments[i]->GetReturnTypeId())) {
            this->supportedFlag = false;
            return;
        }
        if (!VisitExpr(*(inExpr.arguments[i]))) {
            this->supportedFlag = false;
            return;
        }
    }
    this->supportedFlag = true;
}

void ExprVerifier::Visit(const BetweenExpr &betweenExpr)
{
    DataTypeId valueTypeId = betweenExpr.value->GetReturnTypeId();
    if (AreInvalidDataTypes(valueTypeId, betweenExpr.lowerBound->GetReturnTypeId()) &&
        AreInvalidDataTypes(valueTypeId, betweenExpr.upperBound->GetReturnTypeId())) {
        this->supportedFlag = false;
        return;
    }
    if (!VisitExpr(betweenExpr)) {
        this->supportedFlag = false;
        return;
    }
    if (!VisitExpr(betweenExpr)) {
        this->supportedFlag = false;
        return;
    }
    if (!VisitExpr(betweenExpr)) {
        this->supportedFlag = false;
        return;
    }

    if (betweenExpr.lowerBound->GetReturnTypeId() == OMNI_INT ||
        betweenExpr.lowerBound->GetReturnTypeId() == OMNI_LONG ||
        betweenExpr.lowerBound->GetReturnTypeId() == OMNI_DATE32 ||
        betweenExpr.lowerBound->GetReturnTypeId() == OMNI_DOUBLE ||
        TypeUtil::IsStringType(betweenExpr.lowerBound->GetReturnTypeId())) {
        this->supportedFlag = true;
        return;
    } else if (betweenExpr.lowerBound->GetReturnTypeId() == OMNI_DECIMAL64) {
        // hard code p, s
    } else if (betweenExpr.lowerBound->GetReturnTypeId() == OMNI_DECIMAL64) {
        // hard code p, s
    }
}

void ExprVerifier::Visit(const IfExpr &ifExpr)
{
    Expr *cond = ifExpr.condition;
    Expr *ifTrue = ifExpr.trueExpr;
    Expr *ifFalse = ifExpr.falseExpr;

    if (!VisitExpr(*cond)) {
        this->supportedFlag = false;
        return;
    }
    if (!VisitExpr(*ifTrue)) {
        this->supportedFlag = false;
        return;
    }
    if (!VisitExpr(*ifFalse)) {
        this->supportedFlag = false;
        return;
    }

    if (TypeUtil::IsDecimalType(ifExpr.GetReturnTypeId())) {
        // hard code p, s
    }

}

void ExprVerifier::Visit(const CoalesceExpr &coalesceExpr)
{
    Expr *value1Expr = coalesceExpr.value1;
    Expr *value2Expr = coalesceExpr.value2;
    if (!VisitExpr(*value1Expr)) {
        this->supportedFlag = false;
        return;
    }
    if (!VisitExpr(*value2Expr)) {
        this->supportedFlag = false;
        return;
    }

    if (TypeUtil::IsDecimalType(coalesceExpr.GetReturnTypeId())) {
        // hard code p, s
    }
}

void ExprVerifier::Visit(const IsNullExpr &isNullExpr)
{
    Expr *valueExpr = isNullExpr.value;
    if (!VisitExpr(*valueExpr)) {
        this->supportedFlag = false;
        return;
    }
    this->supportedFlag = true;
}

void ExprVerifier::Visit(const FuncExpr &funcExpr)
{
    int numArgs = funcExpr.arguments.size();
    for (int i = 0; i < numArgs; i++) {
        if (!VisitExpr(*funcExpr.arguments[i])) {
            this->supportedFlag = false;
            return;
        }
    }
    this->supportedFlag = true;
}

void ExprVerifier::Visit(const SwitchExpr &switchExpr)
{
    std::vector<std::pair<Expr *, Expr *>> whenClause = switchExpr.whenClause;
    const int size = whenClause.size();

    for (int i = 0; i < size; i++) {
        Expr *cond = whenClause[i].first;
        Expr *resExpr = whenClause[i].second;
        if (!VisitExpr(*cond)) {
            this->supportedFlag = false;
            return;
        }
        if (!VisitExpr(*resExpr)) {
            this->supportedFlag = false;
            return;
        }
    }

    Expr *elseExpr = switchExpr.falseExpr;
    if (!VisitExpr(*elseExpr)) {
        this->supportedFlag = false;
        return;
    }

    this->supportedFlag = true;
}
