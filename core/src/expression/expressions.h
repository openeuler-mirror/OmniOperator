/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description:
 */
#ifndef __EXPRESSIONS_H__
#define __EXPRESSIONS_H__

#include <codegen/function.h>
#include <map>
#include <string>
#include <vector>
#include "type/data_type.h"
#include "type/decimal128.h"
#include <locale>
#include <regex>
#include <codecvt>
#include "vector/vector_common.h"
#include "vectorization/VectorFunction.h"

class ExprVisitor;

namespace omniruntime {
namespace expressions {
using namespace type;
using namespace codegen;
using namespace vectorization;

enum class Operator {
    // Comparison
    EQ,
    NEQ,
    LT,
    LTE,
    GT,
    GTE,
    // Logical
    AND,
    OR,
    NOT,
    // Arithmetic
    ADD,
    SUB,
    MUL,
    DIV,
    MOD,
    TRY_ADD,
    TRY_SUB,
    TRY_MUL,
    TRY_DIV,
    INVALIDOP
};

enum class OperatorType { COMPARISON, LOGICAL, ARITHMETIC, INVALIDOPTYPE };

enum class ExprType {
    LITERAL_E,
    FIELD_E,
    BINARY_E,
    UNARY_E,
    IN_E,
    BETWEEN_E,
    IF_E,
    SWITCH_E,
    COALESCE_E,
    IS_NULL_E,
    FUNC_E,
    INVALID_E,
};

const std::map<std::string, Operator> OPERATOR_FROM_STRING = {{"EQUAL", Operator::EQ}, {"LESS_THAN", Operator::LT},
    {"LESS_THAN_OR_EQUAL", Operator::LTE}, {"GREATER_THAN_OR_EQUAL", Operator::GTE}, {"GREATER_THAN", Operator::GT},
    {"NOT_EQUAL", Operator::NEQ}, {"AND", Operator::AND}, {"OR", Operator::OR}, {"NOT", Operator::NOT},
    {"not", Operator::NOT}, {"ADD", Operator::ADD}, {"SUBTRACT", Operator::SUB}, {"MULTIPLY", Operator::MUL},
    {"DIVIDE", Operator::DIV}, {"MODULUS", Operator::MOD}, {"TRY_ADD", Operator::TRY_ADD},
    {"TRY_SUBTRACT", Operator::TRY_SUB}, {"TRY_MULTIPLY", Operator::TRY_MUL}, {"TRY_DIVIDE", Operator::TRY_DIV}};

bool IsNullLiteral(const std::string &value);
bool IsComparisonOperator(Operator op);
bool IsLogicalOperator(Operator op);
Operator StringToOperator(const std::string &opStr);

enum ExprFunctionType { BUILTIN = 0, HIVE_UDF };

class Expr {
public:
    DataTypePtr dataType; // dataType of returned value
    std::shared_ptr<VectorFunction> vectorFunction;
    DataTypePtr GetReturnType() const;
    omniruntime::type::DataTypeId GetReturnTypeId() const;
    virtual ExprType GetType() const;
    virtual ~Expr() = default;
    virtual void Accept(ExprVisitor &visitor) const = 0;
    static void DeleteExprs(const std::vector<Expr *> &exprs);
    static void DeleteExprs(const std::vector<std::vector<Expr *>> &exprs);

    virtual uint8_t *compute(omniruntime::vec::VectorBatch *vecBatch, uint8_t *bitMark) { return nullptr; }

    virtual bool supportVectorized() { return false; }
};

class LiteralExpr : public Expr {
public:
    bool isNull = false;
    bool boolVal = false;
    int8_t byteVal = 0;
    int16_t shortVal = 0;
    int32_t intVal = 0;
    int64_t longVal = 0;
    double doubleVal = 0;
    float floatVal = 0;
    std::string *stringVal = nullptr;

    LiteralExpr();
    ~LiteralExpr() override;
    explicit LiteralExpr(bool val, DataTypePtr colType);
    explicit LiteralExpr(int8_t val, DataTypePtr colType, bool isNull = false);
    explicit LiteralExpr(int16_t val, DataTypePtr colType, bool isNull = false);
    explicit LiteralExpr(int32_t val, DataTypePtr colType, bool isNull = false);
    explicit LiteralExpr(int64_t val, DataTypePtr colType);
    explicit LiteralExpr(double val, DataTypePtr colType);
    explicit LiteralExpr(float val, DataTypePtr colType);
    explicit LiteralExpr(std::string *val, DataTypePtr colType);
    void Accept(ExprVisitor &visitor) const override;
    ExprType GetType() const override;
    uint8_t *compute(omniruntime::vec::VectorBatch *vecBatch, uint8_t *bitMark) override;
    bool supportVectorized() override
    {
        return true;
    }
    LiteralExpr* Copy()
    {
        auto newExpr = new LiteralExpr();
        newExpr->dataType = dataType;
        newExpr->isNull = isNull;
        newExpr->boolVal = boolVal;
        newExpr->shortVal = shortVal;
        newExpr->intVal = intVal;
        newExpr->longVal = longVal;
        newExpr->doubleVal = doubleVal;
        newExpr->stringVal = (stringVal != nullptr) ? new std::string(*stringVal) : nullptr;
        return newExpr;
    };
};

class FieldExpr : public Expr {
public:
    bool isNull = false;
    int32_t colVal = 0;
    int32_t oColVal = -1;
    int32_t ordinal = -1;
    Expr* input = nullptr;

    bool operator==(const FieldExpr &other) const
    {
        return isNull == other.isNull && colVal == other.colVal &&
               ordinal == other.ordinal && input == other.input;
    }

    FieldExpr();
    ~FieldExpr() override;
    FieldExpr(int32_t colIdx, DataTypePtr colType);
    FieldExpr(int32_t colIdx, DataTypePtr colType, int32_t oridinal);
    FieldExpr(int32_t colIdx, DataTypePtr colType, int32_t oridinal, Expr* input);
    void Accept(ExprVisitor &visitor) const override;
    ExprType GetType() const override;

    omniruntime::vec::BaseVector *GetFieldVector(omniruntime::vec::VectorBatch *vecBatch);

    bool supportVectorized() override
    {
        return this->dataType->GetId() == OMNI_ROW || this->dataType->GetId() == OMNI_VARCHAR || this->dataType->GetId() == OMNI_MAP ;
    }

    bool FieldIsArray();
    bool FieldIsMap();
};

class UnaryExpr : public Expr {
public:
    Operator op = Operator::EQ;
    Expr *exp = nullptr;

    UnaryExpr();
    ~UnaryExpr() override;
    UnaryExpr(Operator logOp, Expr *bodyExpr);
    UnaryExpr(Operator uop, Expr *expr, DataTypePtr dt);

    void Accept(ExprVisitor &visitor) const override;
    ExprType GetType() const override;

    uint8_t* computeNOT(omniruntime::vec::VectorBatch *vecBatch, uint8_t *bitMark);
    uint8_t *compute(omniruntime::vec::VectorBatch *vecBatch, uint8_t *bitMark) override;

    bool supportVectorized() override
    {
        return exp->supportVectorized();
    }
};

class BinaryExpr : public Expr {
public:
    Operator op = Operator::EQ;
    Expr *left = nullptr;
    Expr *right = nullptr;

    BinaryExpr();
    ~BinaryExpr() override;
    BinaryExpr(Operator bop, Expr *leftExpr, Expr *rightExpr, DataTypePtr dt);
    void Accept(ExprVisitor &visitor) const override;
    ExprType GetType() const override;

    uint8_t* computeEQ(omniruntime::vec::VectorBatch *vecBatch, uint8_t *bitMark);
    uint8_t* computeAND(omniruntime::vec::VectorBatch *vecBatch, uint8_t *bitMark);
    uint8_t* computeOR(omniruntime::vec::VectorBatch *vecBatch, uint8_t *bitMark);
    uint8_t* computeNEQ(omniruntime::vec::VectorBatch *vecBatch, uint8_t *bitMark);
    uint8_t *compute(omniruntime::vec::VectorBatch *vecBatch, uint8_t *bitMark) override;

    bool supportVectorized() override
    {
        return left->supportVectorized() && right->supportVectorized();
    }
};

class InExpr : public Expr {
public:
    // first element of arguments is the value to be compared to every other argument
    std::vector<Expr *> arguments;

    InExpr();
    ~InExpr() override;
    explicit InExpr(std::vector<Expr *> args);

    void Accept(ExprVisitor &visitor) const override;
    ExprType GetType() const override;
};

class BetweenExpr : public Expr {
public:
    Expr *value = nullptr;
    Expr *lowerBound = nullptr;
    Expr *upperBound = nullptr;

    BetweenExpr();
    ~BetweenExpr() override;
    BetweenExpr(Expr *val, Expr *lowBound, Expr *upBound);

    void Accept(ExprVisitor &visitor) const override;
    ExprType GetType() const override;
};

class SwitchExpr : public Expr {
public:
    std::vector<std::pair<Expr *, Expr *>> whenClause;
    Expr *falseExpr = nullptr;
    SwitchExpr();
    ~SwitchExpr() override;
    SwitchExpr(const std::vector<std::pair<Expr *, Expr *>> &whens, Expr *fexp);

    void Accept(ExprVisitor &visitor) const override;
    ExprType GetType() const override;
};

class IfExpr : public Expr {
public:
    Expr *condition = nullptr;
    Expr *trueExpr = nullptr;
    Expr *falseExpr = nullptr;

    IfExpr();
    ~IfExpr() override;
    IfExpr(Expr *cond, Expr *texp, Expr *fexp);

    void Accept(ExprVisitor &visitor) const override;
    ExprType GetType() const override;
};

class CoalesceExpr : public Expr {
public:
    Expr *value1 = nullptr;
    Expr *value2 = nullptr;

    CoalesceExpr();
    ~CoalesceExpr() override;
    CoalesceExpr(Expr *val1, Expr *val2);

    void Accept(ExprVisitor &visitor) const override;
    ExprType GetType() const override;
};

class IsNullExpr : public Expr {
public:
    Expr *value = nullptr;
    IsNullExpr();
    ~IsNullExpr() override;
    explicit IsNullExpr(Expr *value);

    void Accept(ExprVisitor &visitor) const override;
    ExprType GetType() const override;

    uint8_t *compute(omniruntime::vec::VectorBatch *vecBatch, uint8_t *bitMark) override;

    bool supportVectorized() override
    {
        return value->supportVectorized();
    }
};

class FuncExpr : public Expr {
public:
    std::string funcName;
    std::vector<Expr *> arguments;
    const Function *function;
    ExprFunctionType functionType;

    FuncExpr();
    ~FuncExpr() override;
    FuncExpr(const std::string &fnName, const std::vector<Expr *> &args, DataTypePtr returnType);
    FuncExpr(
        const std::string &fnName, const std::vector<Expr *> &args, DataTypePtr returnType, const Function *function);
    FuncExpr(const std::string &fnName, const std::vector<Expr *> &args, DataTypePtr returnType,
        ExprFunctionType functionType);

    void Accept(ExprVisitor &visitor) const override;
    ExprType GetType() const override;
    static inline bool IsCastStrStr(const omniruntime::expressions::FuncExpr &e)
    {
        return (e.funcName == "CAST" || e.funcName == "CAST_null") &&
            e.arguments[0]->GetReturnTypeId() == omniruntime::type::OMNI_VARCHAR &&
            e.GetReturnTypeId() == omniruntime::type::OMNI_VARCHAR;
    }

    uint8_t *compute(omniruntime::vec::VectorBatch *vecBatc, uint8_t *bitMark) override;
    std::wstring_convert<std::codecvt_utf8<wchar_t>> convert;

    bool supportVectorized() override
    {
        return funcName == "RLike";
    }
};
} // namespace expressions
} // namespace omniruntime
#endif
