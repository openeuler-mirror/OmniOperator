/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description:
 */
#include "expressions.h"
#include <string>
#include <algorithm>
#include <utility>
#include "vectorization/functions/IsNull.h"
#include "type/data_type.h"
#include "codegen/func_registry.h"
#include "util/type_util.h"
#include "expr_verifier.h"
#include "expr_printer.h"
#include "arm_neon.h"

using namespace std;
using namespace omniruntime::type;
using namespace omniruntime::codegen;

namespace omniruntime {
namespace expressions {
// Prevent ExprVerifier from being optimized out by the compiler.
static ExprVerifier globalExprVerifier;
static ExprPrinter globalExprPrinter;
const static int32_t NEON_BYTE_SIZE = 16;

bool IsNullLiteral(const std::string &value)
{
    const std::string loweredNullValue = "null";
    if (value.size() != loweredNullValue.size()) {
        return false;
    }
    for (uint32_t i = 0; i < loweredNullValue.size(); i++) {
        if (tolower(value[i]) != loweredNullValue[i]) {
            return false;
        }
    }
    return true;
}

bool IsComparisonOperator(Operator op)
{
    return op == Operator::GT || op == Operator::GTE || op == Operator::LT || op == Operator::LTE || op == Operator::EQ
        || op == Operator::NEQ;
}

bool IsLogicalOperator(Operator op)
{
    return op == Operator::AND || op == Operator::OR || op == Operator::NOT;
}

Operator StringToOperator(const std::string &opStr)
{
    auto opItr = OPERATOR_FROM_STRING.find(opStr);
    if (opItr != OPERATOR_FROM_STRING.end()) {
        return opItr->second;
    }
    return Operator::INVALIDOP;
}

ExprType Expr::GetType() const
{
    return ExprType::INVALID_E;
}

DataTypePtr Expr::GetReturnType() const
{
    return dataType;
}

DataTypeId Expr::GetReturnTypeId() const
{
    return dataType->GetId();
}

void Expr::DeleteExprs(const std::vector<Expr *> &exprs)
{
    for (Expr *exp : exprs) {
        delete exp;
    }
}

void Expr::DeleteExprs(const std::vector<std::vector<Expr *>> &exprs)
{
    for (const std::vector<Expr *> &expr : exprs) {
        Expr::DeleteExprs(expr);
    }
}

// Literal Expression methods
LiteralExpr::LiteralExpr() = default;

LiteralExpr::~LiteralExpr()
{
    delete stringVal;
}

ExprType LiteralExpr::GetType() const
{
    return ExprType::LITERAL_E;
}

// Helper constructors for different data types
LiteralExpr::LiteralExpr(bool val, DataTypePtr dt)
{
    dataType = std::move(dt);
    boolVal = val;
}

LiteralExpr::LiteralExpr(int8_t val, DataTypePtr dt, bool isNulls)
{
    dataType = std::move(dt);
    byteVal = val;
    isNull = isNulls;
}

LiteralExpr::LiteralExpr(int16_t val, DataTypePtr dt, bool isNulls)
{
    dataType = std::move(dt);
    shortVal = val;
    isNull = isNulls;
}

LiteralExpr::LiteralExpr(int32_t val, DataTypePtr dt, bool isNulls)
{
    dataType = std::move(dt);
    intVal = val;
    isNull = isNulls;
}

LiteralExpr::LiteralExpr(int64_t val, DataTypePtr dt)
{
    dataType = std::move(dt);
    longVal = val;
}

LiteralExpr::LiteralExpr(double val, DataTypePtr dt)
{
    dataType = std::move(dt);
    doubleVal = val;
}

LiteralExpr::LiteralExpr(float val, DataTypePtr dt)
{
    dataType = std::move(dt);
    floatVal = val;
}

LiteralExpr::LiteralExpr(std::string *val, DataTypePtr dt)
{
    dataType = std::move(dt);
    stringVal = val;
}

uint8_t* LiteralExpr::compute(omniruntime::vec::VectorBatch *vecBatch, uint8_t *bitMark)
{
    if (dataType->GetId() != OMNI_BOOLEAN) {
        throw omniruntime::exception::OmniException(
            "OPERATOR_RUNTIME_ERROR", "LiteralExpr::compute Only Support Boolean.");
    }
    auto rowSize = vecBatch->GetRowCount();
    if (boolVal) {
        memset_s(bitMark, BitUtil::Nbytes(rowSize), 0xFF, BitUtil::Nbytes(rowSize));
    } else {
        memset_s(bitMark, BitUtil::Nbytes(rowSize), 0, BitUtil::Nbytes(rowSize));
    }
    return bitMark;
}

// FieldExpr
FieldExpr::FieldExpr() = default;

FieldExpr::~FieldExpr()
{
    if (input != nullptr) {
        delete input;
        input = nullptr;
    }
}

ExprType FieldExpr::GetType() const
{
    return ExprType::FIELD_E;
}

// Helper constructors
FieldExpr::FieldExpr(int32_t colIdx, DataTypePtr colType)
{
    dataType = std::move(colType);
    colVal = colIdx;
}

omniruntime::vec::BaseVector *FieldExpr::GetFieldVector(omniruntime::vec::VectorBatch *vecBatch)
{
    if (!input) {
        return vecBatch->Get(colVal);
    }
    // has parent
    if (input->dataType->GetId() == OMNI_ROW) {
        auto rowVec = static_cast<FieldExpr *>(input)->GetFieldVector(vecBatch);
        auto resVec = (reinterpret_cast<omniruntime::vec::RowVector *>(rowVec))->ChildAt(this->ordinal);
        return resVec.get();
    } else {
        throw omniruntime::exception::OmniException("OPERATOR_RUNTIME_ERROR", "EQ left Only Support Field.");
    }
}

FieldExpr::FieldExpr(int32_t colIdx, DataTypePtr colType, int32_t oridinal)
{
    dataType = std::move(colType);
    colVal = colIdx;
    this->ordinal = oridinal;
}

int32_t GetColVec2(FieldExpr *expr)
{
    if (expr->input != nullptr) {
        return GetColVec2(reinterpret_cast<FieldExpr *>(expr->input));
    }
    return expr->colVal;
}

FieldExpr::FieldExpr(int32_t colIdx, DataTypePtr colType, int32_t oridinal, Expr *input)
{
    dataType = std::move(colType);
    colVal = colIdx;
    this->ordinal = oridinal;
    this->input = input;
}

bool FieldExpr::FieldIsArray()
{
    return (dataType->GetId() == DataTypeId::OMNI_ARRAY);
}

bool FieldExpr::FieldIsMap()
{
    return (dataType->GetId() == DataTypeId::OMNI_MAP);
}

BinaryExpr::BinaryExpr()
{
    dataType = BooleanType();
}

std::string GetStringOp(Operator op)
{
    switch (op) {
        case Operator::EQ:
            return "eq";
        case Operator::NEQ:
            return "neq";
        case Operator::LT:
            return "lt";
        case Operator::LTE:
            return "lte";
        case Operator::GT:
            return "greaterThan";
        case Operator::GTE:
            return "gte";
        case Operator::AND:
            return "and";
        case Operator::OR:
            return "or";
        case Operator::ADD:
            return "add";
        case Operator::SUB:
            return "sub";
        case Operator::MUL:
            return "mul";
        case Operator::DIV:
            return "div";
        case Operator::MOD:
            return "mod";
        case Operator::NOT:
            return "not";
        default:
            return "Invalid";
    }
}

BinaryExpr::BinaryExpr(Operator bop, Expr *leftExpr, Expr *rightExpr, DataTypePtr dt)
{
    op = bop;
    left = leftExpr;
    right = rightExpr;
    dataType = std::move(dt);
    std::vector<omniruntime::type::DataTypeId> args = {left->dataType->GetId(), right->dataType->GetId()};
    auto signature = std::make_shared<FunctionSignature>(GetStringOp(bop), args, dataType->GetId());
    vectorFunction = VectorFunction::Find(signature);
}

BinaryExpr::~BinaryExpr()
{
    delete left;
    delete right;
}

ExprType BinaryExpr::GetType() const
{
    return ExprType::BINARY_E;
}

uint8_t *BinaryExpr::computeAND(omniruntime::vec::VectorBatch *vecBatch, uint8_t *bitMark)
{
    auto vectorSize = vecBatch->GetRowCount();

    auto bitMarkBufLeft = std::make_unique<omniruntime::mem::AlignedBuffer<uint8_t>>(BitUtil::Nbytes(vectorSize) + 8,
        true);
    uint8_t *leftResult = left->compute(vecBatch, bitMarkBufLeft->GetBuffer());

    auto bitMarkBufRight = std::make_unique<omniruntime::mem::AlignedBuffer<uint8_t>>(BitUtil::Nbytes(vectorSize) + 8,
        true);
    uint8_t *rightResult = right->compute(vecBatch, bitMarkBufRight->GetBuffer());

    int32_t step = static_cast<int32_t>(NEON_BYTE_SIZE / sizeof(uint8_t));
    int32_t byteLen = BitUtil::Nbytes(vectorSize);
    int32_t index = 0;
    for (; index + step <= byteLen; index += step) {
        uint8x16_t leftBatch = vld1q_u8(leftResult + index);
        uint8x16_t rightBatch = vld1q_u8(rightResult + index);
        uint8x16_t result = leftBatch & rightBatch;
        vst1q_u8(bitMark + index, result);
    }
    for (; index < byteLen; index++) {
        bitMark[index] = leftResult[index] & rightResult[index];
    }
    return bitMark;
}

uint8_t *BinaryExpr::computeOR(omniruntime::vec::VectorBatch *vecBatch, uint8_t *bitMark)
{
    auto vectorSize = vecBatch->GetRowCount();

    auto bitMarkBufLeft = std::make_unique<omniruntime::mem::AlignedBuffer<uint8_t>>(BitUtil::Nbytes(vectorSize) + 8,
        true);
    uint8_t *leftResult = left->compute(vecBatch, bitMarkBufLeft->GetBuffer());

    auto bitMarkBufRight = std::make_unique<omniruntime::mem::AlignedBuffer<uint8_t>>(BitUtil::Nbytes(vectorSize) + 8,
        true);
    uint8_t *rightResult = right->compute(vecBatch, bitMarkBufRight->GetBuffer());

    int32_t step = static_cast<int32_t>(NEON_BYTE_SIZE / sizeof(uint8_t));
    int32_t byteLen = BitUtil::Nbytes(vectorSize);
    int32_t index = 0;
    for (; index + step <= byteLen; index += step) {
        uint8x16_t leftBatch = vld1q_u8(leftResult + index);
        uint8x16_t rightBatch = vld1q_u8(rightResult + index);
        uint8x16_t result = leftBatch | rightBatch;
        vst1q_u8(bitMark + index, result);
    }
    for (; index < byteLen; index++) {
        bitMark[index] = leftResult[index] | rightResult[index];
    }
    return bitMark;
}

bool ALWAYS_INLINE IsEqual(std::string_view src, std::string *target)
{
    return src == *target;
}

uint8_t *BinaryExpr::computeEQ(omniruntime::vec::VectorBatch *vecBatch, uint8_t *bitMark)
{
    auto field = dynamic_cast<FieldExpr *>(left);
    if (!field) {
        throw omniruntime::exception::OmniException("OPERATOR_RUNTIME_ERROR", "EQ left Only Support Field.");
    }

    auto literal = dynamic_cast<LiteralExpr *>(right);
    if (!literal) {
        throw omniruntime::exception::OmniException("OPERATOR_RUNTIME_ERROR", "EQ right Only Support literal.");
    }

    auto vector = field->GetFieldVector(vecBatch);
    auto rows = vector->GetSize();
    auto dataTypeId = vector->GetTypeId();
    switch (dataTypeId) {
        case OMNI_CHAR:
        case OMNI_VARCHAR: {
            auto target = literal->stringVal;
            if (target == nullptr) {
                throw omniruntime::exception::OmniException("OPERATOR_RUNTIME_ERROR",
                    "EQ Not Support stringVal is null.");
            }
            if (vector->GetEncoding() == vec::OMNI_FLAT) {
                auto varCharVector = reinterpret_cast<vec::Vector<vec::LargeStringContainer<std::string_view>> *>(
                    vector);
                for (int index = 0; index < rows; index++) {
                    bool result = vector->IsNull(index) ? 0 : IsEqual(varCharVector->GetValue(index), target);
                    BitUtil::SetBit(bitMark, index, result);
                }
            } else if (vector->GetEncoding() == vec::OMNI_DICTIONARY) {
                auto dictVarchar = reinterpret_cast<vec::Vector<vec::DictionaryContainer<std::string_view,
                    vec::LargeStringContainer>> *>(vector);
                for (int index = 0; index < rows; index++) {
                    bool result = vector->IsNull(index) ? 0 : IsEqual(dictVarchar->GetValue(index), target);
                    BitUtil::SetBit(bitMark, index, result);
                }
            } else {
                throw omniruntime::exception::OmniException("OPERATOR_RUNTIME_ERROR",
                    "EQ Not Support ENCODING for varchar : " + std::to_string(static_cast<int>(dataTypeId)));
            }
            break;
        }
        default:
            throw omniruntime::exception::OmniException("OPERATOR_RUNTIME_ERROR",
                "EQ Not Support Type : " + std::to_string(static_cast<int>(dataTypeId)));
    }

    return bitMark;
}

uint8_t *BinaryExpr::computeNEQ(omniruntime::vec::VectorBatch *vecBatch, uint8_t *bitMark)
{
    auto field = dynamic_cast<FieldExpr *>(left);
    if (!field) {
        throw omniruntime::exception::OmniException("OPERATOR_RUNTIME_ERROR", "NEQ left Only Support Field.");
    }

    auto literal = dynamic_cast<LiteralExpr *>(right);
    if (!literal) {
        throw omniruntime::exception::OmniException("OPERATOR_RUNTIME_ERROR", "NEQ right Only Support literal.");
    }

    auto vector = field->GetFieldVector(vecBatch);
    auto rows = vector->GetSize();
    auto dataTypeId = vector->GetTypeId();
    switch (dataTypeId) {
        case OMNI_CHAR:
        case OMNI_VARCHAR: {
            auto target = literal->stringVal;
            if (target == nullptr) {
                throw omniruntime::exception::OmniException("OPERATOR_RUNTIME_ERROR",
                    "NEQ Not Support stringVal is null.");
            }
            if (vector->GetEncoding() == vec::OMNI_FLAT) {
                auto varCharVector = reinterpret_cast<vec::Vector<vec::LargeStringContainer<std::string_view>> *>(
                    vector);
                for (int index = 0; index < rows; index++) {
                    bool result = vector->IsNull(index) ? 0 : IsEqual(varCharVector->GetValue(index), target);
                    BitUtil::SetBit(bitMark, index, !result);
                }
            } else if (vector->GetEncoding() == vec::OMNI_DICTIONARY) {
                auto dictVarchar = reinterpret_cast<vec::Vector<vec::DictionaryContainer<std::string_view,
                    vec::LargeStringContainer>> *>(vector);
                for (int index = 0; index < rows; index++) {
                    bool result = vector->IsNull(index) ? 0 : IsEqual(dictVarchar->GetValue(index), target);
                    BitUtil::SetBit(bitMark, index, !result);
                }
            } else {
                throw omniruntime::exception::OmniException("OPERATOR_RUNTIME_ERROR",
                    "NEQ Not Support ENCODING for varchar : " + std::to_string(static_cast<int>(dataTypeId)));
            }
            break;
        }
        default:
            throw omniruntime::exception::OmniException("OPERATOR_RUNTIME_ERROR",
                "NEQ Not Support Type : " + std::to_string(static_cast<int>(dataTypeId)));
    }

    return bitMark;
}

uint8_t *BinaryExpr::compute(omniruntime::vec::VectorBatch *vecBatch, uint8_t *bitMark)
{
    switch (op) {
        case Operator::EQ:
            return computeEQ(vecBatch, bitMark);
        case Operator::AND:
            return computeAND(vecBatch, bitMark);
        case Operator::OR:
            return computeOR(vecBatch, bitMark);
        case Operator::NEQ:
            return computeNEQ(vecBatch, bitMark);
        default:
            throw omniruntime::exception::OmniException("OPERATOR_RUNTIME_ERROR",
                "BinaryExpr Not Support: " + std::to_string(static_cast<int>(op)));
    }
}

UnaryExpr::UnaryExpr()
{
    dataType = BooleanType();
}

UnaryExpr::UnaryExpr(Operator logOp, Expr *bodyExpr) : op(logOp), exp(bodyExpr) {}

UnaryExpr::UnaryExpr(Operator uop, Expr *expr, DataTypePtr dt) : op(uop), exp(expr)
{
    dataType = std::move(dt);
    std::vector<omniruntime::type::DataTypeId> args = {exp->dataType->GetId()};
    auto signature = std::make_shared<FunctionSignature>(GetStringOp(op), args, dataType->GetId());
    vectorFunction = VectorFunction::Find(signature);
}

UnaryExpr::~UnaryExpr()
{
    delete exp;
}

ExprType UnaryExpr::GetType() const
{
    return ExprType::UNARY_E;
}

uint8_t *UnaryExpr::computeNOT(omniruntime::vec::VectorBatch *vecBatch, uint8_t *bitMark)
{
    auto childExpr = dynamic_cast<IsNullExpr *>(this->exp);
    if (!childExpr) {
        throw omniruntime::exception::OmniException("OPERATOR_RUNTIME_ERROR", "Not Only Support isNullExpr.");
    }
    auto vectorSize = vecBatch->GetRowCount();
    uint8_t *childResult = childExpr->compute(vecBatch, bitMark);
    int32_t step = static_cast<int32_t>(NEON_BYTE_SIZE / sizeof(uint8_t));
    int32_t byteLen = BitUtil::Nbytes(vectorSize);
    int32_t index = 0;
    for (; index + step <= byteLen; index += step) {
        uint8x16_t valuesBatch = vld1q_u8(childResult + index);
        uint8x16_t result = ~valuesBatch;
        vst1q_u8(bitMark + index, result);
    }
    for (; index < byteLen; index++) {
        bitMark[index] = ~childResult[index];
    }
    return bitMark;
}

uint8_t *UnaryExpr::compute(omniruntime::vec::VectorBatch *vecBatch, uint8_t *bitMark)
{
    switch (op) {
        case Operator::NOT:
            return computeNOT(vecBatch, bitMark);
        default:
            throw omniruntime::exception::OmniException("OPERATOR_RUNTIME_ERROR",
                "UnaryExpr Not Support: " + std::to_string(static_cast<int>(op)));
    }
}

InExpr::InExpr()
{
    dataType = BooleanType();
}

InExpr::~InExpr()
{
    DeleteExprs(arguments);
}

InExpr::InExpr(std::vector<Expr *> args)
{
    dataType = BooleanType();
    arguments = std::move(args);
}

ExprType InExpr::GetType() const
{
    return ExprType::IN_E;
}

BetweenExpr::BetweenExpr()
{
    dataType = BooleanType();
}

BetweenExpr::~BetweenExpr()
{
    delete value;
    delete lowerBound;
    delete upperBound;
}

BetweenExpr::BetweenExpr(Expr *val, Expr *lowBound, Expr *upBound)
{
    dataType = BooleanType();
    value = val;
    lowerBound = lowBound;
    upperBound = upBound;
}

ExprType BetweenExpr::GetType() const
{
    return ExprType::BETWEEN_E;
}

SwitchExpr::SwitchExpr() : whenClause(), falseExpr() {}

SwitchExpr::~SwitchExpr()
{
    for (std::pair<Expr *, Expr *> &vec : whenClause) {
        delete vec.first;
        delete vec.second;
    }
    delete falseExpr;
}

SwitchExpr::SwitchExpr(const std::vector<std::pair<Expr *, Expr *>> &whens, Expr *fexp)
{
    dataType = fexp->GetReturnType();
    whenClause = whens;
    falseExpr = fexp;
}

ExprType SwitchExpr::GetType() const
{
    return ExprType::SWITCH_E;
}

IfExpr::IfExpr() : condition(), trueExpr(), falseExpr() {}

IfExpr::~IfExpr()
{
    delete condition;
    delete trueExpr;
    delete falseExpr;
}

IfExpr::IfExpr(Expr *cond, Expr *texp, Expr *fexp)
{
    dataType = texp->GetReturnType();
    condition = cond;
    trueExpr = texp;
    falseExpr = fexp;
}

ExprType IfExpr::GetType() const
{
    return ExprType::IF_E;
}

CoalesceExpr::CoalesceExpr() : value1(), value2() {}

CoalesceExpr::~CoalesceExpr()
{
    delete value1;
    delete value2;
}

CoalesceExpr::CoalesceExpr(Expr *val1, Expr *val2)
{
    dataType = val1->GetReturnType();
    value1 = val1;
    value2 = val2;
}

ExprType CoalesceExpr::GetType() const
{
    return ExprType::COALESCE_E;
}

IsNullExpr::IsNullExpr() : value() {}

IsNullExpr::~IsNullExpr()
{
    delete value;
}

IsNullExpr::IsNullExpr(Expr *value)
{
    dataType = BooleanType();
    std::vector<omniruntime::type::DataTypeId> args = {value->dataType->GetId()};
    auto signature = std::make_shared<FunctionSignature>("isnull", args, dataType->GetId());
    vectorFunction = VectorFunction::Find(signature);
    this->value = value;
}

uint8_t *IsNullExpr::compute(omniruntime::vec::VectorBatch *vecBatch, uint8_t *bitMark)
{
    auto expr = dynamic_cast<FieldExpr *>(value);
    if (!expr) {
        throw omniruntime::exception::OmniException("OPERATOR_RUNTIME_ERROR", "IsNullExpr Only Support Field.");
    }
    auto vector = expr->GetFieldVector(vecBatch);
    auto rowSize = vecBatch->GetRowCount();
    if (vector->HasNull()) {
        errno_t opIsNullRet = memcpy_s(bitMark, BitUtil::Nbytes(rowSize),
            vec::unsafe::UnsafeBaseVector::GetNulls(vector), BitUtil::Nbytes(rowSize));
        if (UNLIKELY(opIsNullRet != EOK)) {
            throw omniruntime::exception::OmniException("OPERATOR_RUNTIME_ERROR", "IS_NULL memcpy_s fail.");
        }
    } else {
        memset_s(bitMark, BitUtil::Nbytes(rowSize), 0, BitUtil::Nbytes(rowSize));
    }
    return bitMark;
}

ExprType IsNullExpr::GetType() const
{
    return ExprType::IS_NULL_E;
}

FuncExpr::FuncExpr() : function(nullptr) {}

FuncExpr::~FuncExpr()
{
    DeleteExprs(arguments);
}

FuncExpr::FuncExpr(const std::string &fnName, const std::vector<Expr *> &args, DataTypePtr returnType)
    : funcName(fnName), arguments(args), functionType(BUILTIN)
{
    dataType = std::move(returnType);

    std::vector<DataTypeId> argTypes(arguments.size());
    std::transform(arguments.begin(), arguments.end(), argTypes.begin(), [](Expr *expr) -> DataTypeId {
        return expr->GetReturnTypeId();
    });
    auto signature = std::make_shared<FunctionSignature>(funcName, argTypes, dataType->GetId());
    this->function = FunctionRegistry::LookupFunction(signature.get());
    vectorFunction = VectorFunction::Find(signature);
}

FuncExpr::FuncExpr(const std::string &fnName, const std::vector<Expr *> &args, DataTypePtr returnType,
    const Function *function)
    : funcName(fnName), arguments(args), function(function), functionType(BUILTIN)
{
    dataType = std::move(returnType);
    std::vector<DataTypeId> argTypes(arguments.size());
    std::transform(arguments.begin(), arguments.end(), argTypes.begin(), [](Expr *expr) -> DataTypeId {
        return expr->GetReturnTypeId();
    });
    auto signature = std::make_shared<FunctionSignature>(funcName, argTypes, dataType->GetId());
    this->function = FunctionRegistry::LookupFunction(signature.get());
    vectorFunction = VectorFunction::Find(signature);
}

FuncExpr::FuncExpr(const std::string &fnName, const std::vector<Expr *> &args, DataTypePtr returnType,
    ExprFunctionType functionType)
    : funcName(fnName), arguments(args), function(nullptr), functionType(functionType)
{
    dataType = std::move(returnType);
    std::vector<DataTypeId> argTypes(arguments.size());
    std::transform(arguments.begin(), arguments.end(), argTypes.begin(), [](Expr *expr) -> DataTypeId {
        return expr->GetReturnTypeId();
    });
    auto signature = std::make_shared<FunctionSignature>(funcName, argTypes, dataType->GetId());
    this->function = FunctionRegistry::LookupFunction(signature.get());
    vectorFunction = VectorFunction::Find(signature);
}

ExprType FuncExpr::GetType() const
{
    return ExprType::FUNC_E;
}

bool ALWAYS_INLINE RLikeStr(std::string_view src, std::wregex re)
{
    std::wstring_convert<std::codecvt_utf8<wchar_t>> convert;
    return std::regex_search(convert.from_bytes(std::string(src)), re);
}

uint8_t *FuncExpr::compute(omniruntime::vec::VectorBatch *vecBatch, uint8_t *bitMark)
{
    if (funcName == "RLike") {
        auto expr = dynamic_cast<FieldExpr *>(arguments[0]);
        auto pattern = dynamic_cast<LiteralExpr *>(arguments[1]);
        if (!expr || !pattern || !pattern->stringVal) {
            throw omniruntime::exception::OmniException("OPERATOR_RUNTIME_ERROR", "RLike args error");
        }
        auto vector = expr->GetFieldVector(vecBatch);
        auto rows = vecBatch->GetRowCount();

        auto dataTypeId = vector->GetTypeId();
        switch (dataTypeId) {
            case OMNI_CHAR:
            case OMNI_VARCHAR: {
                auto target = pattern->stringVal;
                std::wregex re(convert.from_bytes(*target));
                if (vector->GetEncoding() == vec::OMNI_FLAT) {
                    auto varCharVector = reinterpret_cast<vec::Vector<vec::LargeStringContainer<std::string_view>> *>(
                        vector);
                    for (int index = 0; index < rows; index++) {
                        bool result = vector->IsNull(index) ? 0 : RLikeStr(varCharVector->GetValue(index), re);
                        BitUtil::SetBit(bitMark, index, result);
                    }
                } else if (vector->GetEncoding() == vec::OMNI_DICTIONARY) {
                    auto dictVarchar = reinterpret_cast<vec::Vector<vec::DictionaryContainer<std::string_view,
                        vec::LargeStringContainer>> *>(vector);
                    for (int index = 0; index < rows; index++) {
                        bool result = vector->IsNull(index) ? 0 : RLikeStr(dictVarchar->GetValue(index), re);
                        BitUtil::SetBit(bitMark, index, result);
                    }
                } else {
                    throw omniruntime::exception::OmniException("OPERATOR_RUNTIME_ERROR",
                        "RLike Not Support ENCODING for varchar : " + std::to_string(static_cast<int>(dataTypeId)));
                }
                break;
            }
            default:
                throw omniruntime::exception::OmniException("OPERATOR_RUNTIME_ERROR",
                    "RLike Not Support Type : " + std::to_string(static_cast<int>(dataTypeId)));
        }
        return bitMark;
    } else {
        throw omniruntime::exception::OmniException("OPERATOR_RUNTIME_ERROR", "FuncExpr Not Support: " + funcName);
    }
}
}
}
