/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description:
 */
#include <utility>
#include "gtest/gtest.h"
#include "expression/expressions.h"
#include "expression/jsonparser/jsonparser.h"

#include "test/util/test_util.h"
#include "expression/parserhelper.h"

using namespace std;
using namespace omniruntime::expressions;

const int32_t int32Val = 1;
const int64_t int64Val = 123456789;
const double doubleVal = 2.0;
const bool boolVal = true;
const int32_t colNum = 1;
const string varcharVal = "hello world";
const int32_t precision64 = 8;
const int32_t precision128 = 17;
const int32_t scale = 0;
const int32_t varcharWidth = INT_MAX;

std::stringstream ss;

map<Operator, string> ArithOps = { { ADD, "ADD" },
                                   { SUB, "SUBTRACT" },
                                   { MUL, "MULTIPLY" },
                                   { DIV, "DIVIDE" },
                                   { MOD, "MODULUS" } };
map<Operator, string> CmpOps = { { GT, "GREATER_THAN" }, { GTE, "GREATER_THAN_OR_EQUAL" },
                                 { LT, "LESS_THAN" },    { LTE, "LESS_THAN_OR_EQUAL" },
                                 { EQ, "EQUAL" },        { NEQ, "NOT_EQUAL" } };

string getInt32TestJSON(int32_t val)
{
    ss.str("");
    ss << R"({ "exprType": "LITERAL", "dataType": 1, "isNull": false, "value": )" << val << R"(})";
    return ss.str();
}

string getDate32TestJson(int32_t val)
{
    ss.str("");
    ss << R"({ "exprType": "LITERAL", "dataType": 8, "isNull": false, "value": )" << val << R"(})";
    return ss.str();
}

string getVarcharTestJson(const string &val, int32_t width)
{
    ss.str("");
    ss << R"({ "exprType": "LITERAL", "dataType": 15, "isNull": false, "value": ")" << val << R"(", "width": )" <<
        width << R"(})";
    return ss.str();
}

string getInt64TestJson(int64_t val)
{
    ss.str("");
    ss << R"({ "exprType": "LITERAL", "dataType": 2, "isNull": false, "value": )" << val << R"(})";
    return ss.str();
}

string getDec64TestJson(int64_t val, int32_t precision, int32_t scale)
{
    ss.str("");
    ss << R"({ "exprType": "LITERAL", "dataType": 6, "isNull": false, "value": )" << val << R"(, "precision": )" <<
        precision << R"(, "scale": )" << scale << R"(})";
    return ss.str();
}

string getDec128TestJson(const string &val, int32_t precision, int32_t scale)
{
    ss.str("");
    ss << R"({ "exprType": "LITERAL", "dataType": 7, "isNull": false, "value": ")" << val << R"(", "precision": )" <<
        precision << R"(, "scale": )" << scale << R"(})";
    return ss.str();
}

string getDoubleTestJson(double val)
{
    ss.str("");
    ss << R"({ "exprType": "LITERAL", "dataType": 3, "isNull": false, "value": )" << val << R"(})";
    return ss.str();
}

string getBoolTestJson(bool val)
{
    ss.str("");
    ss << R"({ "exprType": "LITERAL", "dataType": 4, "isNull": false, "value": )" << boolalpha << val << R"(})";
    return ss.str();
}

string getNoneTestJson(const string &val)
{
    ss.str("");
    ss << R"({ "exprType": "LITERAL", "dataType": 0, "isNull": false, "value": ")" << val << R"("})";
    return ss.str();
}

string getNullTestJson(int32_t dt)
{
    ss.str("");
    ss << R"({ "exprType": "LITERAL", "dataType": )" << dt << R"(, "isNull": true})";
    return ss.str();
}

string getFieldRefTestJson(int32_t dt, int32_t colval)
{
    ss.str("");
    ss << R"({ "exprType": "FIELD_REFERENCE", "dataType": )" << dt << R"(, "colVal": )" << colval << R"(})";
    return ss.str();
}

string getDecimalFieldRefTestJson(int32_t dt, int32_t colval, int32_t precision, int32_t scale)
{
    ss.str("");
    ss << R"({ "exprType": "FIELD_REFERENCE", "dataType": )" << dt << R"(, "colVal": )" << colval <<
        R"(, "precision": )" << precision << R"(, "scale": )" << scale << R"(})";
    return ss.str();
}


string getAndTestJson(const string &left, const string &right)
{
    ss.str("");
    ss << R"({"exprType": "BINARY", "returnType": 4, "operator": "AND", "left":)" << left << R"(, "right":)" << right <<
        R"(})";
    return ss.str();
}

string getOrTestJson(const string &left, const string &right)
{
    ss.str("");
    ss << R"({"exprType": "BINARY", "returnType": 4, "operator": "OR", "left":)" << left << R"(, "right":)" << right <<
        R"(})";
    return ss.str();
}

string getCoalesceTestJson(int32_t rt, const string &left, const string &right)
{
    ss.str("");
    ss << R"({"exprType": "COALESCE", "returnType": )" << rt << R"(, "value1": )" << left << R"(, "value2": )" <<
        right << R"(})";
    return ss.str();
}

string getBetweenTestJson(const string &val, const string &val1, const string &val2)
{
    ss.str("");
    ss << R"({"exprType": "BETWEEN", "returnType": 4, "value": )" << val << R"(, "lower_bound": )" << val1 <<
        R"(, "upper_bound": )" << val2 << R"(})";
    return ss.str();
}

string getIfTestJson(int32_t rt, const string &val, const string &val1, const string &val2)
{
    ss.str("");
    ss << R"({"exprType": "IF", "returnType": )" << rt << R"(, "condition": )" << val << R"(, "if_true": )" << val1 <<
        R"(, "if_false": )" << val2 << R"(})";
    return ss.str();
}

string getSwitchTestJson(int32_t rt, const string &val, const string &val1, const string &val2)
{
    ss.str("");
    ss << R"({"exprType": "SWITCH", "returnType": )" << rt << R"(, "numOfCases": 1, "input": )" << val <<
        R"(, "Case1": )" << val1 << R"(, "else": )" << val2 << R"(})";
    return ss.str();
}

string getWhenTestJson(int32_t rt, const string &val, const string &val1)
{
    ss.str("");
    ss << R"({"exprType": "WHEN", "returnType": )" << rt << R"(, "when": )" << val << R"(, "result": )" << val1 <<
        R"(})";
    return ss.str();
}

string getInTestJson(const vector<string> &args)
{
    ss.str("");
    ss << R"({"exprType": "IN", "returnType": 4, "arguments": [)" << args.at(0);
    for (int i = 1; i < args.size(); i++) {
        ss << R"(, )" << args.at(i);
    }
    ss << R"(]})";
    return ss.str();
}

string getBinaryTestJson(int32_t rt, const string &op, const string &left, const string &right)
{
    ss.str("");
    ss << R"({ "exprType": "BINARY", "returnType": )" << rt << R"(, "operator": ")" << op << R"(", "left": )" << left <<
        R"(, "right": )" << right << R"(})";
    return ss.str();
}

string getDecimalBinaryTestJson(int32_t rt, const string &op, const string &left, const string &right,
                                int32_t returnPrecision, int32_t returnScale)
{
    ss.str("");
    ss << R"({ "exprType": "BINARY", "returnType": )" << rt << R"(, "precision": )" << returnPrecision
    << R"(, "scale": )" << returnScale << R"(, "operator": ")" << op << R"(", "left": )" << left <<
    R"(, "right": )" << right << R"(})";
    return ss.str();
}

string getUnaryTestJson(const string &op, const string &expr)
{
    ss.str("");
    ss << R"({ "exprType": "UNARY", "returnType": 4, "operator": ")" << op << R"(", "expr": )" << expr << R"(})";
    return ss.str();
}

string getFuncTestJson(int32_t rt, const string &func, const vector<string> &args)
{
    ss.str("");
    ss << R"({"exprType": "FUNCTION", "returnType": )" << rt << R"(, "function_name": ")" << func <<
        R"(", "arguments": [)" << args.at(0);
    for (int i = 1; i < args.size(); i++) {
        ss << R"(, )" << args.at(i);
    }
    ss << R"(]})";
    return ss.str();
}

class TestExpr {
public:
    DataType *dataType = nullptr;
    virtual bool isEqual(Expr *that) const
    {
        return false;
    };
    virtual bool operator == (const Expr &rhs) const
    {
        return false;
    };
    virtual ~TestExpr() = default;
};

class TestLiteralExpr : public TestExpr {
    LiteralExpr *expr = nullptr;

public:
    TestLiteralExpr(DataType &dt, int32_t colVal)
        : expr(make_unique<LiteralExpr>(colVal, make_unique<DataType>(dt)).release())
    {
        dataType = &(expr->GetReturnType());
    }
    template <typename T>
    explicit TestLiteralExpr(T val, DataType &dt)
        : expr(make_unique<LiteralExpr>(val, make_unique<DataType>(dt)).release())
    {
        dataType = &(expr->GetReturnType());
    }
    // get default expr of null type-dt expression
    explicit TestLiteralExpr(DataTypeId id)
    {
        expr = ParserHelper::GetDefaultValueForType(id);
        expr->isNull = true;
    }
    ~TestLiteralExpr() override
    {
        delete expr;
    }
    bool operator == (const LiteralExpr &rhs) const
    {
        bool stringIsNull = false;
        if (expr->stringVal == nullptr && rhs.stringVal == nullptr)
            stringIsNull = true;

        return expr->GetReturnType() == rhs.GetReturnType() && expr->isNull == rhs.isNull &&
            expr->boolVal == rhs.boolVal && expr->intVal == rhs.intVal && expr->longVal == rhs.longVal &&
            expr->doubleVal == rhs.doubleVal && (stringIsNull || *(expr->stringVal) == *(rhs.stringVal));
    }

    bool isEqual(Expr *that) const override
    {
        if (typeid(LiteralExpr) != typeid(*that))
            return false;
        auto *rhs = dynamic_cast<LiteralExpr *>(that);
        bool result = (*this == *rhs);
        EXPECT_TRUE(result);
        return result;
    }
};

class TestFieldExpr : public TestExpr {
    FieldExpr *expr = nullptr;

public:
    TestFieldExpr(int32_t dt, int32_t colVal)
        : expr(make_unique<FieldExpr>(colVal, make_unique<DataType>(dt)).release())
    {
        dataType = &(expr->GetReturnType());
    }
    ~TestFieldExpr() override
    {
        delete expr;
    }
    bool operator == (const FieldExpr &rhs) const
    {
        return expr->GetReturnType() == rhs.GetReturnType() && expr->isNull == rhs.isNull && expr->colVal == rhs.colVal;
    }

    bool isEqual(Expr *that) const override
    {
        if (typeid(FieldExpr) != typeid(*that))
            return false;
        auto *rhs = dynamic_cast<FieldExpr *>(that);
        bool result = (*this == *rhs);
        EXPECT_TRUE(result);
        return result;
    }
};

class TestBinaryExpr : public TestExpr {
    Operator op = EQ;
    TestExpr *left = nullptr;
    TestExpr *right = nullptr;

public:
    TestBinaryExpr(Operator op, TestExpr *left, TestExpr *right, DataTypePtr dt) : op(op), left(left), right(right)
    {
        dataType = dt.release();
    }
    ~TestBinaryExpr() override
    {
        delete left;
        delete right;
    }
    bool operator == (const BinaryExpr &rhs) const
    {
        return (op == rhs.op && *dataType == rhs.GetReturnType() && left->isEqual(rhs.left) &&
            right->isEqual(rhs.right));
    }

    bool isEqual(Expr *that) const override
    {
        if (typeid(BinaryExpr) != typeid(*that))
            return false;
        auto *rhs = dynamic_cast<BinaryExpr *>(that);
        bool result = (*this == *rhs);
        EXPECT_TRUE(result);
        return result;
    }
};

class TestUnaryExpr : public TestExpr {
    Operator op = EQ;
    TestExpr *expr = nullptr;

public:
    TestUnaryExpr(Operator op, TestExpr *expr) : op(op), expr(expr)
    {
        dataType = BooleanType().release();
    }
    ~TestUnaryExpr() override
    {
        delete expr;
    }
    bool operator == (const UnaryExpr &rhs) const
    {
        return (op == rhs.op && *dataType == rhs.GetReturnType() && expr->isEqual(rhs.exp));
    }

    bool isEqual(Expr *that) const override
    {
        if (typeid(UnaryExpr) != typeid(*that))
            return false;
        auto *rhs = dynamic_cast<UnaryExpr *>(that);
        bool result = (*this == *rhs);
        EXPECT_TRUE(result);
        return result;
    }
};

class TestBetweenExpr : public TestExpr {
    TestExpr *value = nullptr;
    TestExpr *lowerBound = nullptr;
    TestExpr *upperBound = nullptr;

public:
    TestBetweenExpr(TestExpr *value, TestExpr *lower, TestExpr *upper)
        : value(value), lowerBound(lower), upperBound(upper)
    {
        dataType = BooleanType().release();
    }
    ~TestBetweenExpr() override
    {
        delete lowerBound;
        delete upperBound;
        delete value;
    }
    bool operator == (const BetweenExpr &rhs) const
    {
        return (*dataType == rhs.GetReturnType() && value->isEqual(rhs.value) && lowerBound->isEqual(rhs.lowerBound) &&
            upperBound->isEqual(rhs.upperBound));
    }

    bool isEqual(Expr *that) const override
    {
        if (typeid(BetweenExpr) != typeid(*that))
            return false;
        auto *rhs = dynamic_cast<BetweenExpr *>(that);
        bool result = (*this == *rhs);
        EXPECT_TRUE(result);
        return result;
    }
};

class TestCoalesceExpr : public TestExpr {
    TestExpr *value1 = nullptr;
    TestExpr *value2 = nullptr;

public:
    TestCoalesceExpr(TestExpr *value1, TestExpr *value2) : value1(value1), value2(value2)
    {
        dataType = value1->dataType;
    }
    ~TestCoalesceExpr() override
    {
        delete value1;
        delete value2;
    }
    bool operator == (const CoalesceExpr &rhs) const
    {
        return (*dataType == rhs.GetReturnType() && value1->isEqual(rhs.value1) && value2->isEqual(rhs.value2));
    }

    bool isEqual(Expr *that) const override
    {
        if (typeid(CoalesceExpr) != typeid(*that))
            return false;
        auto *rhs = dynamic_cast<CoalesceExpr *>(that);
        bool result = (*this == *rhs);
        EXPECT_TRUE(result);
        return result;
    }
};

class TestFuncExpr : public TestExpr {
    string funcName;
    vector<TestExpr *> args;

public:
    TestFuncExpr(DataType &rt, string funcName, vector<TestExpr *> args)
        : funcName(std::move(funcName)), args(std::move(args))
    {
        dataType = &rt;
    }
    ~TestFuncExpr() override
    {
        for (TestExpr *exp : args) {
            delete exp;
        }
    }
    bool operator == (const FuncExpr &rhs) const
    {
        return (*dataType == rhs.GetReturnType() && funcName == rhs.funcName &&
            std::equal(args.begin(), args.end(), rhs.arguments.begin(),
            [](TestExpr *left, Expr *right) { return left->isEqual(right); }));
    }

    bool isEqual(Expr *that) const override
    {
        if (typeid(FuncExpr) != typeid(*that))
            return false;
        auto *rhs = dynamic_cast<FuncExpr *>(that);
        bool result = (*this == *rhs);
        EXPECT_TRUE(result);
        return result;
    }
};

class TestSwitchExpr : public TestExpr {
    vector<pair<TestExpr *, TestExpr *>> whenClause;
    TestExpr *elseExpr = nullptr;

public:
    TestSwitchExpr(vector<pair<TestExpr *, TestExpr *>> whenClause, TestExpr *elseExpr)
        : whenClause(std::move(whenClause)), elseExpr(elseExpr)
    {
        dataType = elseExpr->dataType;
    }
    ~TestSwitchExpr() override
    {
        for (pair<TestExpr *, TestExpr *> pair : whenClause) {
            delete pair.first;
            delete pair.second;
        }
        delete elseExpr;
    }
    bool operator == (const SwitchExpr &rhs) const
    {
        return (*dataType == rhs.GetReturnType() && elseExpr->isEqual(rhs.falseExpr) &&
            std::equal(whenClause.begin(), whenClause.end(), rhs.whenClause.begin(),
            [](pair<TestExpr *, TestExpr *> left, std::pair<Expr *, Expr *> right) {
                return left.first->isEqual(right.first) && left.second->isEqual(right.second);
            }));
    }

    bool isEqual(Expr *that) const override
    {
        if (typeid(SwitchExpr) != typeid(*that))
            return false;
        auto *rhs = dynamic_cast<SwitchExpr *>(that);
        bool result = (*this == *rhs);
        EXPECT_TRUE(result);
        return result;
    }
};

class TestIfExpr : public TestExpr {
    TestExpr *condition = nullptr;
    TestExpr *tExpr = nullptr;
    TestExpr *fExpr = nullptr;

public:
    TestIfExpr(TestBinaryExpr *cond, TestExpr *tExr, TestExpr *fExp) : condition(cond), tExpr(tExr), fExpr(fExp)
    {
        dataType = tExr->dataType;
    }
    ~TestIfExpr() override
    {
        delete condition;
        delete tExpr;
        delete fExpr;
    }
    bool operator == (const IfExpr &rhs) const
    {
        return (*dataType == rhs.GetReturnType() && condition->isEqual(rhs.condition) && tExpr->isEqual(rhs.trueExpr) &&
            fExpr->isEqual(rhs.falseExpr));
    }

    bool isEqual(Expr *that) const override
    {
        if (typeid(IfExpr) != typeid(*that))
            return false;
        auto *rhs = dynamic_cast<IfExpr *>(that);
        bool result = (*this == *rhs);
        EXPECT_TRUE(result);
        return result;
    }
};

class TestInExpr : public TestExpr {
    vector<TestExpr *> args;

public:
    explicit TestInExpr(vector<TestExpr *> args) : args(std::move(args))
    {
        dataType = BooleanType().release();
    }
    ~TestInExpr() override
    {
        for (TestExpr *exp : args) {
            delete exp;
        }
    }
    bool operator == (const InExpr &rhs) const
    {
        return (*dataType == rhs.GetReturnType() && std::equal(args.begin(), args.end(), rhs.arguments.begin(),
            [](TestExpr *left, Expr *right) { return left->isEqual(right); }));
    }

    bool isEqual(Expr *that) const override
    {
        if (typeid(InExpr) != typeid(*that))
            return false;
        auto *rhs = dynamic_cast<InExpr *>(that);
        bool result = (*this == *rhs);
        EXPECT_TRUE(result);
        return result;
    }
};

TEST(JSONParserTest, Literal_Bool)
{
    string unparsedBoolJson = getBoolTestJson(boolVal);
    Expr *boolExpr = JSONParser::ParseJSON(nlohmann::json::parse(unparsedBoolJson));
    TestLiteralExpr expectedExpr(boolVal, *BooleanType());
    expectedExpr.isEqual(boolExpr);
    delete boolExpr;
}

TEST(JSONParserTest, Literal_Integer)
{
    string unparsedIntJson = getInt32TestJSON(int32Val);
    Expr *intExpr = JSONParser::ParseJSON(nlohmann::json::parse(unparsedIntJson));
    TestLiteralExpr expectedExpr(int32Val, *IntType());
    expectedExpr.isEqual(intExpr);
    delete intExpr;
}

TEST(JSONParserTest, Literal_Date32)
{
    string unparsedDateJson = getDate32TestJson(int32Val);
    Expr *intExpr = JSONParser::ParseJSON(nlohmann::json::parse(unparsedDateJson));
    TestLiteralExpr expectedExpr(int32Val, *Date32Type());
    expectedExpr.isEqual(intExpr);
    delete intExpr;
}

TEST(JSONParserTest, Literal_Long)
{
    string unparsedLongJson = getInt64TestJson(int64Val);
    Expr *longExpr = JSONParser::ParseJSON(nlohmann::json::parse(unparsedLongJson));
    TestLiteralExpr expectedExpr(int64Val, *LongType());
    expectedExpr.isEqual(longExpr);
    delete longExpr;
}

TEST(JSONParserTest, Literal_Double)
{
    string unparsedDoubleJson = getDoubleTestJson(doubleVal);
    Expr *doubleExpr = JSONParser::ParseJSON(nlohmann::json::parse(unparsedDoubleJson));
    TestLiteralExpr expectedExpr(doubleVal, *DoubleType());
    expectedExpr.isEqual(doubleExpr);
    delete doubleExpr;
}

TEST(JSONParserTest, Literal_Decimal64)
{
    string unparsedD64Json = getDec64TestJson(int64Val, precision64, scale);
    Expr *d64Expr = JSONParser::ParseJSON(nlohmann::json::parse(unparsedD64Json));
    TestLiteralExpr expectedExpr(int64Val, *Decimal64Type(8, 0));
    expectedExpr.isEqual(d64Expr);
    delete d64Expr;
}

TEST(JSONParserTest, DISABLED_Literal_Decimal128)
{
    string unparsedD128Json = getDec128TestJson("12", precision128, scale);
    Expr *d128Expr = JSONParser::ParseJSON(nlohmann::json::parse(unparsedD128Json));
    TestLiteralExpr expectedExpr(new std::string(to_string(int64Val)), *Decimal128Type(17, 0));
    expectedExpr.isEqual(d128Expr);
    delete d128Expr;
}

TEST(JSONParserTest, Literal_Varchar)
{
    string unparsedVarcharJson = getVarcharTestJson(varcharVal, varcharWidth);
    Expr *varcharExpr = JSONParser::ParseJSON(nlohmann::json::parse(unparsedVarcharJson));
    TestLiteralExpr expectedExpr(make_unique<string>(varcharVal).release(), *VarcharType());
    expectedExpr.isEqual(varcharExpr);
    delete varcharExpr;
}

TEST(JSONParserTest, Literal_Unknown_Null)
{
    string unparsedNoneWithNull = getNullTestJson(OMNI_NONE);
    Expr *noneWithNull = JSONParser::ParseJSON(nlohmann::json::parse(unparsedNoneWithNull));
    // None type default DataExpr
    TestLiteralExpr expectedExpr(OMNI_NONE);
    expectedExpr.isEqual(noneWithNull);
}

TEST(JSONParserTest, Literal_Int32_Null)
{
    string unparsedNullInt32 = getNullTestJson(OMNI_INT);
    Expr *nullInt32 = JSONParser::ParseJSON(nlohmann::json::parse(unparsedNullInt32));
    // Int32 default DataExpr
    TestLiteralExpr expectedExpr(OMNI_INT);
    expectedExpr.isEqual(nullInt32);
}

TEST(JSONParserTest, FieldReference)
{
    string unparsedFieldRefJson = getFieldRefTestJson(OMNI_LONG, colNum);
    Expr *fieldRefExpr = JSONParser::ParseJSON(nlohmann::json::parse(unparsedFieldRefJson));
    TestFieldExpr expectedExpr(OMNI_LONG, colNum);
    expectedExpr.isEqual(fieldRefExpr);
    delete fieldRefExpr;
}


TEST(JSONParserTest, Decimal128FieldReference)
{
    string unparsedFieldRefJson = getDecimalFieldRefTestJson(OMNI_DECIMAL128, colNum, 0, 0);
    Expr *fieldRefExpr = JSONParser::ParseJSON(nlohmann::json::parse(unparsedFieldRefJson));
    TestFieldExpr expectedExpr(OMNI_DECIMAL128, colNum);
    expectedExpr.isEqual(fieldRefExpr);
    delete fieldRefExpr;
}

TEST(JSONParserTest, Decimal64FieldReference)
{
    string unparsedFieldRefJson = getDecimalFieldRefTestJson(OMNI_DECIMAL64, colNum, 0, 0);
    Expr *fieldRefExpr = JSONParser::ParseJSON(nlohmann::json::parse(unparsedFieldRefJson));
    TestFieldExpr expectedExpr(OMNI_DECIMAL64, colNum);
    expectedExpr.isEqual(fieldRefExpr);
    delete fieldRefExpr;
}

TEST(JSONParserTest, BinaryExpr_EQ)
{
    string unparsedBinaryJson =
        getBinaryTestJson(OMNI_BOOLEAN, CmpOps.at(EQ), getInt32TestJSON(int32Val), getInt64TestJson(int64Val));
    Expr *addExpr = JSONParser::ParseJSON(nlohmann::json::parse(unparsedBinaryJson));
    TestBinaryExpr expectedExpr(EQ, make_unique<TestLiteralExpr>(int32Val, *IntType()).release(),
        make_unique<TestLiteralExpr>(int64Val, *LongType()).release(), BooleanType());
    expectedExpr.isEqual(addExpr);
    delete addExpr;
}

TEST(JSONParserTest, BinaryExpr_ADD)
{
    string unparsedBinaryJson =
        getBinaryTestJson(OMNI_LONG, ArithOps.at(ADD), getInt32TestJSON(int32Val), getInt64TestJson(int64Val));
    Expr *addExpr = JSONParser::ParseJSON(nlohmann::json::parse(unparsedBinaryJson));
    TestBinaryExpr expectedExpr(ADD, make_unique<TestLiteralExpr>(int32Val, *IntType()).release(),
        make_unique<TestLiteralExpr>(int64Val, *LongType()).release(), LongType());
    expectedExpr.isEqual(addExpr);
    delete addExpr;
}

TEST(JSONParserTest, BinaryExpr_ADD_DECIMAL64)
{
    string unparsedBinaryJson =
            getDecimalBinaryTestJson(OMNI_DECIMAL64, ArithOps.at(ADD), getDec64TestJson((int64_t) 65, 6, 2),
                                     getInt64TestJson(int64Val), 10, 3);
    Expr *addExpr = JSONParser::ParseJSON(nlohmann::json::parse(unparsedBinaryJson));
    TestBinaryExpr expectedExpr(ADD, make_unique<TestLiteralExpr>((int64_t) 65, *Decimal64Type(6, 2)).release(),
                                make_unique<TestLiteralExpr>(int64Val, *LongType()).release(), Decimal64Type(10, 3));
    expectedExpr.isEqual(addExpr);
    delete addExpr;
}


TEST(JSONParserTest, BinaryExpr_ADD_DECIMAL128)
{
    string unparsedBinaryJson =
            getDecimalBinaryTestJson(OMNI_DECIMAL128, ArithOps.at(DIV), getDec128TestJson("123456", 32, 2),
                                     getInt32TestJSON(int32Val), 35, 4);
    auto divExpr = JSONParser::ParseJSON(nlohmann::json::parse(unparsedBinaryJson));
    TestBinaryExpr expectedExpr(DIV, make_unique<TestLiteralExpr>(new std::string("123456"), *Decimal128Type(32, 2)).release(),
                                make_unique<TestLiteralExpr>(int32Val, *IntType()).release(), Decimal128Type(35, 4));
    expectedExpr.isEqual(divExpr);
    delete divExpr;
}



TEST(JSONParserTest, UnaryExpr_NOT)
{
    string unparsedUnaryJson = getUnaryTestJson("NOT", getBoolTestJson(boolVal));
    Expr *unaryExpr = JSONParser::ParseJSON(nlohmann::json::parse(unparsedUnaryJson));
    TestUnaryExpr expectedExpr(NOT, make_unique<TestLiteralExpr>(boolVal, *BooleanType()).release());
    expectedExpr.isEqual(unaryExpr);
    delete unaryExpr;
}

TEST(JSONParserTest, BetweenExpr)
{
    string unparsedBetweenJson =
        getBetweenTestJson(getInt32TestJSON(int32Val), getInt32TestJSON(int32Val), getInt32TestJSON(int32Val));
    Expr *betweenExpr = JSONParser::ParseJSON(nlohmann::json::parse(unparsedBetweenJson));
    TestBetweenExpr expectedExpr(make_unique<TestLiteralExpr>(int32Val, *IntType()).release(),
        make_unique<TestLiteralExpr>(int32Val, *IntType()).release(),
        make_unique<TestLiteralExpr>(int32Val, *IntType()).release());
    expectedExpr.isEqual(betweenExpr);
    delete betweenExpr;
}

TEST(JSONParserTest, CoalesceExpr)
{
    string unparsedCoalesceJson =
        getCoalesceTestJson(OMNI_VARCHAR, getVarcharTestJson(varcharVal, varcharWidth), getVarcharTestJson("", 0));
    Expr *coalesceExpr = JSONParser::ParseJSON(nlohmann::json::parse(unparsedCoalesceJson));
    TestCoalesceExpr expectedExpr(
        make_unique<TestLiteralExpr>(make_unique<string>(varcharVal).release(), *VarcharType()).release(),
        make_unique<TestLiteralExpr>(make_unique<string>("").release(), *VarcharType(0)).release());
    expectedExpr.isEqual(coalesceExpr);
    delete coalesceExpr;
}

TEST(JSONParserTest, FuncExpr_substr)
{
    // substr(string, int)
    vector<string> argsJson = { getVarcharTestJson(varcharVal, varcharWidth), getInt32TestJSON(int32Val) };
    string unparsedFuncJson = getFuncTestJson(OMNI_VARCHAR, "substr", argsJson);
    Expr *funcExpr = JSONParser::ParseJSON(nlohmann::json::parse(unparsedFuncJson));
    vector<TestExpr *> args = {
        make_unique<TestLiteralExpr>(make_unique<string>(varcharVal).release(), *VarcharType()).release(),
        make_unique<TestLiteralExpr>(int32Val, *IntType()).release()
    };
    TestFuncExpr expectedExpr(*VarcharType(), "substr", args);
    expectedExpr.isEqual(funcExpr);
    delete funcExpr;
}

TEST(JSONParserTest, InExpr)
{
    vector<string> argsJson = { getInt32TestJSON(int32Val), getInt32TestJSON(0), getInt32TestJSON(2) };
    string unparsedInJson = getInTestJson(argsJson);
    Expr *inExpr = JSONParser::ParseJSON(nlohmann::json::parse(unparsedInJson));
    vector<TestExpr *> args = { make_unique<TestLiteralExpr>(int32Val, *IntType()).release(),
        make_unique<TestLiteralExpr>(0, *IntType()).release(), make_unique<TestLiteralExpr>(2, *IntType()).release() };
    TestInExpr expectedExpr(args);
    expectedExpr.isEqual(inExpr);
    delete inExpr;
}

TEST(JSONParserTest, SwitchExpr)
{
    string whenJson = getWhenTestJson(OMNI_INT, getInt64TestJson(int64Val), getInt32TestJSON(int32Val));
    string unparsedSwitchJson =
        getSwitchTestJson(OMNI_INT, getInt32TestJSON(int32Val), whenJson, getInt32TestJSON(4));
    Expr *switchExpr = JSONParser::ParseJSON(nlohmann::json::parse(unparsedSwitchJson));
    auto condition = new TestBinaryExpr(EQ, make_unique<TestLiteralExpr>(int32Val, *IntType()).release(),
        make_unique<TestLiteralExpr>(int64Val, *LongType()).release(), BooleanType());
    vector<pair<TestExpr *, TestExpr *>> whenClause = { { condition,
        make_unique<TestLiteralExpr>(int32Val, *IntType()).release() } };
    TestSwitchExpr expectedExpr(whenClause, make_unique<TestLiteralExpr>(4, *IntType()).release());
    expectedExpr.isEqual(switchExpr);
    delete switchExpr;
}

TEST(JSONParserTest, IfExpr)
{
    string conditionJson =
        getBinaryTestJson(OMNI_BOOLEAN, CmpOps.at(EQ), getInt32TestJSON(int32Val), getInt64TestJson(int64Val));
    string unparsedIfJson = getIfTestJson(OMNI_INT, conditionJson, getInt32TestJSON(int32Val), getInt32TestJSON(4));
    Expr *ifExpr = JSONParser::ParseJSON(nlohmann::json::parse(unparsedIfJson));
    auto condition = new TestBinaryExpr(EQ, make_unique<TestLiteralExpr>(int32Val, *IntType()).release(),
        make_unique<TestLiteralExpr>(int64Val, *LongType()).release(), BooleanType());
    TestIfExpr expectedExpr(condition, make_unique<TestLiteralExpr>(int32Val, *IntType()).release(),
        make_unique<TestLiteralExpr>(4, *IntType()).release());
    expectedExpr.isEqual(ifExpr);
    delete ifExpr;
}

// This ut mimics tpcds-q87
TEST(JSONParserTest, UnsupportedFunctionExpr)
{
    vector<string> argsJson1 = { getNullTestJson(OMNI_INT) };
    string conditionJson = getFuncTestJson(OMNI_BOOLEAN, "not", argsJson1);
    string unparsedIfJson = getIfTestJson(OMNI_INT, conditionJson, getInt32TestJSON(1), getInt32TestJSON(0));
    vector<string> argsJson2 = { unparsedIfJson };
    string unparsedCastJson = getFuncTestJson(OMNI_LONG, "CAST", argsJson2);

    Expr *castExpr = JSONParser::ParseJSON(nlohmann::json::parse(unparsedCastJson));
    EXPECT_EQ(castExpr, nullptr);
    delete castExpr;
}

TEST(JSONParserTest, UnsupportedFunctionExprs)
{
    vector<string> argsJson = { getNullTestJson(OMNI_INT), getNullTestJson(OMNI_LONG) };
    string castJson = getFuncTestJson(OMNI_LONG, "CAST", argsJson);
    nlohmann::json expr = nlohmann::json::parse(castJson);
    nlohmann::json exprs[] = {expr, expr};

    vector<Expr *> res = JSONParser::ParseJSON(exprs, 2);
    EXPECT_EQ(res[0], nullptr);
    delete res[0];
}

TEST(JSONParserTest, UnsupportedDecimal128FunctionExprs)
{
    vector<string> argsJson = { getDecimalFieldRefTestJson(OMNI_DECIMAL128, 1, 0, 0) };
    string castJson = getFuncTestJson(OMNI_LONG, "CAST", argsJson);

    auto parsedExpr = JSONParser::ParseJSON(nlohmann::json::parse(castJson));
    EXPECT_EQ(parsedExpr, nullptr);
    delete parsedExpr;
}