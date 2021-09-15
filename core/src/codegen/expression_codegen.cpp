/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: llvm code generation methods
 */
#include "expression_codegen.h"

#include <chrono>

#include "llvm/IR/LegacyPassManager.h"
#include "llvm/Support/TargetSelect.h"
#include "llvm/Transforms/Scalar.h"
#include "llvm/Transforms/IPO.h"

using namespace llvm;
using namespace orc;
using namespace omniruntime::expressions;

namespace {
const int INT32_VALUE = 32;
const int INT64_VALUE = 64;
const int IS_EVEN = 2;
const int MULTIPLES = 2;
const int FEXPR_VALUE3 = 3;
const int FEXPR_VALUE2 = 2;
const int LENGTH_LOC = 2;
const int STARTEXT_VALUE = 2;
const int SUBSTREXT_VALUE = 3;
}

Value *ExpressionCodeGen::CreateConstantBool(bool v)
{
    return ConstantInt::get(*context, APInt(1, v));
}

Value *ExpressionCodeGen::CreateConstantInt(int32_t v)
{
    return ConstantInt::get(*context, APInt(INT32_VALUE, v, true));
}

Value *ExpressionCodeGen::CreateConstantLong(int64_t v)
{
    return ConstantInt::get(*context, APInt(INT64_VALUE, v, true));
}

Value *ExpressionCodeGen::CreateConstantDouble(double v)
{
    return ConstantFP::get(*context, APFloat(v));
}


Type *ExpressionCodeGen::ToLlvmType(DataType t)
{
    switch (t) {
        case DataType::INT32D:
            return Type::getInt32Ty(*context);
        case DataType::INT64D:
            return Type::getInt64Ty(*context);
        case DataType::DOUBLED:
            return Type::getDoubleTy(*context);
        case DataType::BOOLD:
            return Type::getInt1Ty(*context);
        case DataType::STRINGD:
            return Type::getInt64Ty(*context);
        default:
            LLVM_DEBUG_LOG("Error: Unknown argument datatype %d", t);
            return nullptr;
    }
}

Type *ExpressionCodeGen::ToPointerType(DataType type)
{
    switch (type) {
        case DataType::BOOLD:
            return Type::getInt1PtrTy(*context);
        case DataType::INT32D:
            return Type::getInt32PtrTy(*context);
        case DataType::INT64D:
            return Type::getInt64PtrTy(*context);
        case DataType::DOUBLED:
            return Type::getDoublePtrTy(*context);
        case DataType::STRINGD:
            return Type::getInt64PtrTy(*context);
        default:
            LLVM_DEBUG_LOG("Unsupported column data type %d", type);
            return Type::getInt64PtrTy(*context);
    }
}

// Tells whether or not a function is a valid name
bool IsFunctionName(std::string fnName)
{
    // Either use a global string set to contain function names, or get function names from module
    return true;
}


ExpressionCodeGen::ExpressionCodeGen(std::string name, Expr &cpExpr, std::vector<DataType> &datatypes)
    : datatypes(datatypes)
{
    funcName = name;
    expr = &cpExpr;
    this->datatypes = datatypes;
    llvm::InitializeNativeTarget();
    llvm::InitializeNativeTargetAsmPrinter();
    llvm::InitializeNativeTargetAsmParser();
    jit = eoe(LLJITBuilder().create());
    context = std::make_unique<LLVMContext>();
    // Create module called the_module
    module = std::make_unique<Module>("the_module", *context);
    module->setDataLayout(jit->getDataLayout());
    // Create IR builder to create IR instructions
    builder = std::make_unique<IRBuilder<>>(*context);

    // Time function registering process
    auto start = std::chrono::steady_clock::now();
    fr = std::make_unique<FunctionRegistry>(jit, context, module).release();
    // Only register the necessary functions for the expression
    // Necessary functions are found using RequiredFunctions method
    fr->RegisterNecessaryFuncs(RequiredFunctions(cpExpr));
    auto end = std::chrono::steady_clock::now();
    std::chrono::duration<double> frTime = end - start;
#ifdef DEBUG
    std::cout << "Time to register functions: " << frTime.count() << "seconds" << std::endl;
#endif
    funcNameToSignature = fr->funcNameToSignatureMap;
}

ExpressionCodeGen::~ExpressionCodeGen()
{
    eoe(rt->remove());
    delete fr;
}


// Goes through the expression tree and determines which functions need to be registered
// Helper function for requiredFunctions method
void ExpressionCodeGen::RequiredFunctionsHelper2(Expr &funcExpr, std::set<std::string> &s)
{
    auto *fExpr = static_cast<FuncExpr *>(&funcExpr);
    std::string fn = fExpr->funcName;

    if (fn == "CAST") {
        s.insert(fn + "_" + dataTypeString(fExpr->arguments[0]->GetExprDataType()) + "_" +
            dataTypeString(fExpr->GetExprDataType()));
    } else if (fn == "abs") {
        s.insert(fn + "_" + dataTypeString(fExpr->GetExprDataType()));
    } else if (fn == "substr") {
        if (fExpr->arguments.size() == STARTEXT_VALUE) {
            s.insert("substrWithStartExt");
        }
        if (fExpr->arguments.size() == SUBSTREXT_VALUE) {
            s.insert("substrExt");
        }
    } else {
        s.insert(fExpr->funcName);
    }
    // Recurse on the arguments
    for (auto arg : fExpr->arguments) {
        RequiredFunctionsHelper(*arg, s);
    }
}

void ExpressionCodeGen::RequiredFunctionsHelper(Expr &cpExpression, std::set<std::string> &s)
{
    // For all types of Expr except for FuncExpr, recurse on the children
    Expr *cpExpr = &cpExpression;
    switch (cpExpr->GetType()) {
        case ExprType::DATA_E: {
            return;
        }

        case ExprType::BINARY_E: {
            auto *bExpr = static_cast<BinaryExpr *>(cpExpr);
            RequiredFunctionsHelper(*(bExpr->left), s);
            RequiredFunctionsHelper(*(bExpr->right), s);
            return;
        }

        case ExprType::UNARY_E: {
            RequiredFunctionsHelper(*(static_cast<UnaryExpr *>(cpExpr)->exp), s);
            return;
        }

        case ExprType::IF_E: {
            auto *ifExpr = static_cast<IfExpr *>(cpExpr);
            RequiredFunctionsHelper(*(ifExpr->condition), s);
            RequiredFunctionsHelper(*(ifExpr->trueExpr), s);
            RequiredFunctionsHelper(*(ifExpr->falseExpr), s);
            return;
        }

        case ExprType::IN_E: {
            for (auto arg : static_cast<InExpr *>(cpExpr)->arguments) {
                RequiredFunctionsHelper(*arg, s);
            }
            return;
        }

        case ExprType::BETWEEN_E: {
            auto *bExpr = static_cast<BetweenExpr *>(cpExpr);
            RequiredFunctionsHelper(*(bExpr->value), s);
            RequiredFunctionsHelper(*(bExpr->lowerBound), s);
            RequiredFunctionsHelper(*(bExpr->upperBound), s);
            return;
        }

        case ExprType::COALESCE_E: {
            auto *cExpr = static_cast<CoalesceExpr *>(cpExpr);
            RequiredFunctionsHelper(*(cExpr->value1), s);
            RequiredFunctionsHelper(*(cExpr->value2), s);
            return;
        }

        // Add the name of the required extern function
        case ExprType::FUNC_E: {
            RequiredFunctionsHelper2(*cpExpr, s);
        }

        default:
            return;
    }
}

// Returns a set of the names of required functions
std::set<std::string> ExpressionCodeGen::RequiredFunctions(Expr &cpExpr)
{
    std::set<std::string> ret;
    RequiredFunctionsHelper(cpExpr, ret);
    return ret;
}

// Other operations which require externed functions
Value *ExpressionCodeGen::StringCmp(Value *lhs, Value *rhs)
{
    // call function
    std::vector<Value *> argVals { lhs, rhs };
    auto f = module->getFunction(fr->strCompareExtStr);
    Value *ret = builder->CreateCall(f, argVals, "call_str_cmp");
    return ret;
}

Value *ExpressionCodeGen::ParseDataExpr(DataExpr &dExpr, std::map<std::string, Value *> &args)
{
    DataExpr *dEx = &dExpr;

    if (dEx->isColumn) {
        return args[std::to_string(dEx->colVal)];
    }
    switch (dEx->GetExprDataType()) {
        case DataType::INT32D: {
            return this->CreateConstantInt(dEx->intVal);
        }
        case DataType::INT64D: {
            return this->CreateConstantLong(dEx->longVal);
        }
        case DataType::DOUBLED: {
            return this->CreateConstantDouble(dEx->doubleVal);
        }
        case DataType::STRINGD: {
            Constant *addr =
                ConstantInt::get(*context, APInt(INT64_VALUE, reinterpret_cast<int64_t>(dEx->stringVal->c_str())));
            return addr;
        }
        case DataType::BOOLD: {
            return this->CreateConstantBool(dEx->boolVal);
        }
        default: {
            LLVM_DEBUG_LOG("Unsupported data type in Data Expr %d", dEx->GetExprDataType());
            return this->CreateConstantBool(false);
        }
    }
}

// Helper methods to parse binary expressions
Value *ExpressionCodeGen::ParseBinaryExprInt(omniruntime::expressions::Operator op, Value &leftVal, Value &rightVal)
{
    Value *left = &leftVal;
    Value *right = &rightVal;

    switch (op) {
        case LT:
            return builder->CreateICmpSLT(left, right, "relational_lt");
        case GT:
            return builder->CreateICmpSGT(left, right, "relational_gt");
        case LTE:
            return builder->CreateICmpSLE(left, right, "relational_le");
        case GTE:
            return builder->CreateICmpSGE(left, right, "relational_ge");
        case EQ:
            return builder->CreateICmpEQ(left, right, "relational_eq");
        case NEQ:
            return builder->CreateNot(builder->CreateICmpEQ(left, right), "relational_neq");
        case ADD:
            return builder->CreateAdd(left, right, "arithmetic_add");
        case SUB:
            return builder->CreateSub(left, right, "arithmetic_sub");
        case MUL:
            return builder->CreateMul(left, right, "arithmetic_mul");
        case DIV:
            return builder->CreateSDiv(left, right, "arithmetic_div");
        case MOD:
            return builder->CreateSRem(left, right, "arithmetic_mod");
        default:
            std::cout << "Unsupported int/long binary operator " << op << std::endl;
            return this->CreateConstantBool(false);
    }
}

Value *ExpressionCodeGen::ParseBinaryExprDouble(omniruntime::expressions::Operator op, Value &leftVal, Value &rightVal)
{
    Value *left = &leftVal;
    Value *right = &rightVal;

    switch (op) {
        case LT:
            return builder->CreateFCmpULT(left, right, "frelational_lt");
        case GT:
            return builder->CreateFCmpUGT(left, right, "frelational_gt");
        case LTE:
            return builder->CreateFCmpULE(left, right, "frelational_le");
        case GTE:
            return builder->CreateFCmpUGE(left, right, "frelational_ge");
        case EQ:
            return builder->CreateFCmpUEQ(left, right, "farithmetic_eq");
        case NEQ:
            return builder->CreateNot(builder->CreateFCmpUEQ(left, right, "farithmetic_neq"));
        case ADD:
            return builder->CreateFAdd(left, right, "farithmetic_add");
        case SUB:
            return builder->CreateFSub(left, right, "farithmetic_sub");
        case MUL:
            return builder->CreateFMul(left, right, "farithmetic_mul");
        case DIV:
            return builder->CreateFDiv(left, right, "farithmetic_div");
        default:
            std::cout << "Unsupported double binary operator " << op << std::endl;
            return this->CreateConstantBool(false);
    }
}

Value *ExpressionCodeGen::ParseBinaryExprString(omniruntime::expressions::Operator op, Value &leftVal, Value &rightVal)
{
    Value *left = &leftVal;
    Value *right = &rightVal;

    switch (op) {
        case LT:
            return builder->CreateICmpSLT(this->StringCmp(left, right), CreateConstantInt(0));
        case GT:
            return builder->CreateICmpSGT(this->StringCmp(left, right), CreateConstantInt(0));
        case LTE:
            return builder->CreateICmpSLE(this->StringCmp(left, right), CreateConstantInt(0));
        case GTE:
            return builder->CreateICmpSGE(this->StringCmp(left, right), CreateConstantInt(0));
        case EQ:
            return builder->CreateICmpEQ(this->StringCmp(left, right), CreateConstantInt(0));
        case NEQ:
            return builder->CreateICmpNE(this->StringCmp(left, right), CreateConstantInt(0));
        default:
            std::cout << "Unsupported string binary operator " << op << std::endl;
            return this->CreateConstantBool(false);
    }
}

Value *ExpressionCodeGen::ParseBinaryExpr(BinaryExpr &binExpr, std::map<std::string, Value *> &args)
{
    BinaryExpr *bExpr = &binExpr;

    if (bExpr->left->GetType() == ExprType::DATA_E || bExpr->right->GetType() == ExprType::DATA_E) {
        DataType biggerType = std::max(bExpr->left->GetExprDataType(), bExpr->right->GetExprDataType());
        bExpr->left->dataType = biggerType;
        bExpr->right->dataType = biggerType;
    }
    Value *left = this->ParseExpr(*(bExpr->left), args);
    Value *right = this->ParseExpr(*(bExpr->right), args);

    if (bExpr->op == omniruntime::expressions::Operator::AND) {
        return builder->CreateAnd(left, right, "logical_and");
    }
    if (bExpr->op == omniruntime::expressions::Operator::OR) {
        return builder->CreateOr(left, right, "logical_or");
    }

    if (bExpr->left->GetExprDataType() == DataType::INT32D || bExpr->left->GetExprDataType() == DataType::INT64D) {
        return this->ParseBinaryExprInt(bExpr->op, *left, *right);
    } else if (bExpr->left->GetExprDataType() == DOUBLED) {
        return this->ParseBinaryExprDouble(bExpr->op, *left, *right);
    } else if (bExpr->left->GetExprDataType() == STRINGD) {
        return this->ParseBinaryExprString(bExpr->op, *left, *right);
    }
    LLVM_DEBUG_LOG("Unsupported binary operator %d", bExpr->op);
    return this->CreateConstantBool(false);
}


Value *ExpressionCodeGen::ParseUnaryExpr(UnaryExpr &unaryExpr, std::map<std::string, Value *> &args)
{
    UnaryExpr *uExpr = &unaryExpr;
    Value *val = this->ParseExpr(*uExpr, args);
    switch (uExpr->op) {
        case NOT:
            return builder->CreateNot(val, "logical_not");
        default:
            // ignore the unary operator if it is invalid
            return val;
    }
}


// Create a LLVM Function that returns the value of a conditional (IF/CASE)
Function *ExpressionCodeGen::CreateConditional(DataType retType, Expr &condExpr, Expr &ifTrueExpr, Expr &ifFalseExpr)
{
    Expr *cond = &condExpr;
    Expr *ifTrue = &ifTrueExpr;
    Expr *ifFalse = &ifFalseExpr;

    std::vector<Type *> args;
    // args contains the types of the arguments
    // args[2 * i] contains the type of the ith argument (where 0 <= i < datatypes.size())
    // args[2 * i+1] contains the boolean type, as argument with index 2i+1 contains whether argument i is null
    // args[2 * datatypes.size()] contains the type of the current row number (int32_t)
    args.reserve(2 * datatypes.size() + 1);
    for (int32_t i = 0; i < datatypes.size(); i++) {
        DataType type = datatypes.at(i);
        args.push_back(this->ToLlvmType(type));
        args.push_back(Type::getInt1Ty(*context));
    }

    Type *retTypePtr = this->ToLlvmType(retType);
    FunctionType *prototype = FunctionType::get(retTypePtr, args, false);
    Function *func = Function::Create(prototype, Function::ExternalLinkage, "IF_CONDITIONAL", module.get());

    int32_t idx = 0;
    for (auto &arg : func->args()) {
        if (idx == datatypes.size() * MULTIPLES) {
            arg.setName("rowIdx");
        } else {
            if (idx % IS_EVEN == 0) {
                arg.setName(std::to_string(idx / MULTIPLES));
            } else {
                // Example: the argument representing whether the value in column 3 is null has name "3_isNull"
                arg.setName(std::to_string(idx / MULTIPLES) + "_isNull");
            }
        }
        idx++;
    }

    BasicBlock *conditionalCheck = BasicBlock::Create(*context, "CONDITIONAL_CHECK", func);
    BasicBlock *trueBlock = BasicBlock::Create(*context, "TRUE_BLOCK", func);
    BasicBlock *falseBlock = BasicBlock::Create(*context, "FALSE_BLOCK", func);

    std::map<std::string, Value *> fArgs;
    for (auto &arg : func->args()) {
        fArgs[arg.getName().str()] = &arg;
    }

    builder->SetInsertPoint(conditionalCheck);
    Value *evCond = this->ParseExpr(*cond, fArgs);
    Value *evTrue = this->ParseExpr(*ifTrue, fArgs);
    Value *evFalse = this->ParseExpr(*ifFalse, fArgs);
    // If cond evaluates to true, control flow goes to trueBlock, returning evTrue
    // Otherwise goes to falseBlock and returns evFalse
    builder->CreateCondBr(evCond, trueBlock, falseBlock);
    builder->SetInsertPoint(trueBlock);
    builder->CreateRet(evTrue);
    builder->SetInsertPoint(falseBlock);
    builder->CreateRet(evFalse);

    llvm::verifyFunction(*func);
    return func;
}


Value *ExpressionCodeGen::ParseIfExpr(IfExpr &ifExpr, std::map<std::string, Value *> &args)
{
    IfExpr *ie = &ifExpr;
    BasicBlock *currentBlock = builder->GetInsertBlock();
    DataType retType = ie->trueExpr->GetExprDataType();
    Expr *cond = ie->condition;
    Expr *ifTrue = ie->trueExpr;
    Expr *ifFalse = ie->falseExpr;

    Function *conditionalFunc = CreateConditional(retType, *cond, *ifTrue, *ifFalse);
    builder->SetInsertPoint(currentBlock);
    std::vector<Value *> passArgs;
    for (std::map<std::string, Value *>::iterator i = args.begin(); i != args.end(); i++) {
        Value *a = i->second;
        passArgs.push_back(a);
    }
    CallInst *condCall = builder->CreateCall(conditionalFunc, passArgs, "EVAL_IF");
    return condCall;
}


Value *ExpressionCodeGen::ParseInExpr(InExpr &inExpr, std::map<std::string, Value *> &args)
{
    InExpr *iExpr = &inExpr;
    Expr *toCompare = iExpr->arguments[0];
    llvm::Value *val = this->ParseExpr(*toCompare, args);

    llvm::Value *inArray = llvm::ConstantInt::get(*context, APInt(1, 0));
    // Handle types correctly
    for (int i = 1; i < iExpr->arguments.size(); i++) {
        // initialize tmpCmp
        llvm::Value *tmpCmp = this->CreateConstantBool(false);

        if (toCompare->GetExprDataType() == DataType::INT64D &&
            iExpr->arguments[i]->GetExprDataType() == DataType::INT32D) {
            Value *argInt64 =
                builder->CreateIntCast(ParseExpr(*(iExpr->arguments[i]), args), Type::getInt64Ty(*context), true);
            tmpCmp = builder->CreateICmpEQ(val, argInt64);
        } else if (toCompare->GetExprDataType() == DataType::INT32D &&
            iExpr->arguments[i]->GetExprDataType() == DataType::INT64D) {
            Value *valInt64 = builder->CreateIntCast(val, Type::getInt64Ty(*context), true);
            tmpCmp = builder->CreateICmpEQ(valInt64, ParseExpr(*(iExpr->arguments[i]), args));
        } else {
            switch (iExpr->arguments[0]->dataType) {
                case INT32D: {
                    tmpCmp = builder->CreateICmpEQ(val, ParseExpr(*(iExpr->arguments[i]), args));
                    break;
                }
                case INT64D: {
                    tmpCmp = builder->CreateICmpEQ(val, ParseExpr(*(iExpr->arguments[i]), args));
                    break;
                }
                case DOUBLED: {
                    tmpCmp = builder->CreateFCmpOEQ(val, ParseExpr(*(iExpr->arguments[i]), args));
                    break;
                }
                case STRINGD: {
                    tmpCmp = builder->CreateICmpEQ(this->StringCmp(val, ParseExpr(*(iExpr->arguments[i]), args)),
                        CreateConstantInt(0));
                    break;
                }
                default: {
                    LLVM_DEBUG_LOG("Unsupported data type in IN expr %d", iExpr->arguments[0]->dataType);
                    tmpCmp = this->CreateConstantBool(false);
                }
            }
        }

        inArray = builder->CreateOr(inArray, tmpCmp);
    }
    return inArray;
}


Value *ExpressionCodeGen::ParseBetweenExpr(BetweenExpr &betweenExpr, std::map<std::string, Value *> &args)
{
    BetweenExpr *bExpr = &betweenExpr;
    DataType biggerType = std::max(std::max(bExpr->lowerBound->GetExprDataType(), bExpr->upperBound->GetExprDataType()),
        bExpr->value->GetExprDataType());
    bExpr->lowerBound->dataType = biggerType;
    bExpr->upperBound->dataType = biggerType;
    bExpr->value->dataType = biggerType;

    llvm::Value *val = ParseExpr(*(bExpr->value), args);
    llvm::Value *lowerVal = ParseExpr(*(bExpr->lowerBound), args);
    llvm::Value *upperVal = ParseExpr(*(bExpr->upperBound), args);

    if (bExpr->value->GetExprDataType() == DataType::INT32D || bExpr->value->GetExprDataType() == DataType::INT64D) {
        llvm::Value *cmpleft = builder->CreateICmpSLE(lowerVal, val, "between_cmpleft");
        llvm::Value *cmpright = builder->CreateICmpSLE(val, upperVal, "between_cmpright");
        llvm::Value *result = builder->CreateAnd(cmpleft, cmpright, "between_and");
        return result;
    } else if (bExpr->value->GetExprDataType() == DOUBLED) {
        llvm::Value *cmpleft = builder->CreateFCmpULE(lowerVal, val, "between_cmpleft");
        llvm::Value *cmpright = builder->CreateFCmpULE(val, upperVal, "between_cmpright");
        llvm::Value *result = builder->CreateAnd(cmpleft, cmpright, "between_and");
        return result;
    } else if (bExpr->value->GetExprDataType() == STRINGD) {
        llvm::Value *cmpleft = builder->CreateICmpSLE(this->StringCmp(lowerVal, val), CreateConstantInt(0));
        llvm::Value *cmpright = builder->CreateICmpSLE(this->StringCmp(val, upperVal), CreateConstantInt(0));
        llvm::Value *result = builder->CreateAnd(cmpleft, cmpright, "between_and");
        return result;
    }

    LLVM_DEBUG_LOG("Error: unsupported data type for between %d", bExpr->value->GetExprDataType());
    return this->CreateConstantBool(false);
}


// Create a LLVM Function which returns the result of a COALESCE
Function *ExpressionCodeGen::CreateCoalesceFunc(DataType retType, DataExpr &dExpr1, Expr &value2Expr)
{
    std::vector<Type *> args;
    // args contains the types of the arguments
    // args[2 * i] contains the type of the ith argument (where 0 <= i < datatypes.size())
    // args[2 * i+1] contains the boolean type, as argument with index 2i+1 contains whether argument i is null
    // args[2 * datatypes.size()] contains the type of the current row number (int32_t)
    args.reserve(2 * datatypes.size() + 1);
    for (int32_t i = 0; i < datatypes.size(); i++) {
        DataType type = datatypes.at(i);
        args.push_back(this->ToLlvmType(type));
        args.push_back(Type::getInt1Ty(*context));
    }

    Type *retTypePtr = this->ToLlvmType(retType);
    FunctionType *prototype = FunctionType::get(retTypePtr, args, false);
    Function *func = Function::Create(prototype, Function::ExternalLinkage, "COALESCE_CONDITIONAL", module.get());

    int32_t idx = 0;
    for (auto &arg : func->args()) {
        if (idx == datatypes.size() * MULTIPLES) {
            arg.setName("rowIdx");
        } else {
            if (idx % IS_EVEN == 0) {
                arg.setName(std::to_string(idx / MULTIPLES));
            } else {
                arg.setName(std::to_string(idx / MULTIPLES) + "_isNull");
            }
        }
        idx++;
    }

    std::map<std::string, Value *> fArgs;
    for (auto &arg : func->args()) {
        fArgs[arg.getName().str()] = &arg;
    }

    return CreateCoalesceFuncHelper(dExpr1, value2Expr, fArgs, *func);
}

Function *ExpressionCodeGen::CreateCoalesceFuncHelper(DataExpr &dataExpr1, Expr &valExpr2,
    std::map<std::string, Value *> fArgs, Function &cfunc)
{
    DataExpr *dExpr1 = &dataExpr1;
    Expr *value2Expr = &valExpr2;
    Function *func = &cfunc;

    BasicBlock *conditionalCheck = BasicBlock::Create(*context, "IS_NULL", func);
    BasicBlock *trueBlock = BasicBlock::Create(*context, "NOTNULL_BLOCK", func);
    BasicBlock *falseBlock = BasicBlock::Create(*context, "NULL_BLOCK", func);

    builder->SetInsertPoint(conditionalCheck);

    Value *value1 = this->ParseExpr(*dExpr1, fArgs);
    Value *value2 = this->ParseExpr(*value2Expr, fArgs);
    if (value2Expr->GetExprDataType() == INT32D && dExpr1->GetExprDataType() == INT64D) {
        value2 = builder->CreateIntCast(value2, Type::getInt64Ty(*context), true);
    }
    if ((value2Expr->GetExprDataType() == INT32D || value2Expr->GetExprDataType() == INT64D) &&
        dExpr1->GetExprDataType() == DOUBLED) {
        value2 = builder->CreateCast(Instruction::SIToFP, value2, Type::getDoubleTy(*context));
    }

    std::string colNullStr = std::to_string(dExpr1->colVal) + "_isNull";
    Value *isNull = fArgs[colNullStr];

    builder->CreateCondBr(isNull, trueBlock, falseBlock);
    builder->SetInsertPoint(trueBlock);
    builder->CreateRet(value2); // return second value if the first one is null
    builder->SetInsertPoint(falseBlock);
    builder->CreateRet(value1); // otherwise just return the first value

    llvm::verifyFunction(*func);
    return func;
}

Value *ExpressionCodeGen::ParseCoalesceExpr(CoalesceExpr &coalesceExpr, std::map<std::string, Value *> &args)
{
    Expr *value1Expr = coalesceExpr.value1;
    Expr *value2Expr = coalesceExpr.value2;

    // Only case that coalesce is used seems to be with a column as first argument
    if (value1Expr->GetType() == ExprType::DATA_E) {
        auto *dExpr1 = static_cast<DataExpr *>(value1Expr);
        if (dExpr1->isColumn) {
            BasicBlock *currentBlock = builder->GetInsertBlock();

            DataType retType = value1Expr->GetExprDataType();
            Function *coalesceFunc = CreateCoalesceFunc(retType, *dExpr1, *value2Expr);
            builder->SetInsertPoint(currentBlock);

            std::vector<Value *> passArgs;
            for (auto &arg : args) {
                Value *a = arg.second;
                passArgs.push_back(a);
            }

            CallInst *condCall = builder->CreateCall(coalesceFunc, passArgs, "EVAL_COALESCE");
            return condCall;
        }
    }
    // If the first argument is not a column DataExpr, just return it
    return this->ParseExpr(*value1Expr, args);
}

// Helper functions for parsing functions
Value *ExpressionCodeGen::ParseFuncExprAbs(FuncExpr &funcExpr, std::map<std::string, Value *> &args)
{
    FuncExpr *fExpr = &funcExpr;
    std::string absFuncName = "Abs_" + dataTypeString(fExpr->dataType);
    std::vector<Value *> argVals { this->ParseExpr(*(fExpr->arguments[0]), args) };
    auto f = module->getFunction(absFuncName);
    if (f) {
        Value *ret = builder->CreateCall(f, argVals, absFuncName);
        return ret;
    } else {
        std::cout << "Unable to parse function " << absFuncName << std::endl;
        return CreateConstantInt(0);
    }
}

Value *ExpressionCodeGen::ParseFuncExprCast(FuncExpr &funcExpr, std::map<std::string, Value *> &args)
{
    FuncExpr *fExpr = &funcExpr;

    llvm::Value *val = ParseExpr(*(fExpr->arguments[0]), args);
    std::vector<Value *> argVals { val };
    DataType from = fExpr->arguments[0]->dataType;
    DataType to = fExpr->GetExprDataType();

    std::string castFuncName = "Cast_" + dataTypeString(from) + "_" + dataTypeString(to);
    std::cout << castFuncName << std::endl;

    // if casting to same type, treat it as constant
    if (from == to) {
        auto *dataExpr = static_cast<DataExpr *>(fExpr->arguments[0]);
        return ParseDataExpr(*dataExpr, args);
    }

    auto f = module->getFunction(castFuncName);
    if (f) {
        Value *ret = builder->CreateCall(f, argVals, castFuncName);
        return ret;
    } else {
        LLVM_DEBUG_LOG("Unable to parse function %s", castFuncName.c_str());
        return CreateConstantInt(0);
    }
}

Value *ExpressionCodeGen::ParseFuncExprSubstr(FuncExpr &funcExpr, std::map<std::string, Value *> &args)
{
    FuncExpr *fExpr = &funcExpr;

    if (fExpr->arguments.size() == FEXPR_VALUE3) {
        Value *str = ParseExpr(*(fExpr->arguments[0]), args);
        Value *startIdx = ParseExpr(*(fExpr->arguments[1]), args);
        Value *length = ParseExpr(*(fExpr->arguments[LENGTH_LOC]), args);
        std::vector<Value *> argVals { str, startIdx, length };

        auto f = module->getFunction(fr->substrExtStr);
        Value *ret = builder->CreateCall(f, argVals, fr->substrExtStr);
        return ret;
    }
    if (fExpr->arguments.size() == FEXPR_VALUE2) {
        Value *str = ParseExpr(*(fExpr->arguments[0]), args);
        Value *startIdx = ParseExpr(*(fExpr->arguments[1]), args);
        std::vector<Value *> argVals { str, startIdx };

        auto f = module->getFunction(fr->substrWithStartExtStr);
        Value *ret = builder->CreateCall(f, argVals, fr->substrWithStartExtStr);
        return ret;
    }
    std::cout << "Error: Incorrect number of arguments used for substr" << std::endl;
    return CreateConstantLong(0);
}

Value *ExpressionCodeGen::ParseFuncExprExt(FuncExpr &funcExpr, std::map<std::string, Value *> &args)
{
    FuncExpr *fExpr = &funcExpr;
    FunctionSignature fs = funcNameToSignature[fExpr->funcName];

    std::vector<Value *> argVals;
    for (int i = 0; i < fExpr->arguments.size(); i++) {
        // Cast arguments to the correct type
        DataType desiredType = fs.GetParams()[i];
        DataType currType = fExpr->arguments[i]->GetExprDataType();
        if (desiredType == DOUBLED && (currType == INT32D || currType == INT64D)) {
            Value *argDouble = builder->CreateCast(Instruction::SIToFP, ParseExpr(*(fExpr->arguments[i]), args),
                Type::getDoubleTy(*context));
            argVals.push_back(argDouble);
        } else if (desiredType == INT64D && currType == INT32D) {
            Value *argInt64 =
                builder->CreateIntCast(ParseExpr(*(fExpr->arguments[i]), args), Type::getInt64Ty(*context), true);
            argVals.push_back(argInt64);
        } else {
            argVals.push_back(ParseExpr(*(fExpr->arguments[i]), args));
        }
    }
    // Assume that the function name of the fExpr and in the module are matching
    auto f = module->getFunction(fExpr->funcName);
    Value *ret = builder->CreateCall(f, argVals, "call_" + fExpr->funcName);
    return ret;
}


// Handles all functions
// Only calls them; registration is done in function registry
Value *ExpressionCodeGen::ParseFuncExpr(FuncExpr &funcExpr, std::map<std::string, Value *> &args)
{
    FuncExpr *fExpr = &funcExpr;

    if (fExpr->funcName == "abs") {
        return this->ParseFuncExprAbs(*fExpr, args);
    }
    if (fExpr->funcName == "substr") {
        return this->ParseFuncExprSubstr(*fExpr, args);
    }
    if (fExpr->funcName == "concat") {
        Value *str1 = ParseExpr(*(fExpr->arguments[0]), args);
        Value *str2 = ParseExpr(*(fExpr->arguments[1]), args);
        std::vector<Value *> argVals { str1, str2 };

        auto f = module->getFunction(fr->concatStrExtStr);
        Value *ret = builder->CreateCall(f, argVals, fr->concatStrExtStr);
        return ret;
    }
    if (fExpr->funcName == "CAST") {
        return this->ParseFuncExprCast(*fExpr, args);
    }
    if (fExpr->funcName == "LIKE") {
        Value *str1 = ParseExpr(*(fExpr->arguments[0]), args);
        Value *str2 = ParseExpr(*(fExpr->arguments[1]), args);
        std::vector<Value *> argVals { str1, str2 };

        auto f = module->getFunction(fr->likeExtStr);
        Value *ret = builder->CreateCall(f, argVals, fr->likeExtStr);
        return ret;
    }
    if (fExpr->funcName == "combine_hash") {
        Value *prevHashVal = ParseExpr(*(fExpr->arguments[0]), args);
        Value *val = ParseExpr(*(fExpr->arguments[1]), args);

        if (fExpr->arguments[0]->GetExprDataType() == INT32D) {
            prevHashVal = builder->CreateIntCast(prevHashVal, Type::getInt64Ty(*context), true);
        }
        if (fExpr->arguments[1]->GetExprDataType() == INT32D) {
            val = builder->CreateIntCast(val, Type::getInt64Ty(*context), true);
        }

        std::vector<Value *> argVals { prevHashVal, val };

        auto f = module->getFunction(fr->combineHashStr);
        Value *ret = builder->CreateCall(f, argVals, fr->combineHashStr);
        DumpCode();
        return ret;
    }
    // external functions
    if (IsFunctionName(fExpr->funcName)) {
        return this->ParseFuncExprExt(*fExpr, args);
    }

    LLVM_DEBUG_LOG("No function found with name %s", fExpr->funcName.c_str());
    return CreateConstantInt(0);
}


// Main codegen for any type of expression
Value *ExpressionCodeGen::ParseExpr(Expr &rootExpr, std::map<std::string, Value *> &args)
{
    Expr *root = &rootExpr;

    switch (root->GetType()) {
        case ExprType::DATA_E: {
            auto *dEx = static_cast<DataExpr *>(root);
            return this->ParseDataExpr(*dEx, args);
        }

        case ExprType::BINARY_E: {
            auto *bExpr = static_cast<BinaryExpr *>(root);
            return this->ParseBinaryExpr(*bExpr, args);
        }

        case ExprType::UNARY_E: {
            auto *uExpr = static_cast<UnaryExpr *>(root);
            return this->ParseUnaryExpr(*uExpr, args);
        }

        case ExprType::IF_E: {
            auto *ie = static_cast<IfExpr *>(root);
            return this->ParseIfExpr(*ie, args);
        }

        case ExprType::IN_E: {
            auto *iExpr = static_cast<InExpr *>(root);
            return this->ParseInExpr(*iExpr, args);
        }

        case ExprType::BETWEEN_E: {
            auto *bExpr = static_cast<BetweenExpr *>(root);
            return this->ParseBetweenExpr(*bExpr, args);
        }

        case ExprType::COALESCE_E: {
            auto *cExpr = static_cast<CoalesceExpr *>(root);
            return this->ParseCoalesceExpr(*cExpr, args);
        }

        case ExprType::FUNC_E: {
            auto *fExpr = static_cast<FuncExpr *>(root);
            return this->ParseFuncExpr(*fExpr, args);
        }

        default: {
            LLVM_DEBUG_LOG("Error: Unsupported expr type %d", root->GetType());
            return this->CreateConstantBool(false);
        }
    }
}

std::string ExpressionCodeGen::DumpCode()
{
    std::string ir;
    llvm::raw_string_ostream stream(ir);
    module->print(stream, nullptr);
    std::cout << " Generated code::" << ir;
    return ir;
}

void AddOptimizationPasses(legacy::FunctionPassManager *fpm, llvm::legacy::PassManager &mpm)
{
    fpm->add(createSCCPPass());
    fpm->add(createNewGVNPass());
    fpm->add(createInductiveRangeCheckEliminationPass());
    fpm->add(createIndVarSimplifyPass());

    fpm->add(createLICMPass());
    fpm->add(createLoopUnrollPass());
    fpm->add(createLoopUnswitchPass());

    fpm->add(createLoopLoadEliminationPass());
    fpm->add(createInductiveRangeCheckEliminationPass());
    fpm->add(createIndVarSimplifyPass());
    fpm->add(createLoopInstSimplifyPass());
    fpm->add(createLoopSimplifyCFGPass());
    fpm->add(createMergedLoadStoreMotionPass());
    fpm->add(createMergeICmpsLegacyPass());
    fpm->add(createAggressiveDCEPass());
    fpm->add(createDeadStoreEliminationPass());

    mpm.add(createFunctionInliningPass());
    mpm.add(createPruneEHPass());
}

Function *ExpressionCodeGen::CreateFunction()
{
    std::vector<Type *> args;
    args.reserve(MULTIPLES * datatypes.size() + 1);
    // Values in args vector follow the format:
    // #0, #0_isNull, #1, #1_isNull, ..., #4, #4_isNull, rowIdx
    for (auto type : datatypes) {
        args.push_back(this->ToLlvmType(type));
        args.push_back(Type::getInt1Ty(*context));
    }

    std::cout << "exprtree: ";
    expr->PrintExprTree();
    std::cout << std::endl;
    FunctionType *prototype = FunctionType::get(this->ToLlvmType(expr->GetExprDataType()), args, false);
    Function *func = Function::Create(prototype, Function::ExternalLinkage, funcName, module.get());

    int32_t idx = 0;
    for (auto &arg : func->args()) {
        if (idx == datatypes.size() * MULTIPLES) {
            arg.setName("rowIdx");
        } else {
            if (idx % IS_EVEN == 0) {
                arg.setName(std::to_string(idx / MULTIPLES));
            } else {
                arg.setName(std::to_string(idx / MULTIPLES) + "_isNull");
            }
        }
        idx++;
    }

    BasicBlock *body = BasicBlock::Create(*context, "CREATED_FUNC_BODY", func);
    builder->SetInsertPoint(body);
    std::map<std::string, Value *> fArgs;
    for (auto &arg : func->args()) {
        fArgs[arg.getName().str()] = &arg;
    }

    Value *ret = this->ParseExpr(*expr, fArgs);
    builder->CreateRet(ret);
    verifyFunction(*func);
    auto fpm = std::make_unique<legacy::FunctionPassManager>(module.get());
    llvm::legacy::PassManager mpm;
    AddOptimizationPasses(fpm.get(), mpm);
    fpm->doInitialization();
    for (auto &F : *module)
        fpm->run(F);
    mpm.run(*module);
    return func;
}
