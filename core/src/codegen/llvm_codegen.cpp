/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2027. All rights reserved.
 * Description: llvm code generation methods
 */
#include "llvm_codegen.h"
#include "llvm/IR/LegacyPassManager.h"
#include "llvm/Transforms/Scalar.h"
#include "llvm/Transforms/IPO.h"
#include <chrono>


using namespace llvm;
using namespace orc;
using namespace omniruntime::expressions;

namespace{
    const int INT32_VALUE = 32;
    const int INT64_VALUE = 64;
    const int IS_EVEN = 2;
    const int MULTIPLES = 2;
    const int FEXPR_VALUE3 = 3;
    const int FEXPR_VALUE2 = 2;
    const int LENGTH_LOC = 2;
}

Value *LLVMCodeGen::createConstantBool(bool v)
{
    return ConstantInt::get(*context, APInt(1, v));
}

Value *LLVMCodeGen::createConstantInt(int32_t v)
{
    return ConstantInt::get(*context, APInt(INT32_VALUE, v, true));
}

Value *LLVMCodeGen::createConstantLong(int64_t v)
{
    return ConstantInt::get(*context, APInt(INT64_VALUE, v, true));
}

Value *LLVMCodeGen::createConstantDouble(double v)
{
    return ConstantFP::get(*context, APFloat(v));
}


Type *LLVMCodeGen::toLLVMType(DataType t)
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
            std::cout << "Error: Unknown argument datatype " << t << std::endl;
            return nullptr;
    }
}

// Tells whether or not a function is a valid name
bool isFunctionName(std::string fnName)
{
    // TODO: either use a global string set to contain function names, or get function names from _module
    return true;
}


LLVMCodeGen::LLVMCodeGen(std::string name,  Expr *cpExpr, std::vector <DataType> *datatypes)
{
    funcName = name;
    expr = cpExpr;
    this->datatypes = datatypes;
    llvm::InitializeNativeTarget();
    llvm::InitializeNativeTargetAsmPrinter();
    llvm::InitializeNativeTargetAsmParser();
    // llvm::InitializeNativeTargetDisassembler();
    JIT = EOE(LLJITBuilder().create());
    context = std::make_unique<LLVMContext>();
    // Create module called the_module
    module = std::make_unique<Module>("the_module", *context);
    module->setDataLayout(JIT->getDataLayout());
    // Create IR builder to create IR instructinos
    builder = std::make_unique<IRBuilder<>>(*context);

    // Time function registering process
    auto start = std::chrono::steady_clock::now();
    fr = std::make_unique<FunctionRegistry>(JIT, context, module).release();
    // Only register the necessary functions for the expression
    // Necessary functions are found using requiredFunctions method
    fr->InitNecessary(requiredFunctions(expr));
    auto end = std::chrono::steady_clock::now();
    std::chrono::duration<double> frTime = end - start;
#ifdef DEBUG
    std::cout << "Time to register functions: " << frTime.count() << "seconds" << std::endl;
#endif
    funcNameToSignature = fr->funcNameToSignatureMap;
}

LLVMCodeGen::~LLVMCodeGen()
{
    EOE(rt->remove());
    delete this->datatypes;
    delete fr;
}


// Goes through the expression tree and determines which functions need to be registered
// Helper function for requiredFunctions method
const int STARTEXT_VALUE = 2;
const int SUBSTREXT_VALUE = 3;

void LLVMCodeGen::requiredFunctionsHelper(Expr *cpExpr, std::set<std::string> &s)
{
    // For all types of Expr except for FuncExpr, recurse on the children
    switch (cpExpr->GetType()) {
        case ExprType::DATA_E: {
            return;
        }

        case ExprType::BINARY_E: {
            BinaryExpr *bExpr = dynamic_cast<BinaryExpr *>(cpExpr);
            requiredFunctionsHelper(bExpr->left, s);
            requiredFunctionsHelper(bExpr->right, s);
            return;
        }

        case ExprType::UNARY_E: {
            UnaryExpr *uExpr = dynamic_cast<UnaryExpr *>(cpExpr);
            requiredFunctionsHelper(uExpr->exp, s);
            return;
        }

        case ExprType::IF_E: {
            IfExpr *ifExpr = dynamic_cast<IfExpr *>(cpExpr);
            requiredFunctionsHelper(ifExpr->condition, s);
            requiredFunctionsHelper(ifExpr->trueExpr, s);
            requiredFunctionsHelper(ifExpr->falseExpr, s);
            return;
        }

        case ExprType::IN_E: {
            InExpr *inExpr = dynamic_cast<InExpr *>(cpExpr);
            for (auto arg : inExpr->arguments) {
                requiredFunctionsHelper(arg, s);
            }
            return;
        }

        case ExprType::BETWEEN_E: {
            BetweenExpr *bExpr = dynamic_cast<BetweenExpr *>(cpExpr);
            requiredFunctionsHelper(bExpr->value, s);
            requiredFunctionsHelper(bExpr->lowerBound, s);
            requiredFunctionsHelper(bExpr->upperBound, s);
            return;
        }

        case ExprType::COALESCE_E: {
            CoalesceExpr *cExpr = dynamic_cast<CoalesceExpr *>(cpExpr);
            requiredFunctionsHelper(cExpr->value1, s);
            requiredFunctionsHelper(cExpr->value2, s);
            return;
        }

            // Add the name of the required extern function
        case ExprType::FUNC_E: {
            FuncExpr *fExpr = dynamic_cast<FuncExpr *>(cpExpr);
            std::string fn = fExpr->funcName;

            if (fn == "CAST" || fn == "abs") {
                s.insert(fn + "_" + dataTypeString(fExpr->arguments[0]->GetExprDataType()));
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
                requiredFunctionsHelper(arg, s);
            }
            return;
        }

        default:
            return;
    }
}

// Returns a set of the names of required functions
std::set<std::string> LLVMCodeGen::requiredFunctions(Expr *cpExpr)
{
    std::set <std::string> ret;
    requiredFunctionsHelper(cpExpr, ret);
    return ret;
}

// Other operations which require externed functions
Value *LLVMCodeGen::stringCmp(Value *LHS, Value *RHS)
{
    // call function
    std::vector <Value*> argVals{LHS, RHS};
    auto f = module->getFunction("strCompareExt");
    Value *ret = builder->CreateCall(f, argVals, "call_str_cmp");
    return ret;
}

Value *LLVMCodeGen::parseDataExpr(DataExpr *dEx, std::map<std::string, Value *> &args)
{
    if (dEx->isColumn) {
        return args[std::to_string(dEx->colVal)];
    }
    switch (dEx->GetExprDataType()) {
        case DataType::INT32D: {
            return this->createConstantInt(dEx->intVal);
        }
        case DataType::INT64D: {
            return this->createConstantLong(dEx->longVal);
        }
        case DataType::DOUBLED: {
            return this->createConstantDouble(dEx->doubleVal);
        }
        case DataType::STRINGD: {
            const char *s = dEx->stringVal->c_str();

            Constant *addr = ConstantInt::get(*context, APInt(INT64_VALUE, (int64_t)(s)));
            return addr;
        }
        case DataType::BOOLD: {
            return this->createConstantBool(dEx->boolVal);
        }
        default: {
            std::cout << "Unsupported data type in Data Expr" << std::endl;
            return this->createConstantBool(false);
        }
    }
}

Value *LLVMCodeGen::parseBinaryExpr(BinaryExpr *bExpr, std::map<std::string, Value *> &args)
{
    if (bExpr->left->GetType() == ExprType::DATA_E || bExpr->right->GetType() == ExprType::DATA_E) {
        DataType biggerType = std::max(bExpr->left->GetExprDataType(), bExpr->right->GetExprDataType());
        bExpr->left->dataType = biggerType;
        bExpr->right->dataType = biggerType;
    }
    Value *left = this->parseExpr(bExpr->left, args);
    Value *right = this->parseExpr(bExpr->right, args);


    if (bExpr->op == omniruntime::expressions::Operator::AND) {
        return builder->CreateAnd(left, right, "logical_and");
    }
    if (bExpr->op == omniruntime::expressions::Operator::OR) {
        return builder->CreateOr(left, right, "logical_or");
    }

    if (bExpr->left->GetExprDataType() == DataType::INT32D || bExpr->left->GetExprDataType() == DataType::INT64D) {
        switch (bExpr->op) {
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
                std::cout << "Error: Binary operator not supported " << bExpr->op << std::endl;
        }
    } else if (bExpr->left->GetExprDataType() == DOUBLED) {
        switch (bExpr->op) {
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
                std::cout << "Unsupported double binary operator" << std::endl;
                break;
        }
    } else if (bExpr->left->GetExprDataType() == STRINGD) {
        switch (bExpr->op) {
            case LT:
                return builder->CreateICmpSLT(this->stringCmp(left, right), createConstantInt(0));
            case GT:
                return builder->CreateICmpSGT(this->stringCmp(left, right), createConstantInt(0));
            case LTE:
                return builder->CreateICmpSLE(this->stringCmp(left, right), createConstantInt(0));
            case GTE:
                return builder->CreateICmpSGE(this->stringCmp(left, right), createConstantInt(0));
            case EQ:
                return builder->CreateICmpEQ(this->stringCmp(left, right), createConstantInt(0));
            case NEQ:
                return builder->CreateICmpNE(this->stringCmp(left, right), createConstantInt(0));
            default:
                std::cout << "Unsupported string binary operator" << std::endl;
        }
    }
    std::cout << "Error: Unsupported double binary expr op " << bExpr->op << std::endl;
    return this->createConstantBool(false);
}


Value *LLVMCodeGen::parseUnaryExpr(UnaryExpr *uExpr, std::map<std::string, Value *> &args)
{
    Value *val = this->parseExpr(uExpr, args);
    switch (uExpr->op) {
        case NOT:
            return builder->CreateNot(val, "logical_not");
        default:
            // ignore the unary operator if it is invalid
            return val;
    }
}


// Create a LLVM Function that returns the value of a conditional (IF/CASE)
Function *LLVMCodeGen::createConditional(DataType retType, Expr *cond, Expr *ifTrue, Expr *ifFalse)
{
    std::vector<Type*> args;
    // args contains the types of the arguments
    // args[2 * i] contains the type of the ith argument (where 0 <= i < datatypes.size())
    // args[2 * i+1] contains the boolean type, as argument with index 2i+1 contains whether argument i is null
    // args[2 * datatypes.size()] contains the type of the current row number (int32_t)
    args.reserve(2 * datatypes->size() + 1);
    for (int32_t i = 0; i < datatypes->size(); i++) {
        DataType type = datatypes->at(i);
        args.push_back(this->toLLVMType(type));
        args.push_back(Type::getInt1Ty(*context));
    }
    // Push back type for current row index
    args.push_back(Type::getInt32Ty(*context));

    Type *retTypePtr = this->toLLVMType(retType);
    FunctionType *prototype = FunctionType::get(retTypePtr, args, false);
    Function *func = Function::Create(prototype, Function::ExternalLinkage, "IF_CONDITIONAL", module.get());

    int32_t idx = 0;
    for (auto &arg : func->args()) {
        if (idx == datatypes->size() * MULTIPLES) arg.setName("rowIdx");
        else {
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

    std::map<std::string, Value*> fArgs;
    for (auto &arg : func->args()) {
        fArgs[arg.getName().str()] = &arg;
    }

    builder->SetInsertPoint(conditionalCheck);
    Value *evCond = this->parseExpr(cond, fArgs);
    Value *evTrue = this->parseExpr(ifTrue, fArgs);
    Value *evFalse = this->parseExpr(ifFalse, fArgs);
    // If cond evalutes to true, control flow goes to trueBlock, returning evTrue
    // Otherwise goes to falseBlock and returns evFalse
    builder->CreateCondBr(evCond, trueBlock, falseBlock);
    builder->SetInsertPoint(trueBlock);
    builder->CreateRet(evTrue);
    builder->SetInsertPoint(falseBlock);
    builder->CreateRet(evFalse);

    llvm::verifyFunction(*func);
    return func;
}


Value *LLVMCodeGen::parseIfExpr(IfExpr *ie, std::map<std::string, Value *> &args)
{
    BasicBlock *currentBlock = builder->GetInsertBlock();
    DataType retType = ie->trueExpr->GetExprDataType();
    Expr *cond = ie->condition;
    Expr *ifTrue = ie->trueExpr;
    Expr *ifFalse = ie->falseExpr;

    Function *conditionalFunc = createConditional(retType, cond, ifTrue, ifFalse);
    builder->SetInsertPoint(currentBlock);
    std::vector<Value*> passArgs;
    for (std::map<std::string, Value *>::iterator i = args.begin(); i != args.end(); i++) {
        Value *a = i->second;
        passArgs.push_back(a);
    }
    CallInst *condCall = builder->CreateCall(conditionalFunc, passArgs, "EVAL_IF");
    return condCall;
}


Value *LLVMCodeGen::parseInExpr(InExpr *iExpr, std::map<std::string, Value *> &args)
{
    Expr *toCompare = iExpr->arguments[0];
    llvm::Value *val = this->parseExpr(toCompare, args);

    llvm::Value *inArray = llvm::ConstantInt::get(*context, APInt(1, 0));
    // Handle types correctly
    for (int i = 1; i < iExpr->arguments.size(); i++) {
        // initialize tmpCmp
        llvm::Value *tmpCmp = this->createConstantBool(false);

        if (toCompare->GetExprDataType() == DataType::INT64D &&
            iExpr->arguments[i]->GetExprDataType() == DataType::INT32D) {
            Value *argInt64 = builder->CreateIntCast(parseExpr(iExpr->arguments[i], args), Type::getInt64Ty(*context),
                                                     true);
            tmpCmp = builder->CreateICmpEQ(val, argInt64);
        } else if (toCompare->GetExprDataType() == DataType::INT32D &&
                   iExpr->arguments[i]->GetExprDataType() == DataType::INT64D) {
            Value *valInt64 = builder->CreateIntCast(val, Type::getInt64Ty(*context), true);
            tmpCmp = builder->CreateICmpEQ(valInt64, parseExpr(iExpr->arguments[i], args));
        } else {
            switch (iExpr->arguments[0]->dataType) {
                case INT32D: {
                    tmpCmp = builder->CreateICmpEQ(val, parseExpr(iExpr->arguments[i], args));
                    break;
                }
                case INT64D: {
                    tmpCmp = builder->CreateICmpEQ(val, parseExpr(iExpr->arguments[i], args));
                    break;
                }
                case DOUBLED: {
                    tmpCmp = builder->CreateFCmpOEQ(val, parseExpr(iExpr->arguments[i], args));
                    break;
                }
                case STRINGD: {
                    tmpCmp = builder->CreateICmpEQ(this->stringCmp(val, parseExpr(iExpr->arguments[i], args)),
                                                   createConstantInt(0));
                    break;
                }
                default: {
                    std::cout << "Unsupported data type in IN expr" << std::endl;
                    tmpCmp = this->createConstantBool(false);
                }
            }
        }

        inArray = builder->CreateOr(inArray, tmpCmp);
    }
    return inArray;
}


Value *LLVMCodeGen::parseBetweenExpr(BetweenExpr *bExpr, std::map<std::string, Value *> &args)
{

    DataType biggerType = std::max(std::max(bExpr->lowerBound->GetExprDataType(),
                                            bExpr->upperBound->GetExprDataType()),
                                   bExpr->value->GetExprDataType());
    bExpr->lowerBound->dataType = biggerType;
    bExpr->upperBound->dataType = biggerType;
    bExpr->value->dataType = biggerType;

    llvm::Value *val = parseExpr(bExpr->value, args);
    llvm::Value *lowerVal = parseExpr(bExpr->lowerBound, args);
    llvm::Value *upperVal = parseExpr(bExpr->upperBound, args);

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
        llvm::Value *cmpleft = builder->CreateICmpSLE(this->stringCmp(lowerVal, val), createConstantInt(0));
        llvm::Value *cmpright = builder->CreateICmpSLE(this->stringCmp(val, upperVal), createConstantInt(0));
        llvm::Value *result = builder->CreateAnd(cmpleft, cmpright, "between_and");
        return result;
    }

    std::cout << "Error: unsupported data type for between" << std::endl;
    return this->createConstantBool(false);
}


// Create a LLVM Function which returns the result of a COALESCE
Function *LLVMCodeGen::createCoalesceFunc(DataType retType, DataExpr *dExpr1, Expr *value2Expr)
{
    std::vector < Type* > args;
    // args contains the types of the arguments
    // args[2 * i] contains the type of the ith argument (where 0 <= i < datatypes.size())
    // args[2 * i+1] contains the boolean type, as argument with index 2i+1 contains whether argument i is null
    // args[2 * datatypes.size()] contains the type of the current row number (int32_t)
    args.reserve(2 * datatypes->size() + 1);
    for (int32_t i = 0; i < datatypes->size(); i++) {
        DataType type = datatypes->at(i);
        args.push_back(this->toLLVMType(type));
        args.push_back(Type::getInt1Ty(*context));
    }
    // Push back type for current row index
    args.push_back(Type::getInt32Ty(*context));

    Type *retTypePtr = this->toLLVMType(retType);
    FunctionType *prototype = FunctionType::get(retTypePtr, args, false);
    Function *func = Function::Create(prototype, Function::ExternalLinkage, "COALESCE_CONDITIONAL", module.get());

    int32_t idx = 0;
    for (auto &arg : func->args()) {
        if (idx == datatypes->size() * MULTIPLES) arg.setName("rowIdx");
        else {
            if (idx % IS_EVEN == 0) {
                arg.setName(std::to_string(idx / MULTIPLES));
            } else {
                arg.setName(std::to_string(idx / MULTIPLES) + "_isNull");
            }
        }
        idx++;
    }

    BasicBlock *conditionalCheck = BasicBlock::Create(*context, "IS_NULL", func);
    BasicBlock *trueBlock = BasicBlock::Create(*context, "NOTNULL_BLOCK", func);
    BasicBlock *falseBlock = BasicBlock::Create(*context, "NULL_BLOCK", func);

    std::map<std::string, Value*> fArgs;
    for (auto &arg : func->args()) {
        fArgs[arg.getName().str()] = &arg;
    }

    builder->SetInsertPoint(conditionalCheck);

    Value *value1 = this->parseExpr(dExpr1, fArgs);
    Value *value2 = this->parseExpr(value2Expr, fArgs);
    if (value2Expr->GetExprDataType() == INT32D && dExpr1->GetExprDataType() == INT64D) {
        value2 = builder->CreateIntCast(value2, Type::getInt64Ty(*context), true);
    }
    if ((value2Expr->GetExprDataType() == INT32D || value2Expr->GetExprDataType() == INT64D)
        && dExpr1->GetExprDataType() == DOUBLED) {
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

Value *LLVMCodeGen::parseCoalesceExpr(CoalesceExpr *cExpr, std::map<std::string, Value *> &args)
{
    Expr *value1Expr = cExpr->value1;
    Expr *value2Expr = cExpr->value2;

    // Only case that coalesce is used seems to be with a column as first argument
    if (value1Expr->GetType() == ExprType::DATA_E) {
        DataExpr *dExpr1 = dynamic_cast<DataExpr *>(value1Expr);
        if (dExpr1->isColumn) {
            BasicBlock *currentBlock = builder->GetInsertBlock();


            DataType retType = value1Expr->GetExprDataType();
            Function *coalesceFunc = createCoalesceFunc(retType, dExpr1, cExpr->value2);
            builder->SetInsertPoint(currentBlock);

            std::vector < Value* > passArgs;
            for (std::map<std::string, Value *>::iterator i = args.begin(); i != args.end(); i++) {
                Value *a = i->second;
                passArgs.push_back(a);
            }

            CallInst *condCall = builder->CreateCall(coalesceFunc, passArgs, "EVAL_COALESCE");
            return condCall;
        }
    }
    // If the first argument is not a column DataExpr, just return it
    return this->parseExpr(value1Expr, args);
}

// Handles all functions
// Only calls them; registration is done in function registry
Value *LLVMCodeGen::parseFuncExpr(FuncExpr *fExpr, std::map<std::string, Value *> &args)
{

    if (fExpr->funcName == "abs") {

        switch (fExpr->dataType) {
            case (DataType::INT32D): {
                std::vector<Value*> argVals{this->parseExpr(fExpr->arguments[0], args)};
                auto f = module->getFunction(abs_int32_str);
                Value *ret = builder->CreateCall(f, argVals, abs_int32_str);
                return ret;
            }
            case (DataType::INT64D): {
                std::vector<Value*> argVals{this->parseExpr(fExpr->arguments[0], args)};
                auto f = module->getFunction(abs_int64_str);
                Value *ret = builder->CreateCall(f, argVals, abs_int64_str);
                return ret;
            }
            case (DataType::DOUBLED): {
                std::vector<Value*> argVals{parseExpr(fExpr->arguments[0], args)};
                auto f = module->getFunction(abs_double_str);
                Value *ret = builder->CreateCall(f, argVals, abs_double_str);
                return ret;
            }
            default: {
                std::cout << "Unsupported data type for function " << fExpr->funcName << std::endl;
            }
        }
    }
    if (fExpr->funcName == "substr" && fExpr->arguments.size() == FEXPR_VALUE3) {
        Value *str = parseExpr(fExpr->arguments[0], args);
        Value *startIdx = parseExpr(fExpr->arguments[1], args);
        Value *length = parseExpr(fExpr->arguments[LENGTH_LOC], args);
        std::vector<Value*> argVals{str, startIdx, length};

        auto f = module->getFunction(substrExt_str);
        Value *ret = builder->CreateCall(f, argVals, substrExt_str);
        return ret;
    }
    if (fExpr->funcName == "substr" && fExpr->arguments.size() == FEXPR_VALUE2) {
        Value *str = parseExpr(fExpr->arguments[0], args);
        Value *startIdx = parseExpr(fExpr->arguments[1], args);
        std::vector<Value*> argVals{str, startIdx};

        auto f = module->getFunction(substrWithStartExt_str);
        Value *ret = builder->CreateCall(f, argVals, substrWithStartExt_str);
        return ret;
    }
    if (fExpr->funcName == "concat") {
        Value *str1 = parseExpr(fExpr->arguments[0], args);
        Value *str2 = parseExpr(fExpr->arguments[1], args);
        std::vector<Value*> argVals{str1, str2};

        auto f = module->getFunction(concatStrExt_str);
        Value *ret = builder->CreateCall(f, argVals, concatStrExt_str);
        return ret;
    }
    if (fExpr->funcName == "CAST") {
        // Simply cast from int or long to double (how it appears in tpch)
        llvm::Value *val = parseExpr(fExpr->arguments[0], args);
        std::vector<Value*> argVals{val};
        switch (fExpr->arguments[0]->dataType) {
            case DataType::INT32D: {
                auto f = module->getFunction(cast_int32_str);
                Value *ret = builder->CreateCall(f, argVals, cast_int32_str);
                return ret;
            }
            case DataType::INT64D: {
                auto f = module->getFunction(cast_int64_str);
                Value *ret = builder->CreateCall(f, argVals, cast_int64_str);
                return ret;
            }
            case DataType::STRINGD: {
                auto f = module->getFunction(cast_string_str);
                Value *ret = builder->CreateCall(f, argVals, cast_string_str);
                return ret;
            }
            default: {
                std::cout << "Unsupported datatype in CAST: " << fExpr->arguments[0]->dataType << std::endl;
            }
        }
    }
    if (fExpr->funcName == "LIKE") {
        Value *str1 = parseExpr(fExpr->arguments[0], args);
        Value *str2 = parseExpr(fExpr->arguments[1], args);
        std::vector<Value*> argVals{str1, str2};

        auto f = module->getFunction(likeExt_str);
        Value *ret = builder->CreateCall(f, argVals, likeExt_str);
        return ret;
    }
    if (fExpr->funcName == "combine_hash") {
        Value *prevHashVal = parseExpr(fExpr->arguments[0], args);
        Value *val = parseExpr(fExpr->arguments[1], args);

        if (fExpr->arguments[0]->GetExprDataType() == INT32D) {
            prevHashVal = builder->CreateIntCast(prevHashVal, Type::getInt64Ty(*context), true);
        }
        if (fExpr->arguments[1]->GetExprDataType() == INT32D) {
            val = builder->CreateIntCast(val, Type::getInt64Ty(*context), true);
        }


        std::vector<Value*> argVals{prevHashVal, val};

        auto f = module->getFunction(combine_hash_str);
        Value *ret = builder->CreateCall(f, argVals, combine_hash_str);
        dumpCode();
        return ret;
    }
    // external functions
    if (isFunctionName(fExpr->funcName)) {
        FunctionSignature *fs = funcNameToSignature[fExpr->funcName];

        std::vector<Value*> argVals;
        for (int i = 0; i < fExpr->arguments.size(); i++) {
            // Cast arguments to the correct type
            DataType desiredType = fs->GetParams()[i];
            DataType currType = fExpr->arguments[i]->GetExprDataType();
            if (desiredType == DOUBLED && (currType == INT32D || currType == INT64D)) {
                Value *argDouble = builder->CreateCast(Instruction::SIToFP, parseExpr(fExpr->arguments[i], args),
                                                       Type::getDoubleTy(*context));
                argVals.push_back(argDouble);
            } else if (desiredType == INT64D && currType == INT32D) {
                Value *argInt64 = builder->CreateIntCast(parseExpr(fExpr->arguments[i], args),
                                                         Type::getInt64Ty(*context), true);
                argVals.push_back(argInt64);
            } else {
                argVals.push_back(parseExpr(fExpr->arguments[i], args));
            }
        }
        // Assume that the function name of the fExpr and in the module are matching
        auto f = module->getFunction(fExpr->funcName);
        Value *ret = builder->CreateCall(f, argVals, "call_" + fExpr->funcName);
        return ret;
    }

    std::cout << "No function found with name " << fExpr->funcName << std::endl;
    return createConstantInt(0);
}


// Main codegen for any type of expression
Value *LLVMCodeGen::parseExpr(Expr *root, std::map<std::string, Value *> &args)
{
    switch (root->GetType()) {
        case ExprType::DATA_E: {
            DataExpr *dEx = dynamic_cast<DataExpr *>(root);
            return this->parseDataExpr(dEx, args);
        }

        case ExprType::BINARY_E: {
            BinaryExpr *bExpr = dynamic_cast<BinaryExpr *>(root);
            return this->parseBinaryExpr(bExpr, args);
        }

        case ExprType::UNARY_E: {
            UnaryExpr *uExpr = dynamic_cast<UnaryExpr *>(root);
            return this->parseUnaryExpr(uExpr, args);
        }

        case ExprType::IF_E: {
            IfExpr *ie = dynamic_cast<IfExpr *>(root);
            return this->parseIfExpr(ie, args);
        }

        case ExprType::IN_E: {
            InExpr *iExpr = dynamic_cast<InExpr *>(root);
            return this->parseInExpr(iExpr, args);
        }

        case ExprType::BETWEEN_E: {
            BetweenExpr *bExpr = dynamic_cast<BetweenExpr *>(root);
            return this->parseBetweenExpr(bExpr, args);
        }

        case ExprType::COALESCE_E: {
            CoalesceExpr *cExpr = dynamic_cast<CoalesceExpr *>(root);
            return this->parseCoalesceExpr(cExpr, args);
        }

        case ExprType::FUNC_E: {
            FuncExpr *fExpr = dynamic_cast<FuncExpr *>(root);
            return this->parseFuncExpr(fExpr, args);
        }

        default: {
            std::cout << "Error: Unsupported expr type " << root->GetType() << std::endl;
            return this->createConstantBool(false);
        }
    }
}

std::string LLVMCodeGen::dumpCode()
{
    std::string ir;
    llvm::raw_string_ostream stream(ir);
    module->print(stream, nullptr);
    std::cout << " Generated code::" << ir;
    return ir;
}

void addOptimizationPasses(legacy::FunctionPassManager *FPM, llvm::legacy::PassManager &MPM)
{
    FPM->add(createSCCPPass());
    FPM->add(createNewGVNPass());
    FPM->add(createInductiveRangeCheckEliminationPass());
    FPM->add(createIndVarSimplifyPass());

    FPM->add(createLICMPass());
    FPM->add(createLoopUnrollPass());
    FPM->add(createLoopUnswitchPass());

    FPM->add(createLoopLoadEliminationPass());
    FPM->add(createInductiveRangeCheckEliminationPass());
    FPM->add(createIndVarSimplifyPass());
    FPM->add(createLoopInstSimplifyPass());
    FPM->add(createLoopSimplifyCFGPass());
    FPM->add(createMergedLoadStoreMotionPass());
    FPM->add(createMergeICmpsLegacyPass());
    FPM->add(createAggressiveDCEPass());
    FPM->add(createDeadStoreEliminationPass());

    MPM.add(createFunctionInliningPass());
    MPM.add(createPruneEHPass());
}

Function *LLVMCodeGen::createFunction()
{
    std::vector<Type*> args;
    args.reserve(MULTIPLES * datatypes->size() + 1);
    // Values in args vector follow the format:
    // #0, #0_isNull, #1, #1_isNull, ..., #4, #4_isNull, rowIdx
    for (int32_t i = 0; i < datatypes->size(); i++) {
        DataType type = datatypes->at(i);
        args.push_back(this->toLLVMType(type));
        args.push_back(Type::getInt1Ty(*context));
    }
    // Push back type for current row index
    args.push_back(Type::getInt32Ty(*context));

    std::cout << "exprtree: ";
    expr->PrintExprTree();
    std::cout << std::endl;
    FunctionType *prototype = FunctionType::get(this->toLLVMType(expr->GetExprDataType()), args, false);
    Function *func = Function::Create(prototype, Function::ExternalLinkage, funcName, module.get());

    int32_t idx = 0;
    for (auto &arg : func->args()) {
        if (idx == datatypes->size() * MULTIPLES) arg.setName("rowIdx");
        else {
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
    std::map<std::string, Value*> fArgs;
    for (auto &arg : func->args()) {
        fArgs[arg.getName().str()] = &arg;
    }

    Value *ret = this->parseExpr(expr, fArgs);
    builder->CreateRet(ret);
    verifyFunction(*func);
    auto FPM = std::make_unique<legacy::FunctionPassManager>(module.get());
    llvm::legacy::PassManager MPM;
    addOptimizationPasses(FPM.get(), MPM);
    FPM->doInitialization();
    for (auto &F : *module) FPM->run(F);
    MPM.run(*module);
    return func;
}
