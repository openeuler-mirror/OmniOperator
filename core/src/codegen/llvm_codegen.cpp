#include "llvm_codegen.h"
#include "llvm/IR/LegacyPassManager.h"
#include "llvm/Transforms/Scalar.h"
#include "llvm/Transforms/IPO.h"
#include <chrono>


using namespace llvm;
using namespace orc;
using namespace omniruntime::expressions;


Value* LLVMCodeGen::createConstantBool(bool v) {
    return ConstantInt::get(*context, APInt(1, v));
}

Value* LLVMCodeGen::createConstantInt(int32_t v) {
    return ConstantInt::get(*context, APInt(32, v, true));
}

Value* LLVMCodeGen::createConstantLong(int64_t v) {
    return ConstantInt::get(*context, APInt(64, v, true));
}

Value* LLVMCodeGen::createConstantDouble(double v) {
    return ConstantFP::get(*context, APFloat(v));
}


Type* LLVMCodeGen::toLLVMType(DataType t) {
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
            cout << "Error: Unknown argument datatype " << t << endl;
            return nullptr;
    }
}

// Tells whether or not a function is a valid name
bool isFunctionName(string fnName) {
    // TODO: either use a global string set to contain function names, or get function names from _module
    return true;
}


LLVMCodeGen::LLVMCodeGen(std::string name, Expr *expr, vector<DataType>* datatypes)
{
    _func_name = name;
    _expr = expr;
    this->datatypes = datatypes;
    llvm::InitializeNativeTarget();
    llvm::InitializeNativeTargetAsmPrinter();
    llvm::InitializeNativeTargetAsmParser();
    // llvm::InitializeNativeTargetDisassembler();
    JIT = EOE(LLJITBuilder().create());
    
    context = make_unique<LLVMContext>();
    // Create module called the_module
    _module = make_unique<Module>("the_module", *context);
    _module->setDataLayout(JIT->getDataLayout());
    // Create IR builder to create IR instructinos
    builder = make_unique<IRBuilder<>>(*context);

    // Time function registering process
    auto start = std::chrono::steady_clock::now();
    FR = new FunctionRegistry(JIT, context, _module);
    // Only register the necessary functions for the expression
    // Necessary functions are found using requiredFunctions method
    FR->initNecessary(requiredFunctions(_expr));
    auto end = std::chrono::steady_clock::now();
    std::chrono::duration<double> frTime = end - start;
    #ifdef DEBUG
    std::cout << "Time to register functions: " << frTime.count() << "seconds" << std::endl;
    #endif

    funcNameToSignature = FR->funcNameToSignatureMap;

}

LLVMCodeGen::~LLVMCodeGen() {
    EOE(rt->remove());
    delete this->datatypes;
    delete FR;
}


// Goes through the expression tree and determines which functions need to be registered
// Helper function for requiredFunctions method
void LLVMCodeGen::requiredFunctionsHelper(Expr* expr, set<string>& s) {
    // For all types of Expr except for FuncExpr, recurse on the children
    switch (expr->getType()) {
        case ExprType::DATA_E: {
            return;
        }

        case ExprType::BINARY_E: {
            BinaryExpr* bExpr = (BinaryExpr*) expr;
            requiredFunctionsHelper(bExpr->left, s);
            requiredFunctionsHelper(bExpr->right, s);
            return;
        }

        case ExprType::UNARY_E: {
            UnaryExpr* uExpr = (UnaryExpr*) expr;
            requiredFunctionsHelper(uExpr->exp, s);
            return;
        }

        case ExprType::IF_E: {
            IfExpr* ifExpr = (IfExpr*) expr;
            requiredFunctionsHelper(ifExpr->condition, s);
            requiredFunctionsHelper(ifExpr->trueExpr, s);
            requiredFunctionsHelper(ifExpr->falseExpr, s);
            return;
        }

        case ExprType::IN_E: {
            InExpr* inExpr = (InExpr*) expr;
            for (auto arg : inExpr->arguments) {
                requiredFunctionsHelper(arg, s);
            }
            return;
        }

        case ExprType::BETWEEN_E: {
            BetweenExpr* bExpr = (BetweenExpr*) expr;
            requiredFunctionsHelper(bExpr->value, s);
            requiredFunctionsHelper(bExpr->lowerBound, s);
            requiredFunctionsHelper(bExpr->upperBound, s);
            return;
        }

        case ExprType::COALESCE_E: {
            CoalesceExpr* cExpr = (CoalesceExpr*) expr;
            requiredFunctionsHelper(cExpr->value1, s);
            requiredFunctionsHelper(cExpr->value2, s);
            return;
        }

        // Add the name of the required extern function
        case ExprType::FUNC_E: {
            FuncExpr* fExpr = (FuncExpr*) expr;
            string fn = fExpr->funcName;

            if (fn == "CAST" || fn == "abs") {
                s.insert(fn + "_" + dataTypeString(fExpr->arguments[0]->getExprDataType()));
            }
            else if (fn == "substr") {
                if (fExpr->arguments.size() == 2) {
                    s.insert("substrWithStartExt");
                }
                if (fExpr->arguments.size() == 3) {
                    s.insert("substrExt");
                }

            }
            else {
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
set<string> LLVMCodeGen::requiredFunctions(Expr* expr) {
    set<string> ret;
    requiredFunctionsHelper(expr, ret);
    return ret;
}


// Other operations which require externed functions
Value* LLVMCodeGen::stringCmp(Value *LHS, Value *RHS) {
    // call function
    vector<Value*> argVals {LHS, RHS};
    auto f = _module->getFunction("strCompareExt");
    Value *ret = builder->CreateCall(f, argVals, "call_str_cmp");
    return ret;
}


Value* LLVMCodeGen::parseDataExpr(DataExpr* dEx, map<string, Value*>& args) {
    if (dEx->isColumn) {
        return args[to_string(dEx->colVal)];
    }
    switch (dEx->getExprDataType()) {
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
            const char* s = dEx->stringVal->c_str();

            Constant *addr = ConstantInt::get(*context, APInt(64, (int64_t)(s)));
            return addr;
        }case DataType::BOOLD: {
            return this->createConstantBool(dEx->boolVal);
        }
        default: {
            std::cout << "Unsupported data type in Data Expr" << endl;
            return this->createConstantBool(false);
        }
    }
}

Value* LLVMCodeGen::parseBinaryExpr(BinaryExpr* bExpr, map<string, Value*>& args) {
    if (bExpr->left->getType() == ExprType::DATA_E || bExpr->right->getType() == ExprType::DATA_E) {
        DataType biggerType = std::max(bExpr->left->getExprDataType(), bExpr->right->getExprDataType());
        bExpr->left->dataType = biggerType;
        bExpr->right->dataType = biggerType;
    }
    Value* left = this->parseExpr(bExpr->left, args);
    Value* right = this->parseExpr(bExpr->right, args);


    if (bExpr->op == omniruntime::expressions::Operator::AND) {
        return builder->CreateAnd(left, right, "logical_and");
    }
    if (bExpr->op == omniruntime::expressions::Operator::OR) {
        return builder->CreateOr(left, right, "logical_or");
    }

    if (bExpr->left->getExprDataType() == DataType::INT32D || bExpr->left->getExprDataType() == DataType::INT64D) {
        switch(bExpr->op) {
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
            std::cout << "Error: Binary operator not supported " << bExpr->op << endl;
        }
    }
    else if (bExpr->left->getExprDataType() == DOUBLED) {
        switch(bExpr->op) {
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
            std::cout << "Unsupported double binary operator" << endl;
            break;
        }
    }
    else if (bExpr->left->getExprDataType() == STRINGD) {
        switch(bExpr->op) {
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
                std::cout << "Unsupported string binary operator" << endl;
        }
    }
    std::cout << "Error: Unsupported double binary expr op " << bExpr->op << endl;
    return this->createConstantBool(false);
}


Value* LLVMCodeGen::parseUnaryExpr(UnaryExpr* uExpr, map<string, Value*>& args){
    Value* val = this->parseExpr(uExpr, args);
    switch(uExpr->op) {
        case NOT:
            return builder->CreateNot(val, "logical_not");
        default:
            // ignore the unary operator if it is invalid
            return val;
    }
}


// Create a LLVM Function that returns the value of a conditional (IF/CASE)
Function* LLVMCodeGen::createConditional(DataType retType, Expr* cond, Expr* ifTrue, Expr* ifFalse) {
    vector<Type*> args;
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

    Type* retTypePtr = this->toLLVMType(retType);
    FunctionType *prototype = FunctionType::get(retTypePtr, args, false);
    Function *func = Function::Create(prototype, Function::ExternalLinkage, "IF_CONDITIONAL", _module.get());

    int32_t idx = 0;
    for (auto& arg : func->args()) {
        if (idx == datatypes->size() * 2) arg.setName("rowIdx");
        else {
            if (idx % 2 == 0) {
                arg.setName(to_string(idx / 2));
            }
            else {
                // Example: the argument representing whether the value in column 3 is null has name "3_isNull"
                arg.setName(to_string(idx / 2) + "_isNull");
            }
        }
        idx++;
    }

    BasicBlock* conditionalCheck = BasicBlock::Create(*context, "CONDITIONAL_CHECK", func);
    BasicBlock* trueBlock = BasicBlock::Create(*context, "TRUE_BLOCK", func);
    BasicBlock* falseBlock = BasicBlock::Create(*context, "FALSE_BLOCK", func);

    map<string, Value*> fArgs;
    for (auto& arg : func->args()) {
        fArgs[arg.getName().str()] = &arg;
    }

    builder->SetInsertPoint(conditionalCheck);
    Value* evCond = this->parseExpr(cond, fArgs);
    Value* evTrue = this->parseExpr(ifTrue, fArgs);
    Value* evFalse = this->parseExpr(ifFalse, fArgs);
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


Value* LLVMCodeGen::parseIfExpr(IfExpr* ie, map<string, Value*>& args) {
    BasicBlock* currentBlock = builder->GetInsertBlock();
    DataType retType = ie->trueExpr->getExprDataType();
    Expr* cond = ie->condition;
    Expr* ifTrue = ie->trueExpr;
    Expr* ifFalse = ie->falseExpr;

    Function* conditionalFunc = createConditional(retType, cond, ifTrue, ifFalse);
    builder->SetInsertPoint(currentBlock);
    vector<Value*> passArgs;
    for (map<string, Value*>::iterator i = args.begin() ; i != args.end() ; i ++ ) {
        Value* a = i->second;
        passArgs.push_back(a);
    }
    CallInst* condCall = builder->CreateCall(conditionalFunc, passArgs, "EVAL_IF");
    return condCall;
}


Value* LLVMCodeGen::parseInExpr(InExpr* iExpr, map<string, Value*>& args) {
    Expr* toCompare = iExpr->arguments[0];
    llvm::Value* val = this->parseExpr(toCompare, args);

    llvm::Value *inArray = llvm::ConstantInt::get(*context, APInt(1, 0));
    // Handle types correctly
    for (int i = 1; i < iExpr->arguments.size(); i++) {
        // initialize tmpCmp
        llvm::Value *tmpCmp = this->createConstantBool(false);
        
        if (toCompare->getExprDataType() == DataType::INT64D && iExpr->arguments[i]->getExprDataType() == DataType::INT32D) {
            Value* argInt64 = builder->CreateIntCast(parseExpr(iExpr->arguments[i], args), Type::getInt64Ty(*context), true);
            tmpCmp = builder->CreateICmpEQ(val, argInt64);
        }
        else if (toCompare->getExprDataType() == DataType::INT32D && iExpr->arguments[i]->getExprDataType() == DataType::INT64D) {
            Value* valInt64 = builder->CreateIntCast(val, Type::getInt64Ty(*context), true);
            tmpCmp = builder->CreateICmpEQ(valInt64, parseExpr(iExpr->arguments[i], args));
        }
        else {
            switch(iExpr->arguments[0]->dataType) {
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
                case STRINGD:{
                    tmpCmp = builder->CreateICmpEQ(this->stringCmp(val, parseExpr(iExpr->arguments[i], args)), createConstantInt(0));
                    break;
                }
                default: {
                    std::cout << "Unsupported data type in IN expr" << endl;
                    tmpCmp = this->createConstantBool(false);
                }
            }
        }

        inArray = builder->CreateOr(inArray, tmpCmp);
    }
    return inArray;
}


Value* LLVMCodeGen::parseBetweenExpr(BetweenExpr* bExpr, map<string, Value*>& args) {

    DataType biggerType = std::max(std::max(bExpr->lowerBound->getExprDataType(),
                                            bExpr->upperBound->getExprDataType()),
                                   bExpr->value->getExprDataType());
    bExpr->lowerBound->dataType = biggerType;
    bExpr->upperBound->dataType = biggerType;
    bExpr->value->dataType = biggerType;

    llvm::Value *val = parseExpr(bExpr->value, args);
    llvm::Value *lowerVal = parseExpr(bExpr->lowerBound, args);
    llvm::Value *upperVal = parseExpr(bExpr->upperBound, args);

    if (bExpr->value->getExprDataType() == DataType::INT32D || bExpr->value->getExprDataType() == DataType::INT64D) {
        llvm::Value *cmpleft = builder->CreateICmpSLE(lowerVal, val, "between_cmpleft");
        llvm::Value *cmpright = builder->CreateICmpSLE(val, upperVal, "between_cmpright");
        llvm::Value *result = builder->CreateAnd(cmpleft, cmpright, "between_and");
        return result;
    }
    else if (bExpr->value->getExprDataType() == DOUBLED) {
        llvm::Value *cmpleft = builder->CreateFCmpULE(lowerVal, val, "between_cmpleft");
        llvm::Value *cmpright = builder->CreateFCmpULE(val, upperVal, "between_cmpright");
        llvm::Value *result = builder->CreateAnd(cmpleft, cmpright, "between_and");
        return result;
    }
    else if (bExpr->value->getExprDataType() == STRINGD) {
        llvm::Value *cmpleft = builder->CreateICmpSLE(this->stringCmp(lowerVal, val), createConstantInt(0));
        llvm::Value *cmpright = builder->CreateICmpSLE(this->stringCmp(val, upperVal), createConstantInt(0));
        llvm::Value *result = builder->CreateAnd(cmpleft, cmpright, "between_and");
        return result;
    }

    std::cout << "Error: unsupported data type for between" << std::endl;
    return this->createConstantBool(false);
}


// Create a LLVM Function which returns the result of a COALESCE
Function* LLVMCodeGen::createCoalesceFunc(DataType retType, DataExpr* dExpr1, Expr* value2Expr) {
    vector<Type*> args;
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

    Type* retTypePtr = this->toLLVMType(retType);
    FunctionType *prototype = FunctionType::get(retTypePtr, args, false);
    Function *func = Function::Create(prototype, Function::ExternalLinkage, "COALESCE_CONDITIONAL", _module.get());

    int32_t idx = 0;
    for (auto& arg : func->args()) {
        if (idx == datatypes->size() * 2) arg.setName("rowIdx");
        else {
            if (idx % 2 == 0) {
                arg.setName(to_string(idx / 2));
            }
            else {
                arg.setName(to_string(idx / 2) + "_isNull");
            }
        }
        idx++;
    }

    BasicBlock* conditionalCheck = BasicBlock::Create(*context, "IS_NULL", func);
    BasicBlock* trueBlock = BasicBlock::Create(*context, "NOTNULL_BLOCK", func);
    BasicBlock* falseBlock = BasicBlock::Create(*context, "NULL_BLOCK", func);

    map<string, Value*> fArgs;
    for (auto& arg : func->args()) {
        fArgs[arg.getName().str()] = &arg;
    }

    builder->SetInsertPoint(conditionalCheck);

    Value* value1 = this->parseExpr(dExpr1, fArgs);
    Value* value2 = this->parseExpr(value2Expr, fArgs);
    if (value2Expr->getExprDataType() == INT32D && dExpr1->getExprDataType() == INT64D) {
        value2 = builder->CreateIntCast(value2, Type::getInt64Ty(*context), true);
    }
    if ((value2Expr->getExprDataType() == INT32D || value2Expr->getExprDataType() == INT64D)
        && dExpr1->getExprDataType() == DOUBLED) {
        value2 = builder->CreateCast(Instruction::SIToFP, value2, Type::getDoubleTy(*context));
    }


    string colNullStr = to_string(dExpr1->colVal) + "_isNull";
    Value* isNull = fArgs[colNullStr];

    builder->CreateCondBr(isNull, trueBlock, falseBlock);
    builder->SetInsertPoint(trueBlock);
    builder->CreateRet(value2); // return second value if the first one is null
    builder->SetInsertPoint(falseBlock);
    builder->CreateRet(value1); // otherwise just return the first value

    llvm::verifyFunction(*func);
    return func;
}


Value* LLVMCodeGen::parseCoalesceExpr(CoalesceExpr* cExpr, map<string, Value*>& args) {
    Expr* value1Expr = cExpr->value1;
    Expr* value2Expr = cExpr->value2;

    // Only case that coalesce is used seems to be with a column as first argument
    if (value1Expr->getType() == ExprType::DATA_E) {
        DataExpr* dExpr1 = (DataExpr*)(value1Expr);
        if (dExpr1->isColumn) {
            BasicBlock* currentBlock = builder->GetInsertBlock();


            DataType retType = value1Expr->getExprDataType();
            Function* coalesceFunc = createCoalesceFunc(retType, dExpr1, cExpr->value2);
            builder->SetInsertPoint(currentBlock);

            vector<Value*> passArgs;
            for (map<string, Value*>::iterator i = args.begin() ; i != args.end() ; i ++ ) {
                Value* a = i->second;
                passArgs.push_back(a);
            }

            CallInst* condCall = builder->CreateCall(coalesceFunc, passArgs, "EVAL_COALESCE");
            return condCall;
        }
    }
    // If the first argument is not a column DataExpr, just return it
    return this->parseExpr(value1Expr, args);
}

// Handles all functions
// Only calls them; registration is done in function registry
Value* LLVMCodeGen::parseFuncExpr(FuncExpr* fExpr, map<string, Value*>& args) {

    if (fExpr->funcName == "abs") {

        switch (fExpr->dataType) {
            case (DataType::INT32D): {
                vector<Value*> argVals {this->parseExpr(fExpr->arguments[0], args)};
                auto f = _module->getFunction(abs_int32_str);
                Value *ret = builder->CreateCall(f, argVals, abs_int32_str);
                return ret;
            }
            case (DataType::INT64D): {
                vector<Value*> argVals {this->parseExpr(fExpr->arguments[0], args)};
                auto f = _module->getFunction(abs_int64_str);
                Value *ret = builder->CreateCall(f, argVals, abs_int64_str);
                return ret;
            }
            case (DataType::DOUBLED): {
                vector<Value*> argVals {parseExpr(fExpr->arguments[0], args)};
                auto f = _module->getFunction(abs_double_str);
                Value *ret = builder->CreateCall(f, argVals, abs_double_str);
                return ret;
            }
            default: {
                std::cout << "Unsupported data type for function " << fExpr->funcName << endl;
            }
        }
    }
    if (fExpr->funcName == "substr" && fExpr->arguments.size() == 3) {
        Value* str = parseExpr(fExpr->arguments[0], args);
        Value* startIdx = parseExpr(fExpr->arguments[1], args);
        Value* length = parseExpr(fExpr->arguments[2], args);
        vector<Value*> argVals {str, startIdx, length};

        auto f = _module->getFunction(substrExt_str);
        Value *ret = builder->CreateCall(f, argVals, substrExt_str);
        return ret;
    }
    if (fExpr->funcName == "substr" && fExpr->arguments.size() == 2) {
        Value* str = parseExpr(fExpr->arguments[0], args);
        Value* startIdx = parseExpr(fExpr->arguments[1], args);
        vector<Value*> argVals {str, startIdx};

        auto f = _module->getFunction(substrWithStartExt_str);
        Value *ret = builder->CreateCall(f, argVals, substrWithStartExt_str);
        return ret;
    }
    if (fExpr->funcName == "concat") {
        Value* str1 = parseExpr(fExpr->arguments[0], args);
        Value* str2 = parseExpr(fExpr->arguments[1], args);
        vector<Value*> argVals {str1, str2};

        auto f = _module->getFunction(concatStrExt_str);
        Value *ret = builder->CreateCall(f, argVals, concatStrExt_str);
        return ret;
    }
    if (fExpr->funcName == "CAST") {
        // Simply cast from int or long to double (how it appears in tpch)
        llvm::Value *val = parseExpr(fExpr->arguments[0], args);
        vector<Value*> argVals {val};
        switch (fExpr->arguments[0]->dataType) {
            case DataType::INT32D: {
                auto f = _module->getFunction(cast_int32_str);
                Value *ret = builder->CreateCall(f, argVals, cast_int32_str);
                return ret;
            }
            case DataType::INT64D: {
                auto f = _module->getFunction(cast_int64_str);
                Value *ret = builder->CreateCall(f, argVals, cast_int64_str);
                return ret;
            }
            case DataType::STRINGD: {
                auto f = _module->getFunction(cast_string_str);
                Value *ret = builder->CreateCall(f, argVals, cast_string_str);
                return ret;
            }
            default: {
                std::cout << "Unsupported datatype in CAST: " << fExpr->arguments[0]->dataType << endl;
            }
        }
    }
    if (fExpr->funcName == "LIKE") {
        Value* str1 = parseExpr(fExpr->arguments[0], args);
        Value* str2 = parseExpr(fExpr->arguments[1], args);
        vector<Value*> argVals {str1, str2};

        auto f = _module->getFunction(likeExt_str);
        Value *ret = builder->CreateCall(f, argVals, likeExt_str);
        return ret;
    }
    if (fExpr->funcName == "combine_hash") {
        Value* prevHashVal = parseExpr(fExpr->arguments[0], args);
        Value* val = parseExpr(fExpr->arguments[1], args);

        if (fExpr->arguments[0]->getExprDataType() == INT32D) {
            prevHashVal = builder->CreateIntCast(prevHashVal, Type::getInt64Ty(*context), true);
        }
        if (fExpr->arguments[1]->getExprDataType() == INT32D) {
            val = builder->CreateIntCast(val, Type::getInt64Ty(*context), true);
        }


        vector<Value*> argVals{prevHashVal, val};

        auto f = _module->getFunction(combine_hash_str);
        Value *ret = builder->CreateCall(f, argVals, combine_hash_str);
        dumpCode();
        return ret;
    }
    // external functions
    if (isFunctionName(fExpr->funcName)) {
        FunctionSignature* fs = funcNameToSignature[fExpr->funcName];

        vector<Value*> argVals;
        for (int i = 0; i < fExpr->arguments.size(); i++) {
            // Cast arguments to the correct type
            DataType desiredType = fs->getParams()[i];
            DataType currType = fExpr->arguments[i]->getExprDataType();
            if (desiredType == DOUBLED && (currType == INT32D || currType == INT64D)) {
                Value* argDouble = builder->CreateCast(Instruction::SIToFP, parseExpr(fExpr->arguments[i], args), Type::getDoubleTy(*context));
                argVals.push_back(argDouble);
            }
            else if (desiredType == INT64D && currType == INT32D) {
                Value* argInt64 = builder->CreateIntCast(parseExpr(fExpr->arguments[i], args), Type::getInt64Ty(*context), true);
                argVals.push_back(argInt64);
            }
            else {
                argVals.push_back(parseExpr(fExpr->arguments[i], args));
            }
        }
        // Assume that the function name of the fExpr and in the module are matching
        auto f = _module->getFunction(fExpr->funcName);
        Value* ret = builder->CreateCall(f, argVals, "call_" + fExpr->funcName);
        return ret;
    }

    cout << "No function found with name " << fExpr->funcName << endl;
    return createConstantInt(0);
}


// Main codegen for any type of expression
Value* LLVMCodeGen::parseExpr(Expr* root, map<string, Value*>& args) {
    switch (root->getType()) {
        case ExprType::DATA_E: {
            DataExpr* dEx = (DataExpr*) root;
            return this->parseDataExpr(dEx, args);
        }

        case ExprType::BINARY_E: {
            BinaryExpr* bExpr = (BinaryExpr*) root;
            return this->parseBinaryExpr(bExpr, args);
        }

        case ExprType::UNARY_E: {
            UnaryExpr* uExpr = (UnaryExpr*) root;
            return this->parseUnaryExpr(uExpr, args);
        }

        case ExprType::IF_E: {
            IfExpr* ie = (IfExpr*) root;
            return this->parseIfExpr(ie, args);
        }

        case ExprType::IN_E: {
            InExpr* iExpr = (InExpr*) root;
            return this->parseInExpr(iExpr, args);
        }

        case ExprType::BETWEEN_E: {
            BetweenExpr* bExpr = (BetweenExpr*) root;
            return this->parseBetweenExpr(bExpr, args);
        }

        case ExprType::COALESCE_E: {
            CoalesceExpr* cExpr = (CoalesceExpr*) root;
            return this->parseCoalesceExpr(cExpr, args);
        }
        
        case ExprType::FUNC_E: {
            FuncExpr* fExpr = (FuncExpr*) root;
            return this->parseFuncExpr(fExpr, args);
        }

        default: {
            std::cout << "Error: Unsupported expr type " << root->getType() << endl;
            return this->createConstantBool(false);
        }
    }
}

std::string LLVMCodeGen::dumpCode() {
  std::string ir;
  llvm::raw_string_ostream stream(ir);
  _module->print(stream, nullptr);
  cout<<" Generated code::" << ir;
  return ir;
}

void addOptimizationPasses(legacy::FunctionPassManager* FPM, llvm::legacy::PassManager& MPM) {
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

Function* LLVMCodeGen::createFunction() {
    vector<Type*> args;
    args.reserve(2 * datatypes->size() + 1);
    // Values in args vector follow the format: 
    // #0, #0_isNull, #1, #1_isNull, ..., #4, #4_isNull, rowIdx
    for (int32_t i = 0; i < datatypes->size(); i++) {
        DataType type = datatypes->at(i);
        args.push_back(this->toLLVMType(type));
        args.push_back(Type::getInt1Ty(*context));
    }
    // Push back type for current row index
    args.push_back(Type::getInt32Ty(*context));

    cout << "exprtree: ";
    _expr->printExprTree();
    cout << endl;
    FunctionType *prototype = FunctionType::get(this->toLLVMType(_expr->getExprDataType()), args, false);
    Function *func = Function::Create(prototype, Function::ExternalLinkage, _func_name, _module.get());
    
    int32_t idx = 0;
    for (auto& arg : func->args()) {
        if (idx == datatypes->size() * 2) arg.setName("rowIdx");
        else {
            if (idx % 2 == 0) {
                arg.setName(to_string(idx / 2));
            }
            else {
                arg.setName(to_string(idx / 2) + "_isNull");
            }
        }
        idx++;
    }

    BasicBlock *body = BasicBlock::Create(*context, "CREATED_FUNC_BODY", func);
    builder->SetInsertPoint(body);
    map<string, Value*> fArgs;
    for (auto& arg : func->args()) {
        fArgs[arg.getName().str()] = &arg;
    }

    Value* ret = this->parseExpr(_expr, fArgs);
    builder->CreateRet(ret);
    verifyFunction(*func);
    // _module->print(errs(), nullptr);
    auto FPM = std::make_unique<legacy::FunctionPassManager>(_module.get());
    llvm::legacy::PassManager MPM;
    addOptimizationPasses(FPM.get(), MPM);
    FPM->doInitialization();

    for (auto &F : *_module) FPM->run(F);

    MPM.run(*_module);
    // cout << "After optimization: " << endl;
    // _module->print(errs(), nullptr);
    return func;
}
