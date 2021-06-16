#include <iostream>
#include <string>
#include <cstring>
#include <memory>
#include <vector>
#include <cassert>
#include <ctime>
#include <regex>

#include "llvm_codegen.h"
#include "../common/expressions.h"
#include "../common/parser/parser.h"
#include "./functions/mathfunctions.h"
#include "./functions/stringfunctions.h"


#include "llvm/ExecutionEngine/Orc/LLJIT.h"

#include "llvm/ADT/APInt.h"
#include "llvm/ADT/STLExtras.h"
#include "llvm/IR/BasicBlock.h"
#include "llvm/IR/Constants.h"
#include "llvm/IR/DerivedTypes.h"
#include "llvm/IR/Function.h"
#include "llvm/IR/Instructions.h"
#include "llvm/IR/IRBuilder.h"
#include "llvm/IRReader/IRReader.h"
#include "llvm/IR/LLVMContext.h"
#include "llvm/IR/Module.h"
#include "llvm/IR/Type.h"
#include "llvm/IR/Verifier.h"
#include "llvm/Support/SourceMgr.h"
#include "llvm/Support/TargetSelect.h"
#include "llvm/Target/TargetMachine.h"

using namespace llvm;
using namespace orc;

unique_ptr<LLVMContext> context;
unique_ptr<IRBuilder<>> builder;
unique_ptr<Module> _module;
ExitOnError EOE;
unique_ptr<LLJIT> JIT;
ResourceTrackerSP rt;
std::string _func_name;

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
            std::cout << "Error: Unknown argument datatype " << t << endl;
            return nullptr;
    }
}


// Tells whether or not a function is a valid name
bool isFunctionName(string fnName) {
    // TODO: either use a global string set to contain function names, or get function names from _module
    return true;
}


// Registers one function given the types, name, and function pointer
void LLVMCodeGen::registerFunc(void* funcAddr, string funcName, llvm::Type *retType, vector<Type*> paramTypes) {
    auto &jd = JIT->getMainJITDylib();
    auto &dl = JIT->getDataLayout();
    MangleAndInterner Mangle(JIT->getExecutionSession(), dl);

    auto s = absoluteSymbols({{Mangle(funcName), JITEvaluatedSymbol(pointerToJITTargetAddress(funcAddr), JITSymbolFlags::Exported)}});
    auto ign = jd.define(s);

    llvm::FunctionType* ft = llvm::FunctionType::get(retType, paramTypes, false);
    Function* fn = llvm::Function::Create(ft, Function::ExternalLinkage, funcName, _module.get());
    FunctionCallee callee = _module->getOrInsertFunction(funcName, ft);

}


void LLVMCodeGen::registerFunctions() {
    // Register all functions in JIT
    

    Type* t1 = Type::getInt1Ty(*context);
    Type* t32 = Type::getInt32Ty(*context);
    Type* t64 = Type::getInt64Ty(*context);
    Type* tdouble = Type::getDoubleTy(*context);

    // strEqualsExt
    vector<Type*> argTypesStrEq {t64, t64};
    // (int32_t (*)(int64_t*, int32_t, int32_t*))
    this->registerFunc((void*)(strEqualsExt), "strEqualsExt", llvm::Type::getInt1Ty(*context), argTypesStrEq);

    // strCompareExt
    vector<Type*> argTypesStrCmp {t64, t64};
    this->registerFunc((void*)strCompareExt, "strCompareExt", llvm::Type::getInt32Ty(*context), argTypesStrCmp);

    // likeExt
    vector<Type*> argTypesLike {t64, t64};
    this->registerFunc((void*)likeExt, "likeExt", llvm::Type::getInt1Ty(*context), argTypesLike);

    // abs_int32
    vector<Type*> argTypesAbs {t32};
    this->registerFunc((void*)abs_int32, "abs_int32", llvm::Type::getInt32Ty(*context), argTypesAbs);


    // abs_int64
    vector<Type*> argTypesAbs64 {t64};
    this->registerFunc((void*)abs_int64, "abs_int64", llvm::Type::getInt64Ty(*context), argTypesAbs64);


    // abs_double
    vector<Type*> argTypesAbsDouble {tdouble};
    this->registerFunc((void*)abs_double, "abs_double", llvm::Type::getDoubleTy(*context), argTypesAbsDouble);


    // substrExt
    vector<Type*> argTypesSubstr {t64, t32, t32};
    this->registerFunc((void*)substrExt, "substrExt", llvm::Type::getInt64Ty(*context), argTypesSubstr);

    // concatStrExt
    vector<Type*> argTypesConcat {t64, t64};
    this->registerFunc((void*)concatStrExt, "concatStrExt", llvm::Type::getInt64Ty(*context), argTypesConcat);

    // cast_int32
    vector<Type*> argTypesCast32 {t32};
    this->registerFunc((void*)cast_int32, "cast_int32", llvm::Type::getDoubleTy(*context), argTypesCast32);

    // cast_int64
    vector<Type*> argTypesCast64 {t64};
    this->registerFunc((void*)cast_int64, "cast_int64", llvm::Type::getDoubleTy(*context), argTypesCast64);

    // cast_string
    vector<Type*> argTypesCastStr {t64};
    this->registerFunc((void*)cast_string, "cast_string", llvm::Type::getInt32Ty(*context), argTypesCastStr);
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

    builder = make_unique<IRBuilder<>>(*context);

   this->registerFunctions();
}

LLVMCodeGen::~LLVMCodeGen() {
    auto ignore = rt->remove();
    delete this->datatypes;
}

int32_t LLVMCodeGen::execute(int64_t* data, int32_t nRows, int32_t* selected) {
    return this->_filter(data, nRows, selected);
}

Function* LLVMCodeGen::createConditional(DataType retType, Expr* cond, Expr* ifTrue, Expr* ifFalse) {
    vector<Type*> args;
    args.reserve(datatypes->size());
    for (int32_t i = 0; i < datatypes->size(); i++) {
        DataType type = datatypes->at(i);
        args.push_back(this->toLLVMType(type));
    }
    Type* retTypePtr = this->toLLVMType(retType);
    FunctionType *prototype = FunctionType::get(retTypePtr, args, false);
    Function *func = Function::Create(prototype, Function::ExternalLinkage, "IF_CONDITIONAL", _module.get());

    int32_t idx = 0;
    for (auto& arg : func->args()) {
        arg.setName(to_string(idx++));
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
    builder->CreateCondBr(evCond, trueBlock, falseBlock);
    builder->SetInsertPoint(trueBlock);
    builder->CreateRet(evTrue);
    builder->SetInsertPoint(falseBlock);
    builder->CreateRet(evFalse);

    llvm::verifyFunction(*func);
    return func;
}


// other operations which require externed functions
Value* LLVMCodeGen::stringEq(Value *LHS, Value *RHS) {
    // call function
    vector<Value*> argVals {LHS, RHS};
    auto f = _module->getFunction("strEqualsExt");
    Value *ret = builder->CreateCall(f, argVals, "call_str_eq");
    return ret;
}

Value* LLVMCodeGen::stringCmp(Value *LHS, Value *RHS) {
    // call function
    vector<Value*> argVals {LHS, RHS};
    auto f = _module->getFunction("strCompareExt");
    Value *ret = builder->CreateCall(f, argVals, "call_str_cmp");
    return ret;
}


// Main codegen for any type of expression
Value* LLVMCodeGen::parseExpr(Expr* root, map<string, Value*>& args) {
    switch (root->getType()) {
        case ExprType::DATA_E: {
            DataExpr* dEx = (DataExpr*) root;
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
                    // TODO: fix this memory leak
                    char* s = new char[dEx->stringVal.size()+1];
                    for (int i = 0; i < dEx->stringVal.size(); i++) {
                        s[i] = dEx->stringVal[i];
                    }
                    s[dEx->stringVal.size()] = '\0';

                    Constant *addr = ConstantInt::get(*context, APInt(64, (int64_t)(s)));
                    return addr;
                }
                default: {
                    std::cout << "Unsupported data type in Data Expr" << endl;
                    return this->createConstantBool(false);
                }
            }
            break;
        }

        case ExprType::BINARY_E: {
            BinaryExpr* bExpr = (BinaryExpr*) root;
            if (bExpr->left->getType() == ExprType::DATA_E || bExpr->right->getType() == ExprType::DATA_E) {
                DataType biggerType = std::max(bExpr->left->getExprDataType(), bExpr->right->getExprDataType());
                bExpr->left->dataType = biggerType;
                bExpr->right->dataType = biggerType;
            }
            Value* left = this->parseExpr(bExpr->left, args);
            Value* right = this->parseExpr(bExpr->right, args);


            if (bExpr->op == expressions::Operator::AND) {
                return builder->CreateAnd(left, right, "logical_and");
            }
            if (bExpr->op == expressions::Operator::OR) {
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
                        return this->stringEq(left, right);
                    case NEQ: 
                        return builder->CreateNot(this->stringEq(left, right), "string_neq");
                    default: 
                        std::cout << "Unsupported string binary operator" << endl;
                }
            }
            std::cout << "Error: Unsupported double binary expr op " << bExpr->op << endl;
            return this->createConstantBool(false);
        }

        case ExprType::UNARY_E: {
            UnaryExpr* uExpr = (UnaryExpr*) root;
            Value* val = this->parseExpr(uExpr, args);
            switch(uExpr->op) {
                case NOT:
                    return builder->CreateNot(val, "logical_not");
                default:
                    // ignore the unary operator if it is invalid
                    return val;
            }
        }

        case ExprType::IF_E: {
            IfExpr* ie = (IfExpr*) root;
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

        case ExprType::IN_E: {
            InExpr* iExpr = (InExpr*) root;
            llvm::Value *val = parseExpr(iExpr->arguments[0], args);
            vector<Value*> argValues;
            for (int i = 1; i < iExpr->arguments.size(); i++) {
                argValues.push_back(parseExpr(iExpr->arguments[i], args));
            }
            llvm::Value *inArray = llvm::ConstantInt::get(*context, APInt(1, 0));
            for (int i = 0; i < argValues.size(); i++) {
                llvm::Value *tmpCmp;
                switch(iExpr->arguments[0]->dataType) {
                    case INT32D: {
                        tmpCmp = builder->CreateICmpEQ(val, argValues[i]);
                        break;
                    }
                    case INT64D: {
                        tmpCmp = builder->CreateICmpEQ(val, argValues[i]);
                        break;
                    }
                    case DOUBLED: {
                        tmpCmp = builder->CreateFCmpOEQ(val, argValues[i]);
                        break;
                    }
                    case STRINGD:{
                        tmpCmp = this->stringEq(val, argValues[i]);
                        break;
                    }
                    default: {
                        std::cout << "Unsupported data type in IN expr" << endl;
                        tmpCmp = this->createConstantBool(false);
                    }
                }
                inArray = builder->CreateOr(inArray, tmpCmp);
            }
            return inArray;
        }

        case ExprType::BETWEEN_E: {
            BetweenExpr* bExpr = (BetweenExpr*) root;
            llvm::Value *val = parseExpr(bExpr->value, args);
            llvm::Value *lowerVal = parseExpr(bExpr->lowerBound, args);
            llvm::Value *upperVal = parseExpr(bExpr->upperBound, args);

            llvm::Value *cmpleft = builder->CreateICmpSLE(lowerVal, val, "between_cmpleft");
            llvm::Value *cmpright = builder->CreateICmpSLE(val, upperVal, "between_cmpright");
            llvm::Value *result = builder->CreateAnd(cmpleft, cmpright, "between_and");
            return result;
        }

        case ExprType::COALESCE_E: {
            CoalesceExpr* cExpr = (CoalesceExpr*) root;
            // TODO: find how null values are represented and finish
            return this->createConstantInt(0);
        }
        
        case ExprType::FUNC_E: {
            FuncExpr* fExpr = (FuncExpr*) root;
            return this->funcCodegen(fExpr, args);
        }

        default: {
            std::cout << "Error: Unsupported expr type " << root->getType() << endl;
            return this->createConstantBool(false);
        }
    }
}



// Handles all functions
// Only calls them; registration is done in the LLVMCodeGen::registerFunctions() method
Value* LLVMCodeGen::funcCodegen(FuncExpr* fExpr, map<string, Value*>& args) {

    if (fExpr->funcName == "abs") {
        
        switch (fExpr->dataType) {
            case (DataType::INT32D): {
                vector<Value*> argVals {this->parseExpr(fExpr->arguments[0], args)};        
                auto f = _module->getFunction("abs_int32");
                Value *ret = builder->CreateCall(f, argVals, "call_abs_32");
                return ret;
            }
            case (DataType::INT64D): {
                vector<Value*> argVals {this->parseExpr(fExpr->arguments[0], args)};        
                auto f = _module->getFunction("abs_int64");
                Value *ret = builder->CreateCall(f, argVals, "call_abs_64");
                return ret;
            }
            case (DataType::DOUBLED): {
                vector<Value*> argVals {parseExpr(fExpr->arguments[0], args)};        
                auto f = _module->getFunction("abs_double");
                Value *ret = builder->CreateCall(f, argVals, "call_abs_double");
                return ret;
            }
            default: {
                std::cout << "Unsupported data type for function " << fExpr->funcName << endl;
            }
        }
    }
    if (fExpr->funcName == "abs2") {
        // pure codegen implementation of abs
        // bit trick from graphics.stanford.edu/~seander/bithacks.html#IntegerAbs
        llvm::Value *tmpconst = this->createConstantInt(31);
        llvm::Value *val = parseExpr(fExpr->arguments[0], args);
        llvm::Value *mask = builder->CreateAShr(val, tmpconst); // should be filled with most significant bit using AShr
        llvm::Value *tmpsum = builder->CreateAdd(val, mask);
        llvm::Value *tmpxor = builder->CreateXor(mask, tmpsum);
        return tmpxor;
    }
    if (fExpr->funcName == "substr") {
        Value* str = parseExpr(fExpr->arguments[0], args);
        Value* startIdx = parseExpr(fExpr->arguments[1], args);
        Value* length = parseExpr(fExpr->arguments[2], args);
        vector<Value*> argVals {str, startIdx, length};
        
        auto f = _module->getFunction("substrExt");
        Value *ret = builder->CreateCall(f, argVals, "call_substr");
        return ret;
    }
    if (fExpr->funcName == "CONCAT") {
        Value* str1 = parseExpr(fExpr->arguments[0], args);
        Value* str2 = parseExpr(fExpr->arguments[1], args);
        vector<Value*> argVals {str1, str2};

        auto f = _module->getFunction("concatStrExt");
        Value *ret = builder->CreateCall(f, argVals, "call_concat");
        return ret;
    }
    if (fExpr->funcName == "CAST") {
        // Simply cast from int or long to double (how it appears in tpch)
        llvm::Value *val = parseExpr(fExpr->arguments[0], args);
        vector<Value*> argVals {val};
        std::cout << "casting with type " << fExpr->dataType << endl;
        switch (fExpr->arguments[0]->dataType) {
            case DataType::INT32D: {
                auto f = _module->getFunction("cast_int32");
                Value *ret = builder->CreateCall(f, argVals, "cast_int32");
                return ret;
            }
            case DataType::INT64D: {
                auto f = _module->getFunction("cast_int64");
                Value *ret = builder->CreateCall(f, argVals, "cast_int64");
                return ret;
            }
            case DataType::STRINGD: {
                std::cout << "casting string!?" << endl;
                auto f = _module->getFunction("cast_string");
                Value *ret = builder->CreateCall(f, argVals, "call_cast_string");
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

        auto f = _module->getFunction("likeExt");
        Value *ret = builder->CreateCall(f, argVals, "call_like");
        return ret;
    }
    // external functions
    
    if (isFunctionName(fExpr->funcName)) {
        vector<Value*> argVals;
        for (int i = 0; i < fExpr->arguments.size(); i++) {
            argVals.push_back(parseExpr(fExpr->arguments[i], args));
        }
        // Assume that the function name of the fExpr and in the module are matching
        auto f = _module->getFunction(fExpr->funcName);
        Value* ret = builder->CreateCall(f, argVals, "call_" + fExpr->funcName);
        return ret;
    }
    return createConstantInt(0);
}



// Logic to generate the filter function.
Function* LLVMCodeGen::generateFunc()
{   
    // cout << "Generating function" << endl;
    vector<Type*> args;
    args.reserve(datatypes->size());
    for (int32_t i = 0; i < datatypes->size(); i++) {
        DataType type = datatypes->at(i);
        // cout << "Adding argument of type " << type << endl;
        args.push_back(this->toLLVMType(type));
    }
    
    FunctionType *prototype = FunctionType::get(Type::getInt1Ty(*context), args, false);
    Function *func = Function::Create(prototype, Function::ExternalLinkage, _func_name, _module.get());
    // cout << "Created function declaration " << _func_name << endl;
    int32_t idx = 0;
    for (auto& arg : func->args()) {
        arg.setName(to_string(idx++));
    }
    BasicBlock *body = BasicBlock::Create(*context, "FILTER_FUNC_BODY", func);
    builder->SetInsertPoint(body);
    map<string, Value*> fArgs;
    for (auto& arg : func->args()) {
        fArgs[arg.getName().str()] = &arg;
    }

    // cout << "Generating filter body" << endl;
    Value* ret = this->parseExpr(_expr, fArgs);
    builder->CreateRet(ret);
    verifyFunction(*func);
    _module->print(errs(), nullptr);
    return func;
}

void LLVMCodeGen::compile()
{
    Function* filterFunc = this->generateFunc();
    int64_t address = this->createWrapper(filterFunc);
    _filter = (int32_t (*)(int64_t*, int32_t, int32_t*)) (intptr_t) address;
}

std::string LLVMCodeGen::dumpCode() {
  std::string ir;
  llvm::raw_string_ostream stream(ir);
  _module->print(stream, nullptr);
  cout<<" Generated code::" << ir;
  return ir;
}

int64_t LLVMCodeGen::createWrapper(Function* filterFunc) {
    int32_t nArgs = this->datatypes->size();
    vector<Type*> args;
    Type* ptrArg = Type::getInt64PtrTy(*context);
    args.push_back(ptrArg);
    args.push_back(Type::getInt32Ty(*context));
    args.push_back(Type::getInt32PtrTy(*context));
    FunctionType* funcSignature = FunctionType::get(Type::getInt32Ty(*context), args, false);
    Function* funcDecl = Function::Create(funcSignature, Function::ExternalLinkage, "FILTER_WRAPPER", _module.get());
    BasicBlock* preLoop = BasicBlock::Create(*context, "PRE_LOOP", funcDecl);
    BasicBlock* loopBody = BasicBlock::Create(*context, "LOOP_BODY", funcDecl);
    BasicBlock* filterPassed = BasicBlock::Create(*context, "FILTER_PASSED", funcDecl);
    BasicBlock* incrementCounter = BasicBlock::Create(*context, "INCREMENT_COUNTER", funcDecl);
    BasicBlock* endBlock = BasicBlock::Create(*context, "END_BLOCK", funcDecl);
    // preprocessing
    Argument* start = funcDecl->getArg(0);
    start->setName("ARGS_ARRAY");
    Argument* numRows = funcDecl->getArg(1);
    numRows->setName("NUM_ROWS");
    Argument* resultsArray = funcDecl->getArg(2);
    resultsArray->setName("RESULTS");
    Value* minusOne = this->createConstantInt(-1);
    Value* zero = this->createConstantInt(0);
    Value* one = this->createConstantInt(1);
    vector<Value*> filterFuncArgs;
    filterFuncArgs.reserve(nArgs);
    Value* gep;
    Value* elementAddr;
    Value* elementPtr;
    Value* elementValue;
    DataType type;
    CallInst* ret;
    // pre loop body
    builder->SetInsertPoint(preLoop);
    // Pointer to the current row index to be processed.
    AllocaInst* indexStore = builder->CreateAlloca(Type::getInt32Ty(*context), nullptr, "INDEX_COUNTER");
    // Initialize row index to 0.
    builder->CreateStore(zero, indexStore);
    // Value of the current row index to be processed.
    Value* curIndexVal;
    // Temp value for next row index.
    Value* nextIndexVal;
    // Pointer to the index of the selected positions array to be filled next.
    AllocaInst* selectedIndexStore = builder->CreateAlloca(Type::getInt32Ty(*context), nullptr, "SELECTED_INDEX_PTR");
    // Initialize index to 0.
    builder->CreateStore(zero, selectedIndexStore);
    // Value of the selected positions index.
    Value* selectedIndexVal;
    // Address of the selected index for writing.
    Value* selectedAddress;
    // Temp value for next selected index.
    Value* nextSelectedIndexVal;
    builder->CreateBr(loopBody);
    // loop body
    builder->SetInsertPoint(loopBody);
    // Get the value of the current row index to process.
    curIndexVal = builder->CreateLoad(indexStore, "CUR_INDEX");
    for (int32_t i = 0; i < nArgs; i++) {
        // Find address of this column in the addresses array argument.
        gep = builder->CreateGEP(start, this->createConstantInt(i));
        // Load the address value.
        elementAddr = builder->CreateLoad(gep);
        type = this->datatypes->at(i);
        // Convert the column address to array of proper datatype.
        switch (type) {
            case DataType::BOOLD:
                elementPtr = builder->CreateIntToPtr(elementAddr, Type::getInt1PtrTy(*context));
                break;
            case DataType::INT32D:
                elementPtr = builder->CreateIntToPtr(elementAddr, Type::getInt32PtrTy(*context));
                break;
            case DataType::INT64D:
                elementPtr = builder->CreateIntToPtr(elementAddr, Type::getInt64PtrTy(*context));
                break;
            case DataType::DOUBLED:
                elementPtr = builder->CreateIntToPtr(elementAddr, Type::getDoublePtrTy(*context));
                break;
            case DataType::STRINGD:
                elementPtr = builder->CreateIntToPtr(elementAddr, Type::getInt64PtrTy(*context));
                break;
            default:
                cout << "Unsupported column data type" << endl;
                elementPtr = builder->CreateIntToPtr(elementAddr, Type::getInt64PtrTy(*context));
        }
        // Find the address of the row to be processed.
        gep = builder->CreateGEP(elementPtr, curIndexVal);
        // Value to be processed.
        elementValue = builder->CreateLoad(gep);
        // Pass to filter function's arguments.
        filterFuncArgs.push_back(elementValue);
    }
    // Get the boolean response for this row from the filter function.
    ret = builder->CreateCall(filterFunc, filterFuncArgs, "ROW_EVAL");
    // If true, add row index to selected array, otherwise, process next row.
    builder->CreateCondBr(ret, filterPassed, incrementCounter);
    // Add row index to results array
    builder->SetInsertPoint(filterPassed);
    // Get value of selected index.
    selectedIndexVal = builder->CreateLoad(selectedIndexStore, "SELECTED_INDEX");
    // Get address of selected index.
    selectedAddress = builder->CreateGEP(resultsArray, selectedIndexVal, "SELECTED_ADDRESS");
    // Set the selected value to the current row index.
    builder->CreateStore(curIndexVal, selectedAddress);
    // Increment the selected index.
    nextSelectedIndexVal = builder->CreateAdd(selectedIndexVal, one, "NEXT_SELECTED_INDEX");
    builder->CreateStore(nextSelectedIndexVal, selectedIndexStore);
    // Increment counter and process next row.
    builder->CreateBr(incrementCounter);
    // Increment loop counter
    builder->SetInsertPoint(incrementCounter);
    // Increment counter.
    nextIndexVal = builder->CreateAdd(curIndexVal, one, "NEXT_INDEX");
    builder->CreateStore(nextIndexVal, indexStore);
    // If there are rows remaining, repeat, otherwise, exit.
    Value* cond = builder->CreateICmpSLT(nextIndexVal, numRows, "END_LOOP_COND");
    builder->CreateCondBr(cond, loopBody, endBlock);
    // Return results
    builder->SetInsertPoint(endBlock);
    // Return the filled in results.
    nextSelectedIndexVal = builder->CreateLoad(selectedIndexStore);
    builder->CreateRet(nextSelectedIndexVal);
    // _module->print(errs(), nullptr);
    auto resTracker = JIT->getMainJITDylib().createResourceTracker();
    auto threadSafeModule = ThreadSafeModule(move(_module), move(context));
    EOE(JIT->addIRModule(resTracker, move(threadSafeModule)));
    rt = resTracker;
    // initModule();
    auto sym = JIT->lookup("FILTER_WRAPPER");
    return sym->getAddress();
}




void simpleTest1(int argc, char** argv) {  
    // string unparsed = "$operator$GREATER_THAN(#0, 0)";
    string unparsed = "AND($operator$GREATER_THAN_OR_EQUAL(ADD(#0, 2), 4), AND($operator$LESS_THAN(#1, 4), $operator$EQUAL(#2, 2)))";


    DataType types[3] = {DataType::INT32D, DataType::INT32D, DataType::INT32D};
    Parser parser{};
    Expr* expr = parser.parseRowExpression(unparsed, reinterpret_cast<int*>(types), 3);
    expr->printExprTree();
    cout << ":::type:::" << expr->getType();
    cout << endl;
    
    int32_t v1[1] = {atoi(argv[1])};
    int32_t v2[1] = {atoi(argv[2])};
    int32_t v3[1] = {atoi(argv[3])};
    int64_t* vals = new int64_t[3];
    vals[0] = (int64_t) v1;
    vals[1] = (int64_t) v2;
    vals[2] = (int64_t) v3;
    int32_t *selected = new int32_t[1];
    cout << v1[0] << " " << v2[0] << " " << v3[0] << endl;

    string testname = "simpleTest1";
    vector<DataType> *typeVec = new vector<DataType>(types, types+3);
    LLVMCodeGen *lc = new LLVMCodeGen(testname, expr, typeVec);

    cout << "codegen created!" << endl; 
    lc->compile();
    cout << "codegen compiled!" << endl;
    // lc.dumpCode();

    int32_t result = lc->execute(vals, 1, selected);
    cout << result << endl; // number of rows that passed filter

    delete[] vals;
    delete[] selected;
    delete lc;
    delete expr;
}

void simpleTest2(int argc, char** argv) {
    // string unparsed = "IN(#0, 1, 2, 3, 4, 5)";
    // string unparsed = "IF($operator$GREATER_THAN(#0, 100), $operator$GREATER_THAN(#0, 200), $operator$LESS_THAN(#0, 0))";
    // string unparsed = "BETWEEN(#1, #0, #2)";
    // string unparsed = "$operator$EQUAL(abs(#0), #1)";
    string unparsed = "AND($operator$EQUAL(abs(#0), abs(#2)), $operator$EQUAL(abs(#0), abs(#1)))";

    DataType types[3] = {DataType::INT32D, DataType::INT32D, DataType::INT32D};
    Parser parser{};
    Expr* expr = parser.parseRowExpression(unparsed, reinterpret_cast<int*>(types), 3);
    expr->printExprTree();
    cout << ":::type:::" << expr->getType();
    cout << endl;
    
    int32_t v1[1] = {atoi(argv[1])};
    int32_t v2[1] = {atoi(argv[2])};
    int32_t v3[1] = {atoi(argv[3])};
    int64_t* vals = new int64_t[3];
    vals[0] = (int64_t) v1;
    vals[1] = (int64_t) v2;
    vals[2] = (int64_t) v3;
    int32_t *selected = new int32_t[1];
    cout << v1[0] << " " << v2[0] << " " << v3[0] << endl;

    string testname = "simpleTest2";
    vector<DataType> *typeVec = new vector<DataType>(types, types+3);
    LLVMCodeGen *lc = new LLVMCodeGen(testname, expr, typeVec);

    cout << "codegen created!" << endl; 
    lc->compile();
    cout << "codegen compiled!" << endl;
    // lc.dumpCode();

    int32_t result = lc->execute(vals, 1, selected);
    cout << result << endl; // number of rows that passed filter

    // TODO: fix all memory leaks
    delete[] vals;
    delete[] selected;
    delete lc;
    delete expr;
}

// For testing different types
void simpleTest3(int argc, char** argv) {
    // string unparsed = "$operator$EQUAL(abs(CAST(#0)), abs(CAST(#1)))";
    string unparsed = "$operator$GREATER_THAN(CAST(#1), #2)";

    DataType types[3] = {DataType::INT32D, DataType::INT64D, DataType::DOUBLED};
    Parser parser{};
    Expr* expr = parser.parseRowExpression(unparsed, reinterpret_cast<int*>(types), 3);
    expr->printExprTree();
    cout << ":::type:::" << expr->getType();
    cout << endl;
    
    int32_t v1[1] = {atoi(argv[1])};
    int64_t v2[1] = {atol(argv[2])};
    double v3[1] = {atof(argv[3])};
    int64_t* vals = new int64_t[3];
    vals[0] = (int64_t) v1;
    vals[1] = (int64_t) v2;
    vals[2] = (int64_t) v3;
    int32_t *selected = new int32_t[1];
    cout << v1[0] << " " << v2[0] << " " << v3[0] << endl;

    string testname = "simpleTest3";
    vector<DataType> *typeVec = new vector<DataType>(types, types+3);
    LLVMCodeGen *lc = new LLVMCodeGen(testname, expr, typeVec);

    cout << "codegen created!" << endl; 
    lc->compile();
    cout << "codegen compiled!" << endl;
    // lc.dumpCode();

    int32_t result = lc->execute(vals, 1, selected);
    cout << result << endl; // number of rows that passed filter

    delete[] vals;
    delete[] selected;
    delete lc;
    delete expr;
}

void stringTest1(int argc, char** argv) {
    // string unparsed = "$operator$EQUAL('hello world', 'hello worldjdflskgj')";
    // string unparsed = "$operator$EQUAL(#1, #2)";
    // string unparsed = "OR($operator$EQUAL(#2, 'Sunday'), $operator$EQUAL(#2, 'Saturday'))";
    // string unparsed = "$operator$EQUAL(substr(#1, 0, 5), #2)";
    // string unparsed = "$operator$EQUAL(CONCAT(#1, #2), 'helloworld')";
    // string unparsed = "IN(substr(#2, 0, 2), '12', '21', '13', '31', '34', '43')";
    // string unparsed = "$operator$GREATER_THAN(CAST(#2), #0)";
    string unparsed = "LIKE(#2, '%hello%world%')";
    

    DataType types[3] = {DataType::INT32D, DataType::STRINGD, DataType::STRINGD};
    Parser parser{};
    Expr* expr = parser.parseRowExpression(unparsed, reinterpret_cast<int*>(types), 3);
    expr->printExprTree();
    cout << ":::type:::" << expr->getType();
    cout << endl;
    
    int32_t v1[1] = {atoi(argv[1])};
    int64_t v2[1] = {(int64_t)(argv[2])};
    int64_t v3[1] = {(int64_t)(argv[3])};
    int64_t* vals = new int64_t[3];
    vals[0] = (int64_t) v1;
    vals[1] = (int64_t) v2;
    vals[2] = (int64_t) v3;
    int32_t *selected = new int32_t[1];
    cout << v1[0] << " " << v2[0] << " " << v3[0] << endl;

    string testname = "stringTest1";
    vector<DataType> *typeVec = new vector<DataType>(types, types+3);
    LLVMCodeGen *lc = new LLVMCodeGen(testname, expr, typeVec);

    cout << "codegen created!" << endl; 
    lc->compile();
    cout << "codegen compiled!" << endl;

    int32_t result = lc->execute(vals, 1, selected);
    cout << result << endl;

    // TODO: fix all memory leaks
    delete[] vals;
    delete[] selected;
    delete lc;
}

// To run any tests use following command: 
// clang++ codegen/*.cpp codegen/functions/*.cpp common/expressions.cpp common/parser/*.cpp -DCMAKE_BUILD_TYPE=Debug -g -O3 -rdynamic -frtti -lre2 `llvm-config --cxxflags --ldflags --system-libs --libs` -o llvm_codegen
// int main(int argc, char** argv) {
//     // simpleTest1(argc, argv);
//     // simpleTest2(argc, argv);
//     // simpleTest3(argc, argv);
//     stringTest1(argc, argv);
// }
