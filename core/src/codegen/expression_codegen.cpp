/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Expression code generator
 */
#include "expression_codegen.h"

#include <thread>
#include <chrono>
#include <utility>

#include "llvm/IR/LegacyPassManager.h"
#include "llvm/Passes/PassBuilder.h"
#include "llvm/Support/TargetSelect.h"
#include "llvm/Transforms/Scalar.h"
#include "llvm/Transforms/IPO.h"
#include "llvm/Transforms/Utils.h"
#include "llvm/ExecutionEngine/Orc/LLJIT.h"
#include "vector"
#include "expr_info_extractor.h"
#include "codegen_context.h"

using namespace llvm;
using namespace orc;
using namespace omniruntime;
using namespace omniruntime::expressions;
using namespace omniruntime::vec;
using std::make_shared;

namespace {
const int INT32_VALUE = 32;
const int INT64_VALUE = 64;
const int EXPRFUNC_OUT_LENGTH_ARG_INDEX = 4;
const int EXPRFUNC_OUT_IS_NULL_INDEX = 7;

std::once_flag codegen_target_init_flag;
}

CodeGenValuePtr ExpressionCodeGen::VisitExpr(const omniruntime::expressions::Expr &e)
{
    e.Accept(*this);
    return this->value;
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
        case DataType::CHARD:
        case DataType::VARCHARD:
            return Type::getInt8PtrTy(*context);
        case DataType::DECIMAL128D:
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
        case DataType::CHARD:
        case DataType::VARCHARD:
            return Type::getInt64PtrTy(*context);
        default:
            LLVM_DEBUG_LOG("Unsupported column data type %d", type);
            return Type::getInt64PtrTy(*context);
    }
}

Type *ExpressionCodeGen::GetFunctionReturnType(DataType type)
{
    if (IsStringDataType(type)) {
        return Type::getInt64Ty(*context);
    } else {
        return this->ToLlvmType(expr->GetExprDataType());
    }
}

Type *ExpressionCodeGen::GetFunctionArgType(DataType type)
{
    switch (type) {
        case DataType::VOIDD:
            return Type::getVoidTy(*context);
        case DataType::INT32D:
        case DataType::INT64D:
        case DataType::DOUBLED:
        case DataType::BOOLD:
        case DataType::DECIMAL128D:
            return ToLlvmType(type);
        case DataType::VARCHARD:
            return Type::getInt64Ty(*context);
        case DataType::INT32PTRD:
            return Type::getInt32PtrTy(*context);
        case DataType::INT8PTRD:
            return Type::getInt8PtrTy(*context);
        default:
            LLVM_DEBUG_LOG("Error: Unknown argument datatype %d", type);
            return nullptr;
    }
}

/**
 * Usage example: std::vector<Value *> values;
 * values.push_back(value1);
 * values.push_back(value2);
 * PrintValues("LLVM DEBUG: %d, %d\n", values);
 */
void ExpressionCodeGen::PrintValues(std::string format, const std::vector<Value *> &values)
{
    auto charType = Type::getInt8Ty(*context);
    std::vector<llvm::Constant *> chars(format.size());
    for (unsigned int i = 0; i < format.size(); i++) {
        chars[i] = ConstantInt::get(charType, format[i]);
    }
    chars.push_back(llvm::ConstantInt::get(charType, 0));
    auto stringType = llvm::ArrayType::get(charType, chars.size());

    // Create the declaration statement
    this->numGlobalValues++;
    auto globalDeclaration = static_cast<llvm::GlobalVariable *>(
        module->getOrInsertGlobal("string" + std::to_string(this->numGlobalValues), stringType));
    globalDeclaration->setInitializer(llvm::ConstantArray::get(stringType, chars));
    globalDeclaration->setConstant(true);
    globalDeclaration->setLinkage(llvm::GlobalValue::LinkageTypes::PrivateLinkage);
    globalDeclaration->setUnnamedAddr(llvm::GlobalValue::UnnamedAddr::Global);

    // Return a cast to an i8*
    auto formatPtr = llvm::ConstantExpr::getBitCast(globalDeclaration, charType->getPointerTo());
    std::vector<Value *> args;
    args.push_back(formatPtr);
    for (auto v : values) {
        args.push_back(v);
    }

    builder->CreateCall(codegenContext->print, args, "printfCall");
}

ExpressionCodeGen::ExpressionCodeGen(std::string name, const Expr &cpExpr)
    :funcName(std::move(name)), expr(&cpExpr) {}

ExpressionCodeGen::~ExpressionCodeGen()
{
    if (rt) {
        eoe(rt->remove());
    }
}

void ExpressionCodeGen::InitializeCodegenTargets()
{
    llvm::InitializeNativeTarget();
    llvm::InitializeNativeTargetAsmPrinter();
    llvm::InitializeNativeTargetAsmParser();
    llvm::InitializeNativeTargetDisassembler();
    llvm::sys::DynamicLibrary::LoadLibraryPermanently(nullptr);
}

bool ExpressionCodeGen::Initialize()
{
    std::call_once(codegen_target_init_flag, InitializeCodegenTargets);
    jit = eoe(LLJITBuilder().create());

    context = std::make_unique<LLVMContext>();
    // Create module called the_module
    module = std::make_unique<Module>("the_module", *context);
    module->setDataLayout(jit->getDataLayout());
    // Create IR builder to create IR instructions
    builder = std::make_unique<IRBuilder<>>(*context);
    fpm = std::make_unique<legacy::FunctionPassManager>(module.get());

    ExprInfoExtractor exprInfoExtractor;
    this->expr->Accept(exprInfoExtractor);
    this->vectorIndexes = exprInfoExtractor.GetVectorIndexes();
    // get functions used in funcExpr  RegisterFunctions(exprInfoExtractor.GetFunctions());
    RegisterFunctions(FunctionRegistry::GetFunctions());
}

void ExpressionCodeGen::RegisterFunctionsHelper(omniruntime::Function &func, std::set<std::string> jitRegisteredFuncs)
{
    for (auto &funcSignature: func.GetSignatures()) {
        if (jitRegisteredFuncs.find(funcSignature.GetName()) == jitRegisteredFuncs.end()) {
            auto &jd = jit->getMainJITDylib();
            auto &dl = jit->getDataLayout();
            llvm::orc::MangleAndInterner mangle(jit->getExecutionSession(), dl);
            std::vector<Type *> args;
            std::vector<DataType> params = funcSignature.GetParams();
            args.reserve(params.size());
            for (auto type: params) {
                args.push_back(this->GetFunctionArgType(type));
            }
            auto s = llvm::orc::absoluteSymbols({
                {mangle(funcSignature.GetName()),
                 JITEvaluatedSymbol(pointerToJITTargetAddress(funcSignature.GetFunctionAddress()),
                                    JITSymbolFlags::Exported)
                }
            }
            );
            auto ign = jd.define(s);
            if (ign) {
                std::cerr << "Error while defining absolute symbol in jd" << std::endl;
            }
            auto ret = this->GetFunctionArgType(funcSignature.GetReturnType());
            llvm::FunctionType *ft = llvm::FunctionType::get(ret, args, false);
            auto linkage = llvm::Function::ExternalLinkage;
            llvm::Function *fn = llvm::Function::Create(ft, linkage, funcSignature.GetName(), *module);
            FunctionCallee callee = module->getOrInsertFunction(funcSignature.GetName(), ft);
            jitRegisteredFuncs.insert(funcSignature.GetName());
        }
    }
}

// Register function in JIT
void ExpressionCodeGen::RegisterFunctions(std::vector<omniruntime::Function> functions)
{
    std::set<std::string> jitRegisteredFuncs;
    for (auto &func : functions) {
        RegisterFunctionsHelper(func, jitRegisteredFuncs);
    }
}


// Other operations which require externed functions
Value *ExpressionCodeGen::StringCmp(Value *lhs, Value *lLen, Value *rhs, Value *rLen)
{
    // call function
    std::vector<Value *> argVals { lhs, lLen, rhs, rLen };
    auto f = module->getFunction(strCompareExtStr);
    auto ret = builder->CreateCall(f, argVals, "call_str_cmp");
    InlineFunctionInfo inlineFunctionInfo;
    auto inlinedFunction = llvm::InlineFunction(*ret, inlineFunctionInfo);
    return ret;
}

// Other operations which require externed functions
Value *ExpressionCodeGen::Decimal128Cmp(const Value &lhs, const Value &rhs)
{
    // call function
    std::vector<Value *> argVals;
    argVals.push_back(const_cast<Value *>(&lhs));
    argVals.push_back(const_cast<Value *>(&rhs));
    auto f = module->getFunction(decimal128CompareExtStr);
    Value *ret = builder->CreateCall(f, argVals, "call_decimal_cmp");
    return ret;
}

void ExpressionCodeGen::BinaryExprNullHelper(const BinaryExpr *binaryExpr, Value *left, Value *right, Value *leftIsNull,
    Value *rightIsNull, PHINode **leftPhi, PHINode **rightPhi, Value **isNeitherNull)
{
    BasicBlock *incomingBlock, *nullBlock, *nextInst;
    Value *nullCond, *leftZero, *rightZero;
    auto op = binaryExpr->op;

    if (op == LT || op == GT || op == LTE || op == GTE || op == EQ || op == NEQ) {
        *isNeitherNull = builder->CreateNot(builder->CreateOr(leftIsNull, rightIsNull));
    }
    if (op == ADD || op == SUB || op == MUL) {
        std::vector<Value *> argLeftVals { left, left, this->codegenContext->executionContext };
        std::vector<Value *> argRightVals { right, right, this->codegenContext->executionContext };
        incomingBlock = builder->GetInsertBlock();
        nullBlock = BasicBlock::Create(*context, "nullBlock", builder->GetInsertBlock()->getParent());
        nextInst = BasicBlock::Create(*context, "nextInst", builder->GetInsertBlock()->getParent());
        nullCond = builder->CreateOr(leftIsNull, rightIsNull);
        builder->CreateCondBr(nullCond, nullBlock, nextInst);
        builder->SetInsertPoint(nullBlock);
        switch (binaryExpr->left->GetExprDataType()) {
            case INT32D:
            case INT64D:
                leftZero = builder->CreateSub(left, left);
                rightZero = builder->CreateSub(right, right);
                break;
            case DOUBLED:
                leftZero = builder->CreateFSub(left, left);
                rightZero = builder->CreateFSub(right, right);
                break;
            case DECIMAL128D:
                leftZero = builder->CreateCall(module->getFunction(subDec128Str), argLeftVals, subDec128Str);
                rightZero = builder->CreateCall(module->getFunction(subDec128Str), argRightVals, subDec128Str);
                break;
            default:
                // Unsupported data-types left as-is
                leftZero = left;
                rightZero = right;
        }
        builder->CreateBr(nextInst);
        builder->SetInsertPoint(nextInst);
        int numberOfPaths = 2;
        *leftPhi = builder->CreatePHI(left->getType(), numberOfPaths, "iftmp");
        *rightPhi = builder->CreatePHI(right->getType(), numberOfPaths, "iftmp");
        (*leftPhi)->addIncoming(leftZero, nullBlock);
        (*leftPhi)->addIncoming(left, incomingBlock);
        (*rightPhi)->addIncoming(rightZero, nullBlock);
        (*rightPhi)->addIncoming(right, incomingBlock);
    }
    if (op == DIV || op == MOD) {
        DivExprNullHelper(binaryExpr, left, right, leftIsNull, rightIsNull, leftPhi, rightPhi);
    }
}

void ExpressionCodeGen::DivExprNullHelper(const BinaryExpr *binaryExpr, Value *left, Value *right, Value *leftIsNull,
    Value *rightIsNull, PHINode **leftPhi, PHINode **rightPhi)
{
    BasicBlock *incomingBlock, *nullBlock, *nextInst;
    Value *nullCond, *leftZero, *rightOne;

    std::vector<Value *> argLeftVals { left, left, this->codegenContext->executionContext };
    std::vector<Value *> argRightVals { right, right, this->codegenContext->executionContext };
    incomingBlock = builder->GetInsertBlock();
    nullBlock = BasicBlock::Create(*context, "nullBlock", builder->GetInsertBlock()->getParent());
    nextInst = BasicBlock::Create(*context, "nextInst", builder->GetInsertBlock()->getParent());
    nullCond = builder->CreateOr(leftIsNull, rightIsNull);
    builder->CreateCondBr(nullCond, nullBlock, nextInst);
    builder->SetInsertPoint(nullBlock);
    switch (binaryExpr->left->GetExprDataType()) {
        case INT32D:
            leftZero = builder->CreateSub(left, left);
            rightOne = builder->CreateSub(builder->CreateAdd(right, CreateConstantInt(1)), right);
            break;
        case INT64D:
            leftZero = builder->CreateSub(left, left);
            rightOne = builder->CreateSub(builder->CreateAdd(right, CreateConstantLong(1)), right);
            break;
        case DOUBLED:
            leftZero = builder->CreateFSub(left, left);
            rightOne = builder->CreateFSub(builder->CreateFAdd(right, CreateConstantDouble(1.0)), right);
            break;
        case DECIMAL128D:
            leftZero = builder->CreateCall(module->getFunction("Sub_decimal128"), argLeftVals, "Sub_decimal128");
            rightOne = builder->CreateCall(module->getFunction("Div_decimal128"), argRightVals, "Div_decimal128");
            break;
        default:
            // Unsupported data-types left as-is
            leftZero = left;
            rightOne = right;
    }
    builder->CreateBr(nextInst);
    builder->SetInsertPoint(nextInst);
    int numberOfPaths = 2;
    *leftPhi = builder->CreatePHI(left->getType(), numberOfPaths, "iftmp");
    *rightPhi = builder->CreatePHI(right->getType(), numberOfPaths, "iftmp");
    (*leftPhi)->addIncoming(leftZero, nullBlock);
    (*leftPhi)->addIncoming(left, incomingBlock);
    (*rightPhi)->addIncoming(rightOne, nullBlock);
    (*rightPhi)->addIncoming(right, incomingBlock);
}

// Helper methods to parse binary expressions
Value *ExpressionCodeGen::BinaryExprIntHelper(const BinaryExpr *binaryExpr, Value *left, Value *right,
    Value *leftIsNull, Value *rightIsNull)
{
    PHINode *leftPhi, *rightPhi;
    Value *isNeitherNull;
    BinaryExprNullHelper(binaryExpr, left, right, leftIsNull, rightIsNull, &leftPhi, &rightPhi, &isNeitherNull);
    switch (binaryExpr->op) {
        case LT:
            return builder->CreateAnd(isNeitherNull, builder->CreateICmpSLT(left, right, "relational_lt"));
        case GT:
            return builder->CreateAnd(isNeitherNull, builder->CreateICmpSGT(left, right, "relational_gt"));
        case LTE:
            return builder->CreateAnd(isNeitherNull, builder->CreateICmpSLE(left, right, "relational_le"));
        case GTE:
            return builder->CreateAnd(isNeitherNull, builder->CreateICmpSGE(left, right, "relational_ge"));
        case EQ:
            return builder->CreateAnd(isNeitherNull, builder->CreateICmpEQ(left, right, "relational_eq"));
        case NEQ:
            return builder->CreateAnd(isNeitherNull,
                builder->CreateNot(builder->CreateICmpEQ(left, right), "relational_neq"));
        case ADD:
            return builder->CreateAdd(leftPhi, rightPhi, "arithmetic_add");
        case SUB:
            return builder->CreateSub(leftPhi, rightPhi, "arithmetic_sub");
        case MUL:
            return builder->CreateMul(leftPhi, right, "arithmetic_mul");
        case DIV:
            return builder->CreateSDiv(leftPhi, rightPhi, "arithmetic_div");
        case MOD:
            return builder->CreateSRem(leftPhi, rightPhi, "arithmetic_mod");
        default:
            std::cout << "Unsupported int/long binary operator " << binaryExpr->op << std::endl;
            return nullptr;
    }
}

Value *ExpressionCodeGen::BinaryExprDoubleHelper(const BinaryExpr *binaryExpr, Value *left, Value *right,
    Value *leftIsNull, Value *rightIsNull)
{
    PHINode *leftPhi, *rightPhi;
    Value *isNeitherNull;
    BinaryExprNullHelper(binaryExpr, left, right, leftIsNull, rightIsNull, &leftPhi, &rightPhi, &isNeitherNull);
    switch (binaryExpr->op) {
        case LT:
            return builder->CreateAnd(isNeitherNull, builder->CreateFCmpULT(left, right, "frelational_lt"));
        case GT:
            return builder->CreateAnd(isNeitherNull, builder->CreateFCmpUGT(left, right, "frelational_gt"));
        case LTE:
            return builder->CreateAnd(isNeitherNull, builder->CreateFCmpULE(left, right, "frelational_le"));
        case GTE:
            return builder->CreateAnd(isNeitherNull, builder->CreateFCmpUGE(left, right, "frelational_ge"));
        case EQ:
            return builder->CreateAnd(isNeitherNull, builder->CreateFCmpUEQ(left, right, "farithmetic_eq"));
        case NEQ:
            return builder->CreateAnd(isNeitherNull,
                builder->CreateNot(builder->CreateFCmpUEQ(left, right, "farithmetic_neq")));
        case ADD:
            return builder->CreateFAdd(leftPhi, rightPhi, "farithmetic_add");
        case SUB:
            return builder->CreateFSub(leftPhi, rightPhi, "farithmetic_sub");
        case MUL:
            return builder->CreateFMul(leftPhi, right, "farithmetic_mul");
        case DIV:
            return builder->CreateFDiv(leftPhi, rightPhi, "farithmetic_div");
        default:
            std::cout << "Unsupported double binary operator " << binaryExpr->op << std::endl;
            return nullptr;
    }
}

Value *ExpressionCodeGen::BinaryExprStringHelper(const BinaryExpr *binaryExpr, Value *leftVal, Value *leftLen,
    Value *rightVal, Value *rightLen, Value *leftIsNull, Value *rightIsNull)
{
    PHINode *leftPhi, *rightPhi;
    Value *isNeitherNull;
    BinaryExprNullHelper(binaryExpr, leftVal, rightVal, leftIsNull, rightIsNull, &leftPhi, &rightPhi, &isNeitherNull);
    switch (binaryExpr->op) {
        case LT:
            return builder->CreateAnd(isNeitherNull,
                builder->CreateICmpSLT(this->StringCmp(leftVal, leftLen, rightVal, rightLen), CreateConstantInt(0)));
        case GT:
            return builder->CreateAnd(isNeitherNull,
                builder->CreateICmpSGT(this->StringCmp(leftVal, leftLen, rightVal, rightLen), CreateConstantInt(0)));
        case LTE:
            return builder->CreateAnd(isNeitherNull,
                builder->CreateICmpSLE(this->StringCmp(leftVal, leftLen, rightVal, rightLen), CreateConstantInt(0)));
        case GTE:
            return builder->CreateAnd(isNeitherNull,
                builder->CreateICmpSGE(this->StringCmp(leftVal, leftLen, rightVal, rightLen), CreateConstantInt(0)));
        case EQ:
            return builder->CreateAnd(isNeitherNull,
                builder->CreateICmpEQ(this->StringCmp(leftVal, leftLen, rightVal, rightLen), CreateConstantInt(0)));
        case NEQ:
            return builder->CreateAnd(isNeitherNull,
                builder->CreateICmpNE(this->StringCmp(leftVal, leftLen, rightVal, rightLen), CreateConstantInt(0)));
        default:
            std::cout << "Unsupported string binary operator " << binaryExpr->op << std::endl;
            return nullptr;
    }
}

Value *ExpressionCodeGen::BinaryExprDecimalHelper(const BinaryExpr *binaryExpr, Value *left, Value *right,
    Value *leftIsNull, Value *rightIsNull)
{
    PHINode *leftPhi, *rightPhi;
    Value *isNeitherNull;
    BinaryExprNullHelper(binaryExpr, left, right, leftIsNull, rightIsNull, &leftPhi, &rightPhi, &isNeitherNull);
    std::vector<Value *> argVals { leftPhi, rightPhi, this->codegenContext->executionContext };

    switch (binaryExpr->op) {
        case LT:
            return builder->CreateAnd(isNeitherNull,
                builder->CreateICmpSLT(this->Decimal128Cmp(*left, *right), CreateConstantInt(0)));
        case GT:
            return builder->CreateAnd(isNeitherNull,
                builder->CreateICmpSGT(this->Decimal128Cmp(*left, *right), CreateConstantInt(0)));
        case LTE:
            return builder->CreateAnd(isNeitherNull,
                builder->CreateICmpSLE(this->Decimal128Cmp(*left, *right), CreateConstantInt(0)));
        case GTE:
            return builder->CreateAnd(isNeitherNull,
                builder->CreateICmpSGE(this->Decimal128Cmp(*left, *right), CreateConstantInt(0)));
        case EQ:
            return builder->CreateAnd(isNeitherNull,
                builder->CreateICmpEQ(this->Decimal128Cmp(*left, *right), CreateConstantInt(0)));
        case NEQ:
            return builder->CreateAnd(isNeitherNull,
                builder->CreateICmpNE(this->Decimal128Cmp(*left, *right), CreateConstantInt(0)));
        case ADD:
            return builder->CreateCall(module->getFunction(addDec128Str), argVals, addDec128Str);
        case SUB:
            return builder->CreateCall(module->getFunction(subDec128Str), argVals, subDec128Str);
        case MUL:
            return builder->CreateCall(module->getFunction(mulDec128Str), argVals, mulDec128Str);
        case DIV:
            return builder->CreateCall(module->getFunction(divDec128Str), argVals, divDec128Str);
        default:
            std::cout << "Unsupported string binary operator " << binaryExpr->op << std::endl;
            return nullptr;
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

bool ExpressionCodeGen::InitializeCodegenContext(iterator_range<llvm::Function::arg_iterator> args)
{
    this->codegenContext = std::make_unique<CodegenContext>();
    for (auto &arg : args) {
        auto argName = arg.getName().str();
        if (argName == "data") {
            codegenContext->data = &arg;
        } else if (argName == "nullBitmap") {
            codegenContext->nullBitmap = &arg;
        } else if (argName == "offsets") {
            codegenContext->offsets = &arg;
        } else if (argName == "rowIdx") {
            codegenContext->rowIdx = &arg;
        } else if (argName == "dataLength" || argName == "isNullPtr") {
            continue;
        } else if (argName == "executionContext") {
            codegenContext->executionContext = &arg;
        } else if (argName == "dictionaryVectors") {
            codegenContext->dictionaryVectors = &arg;
        } else {
            LLVM_DEBUG_LOG("Invalid argument %s", argName.c_str());
            return false;
        }
    }

    codegenContext->print = module->getOrInsertFunction("printf",
        FunctionType::get(IntegerType::getInt32Ty(*context), PointerType::get(Type::getInt8Ty(*context), 0), true));

    return true;
}

llvm::Function *ExpressionCodeGen::CreateFunction()
{
    int32_t argsSize = 8;
    std::vector<Type *> args;
    args.reserve(argsSize);
    // Values in args vector follow the format:
    // value*, bitmap*, offset*, rowIdx, outputLength*, executionContext, isNullPtr
    args.push_back(Type::getInt64PtrTy(*context));
    args.push_back(Type::getInt64PtrTy(*context));
    args.push_back(Type::getInt64PtrTy(*context));
    args.push_back(Type::getInt32Ty(*context));
    args.push_back(Type::getInt32PtrTy(*context));
    args.push_back(Type::getInt64Ty(*context));
    args.push_back(Type::getInt64PtrTy(*context));
    args.push_back(Type::getInt1PtrTy(*context));
#ifdef DEBUG_LLVM
    std::cout << "exprtree: ";
    ExprPrinter p;
    expr->Accept(p);
    std::cout << std::endl;
#endif
    FunctionType *prototype = FunctionType::get(GetFunctionReturnType(expr->GetExprDataType()), args, false);
    func = llvm::Function::Create(prototype, llvm::Function::ExternalLinkage, funcName, module.get());

    std::string argNames[] = {
        "data", "nullBitmap", "offsets", "rowIdx",
        "dataLength", "executionContext", "dictionaryVectors", "isNullPtr"
    };
    int32_t idx = 0;
    for (auto &arg : func->args()) {
        arg.setName(argNames[idx]);
        idx++;
    }

    BasicBlock *body = BasicBlock::Create(*context, "CREATED_FUNC_BODY", func);
    builder->SetInsertPoint(body);

    if (!InitializeCodegenContext(func->args())) {
        return nullptr;
    }

    // Generate code
    auto result = VisitExpr(*expr);
    if (result->data == nullptr) {
        return nullptr;
    }
    int32_t outputLengthIndex = EXPRFUNC_OUT_LENGTH_ARG_INDEX;
    // Update final output Length
    if (result->length != nullptr) {
        Argument *outputLength = func->getArg(outputLengthIndex);
        Value *lengthGep = builder->CreateGEP(outputLength, this->CreateConstantInt(0), "OUTPUT_LENGTH_ADDRESS");
        builder->CreateStore(result->length, lengthGep);
    }

    builder->CreateStore(result->isNull, func->getArg(EXPRFUNC_OUT_IS_NULL_INDEX));

    // cast char* to int64 for output
    if (expr->GetExprDataType() == DataType::VARCHARD) {
        result->data = builder->CreatePtrToInt(result->data, Type::getInt64Ty(*context));
    }
    // Return value
    builder->CreateRet(result->data);
    verifyFunction(*func);
    return func;
}

Value *ExpressionCodeGen::GetIntToPtr(const DataExpr &dExpr, Value *elementAddr)
{
    Value *elementPtr = nullptr;
    const DataExpr *dEx = &dExpr;
    // Convert the column address to array of proper datatype.
    switch (dEx->GetExprDataType()) {
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
        case DataType::CHARD:
        case DataType::VARCHARD:
            elementPtr = builder->CreateIntToPtr(elementAddr, Type::getInt8PtrTy(*context));
            break;
        case DataType::DECIMAL128D:
            elementPtr = builder->CreateIntToPtr(elementAddr, Type::getInt64PtrTy(*context));
            break;
        default:
            LLVM_DEBUG_LOG("Unsupported column data type %d", dEx->GetExprDataType());
            elementPtr = builder->CreateIntToPtr(elementAddr, Type::getInt64PtrTy(*context));
            break;
    }
    return elementPtr;
}


CodeGenValue *ExpressionCodeGen::DataExprConstantHelper(const DataExpr &dExpr)
{
    const DataExpr *dEx = &dExpr;
    CodeGenValue *codeGenValue = nullptr;
    bool isNullLiteral = dExpr.isNull;
    switch (dEx->GetExprDataType()) {
        case DataType::INT32D: {
            codeGenValue =
                new CodeGenValue(this->CreateConstantInt(dEx->intVal), this->CreateConstantBool(isNullLiteral));
            break;
        }
        case DataType::INT64D: {
            codeGenValue =
                new CodeGenValue(this->CreateConstantLong(dEx->longVal), this->CreateConstantBool(isNullLiteral));
            break;
        }
        case DataType::DOUBLED: {
            codeGenValue =
                new CodeGenValue(this->CreateConstantDouble(dEx->doubleVal), this->CreateConstantBool(isNullLiteral));
            break;
        }
        case DataType::CHARD:
        case DataType::VARCHARD: {
            Constant *strValConst =
                ConstantInt::get(*context, APInt(INT64_VALUE, reinterpret_cast<int64_t>(dEx->stringVal->c_str())));
            Value *strValPtr = ConstantExpr::getIntToPtr(strValConst, Type::getInt8PtrTy(*context));
            Constant *strLenConst =
                ConstantInt::get(*context, APInt(INT32_VALUE, static_cast<int32_t>(dEx->stringVal->length())));
            codeGenValue = new CodeGenValue(strValPtr, this->CreateConstantBool(isNullLiteral), strLenConst);
            break;
        }
        case DataType::BOOLD: {
            codeGenValue =
                new CodeGenValue(this->CreateConstantBool(dEx->boolVal), this->CreateConstantBool(isNullLiteral));
            break;
        }
        case DataType::DECIMAL64D: {
            codeGenValue =
                new CodeGenValue(this->CreateConstantLong(dEx->longVal), this->CreateConstantBool(isNullLiteral));
            break;
        }
        case DataType::DECIMAL128D: {
            int32_t length = 2;
            Decimal128 decValue = dEx->longVal;
            auto decimal = std::make_unique<int64_t[]>(length).release();

            decimal[0] = decValue.LowBits();
            decimal[1] = decValue.HighBits();

            Constant *addr = ConstantInt::get(*context, APInt(INT64_VALUE, reinterpret_cast<int64_t>(decimal)));
            codeGenValue = new CodeGenValue(addr, this->CreateConstantBool(isNullLiteral));
            break;
        }
        case DataType::UNKNOWND: {
            codeGenValue = new CodeGenValue(this->CreateConstantInt(dEx->intVal), this->CreateConstantBool(true));
            break;
        }
        default: {
            LLVM_DEBUG_LOG("Unsupported data type in Data Expr %d", dEx->GetExprDataType());
            codeGenValue = new CodeGenValue(this->CreateConstantBool(dEx->boolVal), this->CreateConstantBool(false));
            break;
        }
    }
    return codeGenValue;
}

Value *ExpressionCodeGen::GetDictionaryVectorValue(DataType vectorType, Value *rowIdx, Value *dictionaryVectorPtr,
    AllocaInst *&lengthAllocaInst)
{
    llvm::Function *dictionaryFunc = nullptr;
    switch (vectorType) {
        case omniruntime::expressions::INT32D:
            dictionaryFunc = module->getFunction(dictionaryGetIntStr);
            break;
        case omniruntime::expressions::INT64D:
            dictionaryFunc = module->getFunction(dictionaryGetLongStr);
            break;
        case omniruntime::expressions::DECIMAL128D:
            dictionaryFunc = module->getFunction(dictionaryGetDecimalStr);
            break;
        case omniruntime::expressions::DOUBLED:
            dictionaryFunc = module->getFunction(dictionaryGetDoubleStr);
            break;
        case omniruntime::expressions::BOOLD:
            dictionaryFunc = module->getFunction(dictionaryGetBooleanStr);
            break;
        case omniruntime::expressions::CHARD:
        case omniruntime::expressions::VARCHARD:
            dictionaryFunc = module->getFunction(dictionaryGetVarcharStr);
            break;
        default:
            LLVM_DEBUG_LOG("Unsupported dictionary value type: %d", vectorType);
            return nullptr;
    }
    std::vector<Value *> funcArgs;
    funcArgs.push_back(dictionaryVectorPtr);
    funcArgs.push_back(rowIdx);

    if (IsStringDataType(vectorType)) {
        lengthAllocaInst = builder->CreateAlloca(Type::getInt32Ty(*context), nullptr, "varchar_length");
        builder->CreateStore(CreateConstantInt(0), lengthAllocaInst);
        funcArgs.push_back(lengthAllocaInst);
    } else if (vectorType == DataType::DECIMAL128D) {
        funcArgs.push_back(this->codegenContext->executionContext);
    }

    auto call = builder->CreateCall(dictionaryFunc, funcArgs, "get_dictionary_value");
    InlineFunctionInfo inlineFunctionInfo;
    auto inlinedFunction = llvm::InlineFunction(*call, inlineFunctionInfo);
    return call;
}

void ExpressionCodeGen::Visit(const DataExpr &dExpr)
{
    const DataExpr *dEx = &dExpr;

    if (dEx->isColumn) {
        Value *rowIdx = this->codegenContext->rowIdx;
        Value *vecBatch = this->codegenContext->data;
        Value *bitmap = this->codegenContext->nullBitmap;
        Value *offsets = this->codegenContext->offsets;
        Value *dictionaryVectors = this->codegenContext->dictionaryVectors;

        Value *colIdx = this->CreateConstantInt(dEx->colVal);
        // Find address of this column in the addresses array argument.
        Value *gep = builder->CreateGEP(vecBatch, colIdx);
        Value *length = nullptr;

        auto dictionaryVectorGEP = builder->CreateGEP(dictionaryVectors, colIdx);
        Value *dictionaryVectorPtr = builder->CreateLoad(dictionaryVectorGEP);
        auto condition = builder->CreateIsNotNull(dictionaryVectorPtr);

        BasicBlock *trueBlock = BasicBlock::Create(*context, "DICTIONARY_NOT_NULL", func);
        BasicBlock *falseBlock = BasicBlock::Create(*context, "DICTIONARY_IS_NULL");
        BasicBlock *mergeBlock = BasicBlock::Create(*context, "ifcont");

        builder->CreateCondBr(condition, trueBlock, falseBlock);

        // If dictionary vector is present, call DictionaryVector methods
        // to get encoded values and length if varchar type
        builder->SetInsertPoint(trueBlock);

        AllocaInst *lengthAllocaInst = nullptr;
        Value *dictionaryValue =
            this->GetDictionaryVectorValue(dExpr.GetExprDataType(), rowIdx, dictionaryVectorPtr, lengthAllocaInst);
        if (dictionaryValue == nullptr) {
            return;
        }

        Value *dictionaryLength = nullptr;
        if (IsStringDataType(dEx->GetExprDataType())) {
            dictionaryLength = builder->CreateLoad(lengthAllocaInst, "varchar_length");
        }

        builder->CreateBr(mergeBlock);
        trueBlock = builder->GetInsertBlock();
        func->getBasicBlockList().push_back(falseBlock);

        // If dictionary vector is not present, get vector values
        // using valuesAddress and length using offsets if varchar type
        builder->SetInsertPoint(falseBlock);
        // Load the address value.
        Value *elementAddr = builder->CreateLoad(gep);

        Value *elementPtr = GetIntToPtr(dExpr, elementAddr);
        Value *dataValue = nullptr;
        if (IsStringDataType(dEx->GetExprDataType())) {
            // Get offset for varchar
            auto offsetsGEP = builder->CreateGEP(offsets, colIdx);
            Value *offsetPtr = builder->CreateLoad(offsetsGEP);
            offsetPtr = builder->CreateIntToPtr(offsetPtr, Type::getInt32PtrTy(*context));
            auto colOffsetGEP = builder->CreateGEP(offsetPtr, rowIdx);
            Value *startOffset = builder->CreateLoad(colOffsetGEP);
            colOffsetGEP = builder->CreateGEP(offsetPtr, builder->CreateAdd(rowIdx, CreateConstantInt(1)));
            Value *endOffset = builder->CreateLoad(colOffsetGEP);
            // Get length for varchar
            length = builder->CreateSub(endOffset, startOffset);
            // Find the address of the row to be processed.
            dataValue = builder->CreateGEP(elementPtr, startOffset);
        } else {
            // Find the address of the row to be processed.
            gep = builder->CreateGEP(elementPtr, rowIdx);
            // Value to be processed.
            dataValue = builder->CreateLoad(gep);
        }

        builder->CreateBr(mergeBlock);
        falseBlock = builder->GetInsertBlock();

        // Get merged data value and length
        int32_t numReservedValues = 2;
        Type *phiType = this->ToLlvmType(dEx->GetExprDataType());
        func->getBasicBlockList().push_back(mergeBlock);
        builder->SetInsertPoint(mergeBlock);

        PHINode *phiValue = builder->CreatePHI(phiType, numReservedValues, "iftmp");
        phiValue->addIncoming(dictionaryValue, trueBlock);
        phiValue->addIncoming(dataValue, falseBlock);

        // Length is only valid for varchar type
        PHINode *phiLength = nullptr;
        if (IsStringDataType(dEx->GetExprDataType())) {
            phiLength = builder->CreatePHI(Type::getInt32Ty(*context), numReservedValues, "length");
            phiLength->addIncoming(dictionaryLength, trueBlock);
            phiLength->addIncoming(length, falseBlock);
        }

        // Get isNull value
        auto bitmapGEP = builder->CreateGEP(bitmap, colIdx);
        Value *bitmapValue = builder->CreateLoad(bitmapGEP);
        bitmapValue = builder->CreateIntToPtr(bitmapValue, Type::getInt1PtrTy(*context));
        bitmapGEP = builder->CreateGEP(bitmapValue, rowIdx);
        bitmapValue = builder->CreateLoad(bitmapGEP);

        this->value.reset(new CodeGenValue(phiValue, bitmapValue, phiLength));
        return;
    }

    this->value.reset(DataExprConstantHelper(dExpr));
}

CodeGenValuePtr CreateInvalidCodeGenValue()
{
    return make_shared<CodeGenValue>(nullptr, nullptr, nullptr);
}

void ExpressionCodeGen::Visit(const BinaryExpr &binaryExpr)
{
    const BinaryExpr *bExpr = &binaryExpr;

    if (bExpr->left->GetType() == ExprType::DATA_E || bExpr->right->GetType() == ExprType::DATA_E) {
        DataType biggerType = std::max(bExpr->left->GetExprDataType(), bExpr->right->GetExprDataType());
        bExpr->left->dataType = biggerType;
        bExpr->right->dataType = biggerType;
    }
    CodeGenValuePtr left = VisitExpr(*(bExpr->left));
    if (!left->IsValidValue()) {
        this->value = CreateInvalidCodeGenValue();
        return;
    }
    Value *leftValue = left->data;
    Value *leftLen = left->length;
    Value *leftNull = left->isNull;
    CodeGenValuePtr right = VisitExpr(*(bExpr->right));
    if (!right->IsValidValue()) {
        this->value = CreateInvalidCodeGenValue();
        return;
    }
    Value *rightValue = right->data;
    Value *rightLen = right->length;
    Value *rightNull = right->isNull;

    if (bExpr->op == omniruntime::expressions::Operator::AND) {
        this->value = make_shared<CodeGenValue>(builder->CreateAnd(builder->CreateNot(leftNull), builder->CreateAnd(
            builder->CreateNot(rightNull), builder->CreateAnd(leftValue, rightValue, "logical_and"))),
            builder->CreateOr(leftNull, rightNull));
        return;
    }
    if (bExpr->op == omniruntime::expressions::Operator::OR) {
        CreateOrExprHelper(leftValue, leftNull, rightValue, rightNull);
        return;
    }

    if (bExpr->left->GetExprDataType() == DataType::INT32D || bExpr->left->GetExprDataType() == DataType::INT64D) {
        this->value =
            make_shared<CodeGenValue>(this->BinaryExprIntHelper(bExpr, leftValue, rightValue, leftNull, rightNull),
            builder->CreateOr(leftNull, rightNull));
        return;
    } else if (bExpr->left->GetExprDataType() == DOUBLED) {
        this->value =
            make_shared<CodeGenValue>(this->BinaryExprDoubleHelper(bExpr, leftValue, rightValue, leftNull, rightNull),
            builder->CreateOr(leftNull, rightNull));
        return;
    } else if (IsStringDataType(bExpr->left->GetExprDataType())) {
        this->value = make_shared<CodeGenValue>(
            this->BinaryExprStringHelper(bExpr, leftValue, leftLen, rightValue, rightLen, leftNull, rightNull),
            builder->CreateOr(leftNull, rightNull));
        return;
    } else if (bExpr->left->GetExprDataType() == DECIMAL128D) {
        this->value =
            make_shared<CodeGenValue>(this->BinaryExprDecimalHelper(bExpr, leftValue, rightValue, leftNull, rightNull),
            builder->CreateOr(leftNull, rightNull));
        return;
    }
    LLVM_DEBUG_LOG("Unsupported binary operator %d", bExpr->op);
    this->value = CreateInvalidCodeGenValue();
}

void ExpressionCodeGen::CreateOrExprHelper(llvm::Value *leftValue, llvm::Value *leftNull, llvm::Value *rightValue,
    llvm::Value *rightNull)
{
    Value *trueValue = CreateConstantBool(true);
    Value *falseValue = CreateConstantBool(false);

    AllocaInst *resultValuePtr = builder->CreateAlloca(Type::getInt1Ty(*context), nullptr, "result_value");
    AllocaInst *resultNullPtr = builder->CreateAlloca(Type::getInt1Ty(*context), nullptr, "result_null");

    BasicBlock *neitherNullBlock = BasicBlock::Create(*context, "NEITHER_NULL_BLOCK", func);
    BasicBlock *eitherNullBlock = BasicBlock::Create(*context, "EITHER_NULL_BLOCK");
    BasicBlock *mergeBlock = BasicBlock::Create(*context, "END_OF_OR");

    auto isNeitherNull = builder->CreateNot(builder->CreateOr(leftNull, rightNull));

    builder->CreateCondBr(isNeitherNull, neitherNullBlock, eitherNullBlock);
    builder->SetInsertPoint(neitherNullBlock);

    builder->CreateStore(builder->CreateOr(leftValue, rightValue), resultValuePtr);
    builder->CreateStore(falseValue, resultNullPtr);
    builder->CreateBr(mergeBlock);

    func->getBasicBlockList().push_back(eitherNullBlock);
    builder->SetInsertPoint(eitherNullBlock);

    BasicBlock *leftNotNullBlock = BasicBlock::Create(*context, "LEFT_NOT_NULL_BLOCK", func);
    BasicBlock *leftNullBlock = BasicBlock::Create(*context, "LEFT_NULL_BLOCK");

    builder->CreateCondBr(builder->CreateNot(leftNull), leftNotNullBlock, leftNullBlock);
    builder->SetInsertPoint(leftNotNullBlock);
    builder->CreateStore(leftValue, resultValuePtr);
    builder->CreateStore(falseValue, resultNullPtr);
    builder->CreateBr(mergeBlock);

    func->getBasicBlockList().push_back(leftNullBlock);
    builder->SetInsertPoint(leftNullBlock);

    BasicBlock *rightNotNullBlock = BasicBlock::Create(*context, "RIGHT_NOT_NULL_BLOCK", func);
    BasicBlock *bothNullBlock = BasicBlock::Create(*context, "BOTH_NULL_BLOCK");

    builder->CreateCondBr(builder->CreateNot(rightNull), rightNotNullBlock, bothNullBlock);
    builder->SetInsertPoint(rightNotNullBlock);
    builder->CreateStore(rightValue, resultValuePtr);
    builder->CreateStore(falseValue, resultNullPtr);
    builder->CreateBr(mergeBlock);

    func->getBasicBlockList().push_back(bothNullBlock);
    builder->SetInsertPoint(bothNullBlock);
    builder->CreateStore(rightValue, resultValuePtr);
    builder->CreateStore(trueValue, resultNullPtr);
    builder->CreateBr(mergeBlock);

    func->getBasicBlockList().push_back(mergeBlock);
    builder->SetInsertPoint(mergeBlock);
    this->value = make_shared<CodeGenValue>(builder->CreateLoad(resultValuePtr), builder->CreateLoad(resultNullPtr));
}

void ExpressionCodeGen::Visit(const UnaryExpr &uExpr)
{
    auto val = VisitExpr(*(uExpr.exp));
    if (!val->IsValidValue()) {
        this->value = CreateInvalidCodeGenValue();
        return;
    }

    switch (uExpr.op) {
        case NOT: {
            Value *notValue = builder->CreateNot(val->data, "logical_not");
            this->value = make_shared<CodeGenValue>(notValue, val->isNull);
            break;
        }
        default: {
            // ignore the unary operator if it is invalid
            this->value = make_shared<CodeGenValue>(val->data, val->isNull);
            break;
        }
    }
}

void ExpressionCodeGen::Visit(const IfExpr &ifExpr)
{
    Expr *cond = ifExpr.condition;
    Expr *ifTrue = ifExpr.trueExpr;
    Expr *ifFalse = ifExpr.falseExpr;

    BasicBlock *trueBlock = BasicBlock::Create(*context, "TRUE_BLOCK", func);
    BasicBlock *falseBlock = BasicBlock::Create(*context, "FALSE_BLOCK");
    BasicBlock *mergeBlock = BasicBlock::Create(*context, "ifcont");

    CodeGenValuePtr evCond = VisitExpr(*cond);
    if (!evCond->IsValidValue()) {
        this->value = CreateInvalidCodeGenValue();
        return;
    }

    // If cond evaluates to true, control flow goes to trueBlock, returning evTrue
    // Otherwise goes to falseBlock and returns evFalse
    builder->CreateCondBr(builder->CreateAnd(builder->CreateNot(evCond->isNull), evCond->data), trueBlock, falseBlock);
    builder->SetInsertPoint(trueBlock);
    auto evTrue = VisitExpr(*ifTrue);
    if (!evTrue->IsValidValue()) {
        this->value = CreateInvalidCodeGenValue();
        return;
    }
    Value *evTrueValue = evTrue->data;
    Value *evTrueLength = evTrue->length;
    Value *evTrueNull = evTrue->isNull;
    builder->CreateBr(mergeBlock);
    // Codegen of 'true' can change the current block, update trueBlock for the PHI.
    trueBlock = builder->GetInsertBlock();

    func->getBasicBlockList().push_back(falseBlock);
    builder->SetInsertPoint(falseBlock);
    auto evFalse = VisitExpr(*ifFalse);
    if (!evFalse->IsValidValue()) {
        this->value = CreateInvalidCodeGenValue();
        return;
    }
    Value *evFalseValue = evFalse->data;
    Value *evFalseLength = evFalse->length;
    Value *evFalseNull = evFalse->isNull;
    builder->CreateBr(mergeBlock);
    // Codegen of 'false' can change the current block, update falseBlock for the PHI.
    falseBlock = builder->GetInsertBlock();
    int32_t numReservedValues = 2;
    // Emit merge block.
    Type *phiType = this->ToLlvmType(ifExpr.GetExprDataType());
    func->getBasicBlockList().push_back(mergeBlock);
    builder->SetInsertPoint(mergeBlock);
    PHINode *pn = builder->CreatePHI(phiType, numReservedValues, "iftmp");
    PHINode *phiNull = builder->CreatePHI(evTrueNull->getType(), numReservedValues, "iftmpNull");

    pn->addIncoming(evTrueValue, trueBlock);
    pn->addIncoming(evFalseValue, falseBlock);
    phiNull->addIncoming(evTrueNull, trueBlock);
    phiNull->addIncoming(evFalseNull, falseBlock);

    PHINode *lengthPhi = nullptr;
    if (IsStringDataType(ifExpr.GetExprDataType())) {
        lengthPhi = builder->CreatePHI(Type::getInt32Ty(*context), numReservedValues, "length");
        lengthPhi->addIncoming(evTrueLength, trueBlock);
        lengthPhi->addIncoming(evFalseLength, falseBlock);
    }

    this->value = make_shared<CodeGenValue>(pn, phiNull, lengthPhi);
}

void ExpressionCodeGen::Visit(const InExpr &inExpr)
{
    const InExpr *iExpr = &inExpr;
    Expr *toCompare = iExpr->arguments[0];
    auto valueToCompare = VisitExpr(*toCompare);
    CodeGenValuePtr argiValue;
    Value *inArray = llvm::ConstantInt::get(*context, APInt(1, 0));
    Value *isNull = llvm::ConstantInt::get(*context, APInt(1, 0));
    // Handle types correctly
    for (int i = 1; i < iExpr->arguments.size(); i++) {
        // initialize tmpCmpData
        Value *tmpCmpData = this->CreateConstantBool(false);
        Value *tmpCmpNull = this->CreateConstantBool(false);

        switch (iExpr->arguments[0]->dataType) {
            case INT32D:
            case INT64D: {
                argiValue = VisitExpr(*(iExpr->arguments[i]));
                if (!argiValue->IsValidValue()) {
                    this->value = CreateInvalidCodeGenValue();
                    return;
                }
                tmpCmpData = builder->CreateAnd(builder->CreateNot(valueToCompare->isNull),
                    builder->CreateAnd(builder->CreateNot(argiValue->isNull),
                    builder->CreateICmpEQ(valueToCompare->data, argiValue->data)));
                tmpCmpNull = builder->CreateOr(valueToCompare->isNull, argiValue->isNull);
                break;
            }
            case DOUBLED: {
                argiValue = VisitExpr(*(iExpr->arguments[i]));
                if (!argiValue->IsValidValue()) {
                    this->value = CreateInvalidCodeGenValue();
                    return;
                }
                tmpCmpData = builder->CreateAnd(builder->CreateNot(valueToCompare->isNull),
                    builder->CreateAnd(builder->CreateNot(argiValue->isNull),
                    builder->CreateFCmpOEQ(valueToCompare->data, argiValue->data)));
                tmpCmpNull = builder->CreateOr(valueToCompare->isNull, argiValue->isNull);
                break;
            }
            case CHARD:
            case VARCHARD: {
                argiValue = VisitExpr(*(iExpr->arguments[i]));
                if (!argiValue->IsValidValue()) {
                    this->value = CreateInvalidCodeGenValue();
                    return;
                }
                tmpCmpData = builder->CreateAnd(builder->CreateNot(valueToCompare->isNull),
                    builder->CreateAnd(builder->CreateNot(argiValue->isNull), builder->CreateICmpEQ(
                    this->StringCmp(valueToCompare->data, valueToCompare->length, argiValue->data, this->value->length),
                    CreateConstantInt(0))));
                tmpCmpNull = builder->CreateOr(valueToCompare->isNull, argiValue->isNull);
                break;
            }
            default: {
                LLVM_DEBUG_LOG("Unsupported data type in IN expr %d", iExpr->arguments[0]->dataType);
                this->value = CreateInvalidCodeGenValue();
                return;
            }
        }

        inArray = builder->CreateAnd(builder->CreateNot(tmpCmpNull), builder->CreateOr(inArray, tmpCmpData));
        isNull = builder->CreateOr(isNull, tmpCmpNull);
    }
    this->value = make_shared<CodeGenValue>(inArray, isNull);
}

void ExpressionCodeGen::Visit(const BetweenExpr &btExpr)
{
    const BetweenExpr *bExpr = &btExpr;
    DataType biggerType = std::max(std::max(bExpr->lowerBound->GetExprDataType(), bExpr->upperBound->GetExprDataType()),
        bExpr->value->GetExprDataType());
    bExpr->lowerBound->dataType = biggerType;
    bExpr->upperBound->dataType = biggerType;
    bExpr->value->dataType = biggerType;

    auto val = VisitExpr(*(bExpr->value));
    if (!val->IsValidValue()) {
        this->value = CreateInvalidCodeGenValue();
        return;
    }
    auto valData = val->data;
    auto valLen = val->length;
    auto valNull = val->isNull;
    auto lowerVal = VisitExpr(*(bExpr->lowerBound));
    if (!lowerVal->IsValidValue()) {
        this->value = CreateInvalidCodeGenValue();
        return;
    }
    auto lowerValData = lowerVal->data;
    auto lowerValLen = lowerVal->length;
    auto lowerValNull = lowerVal->isNull;
    auto upperVal = VisitExpr(*(bExpr->upperBound));
    if (!upperVal->IsValidValue()) {
        this->value = CreateInvalidCodeGenValue();
        return;
    }
    auto upperValData = upperVal->data;
    auto upperValLen = upperVal->length;
    auto upperValNull = upperVal->isNull;

    auto isAnyNull = builder->CreateOr(builder->CreateOr(valNull, lowerValNull), upperValNull);
    auto isNeitherNull = builder->CreateNot(isAnyNull);
    Value *cmpLeft, *cmpRight;
    bool supportedType = false;
    if (bExpr->value->GetExprDataType() == DataType::INT32D || bExpr->value->GetExprDataType() == DataType::INT64D) {
        cmpLeft = builder->CreateICmpSLE(lowerValData, valData, "between_cmpleft");
        cmpRight = builder->CreateICmpSLE(valData, upperValData, "between_cmpright");
        supportedType = true;
    } else if (bExpr->value->GetExprDataType() == DOUBLED) {
        cmpLeft = builder->CreateFCmpULE(lowerValData, valData, "between_cmpleft");
        cmpRight = builder->CreateFCmpULE(valData, upperValData, "between_cmpright");
        supportedType = true;
    } else if (IsStringDataType(bExpr->value->GetExprDataType())) {
        cmpLeft =
            builder->CreateICmpSLE(this->StringCmp(lowerValData, lowerValLen, valData, valLen), CreateConstantInt(0));
        cmpRight =
            builder->CreateICmpSLE(this->StringCmp(valData, valLen, upperValData, upperValLen), CreateConstantInt(0));
        supportedType = true;
    } else if (bExpr->value->GetExprDataType() == DECIMAL128D) {
        cmpLeft = builder->CreateICmpSLE(this->Decimal128Cmp(*lowerValData, *valData), CreateConstantInt(0));
        cmpRight = builder->CreateICmpSLE(this->Decimal128Cmp(*valData, *upperValData), CreateConstantInt(0));
        supportedType = true;
    }

    if (supportedType) {
        std::vector<Value *> andValues;
        andValues.push_back(isNeitherNull);
        andValues.push_back(cmpLeft);
        andValues.push_back(cmpRight);
        Value *result = builder->CreateAnd(andValues);
        this->value = make_shared<CodeGenValue>(result, isAnyNull);
        return;
    }

    LLVM_DEBUG_LOG("Error: unsupported data type for between %d", bExpr->value->GetExprDataType());
    this->value = CreateInvalidCodeGenValue();
}

void ExpressionCodeGen::Visit(const CoalesceExpr &cExpr)
{
    Expr *value1Expr = cExpr.value1;
    Expr *value2Expr = cExpr.value2;
    CodeGenValuePtr value1 = VisitExpr(*value1Expr);
    if (!value1->IsValidValue()) {
        this->value = CreateInvalidCodeGenValue();
        return;
    }

    BasicBlock *isNullBlock = BasicBlock::Create(*context, "coalesceVal1IsNull", func);
    BasicBlock *isNotNullBlock = BasicBlock::Create(*context, "coalesceVal1IsNotNull");
    BasicBlock *mergeBlock = BasicBlock::Create(*context, "coalesceCont");

    // If cond evaluates to true, control flow goes to trueBlock, returning evTrue
    // Otherwise goes to falseBlock and returns evFalse
    builder->CreateCondBr(value1->isNull, isNullBlock, isNotNullBlock);

    builder->SetInsertPoint(isNullBlock);
    auto value2 = VisitExpr(*value2Expr);
    if (!value2->IsValidValue()) {
        this->value = CreateInvalidCodeGenValue();
        return;
    }
    Value *value2Data = value2->data;
    Value *value2Length = value2->length;
    builder->CreateBr(mergeBlock);
    // Codegen of 'true' can change the current block, update trueBlock for the PHI.
    isNullBlock = builder->GetInsertBlock();

    func->getBasicBlockList().push_back(isNotNullBlock);
    builder->SetInsertPoint(isNotNullBlock);
    Value *value1Data = value1->data;
    Value *value1Length = value1->length;
    builder->CreateBr(mergeBlock);
    // Codegen of 'false' can change the current block, update falseBlock for the PHI.
    isNotNullBlock = builder->GetInsertBlock();
    int32_t numReservedValues = 2;

    // Emit merge block.
    Type *phiType = this->ToLlvmType(cExpr.GetExprDataType());
    func->getBasicBlockList().push_back(mergeBlock);
    builder->SetInsertPoint(mergeBlock);
    PHINode *pn = builder->CreatePHI(phiType, numReservedValues, "iftmp");
    PHINode *pnNull = builder->CreatePHI(value1->isNull->getType(), numReservedValues, "iftmp");

    PHINode *lengthPhi = nullptr;
    if (IsStringDataType(cExpr.GetExprDataType())) {
        lengthPhi = builder->CreatePHI(Type::getInt32Ty(*context), numReservedValues, "length");
        lengthPhi->addIncoming(value1Length, isNotNullBlock);
        lengthPhi->addIncoming(value2Length, isNullBlock);
    }

    pn->addIncoming(value1Data, isNotNullBlock);
    pn->addIncoming(value2Data, isNullBlock);
    pnNull->addIncoming(value1->isNull, isNotNullBlock);
    pnNull->addIncoming(value2->isNull, isNullBlock);
    this->value = make_shared<CodeGenValue>(pn, pnNull, lengthPhi);
}

void ExpressionCodeGen::Visit(const IsNullExpr &isNullExpr)
{
    Expr *valueExpr = isNullExpr.value;
    auto value = VisitExpr(*valueExpr);
    if (!value->IsValidValue()) {
        this->value = CreateInvalidCodeGenValue();
        return;
    }
    Value *isNullValue = value->isNull;

    Value *result = builder->CreateICmpEQ(isNullValue, CreateConstantBool(true), "isNullCompare");
    this->value = make_shared<CodeGenValue>(result, this->CreateConstantBool(false));
}

// Handles all functions
void ExpressionCodeGen::Visit(const FuncExpr &fExpr)
{
    std::vector<Value *> argVals;
    int numArgs = fExpr.arguments.size();
    CodeGenValuePtr resultPtr;
    Value *isAnyNull = this->CreateConstantBool(false);
    for (int i = 0; i < numArgs; i++) {
        resultPtr = VisitExpr(*(fExpr.arguments[i]));
        argVals.push_back(resultPtr->data);
        isAnyNull = builder->CreateOr(isAnyNull, resultPtr->isNull);
        if (fExpr.dataType == DataType::CHARD && fExpr.funcName.compare("concat") == 0) {
            if (i == 0) {
                argVals.push_back(CreateConstantInt(fExpr.arguments[i]->width));
            }
        }
        if ((IsStringDataType(fExpr.arguments[i]->dataType))) {
            argVals.push_back(this->value->length);
        }

        if (fExpr.funcName == mm3hashStr) {
            argVals.push_back(this->value->isNull);
        }
    }
    Value *ret = nullptr;
    Value *outputLen = nullptr;
    AllocaInst *outputLenPtr = nullptr;
    if (IsStringDataType(fExpr.GetExprDataType())) {
        outputLenPtr = builder->CreateAlloca(Type::getInt32Ty(*context), nullptr, "output_len");
        argVals.push_back(outputLenPtr);
    }
    if (fExpr.function->IsExecutionContextSet()) {
        argVals.push_back(this->codegenContext->executionContext);
    }
    auto f = module->getFunction(fExpr.function->GetFuncID());
    if (f) {
        ret = builder->CreateCall(f, argVals, fExpr.function->GetFuncID());
        InlineFunctionInfo inlineFunctionInfo;
        auto inlinedFunction = llvm::InlineFunction(*((CallInst *)ret), inlineFunctionInfo);
        outputLen = (outputLenPtr == nullptr) ? nullptr : builder->CreateLoad(outputLenPtr);
    } else {
        std::cout << "Unable to parse function " << fExpr.funcName.c_str() << std::endl;
        this->value = make_shared<CodeGenValue>(nullptr, nullptr, nullptr);
        return;
    }
    this->value = make_shared<CodeGenValue>(ret, isAnyNull, outputLen);
}

void ExpressionCodeGen::OptimizeFunctionsAndModule()
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
    fpm->add(createPromoteMemoryToRegisterPass());

    mpm.add(createFunctionInliningPass());
    mpm.add(createPruneEHPass());

    fpm->doInitialization();
    for (auto &F : *module)
        fpm->run(F);
    mpm.run(*module);
}
