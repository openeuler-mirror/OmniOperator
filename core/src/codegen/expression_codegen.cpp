/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Expression code generator
 */
#include "expression_codegen.h"

#include <thread>
#include <chrono>
#include <utility>

#include <llvm/IR/LegacyPassManager.h>
#include <llvm/Passes/PassBuilder.h>
#include <llvm/Support/TargetSelect.h>
#include <llvm/Transforms/Scalar.h>
#include <llvm/Transforms/IPO.h>
#include <llvm/Transforms/Utils.h>
#include <llvm/ExecutionEngine/Orc/LLJIT.h>
#include "vector"
#include "expr_info_extractor.h"
#include "codegen_context.h"
#include "type/decimal128_utils.h"

using namespace llvm;
using namespace orc;
using namespace omniruntime;
using namespace omniruntime::expressions;
using namespace omniruntime::type;
using namespace std;

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

std::vector<Type *> ExpressionCodeGen::GetFunctionArgTypeVector(std::vector<DataTypeId> &params, DataTypeId &retTypeId,
    bool needsContext)
{
    std::vector<Type *> args;
    if (needsContext) {
        args.push_back(llvmTypes->I64Type());
    }
    for (auto type : params) {
        if (type == OMNI_DECIMAL128) {
            // add high and low
            args.push_back(llvmTypes->I64Type());
            args.push_back(llvmTypes->I64Type());
        } else {
            args.push_back(llvmTypes->ToLLVMType(type));
            if (TypeUtil::IsStringType(type)) {
                if (type == OMNI_CHAR) {
                    // Add Type for width support
                    args.push_back(llvmTypes->I32Type());
                }
                // Add Type for Length of the string
                args.push_back(llvmTypes->I32Type());
            }
        }
    }
    // return arguments
    if (TypeUtil::IsStringType(retTypeId)) {
        args.push_back(llvmTypes->I32PtrType());
    } else if (retTypeId == OMNI_DECIMAL128) {
        // Add high and low output pointers
        args.push_back(llvmTypes->I64PtrType());
        args.push_back(llvmTypes->I64PtrType());
    }
    return args;
}

Constant *ExpressionCodeGen::CreateStringConstant(std::string s)
{
    auto charType = Type::getInt8Ty(*context);
    std::vector<llvm::Constant *> chars(s.size());
    for (unsigned int i = 0; i < s.size(); i++) {
        chars[i] = ConstantInt::get(charType, s[i]);
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
    auto stringPtr = llvm::ConstantExpr::getBitCast(globalDeclaration, charType->getPointerTo());
    return stringPtr;
}

/**
 * Usage example: std::vector<Value *> values;
 * values.push_back(value1);
 * values.push_back(value2);
 * PrintValues("LLVM DEBUG: %d, %d\n", values);
 */
void ExpressionCodeGen::PrintValues(std::string format, const std::vector<Value *> &values)
{
    // Return a cast to an i8*
    auto formatPtr = CreateStringConstant(std::move(format));
    std::vector<Value *> args;
    args.push_back(formatPtr);
    for (auto v : values) {
        args.push_back(v);
    }

    builder->CreateCall(codegenContext->print, args, "printfCall");
}

ExpressionCodeGen::ExpressionCodeGen(std::string name, const Expr &cpExpr) : expr(&cpExpr), funcName(std::move(name)) {}

ExpressionCodeGen::~ExpressionCodeGen()
{
    if (rt) {
        eoe(rt->remove());
    }
}

llvm::IRBuilder<> &ExpressionCodeGen::GetIRBuilder()
{
    return *builder;
}

Module &ExpressionCodeGen::GetModule()
{
    return *module;
}

LLVMContext &ExpressionCodeGen::GetContext()
{
    return *context;
}

void ExpressionCodeGen::InitializeCodegenTargets()
{
    llvm::InitializeNativeTarget();
    llvm::InitializeNativeTargetAsmPrinter();
    llvm::InitializeNativeTargetAsmParser();
    llvm::InitializeNativeTargetDisassembler();
    llvm::sys::DynamicLibrary::LoadLibraryPermanently(nullptr);
}

void ExpressionCodeGen::Initialize()
{
    std::call_once(codegen_target_init_flag, InitializeCodegenTargets);
    jit = eoe(LLJITBuilder().create());

    context = std::make_unique<LLVMContext>();
    llvmTypes = std::make_unique<LLVMTypes>(*context);
    // Create module called the_module
    module = std::make_unique<Module>("the_module", *context);
    module->setDataLayout(jit->getDataLayout());
    // Create IR builder to create IR instructions
    builder = std::make_unique<IRBuilder<>>(*context);
    codeGenUtils = std::make_unique<CodeGenUtils>(*context, *module, *builder);
    decimalIRBuilder = std::make_unique<DecimalIRBuilder>(*context, *module, *builder, *codeGenUtils);
    fpm = std::make_unique<legacy::FunctionPassManager>(module.get());

    ExprInfoExtractor exprInfoExtractor;
    this->expr->Accept(exprInfoExtractor);
    this->vectorIndexes = exprInfoExtractor.GetVectorIndexes();
    // get functions used in funcExpr  RegisterFunctions(exprInfoExtractor.GetFunctions());
    RegisterFunctions(FunctionRegistry::GetFunctions());
}

// Register function in JIT
void ExpressionCodeGen::RegisterFunctions(const std::vector<omniruntime::Function> &functions)
{
    std::set<std::string> jitRegisteredFuncs;
    for (auto &func : functions) {
        auto &jd = jit->getMainJITDylib();
        auto &dl = jit->getDataLayout();
        llvm::orc::MangleAndInterner mangle(jit->getExecutionSession(), dl);
        std::vector<DataTypeId> params = func.GetParamTypes();
        DataTypeId retType = func.GetReturnType();
        std::vector<Type *> args = this->GetFunctionArgTypeVector(params, retType, func.IsExecutionContextSet());
        auto s = llvm::orc::absoluteSymbols({ { mangle(func.GetId()),
            JITEvaluatedSymbol(pointerToJITTargetAddress(func.GetAddress()), JITSymbolFlags::Exported) } });
        auto ign = jd.define(s);
        if (ign) {
            LogError("Error while defining absolute symbol in jd");
        }
        llvm::Type *ret = (retType == OMNI_DECIMAL128) ? llvmTypes->VoidType() : llvmTypes->ToLLVMType(retType);
        llvm::FunctionType *ft = llvm::FunctionType::get(ret, args, false);
        auto linkage = llvm::Function::ExternalLinkage;
        llvm::Function::Create(ft, linkage, func.GetId(), *module);
        module->getOrInsertFunction(func.GetId(), ft);
    }
}

// Other operations which require externed functions
Value *ExpressionCodeGen::StringCmp(Value *lhs, Value *lLen, Value *rhs, Value *rLen)
{
    // call function
    std::vector<Value *> argVals { lhs, lLen, rhs, rLen };
    auto signature = FunctionSignature(strCompareStr, std::vector<DataTypeId> { OMNI_VARCHAR, OMNI_VARCHAR }, OMNI_INT);
    auto f = module->getFunction(FunctionRegistry::LookupFunction(&signature)->GetId());
    auto ret = codeGenUtils->CreateCall(f, argVals, "call_str_cmp");
    InlineFunctionInfo inlineFunctionInfo;
    llvm::InlineFunction(*ret, inlineFunctionInfo);
    return ret;
}

void ExpressionCodeGen::BinaryExprNullHelper(const BinaryExpr *binaryExpr, Value *left, Value *right, Value *leftIsNull,
    Value *rightIsNull, PHINode **leftPhi, PHINode **rightPhi, Value **isNeitherNull)
{
    BasicBlock *incomingBlock, *nullBlock, *nextInst;
    Value *nullCond, *leftZero, *rightZero;
    auto op = binaryExpr->op;

    if (op == omniruntime::expressions::Operator::LT || op == omniruntime::expressions::Operator::GT ||
        op == omniruntime::expressions::Operator::LTE || op == omniruntime::expressions::Operator::GTE ||
        op == omniruntime::expressions::Operator::EQ || op == omniruntime::expressions::Operator::NEQ) {
        *isNeitherNull = builder->CreateNot(builder->CreateOr(leftIsNull, rightIsNull));
    }
    if (op == omniruntime::expressions::Operator::ADD || op == omniruntime::expressions::Operator::SUB ||
        op == omniruntime::expressions::Operator::MUL) {
        std::vector<Value *> argLeftVals { left, left };
        std::vector<Value *> argRightVals { right, right };
        incomingBlock = builder->GetInsertBlock();
        nullBlock = BasicBlock::Create(*context, "nullBlock", builder->GetInsertBlock()->getParent());
        nextInst = BasicBlock::Create(*context, "nextInst", builder->GetInsertBlock()->getParent());
        nullCond = builder->CreateOr(leftIsNull, rightIsNull);
        builder->CreateCondBr(nullCond, nullBlock, nextInst);
        builder->SetInsertPoint(nullBlock);
        switch (binaryExpr->left->GetReturnType().GetId()) {
            case OMNI_INT:
            case OMNI_DATE32:
            case OMNI_LONG:
            case OMNI_DECIMAL64:
                leftZero = builder->CreateSub(left, left);
                rightZero = builder->CreateSub(right, right);
                break;
            case OMNI_DOUBLE:
                leftZero = builder->CreateFSub(left, left);
                rightZero = builder->CreateFSub(right, right);
                break;
            case OMNI_DECIMAL128: {
                std::vector<DataTypeId> params { OMNI_DECIMAL128, OMNI_DECIMAL128 };
                std::string funcId = FunctionSignature(subDec128Str, params, OMNI_DECIMAL128).ToString();
                leftZero = decimalIRBuilder->CallDecimalFunction(funcId,
                    llvmTypes->ToLLVMType(binaryExpr->GetReturnTypeId()), argLeftVals);
                rightZero = decimalIRBuilder->CallDecimalFunction(funcId,
                    llvmTypes->ToLLVMType(binaryExpr->GetReturnTypeId()), argRightVals);
                break;
            }
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
    if (op == omniruntime::expressions::Operator::DIV || op == omniruntime::expressions::Operator::MOD) {
        DivExprNullHelper(binaryExpr, left, right, leftIsNull, rightIsNull, leftPhi, rightPhi);
    }
}

void ExpressionCodeGen::DivExprNullHelper(const BinaryExpr *binaryExpr, Value *left, Value *right, Value *leftIsNull,
    Value *rightIsNull, PHINode **leftPhi, PHINode **rightPhi)
{
    BasicBlock *incomingBlock, *nullBlock, *nextInst;
    Value *nullCond, *leftZero, *rightOne;

    std::vector<Value *> argLeftVals { left, left };
    std::vector<Value *> argRightVals { right, right };
    incomingBlock = builder->GetInsertBlock();
    nullBlock = BasicBlock::Create(*context, "nullBlock", builder->GetInsertBlock()->getParent());
    nextInst = BasicBlock::Create(*context, "nextInst", builder->GetInsertBlock()->getParent());
    nullCond = builder->CreateOr(leftIsNull, rightIsNull);
    builder->CreateCondBr(nullCond, nullBlock, nextInst);
    builder->SetInsertPoint(nullBlock);
    switch (binaryExpr->left->GetReturnTypeId()) {
        case OMNI_INT:
        case OMNI_DATE32:
            leftZero = llvmTypes->CreateConstantInt(0);
            rightOne = llvmTypes->CreateConstantInt(1);
            break;
        case OMNI_LONG:
        case OMNI_DECIMAL64:
            leftZero = llvmTypes->CreateConstantLong(0);
            rightOne = llvmTypes->CreateConstantLong(1);
            break;
        case OMNI_DOUBLE:
            leftZero = llvmTypes->CreateConstantDouble(0.0);
            rightOne = llvmTypes->CreateConstantDouble(1.0);
            break;
        case OMNI_DECIMAL128: {
            std::vector<DataTypeId> params { OMNI_DECIMAL128, OMNI_DECIMAL128 };
            std::string funcId = FunctionSignature(subDec128Str, params, OMNI_DECIMAL128).ToString();
            leftZero = decimalIRBuilder->CallDecimalFunction(funcId,
                llvmTypes->ToLLVMType(binaryExpr->GetReturnTypeId()), argLeftVals);

            std::vector<DataTypeId> divParams { OMNI_DECIMAL128, OMNI_INT, OMNI_INT, OMNI_DECIMAL128,
                OMNI_INT,        OMNI_INT, OMNI_INT, OMNI_INT };
            auto leftType = binaryExpr->left->GetReturnType();
            auto rightType = binaryExpr->right->GetReturnType();
            auto returnType = binaryExpr->GetReturnType();
            std::vector<Value *> divArgs { right,
                llvmTypes->CreateConstantInt(leftType.GetPrecision()),
                llvmTypes->CreateConstantInt(leftType.GetScale()),
                right,
                llvmTypes->CreateConstantInt(rightType.GetPrecision()),
                llvmTypes->CreateConstantInt(rightType.GetScale()),
                llvmTypes->CreateConstantInt(returnType.GetPrecision()),
                llvmTypes->CreateConstantInt(returnType.GetScale()) };
            funcId = FunctionSignature(divDec128Str, divParams, OMNI_DECIMAL128).ToString();
            rightOne = decimalIRBuilder->CallDecimalFunction(funcId,
                llvmTypes->ToLLVMType(binaryExpr->GetReturnTypeId()), divArgs, codegenContext->executionContext);
            break;
        }
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

void ExpressionCodeGen::Decimal64MultiplyHelper(const BinaryExpr *binaryExpr, Value *output, Value *leftIsNull,
    Value *rightIsNull)
{
    PHINode *phi;
    Value *rescaleValue;
    BasicBlock *incomingBlock, *rescaleBlock, *nextInst;

    incomingBlock = builder->GetInsertBlock();
    rescaleBlock = BasicBlock::Create(*context, "rescaleBlock", builder->GetInsertBlock()->getParent());
    nextInst = BasicBlock::Create(*context, "nextInst", builder->GetInsertBlock()->getParent());

    Value *leftScale = llvmTypes->CreateConstantInt(binaryExpr->left->GetReturnType().GetScale());
    Value *rightScale = llvmTypes->CreateConstantInt(binaryExpr->right->GetReturnType().GetScale());
    Value *returnScale = llvmTypes->CreateConstantInt(binaryExpr->GetReturnType().GetScale());
    Value *deltaScale = builder->CreateSub(builder->CreateAdd(leftScale, rightScale), returnScale);
    Value *rescaleCond = builder->CreateICmpNE(deltaScale, llvmTypes->CreateConstantLong(0));

    builder->CreateCondBr(rescaleCond, rescaleBlock, nextInst);
    builder->SetInsertPoint(rescaleBlock);
    std::vector<Value *> argVals { output, deltaScale };
    std::vector<DataTypeId> params { OMNI_DECIMAL64, OMNI_INT };
    Type *returnType = llvmTypes->ToLLVMType(OMNI_DECIMAL64);
    std::string funcId = FunctionSignature(downScaleDec64Str, params, OMNI_DECIMAL64).ToString();
    rescaleValue = decimalIRBuilder->CallDecimalFunction(funcId, returnType, argVals);
    builder->CreateBr(nextInst);

    builder->SetInsertPoint(nextInst);
    int numberOfPaths = 2;
    phi = builder->CreatePHI(output->getType(), numberOfPaths, "iftmp");
    phi->addIncoming(output, incomingBlock);
    phi->addIncoming(rescaleValue, rescaleBlock);
    this->value = make_shared<CodeGenValue>(phi, builder->CreateOr(leftIsNull, rightIsNull));
}

Value *ExpressionCodeGen::HandleDivisionByZero(Value *divisorValue, DataTypeId type)
{
    BasicBlock *incomingBlock, *zeroBlock, *nextInst;
    Value *zeroCond;
    PHINode *phi;

    incomingBlock = builder->GetInsertBlock();
    zeroBlock = BasicBlock::Create(*context, "zeroBlock", builder->GetInsertBlock()->getParent());
    nextInst = BasicBlock::Create(*context, "nextInst", builder->GetInsertBlock()->getParent());

    Value *zero;
    Value *defaultValue;
    // Set the divisor values to 1 by default when they are 0
    // to avoid segfaults and return error at the same time
    switch (type) {
        case OMNI_INT:
        case OMNI_DATE32:
            zero = llvmTypes->CreateConstantInt(0);
            defaultValue = llvmTypes->CreateConstantInt(1);
            break;
        case OMNI_LONG:
        case OMNI_DECIMAL64:
        case OMNI_DECIMAL128:
            zero = llvmTypes->CreateConstantLong(0);
            defaultValue = llvmTypes->CreateConstantLong(1);
            break;
        case OMNI_DOUBLE:
            zero = llvmTypes->CreateConstantDouble(0.0);
            defaultValue = llvmTypes->CreateConstantDouble(1.0);
            break;
        default:
            zero = nullptr;
            defaultValue = nullptr;
    }

    if (type == OMNI_DOUBLE) {
        zeroCond = builder->CreateFCmpUEQ(zero, divisorValue);
    } else {
        zeroCond = builder->CreateICmpEQ(zero, divisorValue);
    }

    builder->CreateCondBr(zeroCond, zeroBlock, nextInst);
    builder->SetInsertPoint(zeroBlock);

    auto executionContext = this->codegenContext->executionContext;
    string message = "Division by zero!";
    auto msgLength = llvmTypes->CreateConstantInt(message.length());

    // Return a cast to an i8*
    auto msgPtr = CreateStringConstant(message);
    std::vector<Value *> args;
    args.push_back(executionContext);
    args.push_back(msgPtr);
    args.push_back(msgLength);

    std::vector<DataTypeId> params { OMNI_LONG, OMNI_VARCHAR };
    std::string funcId = FunctionSignature("ContextSetError", params, OMNI_BOOLEAN).ToString();
    auto f = module->getFunction(funcId);
    builder->CreateCall(f, args, "set_error_call");

    builder->CreateBr(nextInst);
    builder->SetInsertPoint(nextInst);
    int numberOfPaths = 2;
    phi = builder->CreatePHI(divisorValue->getType(), numberOfPaths, "iftmp");
    phi->addIncoming(defaultValue, zeroBlock);
    phi->addIncoming(divisorValue, incomingBlock);
    return phi;
}

CodeGenValuePtr CreateInvalidCodeGenValue()
{
    return make_shared<CodeGenValue>(nullptr, nullptr, nullptr);
}

// Helper methods to parse binary expressions
void ExpressionCodeGen::BinaryExprIntHelper(const BinaryExpr *binaryExpr, Value *left, Value *right, Value *leftIsNull,
    Value *rightIsNull)
{
    PHINode *leftPhi, *rightPhi;
    Value *isNeitherNull;
    Value *output = nullptr;
    BinaryExprNullHelper(binaryExpr, left, right, leftIsNull, rightIsNull, &leftPhi, &rightPhi, &isNeitherNull);
    switch (binaryExpr->op) {
        case omniruntime::expressions::Operator::LT:
            output = builder->CreateAnd(isNeitherNull, builder->CreateICmpSLT(left, right, "relational_lt"));
            break;
        case omniruntime::expressions::Operator::GT:
            output = builder->CreateAnd(isNeitherNull, builder->CreateICmpSGT(left, right, "relational_gt"));
            break;
        case omniruntime::expressions::Operator::LTE:
            output = builder->CreateAnd(isNeitherNull, builder->CreateICmpSLE(left, right, "relational_le"));
            break;
        case omniruntime::expressions::Operator::GTE:
            output = builder->CreateAnd(isNeitherNull, builder->CreateICmpSGE(left, right, "relational_ge"));
            break;
        case omniruntime::expressions::Operator::EQ:
            output = builder->CreateAnd(isNeitherNull, builder->CreateICmpEQ(left, right, "relational_eq"));
            break;
        case omniruntime::expressions::Operator::NEQ:
            output = builder->CreateAnd(isNeitherNull,
                builder->CreateNot(builder->CreateICmpEQ(left, right), "relational_neq"));
            break;
        case omniruntime::expressions::Operator::ADD:
            output = builder->CreateAdd(leftPhi, rightPhi, "arithmetic_add");
            break;
        case omniruntime::expressions::Operator::SUB:
            output = builder->CreateSub(leftPhi, rightPhi, "arithmetic_sub");
            break;
        case omniruntime::expressions::Operator::MUL:
            output = builder->CreateMul(leftPhi, right, "arithmetic_mul");
            break;
        case omniruntime::expressions::Operator::DIV: {
            auto divisor = HandleDivisionByZero(rightPhi, binaryExpr->right->GetReturnTypeId());
            output = builder->CreateSDiv(leftPhi, divisor, "arithmetic_div");
            break;
        }
        case omniruntime::expressions::Operator::MOD: {
            auto divisor = HandleDivisionByZero(rightPhi, binaryExpr->right->GetReturnTypeId());
            output = builder->CreateSRem(leftPhi, divisor, "arithmetic_mod");
            break;
        }
        default:
            LogError("Unsupported int/long binary operator %d", static_cast<uint32_t>(binaryExpr->op));
            this->value = CreateInvalidCodeGenValue();
    }

    // Only add, minus, multiply promote decimal64 to decimal128, div doesn't.
    if (binaryExpr->GetReturnTypeId() == OMNI_DECIMAL128) {
        std::vector<DataTypeId> params { OMNI_DECIMAL64 };
        Type *returnType = llvmTypes->ToLLVMType(binaryExpr->GetReturnTypeId());
        std::string funcId = FunctionSignature("CAST", params, OMNI_DECIMAL128).ToString();
        auto ret = decimalIRBuilder->CallDecimalFunction(funcId, returnType, { output });
        this->value = decimalIRBuilder->BuildDecimalValue(ret, binaryExpr->GetReturnType(),
            builder->CreateOr(leftIsNull, rightIsNull));
    } else if (binaryExpr->op == omniruntime::expressions::Operator::MUL &&
        binaryExpr->GetReturnTypeId() == OMNI_DECIMAL64) {
        Decimal64MultiplyHelper(binaryExpr, output, leftIsNull, rightIsNull);
    } else if (binaryExpr->GetReturnTypeId() == OMNI_DECIMAL64) {
        this->value = decimalIRBuilder->BuildDecimalValue(output, binaryExpr->GetReturnType(),
            builder->CreateOr(leftIsNull, rightIsNull));
    } else {
        this->value = make_shared<CodeGenValue>(output, builder->CreateOr(leftIsNull, rightIsNull));
    }
}

void ExpressionCodeGen::Decimal64Helper(const BinaryExpr *binaryExpr, Value *left, Value *right, Value *leftIsNull,
    Value *rightIsNull)
{
    if (binaryExpr->op == omniruntime::expressions::Operator::DIV) {
        PHINode *leftPhi, *rightPhi;
        Value *isNeitherNull = nullptr;
        Value *output = nullptr;
        BinaryExprNullHelper(binaryExpr, left, right, leftIsNull, rightIsNull, &leftPhi, &rightPhi, &isNeitherNull);
        std::vector<Value *> argVals { leftPhi, rightPhi };
        std::vector<DataTypeId> params { binaryExpr->left->GetReturnTypeId(), binaryExpr->right->GetReturnTypeId() };
        Type *returnType = llvmTypes->ToLLVMType(binaryExpr->GetReturnTypeId());
        std::string funcId = FunctionSignature(divDec64Str, params, OMNI_DECIMAL64).ToString();
        output = decimalIRBuilder->CallDecimalFunction(funcId, returnType, argVals, codegenContext->executionContext);
        this->value = decimalIRBuilder->BuildDecimalValue(output, binaryExpr->GetReturnType(),
            builder->CreateOr(leftIsNull, rightIsNull));
    } else {
        this->BinaryExprIntHelper(binaryExpr, left, right, leftIsNull, rightIsNull);
    }
}

Value *ExpressionCodeGen::BinaryExprDoubleHelper(const BinaryExpr *binaryExpr, Value *left, Value *right,
    Value *leftIsNull, Value *rightIsNull)
{
    PHINode *leftPhi, *rightPhi;
    Value *isNeitherNull;
    BinaryExprNullHelper(binaryExpr, left, right, leftIsNull, rightIsNull, &leftPhi, &rightPhi, &isNeitherNull);
    switch (binaryExpr->op) {
        case omniruntime::expressions::Operator::LT:
            return builder->CreateAnd(isNeitherNull, builder->CreateFCmpULT(left, right, "frelational_lt"));
        case omniruntime::expressions::Operator::GT:
            return builder->CreateAnd(isNeitherNull, builder->CreateFCmpUGT(left, right, "frelational_gt"));
        case omniruntime::expressions::Operator::LTE:
            return builder->CreateAnd(isNeitherNull, builder->CreateFCmpULE(left, right, "frelational_le"));
        case omniruntime::expressions::Operator::GTE:
            return builder->CreateAnd(isNeitherNull, builder->CreateFCmpUGE(left, right, "frelational_ge"));
        case omniruntime::expressions::Operator::EQ:
            return builder->CreateAnd(isNeitherNull, builder->CreateFCmpUEQ(left, right, "farithmetic_eq"));
        case omniruntime::expressions::Operator::NEQ:
            return builder->CreateAnd(isNeitherNull,
                builder->CreateNot(builder->CreateFCmpUEQ(left, right, "farithmetic_neq")));
        case omniruntime::expressions::Operator::ADD:
            return builder->CreateFAdd(leftPhi, rightPhi, "farithmetic_add");
        case omniruntime::expressions::Operator::SUB:
            return builder->CreateFSub(leftPhi, rightPhi, "farithmetic_sub");
        case omniruntime::expressions::Operator::MUL:
            return builder->CreateFMul(leftPhi, right, "farithmetic_mul");
        case omniruntime::expressions::Operator::DIV:
            return codeGenUtils->CallExternFunction("divide", { OMNI_DOUBLE, OMNI_DOUBLE }, OMNI_DOUBLE,
                { leftPhi, rightPhi }, "farithmetic_divide");
        case omniruntime::expressions::Operator::MOD: {
            return codeGenUtils->CallExternFunction("modulus", { OMNI_DOUBLE, OMNI_DOUBLE }, OMNI_DOUBLE,
                { leftPhi, rightPhi }, "farithmetic_mod");
        }
        default: {
            LogWarn("Unsupported double binary operator %d", static_cast<uint32_t>(binaryExpr->op));
            return nullptr;
        }
    }
}

Value *ExpressionCodeGen::BinaryExprStringHelper(const BinaryExpr *binaryExpr, Value *leftVal, Value *leftLen,
    Value *rightVal, Value *rightLen, Value *leftIsNull, Value *rightIsNull)
{
    PHINode *leftPhi, *rightPhi;
    Value *isNeitherNull;
    BinaryExprNullHelper(binaryExpr, leftVal, rightVal, leftIsNull, rightIsNull, &leftPhi, &rightPhi, &isNeitherNull);
    switch (binaryExpr->op) {
        case omniruntime::expressions::Operator::LT:
            return builder->CreateAnd(isNeitherNull, builder->CreateICmpSLT(
                this->StringCmp(leftVal, leftLen, rightVal, rightLen), llvmTypes->CreateConstantInt(0)));
        case omniruntime::expressions::Operator::GT:
            return builder->CreateAnd(isNeitherNull, builder->CreateICmpSGT(
                this->StringCmp(leftVal, leftLen, rightVal, rightLen), llvmTypes->CreateConstantInt(0)));
        case omniruntime::expressions::Operator::LTE:
            return builder->CreateAnd(isNeitherNull, builder->CreateICmpSLE(
                this->StringCmp(leftVal, leftLen, rightVal, rightLen), llvmTypes->CreateConstantInt(0)));
        case omniruntime::expressions::Operator::GTE:
            return builder->CreateAnd(isNeitherNull, builder->CreateICmpSGE(
                this->StringCmp(leftVal, leftLen, rightVal, rightLen), llvmTypes->CreateConstantInt(0)));
        case omniruntime::expressions::Operator::EQ:
            return builder->CreateAnd(isNeitherNull, builder->CreateICmpEQ(
                this->StringCmp(leftVal, leftLen, rightVal, rightLen), llvmTypes->CreateConstantInt(0)));
        case omniruntime::expressions::Operator::NEQ:
            return builder->CreateAnd(isNeitherNull, builder->CreateICmpNE(
                this->StringCmp(leftVal, leftLen, rightVal, rightLen), llvmTypes->CreateConstantInt(0)));
        default: {
            LogWarn("Unsupported string binary operator %d", static_cast<uint32_t>(binaryExpr->op));
            return nullptr;
        }
    }
}

void ExpressionCodeGen::BinaryExprDecimalHelper(const BinaryExpr *binaryExpr, Value *left, Value *right,
    Value *leftIsNull, Value *rightIsNull)
{
    PHINode *leftPhi, *rightPhi;
    Value *isNeitherNull = nullptr;
    Value *output = nullptr;
    BinaryExprNullHelper(binaryExpr, left, right, leftIsNull, rightIsNull, &leftPhi, &rightPhi, &isNeitherNull);
    std::vector<Value *> argVals { leftPhi, rightPhi };
    std::vector<DataTypeId> params { binaryExpr->left->GetReturnTypeId(), binaryExpr->right->GetReturnTypeId() };
    Type *returnType = llvmTypes->ToLLVMType(binaryExpr->GetReturnTypeId());
    std::string decimal128CmpFuncId = FunctionSignature(decimal128CompareStr, params, OMNI_INT).ToString();
    switch (binaryExpr->op) {
        case omniruntime::expressions::Operator::LT:
            output = builder->CreateAnd(isNeitherNull, builder->CreateICmpSLT(
                decimalIRBuilder->CallDecimalFunction(decimal128CmpFuncId, returnType, { left, right }),
                llvmTypes->CreateConstantInt(0)));
            break;
        case omniruntime::expressions::Operator::GT:
            output = builder->CreateAnd(isNeitherNull, builder->CreateICmpSGT(
                decimalIRBuilder->CallDecimalFunction(decimal128CmpFuncId, returnType, { left, right }),
                llvmTypes->CreateConstantInt(0)));
            break;
        case omniruntime::expressions::Operator::LTE:
            output = builder->CreateAnd(isNeitherNull, builder->CreateICmpSLE(
                decimalIRBuilder->CallDecimalFunction(decimal128CmpFuncId, returnType, { left, right }),
                llvmTypes->CreateConstantInt(0)));
            break;
        case omniruntime::expressions::Operator::GTE:
            output = builder->CreateAnd(isNeitherNull, builder->CreateICmpSGE(
                decimalIRBuilder->CallDecimalFunction(decimal128CmpFuncId, returnType, { left, right }),
                llvmTypes->CreateConstantInt(0)));
            break;
        case omniruntime::expressions::Operator::EQ: {
            output = builder->CreateAnd(isNeitherNull, builder->CreateICmpEQ(
                decimalIRBuilder->CallDecimalFunction(decimal128CmpFuncId, returnType, { left, right }),
                llvmTypes->CreateConstantInt(0)));
            break;
        }
        case omniruntime::expressions::Operator::NEQ:
            output = builder->CreateAnd(isNeitherNull, builder->CreateICmpNE(
                decimalIRBuilder->CallDecimalFunction(decimal128CmpFuncId, returnType, { left, right }),
                llvmTypes->CreateConstantInt(0)));
            break;
        case omniruntime::expressions::Operator::ADD: {
            std::string funcId = FunctionSignature(addDec128Str, params, OMNI_DECIMAL128).ToString();
            output = decimalIRBuilder->CallDecimalFunction(funcId, returnType, argVals);
            break;
        }
        case omniruntime::expressions::Operator::SUB: {
            std::string funcId = FunctionSignature(subDec128Str, params, OMNI_DECIMAL128).ToString();
            output = decimalIRBuilder->CallDecimalFunction(funcId, returnType, argVals);
            break;
        }
        case omniruntime::expressions::Operator::MUL: {
            std::string funcId = FunctionSignature(mulDec128Str, params, OMNI_DECIMAL128).ToString();
            output = decimalIRBuilder->CallDecimalFunction(funcId, returnType, argVals);
            break;
        }
        case omniruntime::expressions::Operator::DIV: {
            auto leftType = binaryExpr->left->GetReturnType();
            auto rightType = binaryExpr->right->GetReturnType();
            auto binaryReturnType = binaryExpr->GetReturnType();
            std::vector<Value *> divArgs { leftPhi,
                llvmTypes->CreateConstantInt(leftType.GetPrecision()),
                llvmTypes->CreateConstantInt(leftType.GetScale()),
                rightPhi,
                llvmTypes->CreateConstantInt(rightType.GetPrecision()),
                llvmTypes->CreateConstantInt(rightType.GetScale()),
                llvmTypes->CreateConstantInt(binaryReturnType.GetPrecision()),
                llvmTypes->CreateConstantInt(binaryReturnType.GetScale()) };
            std::vector<DataTypeId> params { binaryExpr->left->GetReturnTypeId(),
                OMNI_INT,
                OMNI_INT,
                binaryExpr->right->GetReturnTypeId(),
                OMNI_INT,
                OMNI_INT,
                OMNI_INT,
                OMNI_INT };
            std::string funcId = FunctionSignature(divDec128Str, params, OMNI_DECIMAL128).ToString();
            output =
                decimalIRBuilder->CallDecimalFunction(funcId, returnType, divArgs, codegenContext->executionContext);
            break;
        }
        default: {
            LogWarn("Unsupported string binary operator %d", static_cast<uint32_t>(binaryExpr->op));
            output = nullptr;
            break;
        }
    }

    if (binaryExpr->GetReturnTypeId() == OMNI_DECIMAL128) {
        this->value = decimalIRBuilder->BuildDecimalValue(output, binaryExpr->GetReturnType(),
            builder->CreateOr(leftIsNull, rightIsNull));
    } else {
        this->value = make_shared<CodeGenValue>(output, builder->CreateOr(leftIsNull, rightIsNull));
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

bool ExpressionCodeGen::AreInvalidDataTypes(DataTypeId type1, DataTypeId type2)
{
    return type1 != type2 && !(TypeUtil::IsStringType(type1) && TypeUtil::IsStringType(type2));
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
            LogWarn("Invalid argument %s", argName.c_str());
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
    FunctionType *prototype = FunctionType::get(llvmTypes->GetFunctionReturnType(expr->GetReturnTypeId()), args, false);
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
        Value *lengthGep = builder->CreateGEP(outputLength, llvmTypes->CreateConstantInt(0), "OUTPUT_LENGTH_ADDRESS");
        builder->CreateStore(result->length, lengthGep);
    }

    builder->CreateStore(result->isNull, func->getArg(EXPRFUNC_OUT_IS_NULL_INDEX));

    // cast char* to int64 for output
    if (expr->GetReturnTypeId() == DataTypeId::OMNI_VARCHAR) {
        result->data = builder->CreatePtrToInt(result->data, llvmTypes->I64Type());
    }
    // Return value
    builder->CreateRet(result->data);
    verifyFunction(*func);
    return func;
}

Value *ExpressionCodeGen::GetIntToPtr(omniruntime::type::DataTypeId dataTypeId, Value *elementAddr)
{
    Value *elementPtr = nullptr;
    // Convert the column address to array of proper datatype.
    switch (dataTypeId) {
        case OMNI_BOOLEAN:
            elementPtr = builder->CreateIntToPtr(elementAddr, llvmTypes->I1PtrType());
            break;
        case OMNI_INT:
        case OMNI_DATE32:
            elementPtr = builder->CreateIntToPtr(elementAddr, llvmTypes->I32PtrType());
            break;
        case OMNI_LONG:
        case OMNI_DECIMAL64:
            elementPtr = builder->CreateIntToPtr(elementAddr, llvmTypes->I64PtrType());
            break;
        case OMNI_DOUBLE:
            elementPtr = builder->CreateIntToPtr(elementAddr, llvmTypes->DoublePtrType());
            break;
        case OMNI_CHAR:
        case OMNI_VARCHAR:
            elementPtr = builder->CreateIntToPtr(elementAddr, llvmTypes->I8PtrType());
            break;
        case OMNI_DECIMAL128:
            elementPtr = builder->CreateIntToPtr(elementAddr, llvmTypes->I128PtrType());
            break;
        default:
            LLVM_DEBUG_LOG("Unsupported column data type %d", dataTypeId);
            elementPtr = builder->CreateIntToPtr(elementAddr, llvmTypes->I64PtrType());
            break;
    }
    return elementPtr;
}

CodeGenValue *ExpressionCodeGen::LiteralExprConstantHelper(const LiteralExpr &lExpr)
{
    CodeGenValue *codeGenValue = nullptr;
    bool isNullLiteral = lExpr.isNull;
    switch (lExpr.GetReturnTypeId()) {
        case OMNI_INT:
        case OMNI_DATE32: {
            codeGenValue = new CodeGenValue(llvmTypes->CreateConstantInt(lExpr.intVal),
                llvmTypes->CreateConstantBool(isNullLiteral));
            break;
        }
        case OMNI_LONG: {
            codeGenValue = new CodeGenValue(llvmTypes->CreateConstantLong(lExpr.longVal),
                llvmTypes->CreateConstantBool(isNullLiteral));
            break;
        }
        case OMNI_DOUBLE: {
            codeGenValue = new CodeGenValue(llvmTypes->CreateConstantDouble(lExpr.doubleVal),
                llvmTypes->CreateConstantBool(isNullLiteral));
            break;
        }
        case OMNI_CHAR:
        case OMNI_VARCHAR: {
            Constant *strValConst = CreateStringConstant(*(lExpr.stringVal));
            Constant *strLenConst =
                ConstantInt::get(*context, APInt(INT32_VALUE, static_cast<int32_t>(lExpr.stringVal->length())));
            codeGenValue = new CodeGenValue(strValConst, llvmTypes->CreateConstantBool(isNullLiteral), strLenConst);
            break;
        }
        case OMNI_BOOLEAN: {
            codeGenValue = new CodeGenValue(llvmTypes->CreateConstantBool(lExpr.boolVal),
                llvmTypes->CreateConstantBool(isNullLiteral));
            break;
        }
        case OMNI_DECIMAL64: {
            Value *precision = llvmTypes->CreateConstantInt(lExpr.GetReturnType().GetPrecision());
            Value *scale = llvmTypes->CreateConstantInt(lExpr.GetReturnType().GetScale());
            codeGenValue = new DecimalValue(llvmTypes->CreateConstantLong(lExpr.longVal),
                llvmTypes->CreateConstantBool(isNullLiteral), precision, scale);
            break;
        }
        case OMNI_DECIMAL128: {
            std::string dec128String = isNullLiteral ? "0" : *lExpr.stringVal;
            __uint128_t dec128 = Decimal128Utils::StrToUint128_t(dec128String.c_str());
            dec128String = Decimal128Utils::Uint128_tToStr(dec128);
            Value *precision = llvmTypes->CreateConstantInt(lExpr.GetReturnType().GetPrecision());
            Value *scale = llvmTypes->CreateConstantInt(lExpr.GetReturnType().GetScale());
            auto const128Val = llvm::ConstantInt::get(llvm::Type::getInt128Ty(*context), dec128String, 10);
            codeGenValue =
                new DecimalValue(const128Val, llvmTypes->CreateConstantBool(isNullLiteral), precision, scale);
            break;
        }
        case OMNI_NONE: {
            codeGenValue =
                new CodeGenValue(llvmTypes->CreateConstantInt(lExpr.intVal), llvmTypes->CreateConstantBool(true));
            break;
        }
        default: {
            LogWarn("Unsupported data type in Data Expr %d", lExpr.GetReturnTypeId());
            codeGenValue =
                new CodeGenValue(llvmTypes->CreateConstantBool(lExpr.boolVal), llvmTypes->CreateConstantBool(false));
            break;
        }
    }
    return codeGenValue;
}

Value *ExpressionCodeGen::GetDictionaryVectorValue(const DataType &dataType, Value *rowIdx, Value *dictionaryVectorPtr,
    AllocaInst *&lengthAllocaInst)
{
    std::vector<DataTypeId> paramTypes = { OMNI_LONG, OMNI_INT };
    DataTypeId typeId = dataType.GetId();
    FunctionSignature dictionaryFuncSignature;
    switch (typeId) {
        case OMNI_INT:
        case OMNI_DATE32:
            dictionaryFuncSignature = FunctionSignature(dictionaryGetIntStr, paramTypes, OMNI_INT);
            break;
        case OMNI_LONG:
        case OMNI_DECIMAL64:
            dictionaryFuncSignature = FunctionSignature(dictionaryGetLongStr, paramTypes, OMNI_LONG);
            break;
        case OMNI_DECIMAL128:
            dictionaryFuncSignature = FunctionSignature(dictionaryGetDecimalStr, paramTypes, OMNI_DECIMAL128);
            break;
        case OMNI_DOUBLE:
            dictionaryFuncSignature = FunctionSignature(dictionaryGetDoubleStr, paramTypes, OMNI_DOUBLE);
            break;
        case OMNI_BOOLEAN:
            dictionaryFuncSignature = FunctionSignature(dictionaryGetBooleanStr, paramTypes, OMNI_BOOLEAN);
            break;
        case OMNI_CHAR:
        case OMNI_VARCHAR:
            dictionaryFuncSignature = FunctionSignature(dictionaryGetVarcharStr, paramTypes, OMNI_VARCHAR);
            break;
        default:
            LogWarn("Unsupported dictionary value type: %d", typeId);
            return nullptr;
    }
    auto dictionaryFunc = module->getFunction(FunctionRegistry::LookupFunction(&dictionaryFuncSignature)->GetId());
    std::vector<Value *> funcArgs;
    funcArgs.push_back(dictionaryVectorPtr);
    funcArgs.push_back(rowIdx);
    if (TypeUtil::IsStringType(typeId)) {
        lengthAllocaInst = builder->CreateAlloca(llvmTypes->I32Type(), nullptr, "varchar_length");
        builder->CreateStore(llvmTypes->CreateConstantInt(0), lengthAllocaInst);
        funcArgs.push_back(lengthAllocaInst);
    }
    Value *result = nullptr;
    if (typeId == OMNI_DECIMAL128) {
        result =
            decimalIRBuilder->CallDecimalFunction(FunctionRegistry::LookupFunction(&dictionaryFuncSignature)->GetId(),
            llvmTypes->ToLLVMType(typeId), funcArgs);
    } else {
        result = codeGenUtils->CreateCall(dictionaryFunc, funcArgs, "get_dictionary_value");
        InlineFunctionInfo inlineFunctionInfo;
        llvm::InlineFunction(*((CallInst *)result), inlineFunctionInfo);
    }
    return result;
}

void ExpressionCodeGen::Visit(const LiteralExpr &lExpr)
{
    this->value.reset(LiteralExprConstantHelper(lExpr));
}

void ExpressionCodeGen::Visit(const FieldExpr &fExpr)
{
    Value *rowIdx = this->codegenContext->rowIdx;
    Value *vecBatch = this->codegenContext->data;
    Value *bitmap = this->codegenContext->nullBitmap;
    Value *offsets = this->codegenContext->offsets;
    Value *dictionaryVectors = this->codegenContext->dictionaryVectors;

    Value *colIdx = llvmTypes->CreateConstantInt(fExpr.colVal);
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
        this->GetDictionaryVectorValue(fExpr.GetReturnType(), rowIdx, dictionaryVectorPtr, lengthAllocaInst);
    if (dictionaryValue == nullptr) {
        return;
    }

    Value *dictionaryLength = nullptr;
    if (TypeUtil::IsStringType(fExpr.GetReturnTypeId())) {
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

    Value *elementPtr = GetIntToPtr(fExpr.GetReturnTypeId(), elementAddr);
    Value *dataValue = nullptr;
    if (TypeUtil::IsStringType(fExpr.GetReturnTypeId())) {
        // Get offset for varchar
        auto offsetsGEP = builder->CreateGEP(offsets, colIdx);
        Value *offsetPtr = builder->CreateLoad(offsetsGEP);
        offsetPtr = builder->CreateIntToPtr(offsetPtr, llvmTypes->I32PtrType());
        auto colOffsetGEP = builder->CreateGEP(offsetPtr, rowIdx);
        Value *startOffset = builder->CreateLoad(colOffsetGEP);
        colOffsetGEP = builder->CreateGEP(offsetPtr, builder->CreateAdd(rowIdx, llvmTypes->CreateConstantInt(1)));
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
    Type *phiType = llvmTypes->ToLLVMType(fExpr.GetReturnTypeId());
    func->getBasicBlockList().push_back(mergeBlock);
    builder->SetInsertPoint(mergeBlock);

    PHINode *phiValue = builder->CreatePHI(phiType, numReservedValues, "iftmp");
    phiValue->addIncoming(dictionaryValue, trueBlock);
    phiValue->addIncoming(dataValue, falseBlock);

    // Length is only valid for varchar type
    PHINode *phiLength = nullptr;
    if (TypeUtil::IsStringType(fExpr.GetReturnTypeId())) {
        phiLength = builder->CreatePHI(llvmTypes->I32Type(), numReservedValues, "length");
        phiLength->addIncoming(dictionaryLength, trueBlock);
        phiLength->addIncoming(length, falseBlock);
    }

    // Get isNull value
    auto bitmapGEP = builder->CreateGEP(bitmap, colIdx);
    Value *bitmapValue = builder->CreateLoad(bitmapGEP);
    bitmapValue = builder->CreateIntToPtr(bitmapValue, llvmTypes->I1PtrType());
    bitmapGEP = builder->CreateGEP(bitmapValue, rowIdx);
    bitmapValue = builder->CreateLoad(bitmapGEP);

    if (TypeUtil::IsDecimalType(fExpr.GetReturnTypeId())) {
        Value *precision = llvmTypes->CreateConstantInt(fExpr.GetReturnType().GetPrecision());
        Value *scale = llvmTypes->CreateConstantInt(fExpr.GetReturnType().GetScale());
        this->value.reset(new DecimalValue(phiValue, bitmapValue, precision, scale));
    } else {
        this->value.reset(new CodeGenValue(phiValue, bitmapValue, phiLength));
    }
}

std::pair<Value *, Value *> ExpressionCodeGen::RescaleDecimals(Expr &expr, CodeGenValue &left, CodeGenValue &right,
    int scaleDiff, DataTypeId typeId)
{
    auto scaledLeft = left.data;
    auto scaledRight = right.data;

    auto leftScaleValue = (Value *)static_cast<DecimalValue &>(left).GetScale();
    auto rightScaleValue = (Value *)static_cast<DecimalValue &>(right).GetScale();
    bool scaleBothValues = false;
    if (expr.GetType() == omniruntime::expressions::ExprType::BINARY_E) {
        auto &bExpr = static_cast<BinaryExpr &>(expr);
        if (bExpr.op == omniruntime::expressions::Operator::DIV) {
            int32_t scale = bExpr.GetReturnType().GetScale() + scaleDiff;
            scaledLeft = decimalIRBuilder->ScaleValue(*left.data, *llvmTypes->CreateConstantInt(scale), typeId);
        } else if (bExpr.op == omniruntime::expressions::Operator::ADD ||
            bExpr.op == omniruntime::expressions::Operator::SUB ||
            bExpr.op == omniruntime::expressions::Operator::MOD || IsComparisonOperator(bExpr.op)) {
            scaleBothValues = true;
        }
    }
    if (expr.GetType() != omniruntime::expressions::ExprType::BINARY_E || scaleBothValues) {
        if (scaleDiff != 0) {
            decimalIRBuilder->ScaleValues(*(left.data), *leftScaleValue, *(right.data), *rightScaleValue, &scaledLeft,
                &scaledRight, typeId);
        }
    }
    return std::make_pair(scaledLeft, scaledRight);
}

void ExpressionCodeGen::Visit(const BinaryExpr &binaryExpr)
{
    auto *bExpr = const_cast<BinaryExpr *>(&binaryExpr);

    if (AreInvalidDataTypes(bExpr->left->GetReturnTypeId(), bExpr->right->GetReturnTypeId())) {
        LogError("return type and args must have the same data types");
        this->value = CreateInvalidCodeGenValue();
        return;
    }

    if (bExpr->left->GetReturnTypeId() == DataTypeId::OMNI_DECIMAL64 &&
        bExpr->right->GetReturnTypeId() == DataTypeId::OMNI_DECIMAL64 &&
        bExpr->GetReturnTypeId() == DataTypeId::OMNI_DECIMAL128) {
        LogError("Not handling decimal type promotion for now");
        this->value = CreateInvalidCodeGenValue();
        return;
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
        this->value = make_shared<CodeGenValue>(builder->CreateAnd(leftValue, rightValue, "logical_and"),
            builder->CreateOr(builder->CreateAnd(leftNull, rightNull),
            builder->CreateOr(builder->CreateAnd(leftNull, rightValue), builder->CreateAnd(rightNull, leftValue))));
        return;
    }
    if (bExpr->op == omniruntime::expressions::Operator::OR) {
        this->value = make_shared<CodeGenValue>(builder->CreateOr(leftValue, rightValue, "logical_or"),
            builder->CreateOr(builder->CreateAnd(leftNull, rightNull),
            builder->CreateOr(builder->CreateAnd(leftNull, builder->CreateNot(rightValue)),
            builder->CreateAnd(rightNull, builder->CreateNot(leftValue)))));
        return;
    }

    if (bExpr->left->GetReturnTypeId() == OMNI_INT || bExpr->left->GetReturnTypeId() == OMNI_LONG ||
        bExpr->left->GetReturnTypeId() == OMNI_DATE32) {
        this->BinaryExprIntHelper(bExpr, leftValue, rightValue, leftNull, rightNull);
        return;
    } else if (bExpr->left->GetReturnTypeId() == OMNI_DECIMAL64) {
        auto scaledValues = RescaleDecimals(*bExpr, *left.get(), *right.get(),
            bExpr->right->dataType->GetScale() - bExpr->left->dataType->GetScale(), OMNI_DECIMAL64);
        auto scaledLeft = scaledValues.first;
        auto scaledRight = scaledValues.second;
        this->Decimal64Helper(bExpr, scaledLeft, scaledRight, leftNull, rightNull);
        return;
    } else if (bExpr->left->GetReturnTypeId() == OMNI_DOUBLE) {
        this->value =
            make_shared<CodeGenValue>(this->BinaryExprDoubleHelper(bExpr, leftValue, rightValue, leftNull, rightNull),
            builder->CreateOr(leftNull, rightNull));
        return;
    } else if (TypeUtil::IsStringType(bExpr->left->GetReturnTypeId())) {
        this->value = make_shared<CodeGenValue>(
            this->BinaryExprStringHelper(bExpr, leftValue, leftLen, rightValue, rightLen, leftNull, rightNull),
            builder->CreateOr(leftNull, rightNull));
        return;
    } else if (bExpr->left->GetReturnTypeId() == OMNI_DECIMAL128) {
        auto scaledLeft = left->data;
        auto scaledRight = right->data;
        if (bExpr->op != omniruntime::expressions::Operator::DIV) {
            // defer div rescaling to functions to handle overflow
            auto scaledValues = RescaleDecimals(*bExpr, *left.get(), *right.get(),
                bExpr->right->dataType->GetScale() - bExpr->left->dataType->GetScale(), OMNI_DECIMAL128);
            scaledLeft = scaledValues.first;
            scaledRight = scaledValues.second;
        }
        this->BinaryExprDecimalHelper(bExpr, scaledLeft, scaledRight, leftNull, rightNull);
        return;
    }
    LogWarn("Unsupported binary operator %d", bExpr->op);
    this->value = CreateInvalidCodeGenValue();
}

void ExpressionCodeGen::Visit(const UnaryExpr &uExpr)
{
    auto val = VisitExpr(*(uExpr.exp));
    if (!val->IsValidValue()) {
        this->value = CreateInvalidCodeGenValue();
        return;
    }

    switch (uExpr.op) {
        case omniruntime::expressions::Operator::NOT: {
            Value *notValue = builder->CreateNot(val->data, "logical_not");
            this->value = make_shared<CodeGenValue>(notValue, val->isNull);
            break;
        }
        default: {
            // ignore the unary operator if it is invalid
            if (TypeUtil::IsDecimalType(uExpr.GetReturnTypeId())) {
                Value *precision = llvmTypes->CreateConstantInt(uExpr.GetReturnType().GetPrecision());
                Value *scale = llvmTypes->CreateConstantInt(uExpr.GetReturnType().GetScale());
                this->value = make_shared<DecimalValue>(val->data, val->isNull, precision, scale);
                return;
            }
            this->value = make_shared<CodeGenValue>(val->data, val->isNull);
            break;
        }
    }
}

void ExpressionCodeGen::Visit(const SwitchExpr &switchExpr)
{
    Type *switchDataType = llvmTypes->VectorToLLVMType(switchExpr.GetReturnType());
    Expr *elseExpr = switchExpr.falseExpr;
    std::vector<std::pair<Expr *, Expr *>> whenClause = switchExpr.whenClause;
    const int size = whenClause.size();

    std::vector<BasicBlock *> condBlockList;
    std::vector<BasicBlock *> trueBlockList;
    BasicBlock *falseBlock = BasicBlock::Create(*context, "FALSE_BLOCK");
    BasicBlock *mergeBlock = BasicBlock::Create(*context, "ifcont");
    int32_t numReservedValues = 2;

    AllocaInst *resultValuePtr = builder->CreateAlloca(switchDataType, numReservedValues, nullptr, "temp_result_value");
    AllocaInst *resultNullPtr =
        builder->CreateAlloca(Type::getInt1Ty(*context), numReservedValues, nullptr, "temp_result_null");
    AllocaInst *resultLengthPtr =
        builder->CreateAlloca(Type::getInt32Ty(*context), numReservedValues, nullptr, "temp_result_length");

    AllocaInst *resultPrecisionPtr =
        builder->CreateAlloca(Type::getInt32Ty(*context), numReservedValues, nullptr, "temp_result_precision");

    AllocaInst *resultScalePtr =
        builder->CreateAlloca(Type::getInt32Ty(*context), numReservedValues, nullptr, "temp_result_scale");

    condBlockList.push_back(BasicBlock::Create(*context, "Condition" + std::to_string(0), func));
    trueBlockList.push_back(BasicBlock::Create(*context, "TRUE_BLOCK" + std::to_string(0), func));

    for (int i = 1; i < size; i++) { // generate block lists used in the next loop to evaluate conditions
        condBlockList.push_back(BasicBlock::Create(*context, "Condition" + std::to_string(i)));
        trueBlockList.push_back(BasicBlock::Create(*context, "TRUE_BLOCK" + std::to_string(i), func));
    }
    for (int i = 0; i < size; i++) { // evaluate condition in the whenClause
        Expr *cond = whenClause[i].first;
        Expr *resExpr = whenClause[i].second;

        // if cond evaluates to true, control flow goes to trueBlock, save evTrue to temp value
        // Otherwise goes to next Block in the list and keeps evaluating next cond in the whenClause
        // If last cond evaluates to false, control flow goes to falseBlock and save evFalse to temp value
        if (i == 0) { // Create the entry of the block
            builder->CreateBr(condBlockList[i]);
        }
        if (i > 0) {
            func->getBasicBlockList().push_back(condBlockList[i]);
        }

        auto elseBranch = falseBlock;
        if (i < size - 1) {
            elseBranch = condBlockList[i + 1];
        }
        builder->SetInsertPoint(condBlockList[i]);
        CodeGenValuePtr evCond = VisitExpr(*cond);
        if (!evCond->IsValidValue()) {
            this->value = CreateInvalidCodeGenValue();
            return;
        }
        builder->CreateCondBr(builder->CreateAnd(builder->CreateNot(evCond->isNull), evCond->data), trueBlockList[i],
            elseBranch);

        builder->SetInsertPoint(trueBlockList[i]);
        auto evTrue = VisitExpr(*resExpr);
        if (!evTrue->IsValidValue()) {
            this->value = CreateInvalidCodeGenValue();
            return;
        }
        builder->CreateStore(evTrue->data, resultValuePtr);
        builder->CreateStore(evTrue->isNull, resultNullPtr);
        if (TypeUtil::IsStringType(switchExpr.GetReturnTypeId())) {
            builder->CreateStore(evTrue->length, resultLengthPtr);
        } else if (TypeUtil::IsDecimalType(switchExpr.GetReturnTypeId())) {
            builder->CreateStore((Value *)static_cast<DecimalValue *>(evTrue.get())->GetPrecision(),
                resultPrecisionPtr);
            builder->CreateStore((Value *)static_cast<DecimalValue *>(evTrue.get())->GetScale(), resultScalePtr);
        }
        builder->CreateBr(mergeBlock);
    }

    func->getBasicBlockList().push_back(falseBlock);
    builder->SetInsertPoint(falseBlock);
    auto evFalse = VisitExpr(*elseExpr);
    if (!evFalse->IsValidValue()) {
        this->value = CreateInvalidCodeGenValue();
        return;
    }
    builder->CreateStore(evFalse->data, resultValuePtr);
    builder->CreateStore(evFalse->isNull, resultNullPtr);
    if (TypeUtil::IsStringType(switchExpr.GetReturnTypeId())) {
        builder->CreateStore(evFalse->length, resultLengthPtr);
    } else if (TypeUtil::IsDecimalType(switchExpr.GetReturnTypeId())) {
        builder->CreateStore((Value *)static_cast<DecimalValue *>(evFalse.get())->GetPrecision(), resultPrecisionPtr);
        builder->CreateStore((Value *)static_cast<DecimalValue *>(evFalse.get())->GetScale(), resultScalePtr);
    }
    builder->CreateBr(mergeBlock);

    func->getBasicBlockList().push_back(mergeBlock);
    builder->SetInsertPoint(mergeBlock);
    if (TypeUtil::IsStringType(switchExpr.GetReturnTypeId())) {
        this->value = make_shared<CodeGenValue>(builder->CreateLoad(resultValuePtr), builder->CreateLoad(resultNullPtr),
            builder->CreateLoad(resultLengthPtr));
    } else if (TypeUtil::IsDecimalType(switchExpr.GetReturnTypeId())) {
        this->value = make_shared<DecimalValue>(builder->CreateLoad(resultValuePtr), builder->CreateLoad(resultNullPtr),
            builder->CreateLoad(resultPrecisionPtr), builder->CreateLoad(resultScalePtr));
    } else {
        this->value =
            make_shared<CodeGenValue>(builder->CreateLoad(resultValuePtr), builder->CreateLoad(resultNullPtr));
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
    Type *phiType = llvmTypes->VectorToLLVMType(ifExpr.GetReturnType());
    func->getBasicBlockList().push_back(mergeBlock);
    builder->SetInsertPoint(mergeBlock);
    PHINode *pn = builder->CreatePHI(phiType, numReservedValues, "iftmp");
    PHINode *phiNull = builder->CreatePHI(evTrueNull->getType(), numReservedValues, "iftmpNull");

    pn->addIncoming(evTrueValue, trueBlock);
    pn->addIncoming(evFalseValue, falseBlock);
    phiNull->addIncoming(evTrueNull, trueBlock);
    phiNull->addIncoming(evFalseNull, falseBlock);

    PHINode *lengthPhi = nullptr;
    if (TypeUtil::IsStringType(ifExpr.GetReturnTypeId())) {
        lengthPhi = builder->CreatePHI(Type::getInt32Ty(*context), numReservedValues, "length");
        lengthPhi->addIncoming(evTrueLength, trueBlock);
        lengthPhi->addIncoming(evFalseLength, falseBlock);
    }

    PHINode *precisionPhi = nullptr;
    PHINode *scalePhi = nullptr;
    if (TypeUtil::IsDecimalType(ifExpr.GetReturnTypeId())) {
        precisionPhi = builder->CreatePHI(Type::getInt32Ty(*context), numReservedValues, "precision");
        auto evTruePrecision = (Value *)static_cast<DecimalValue *>(evTrue.get())->GetPrecision();
        auto evFalsePrecision = (Value *)static_cast<DecimalValue *>(evFalse.get())->GetPrecision();
        precisionPhi->addIncoming(evTruePrecision, trueBlock);
        precisionPhi->addIncoming(evFalsePrecision, falseBlock);

        scalePhi = builder->CreatePHI(Type::getInt32Ty(*context), numReservedValues, "scale");
        auto evTrueScale = (Value *)static_cast<DecimalValue *>(evTrue.get())->GetScale();
        auto evFalseScale = (Value *)static_cast<DecimalValue *>(evFalse.get())->GetScale();
        scalePhi->addIncoming(evTrueScale, trueBlock);
        scalePhi->addIncoming(evFalseScale, falseBlock);

        this->value = make_shared<DecimalValue>(pn, phiNull, precisionPhi, scalePhi);
        return;
    }

    this->value = make_shared<CodeGenValue>(pn, phiNull, lengthPhi);
}

void ExpressionCodeGen::Visit(const InExpr &inExpr)
{
    auto iExpr = const_cast<InExpr *>(&inExpr);
    Expr *toCompare = iExpr->arguments[0];
    CodeGenValuePtr argiValue;
    Value *inArray = llvm::ConstantInt::get(*context, APInt(1, 0));
    Value *isNull = llvm::ConstantInt::get(*context, APInt(1, 0));
    Type *retType = llvmTypes->ToLLVMType(iExpr->GetReturnTypeId());

    for (size_t i = 1; i < iExpr->arguments.size(); i++) {
        Value *tmpCmpData = llvmTypes->CreateConstantBool(false);
        Value *tmpCmpNull = llvmTypes->CreateConstantBool(false);

        if (AreInvalidDataTypes(toCompare->GetReturnTypeId(), iExpr->arguments[i]->GetReturnTypeId())) {
            LogError("Arg 1 and arg %d have different data types", i + 1);
            this->value = CreateInvalidCodeGenValue();
            return;
        }
        auto valueToCompare = VisitExpr(*toCompare);
        if (!valueToCompare->IsValidValue()) {
            this->value = CreateInvalidCodeGenValue();
            return;
        }

        argiValue = VisitExpr(*(iExpr->arguments[i]));
        if (!argiValue->IsValidValue()) {
            this->value = CreateInvalidCodeGenValue();
            return;
        }

        switch (iExpr->arguments[0]->GetReturnTypeId()) {
            case OMNI_INT:
            case OMNI_DATE32:
            case OMNI_LONG: {
                InExprIntegerHelper(argiValue, valueToCompare, tmpCmpData, tmpCmpNull);
                break;
            }
            case OMNI_DECIMAL64: {
                InExprDecimal64Helper(inExpr, i, valueToCompare, argiValue, tmpCmpData, tmpCmpNull);
                break;
            }
            case OMNI_DOUBLE: {
                InExprDoubleHelper(argiValue, valueToCompare, tmpCmpData, tmpCmpNull);
                break;
            }
            case OMNI_CHAR:
            case OMNI_VARCHAR: {
                InExprStringHelper(argiValue, valueToCompare, tmpCmpData, tmpCmpNull);
                break;
            }
            case OMNI_DECIMAL128: {
                InExprDecimal128Helper(inExpr, retType, i, valueToCompare, argiValue, tmpCmpData, tmpCmpNull);
                break;
            }
            default: {
                LogWarn("Unsupported data type in IN expr %d", iExpr->arguments[0]->GetReturnTypeId());
                this->value = CreateInvalidCodeGenValue();
                return;
            }
        }
        inArray = builder->CreateAnd(builder->CreateNot(tmpCmpNull), builder->CreateOr(inArray, tmpCmpData));
        isNull = builder->CreateOr(isNull, tmpCmpNull);
    }
    this->value = make_shared<CodeGenValue>(inArray, isNull);
}

void ExpressionCodeGen::InExprDecimal128Helper(const InExpr &inExpr, Type *retType, size_t i,
    CodeGenValuePtr &valueToCompare, CodeGenValuePtr &argiValue, Value *&tmpCmpData, Value *&tmpCmpNull)
{
    auto iExpr = const_cast<InExpr *>(&inExpr);
    vector<DataTypeId> params { OMNI_DECIMAL128, OMNI_DECIMAL128 };
    string funcId = FunctionSignature(decimal128CompareStr, params, OMNI_INT).ToString();

    auto scaledValues = RescaleDecimals(*iExpr, *valueToCompare, *argiValue,
        inExpr.arguments[i]->dataType->GetScale() - inExpr.arguments[0]->dataType->GetScale(), OMNI_DECIMAL128);

    auto scaledCompareTo = scaledValues.first;
    auto scaledArgi = scaledValues.second;

    tmpCmpData =
        builder->CreateICmpEQ(decimalIRBuilder->CallDecimalFunction(funcId, retType, { scaledCompareTo, scaledArgi }),
        llvmTypes->CreateConstantInt(0));

    tmpCmpNull = builder->CreateOr(valueToCompare->isNull, argiValue->isNull);
}

void ExpressionCodeGen::InExprStringHelper(CodeGenValuePtr &argiValue, CodeGenValuePtr &valueToCompare,
    Value *&tmpCmpData, Value *&tmpCmpNull)
{
    tmpCmpData =
        builder->CreateICmpEQ(StringCmp(valueToCompare->data, valueToCompare->length, argiValue->data, value->length),
        llvmTypes->CreateConstantInt(0));
    tmpCmpNull = builder->CreateOr(valueToCompare->isNull, argiValue->isNull);
}

void ExpressionCodeGen::InExprDoubleHelper(CodeGenValuePtr &argiValue, CodeGenValuePtr &valueToCompare,
    Value *&tmpCmpData, Value *&tmpCmpNull)
{
    tmpCmpData = builder->CreateFCmpOEQ(valueToCompare->data, argiValue->data);
    tmpCmpNull = builder->CreateOr(valueToCompare->isNull, argiValue->isNull);
}

void ExpressionCodeGen::InExprDecimal64Helper(const InExpr &inExpr, size_t i, CodeGenValuePtr &valueToCompare,
    CodeGenValuePtr &argiValue, Value *&tmpCmpData, Value *&tmpCmpNull)
{
    auto iExpr = const_cast<InExpr *>(&inExpr);
    auto scaledValues = RescaleDecimals(*iExpr, *valueToCompare, *argiValue,
        inExpr.arguments[i]->dataType->GetScale() - inExpr.arguments[0]->dataType->GetScale(), OMNI_DECIMAL64);
    auto scaledCompareTo = scaledValues.first;
    auto scaledArgi = scaledValues.second;

    tmpCmpData = builder->CreateICmpEQ(scaledCompareTo, scaledArgi);
    tmpCmpNull = builder->CreateOr(valueToCompare->isNull, argiValue->isNull);
}

void ExpressionCodeGen::InExprIntegerHelper(CodeGenValuePtr &argiValue, CodeGenValuePtr &valueToCompare,
    Value *&tmpCmpData, Value *&tmpCmpNull)
{
    tmpCmpData = builder->CreateICmpEQ(valueToCompare->data, argiValue->data);
    tmpCmpNull = builder->CreateOr(valueToCompare->isNull, argiValue->isNull);
}

bool ExpressionCodeGen::VisitBetweenExprHelper(BetweenExpr &bExpr, const shared_ptr<CodeGenValue> &val,
    const shared_ptr<CodeGenValue> &lowerVal, const shared_ptr<CodeGenValue> &upperVal,
    std::pair<Value **, Value **> cmpPair)
{
    Type *retType = llvmTypes->ToLLVMType(bExpr.GetReturnTypeId());
    auto cmpLeft = cmpPair.first;
    auto cmpRight = cmpPair.second;
    if (bExpr.value->GetReturnTypeId() == OMNI_INT || bExpr.value->GetReturnTypeId() == OMNI_LONG ||
        bExpr.value->GetReturnTypeId() == OMNI_DATE32) {
        *cmpLeft = builder->CreateICmpSLE(lowerVal->data, val->data, "between_cmpleft");
        *cmpRight = builder->CreateICmpSLE(val->data, upperVal->data, "between_cmpright");
        return true;
    } else if (bExpr.value->GetReturnTypeId() == OMNI_DOUBLE) {
        *cmpLeft = builder->CreateFCmpULE(lowerVal->data, val->data, "between_cmpleft");
        *cmpRight = builder->CreateFCmpULE(val->data, upperVal->data, "between_cmpright");
        return true;
    } else if (TypeUtil::IsStringType(bExpr.value->GetReturnTypeId())) {
        *cmpLeft = builder->CreateICmpSLE(this->StringCmp(lowerVal->data, lowerVal->length, val->data, val->length),
            llvmTypes->CreateConstantInt(0));
        *cmpRight = builder->CreateICmpSLE(this->StringCmp(val->data, val->length, upperVal->data, upperVal->length),
            llvmTypes->CreateConstantInt(0));
        return true;
    } else if (TypeUtil::IsDecimalType(bExpr.value->GetReturnTypeId())) {
        auto retTypeId = bExpr.value->GetReturnTypeId();

        auto scaledValues1 = RescaleDecimals(bExpr, *lowerVal, *val,
            bExpr.value->dataType->GetScale() - bExpr.lowerBound->dataType->GetScale(), retTypeId);
        auto cmpLeftScaledLower = scaledValues1.first;
        auto cmpLeftScaledVal = scaledValues1.second;

        auto scaledValues2 = RescaleDecimals(bExpr, *val, *upperVal,
            bExpr.upperBound->dataType->GetScale() - bExpr.value->dataType->GetScale(), retTypeId);
        auto cmpRightScaledVal = scaledValues2.first;
        auto cmpRightScaledUpper = scaledValues2.second;

        if (retTypeId == OMNI_DECIMAL64) {
            *cmpLeft = builder->CreateICmpSLE(cmpLeftScaledLower, cmpLeftScaledVal, "between_cmpleft");
            *cmpRight = builder->CreateICmpSLE(cmpRightScaledVal, cmpRightScaledUpper, "between_cmpright");
        } else if (retTypeId == OMNI_DECIMAL128) {
            std::vector<DataTypeId> params { OMNI_DECIMAL128, OMNI_DECIMAL128 };

            std::string funcId = FunctionSignature(decimal128CompareStr, params, OMNI_INT).ToString();

            *cmpLeft = builder->CreateICmpSLE(
                decimalIRBuilder->CallDecimalFunction(funcId, retType, { cmpLeftScaledLower, cmpLeftScaledVal }),
                llvmTypes->CreateConstantInt(0));
            *cmpRight = builder->CreateICmpSLE(
                decimalIRBuilder->CallDecimalFunction(funcId, retType, { cmpRightScaledVal, cmpRightScaledUpper }),
                llvmTypes->CreateConstantInt(0));
        }
        return true;
    }
    return false;
}

void ExpressionCodeGen::Visit(const BetweenExpr &btExpr)
{
    auto bExpr = const_cast<BetweenExpr *>(&btExpr);

    auto val = VisitExpr(*(bExpr->value));
    if (!val->IsValidValue()) {
        this->value = CreateInvalidCodeGenValue();
        return;
    }

    DataTypeId valueTypeId = bExpr->value->GetReturnTypeId();
    if (AreInvalidDataTypes(valueTypeId, bExpr->lowerBound->GetReturnTypeId()) &&
        AreInvalidDataTypes(valueTypeId, bExpr->upperBound->GetReturnTypeId())) {
        LogError("Value, lower bound, and upper bound must have the same type");
        this->value = CreateInvalidCodeGenValue();
        return;
    }

    auto valNull = val->isNull;
    auto lowerVal = VisitExpr(*(bExpr->lowerBound));
    if (!lowerVal->IsValidValue()) {
        this->value = CreateInvalidCodeGenValue();
        return;
    }

    auto lowerValNull = lowerVal->isNull;
    auto upperVal = VisitExpr(*(bExpr->upperBound));
    if (!upperVal->IsValidValue()) {
        this->value = CreateInvalidCodeGenValue();
        return;
    }

    auto upperValNull = upperVal->isNull;
    auto isAnyNull = builder->CreateOr(builder->CreateOr(valNull, lowerValNull), upperValNull);
    auto isNeitherNull = builder->CreateNot(isAnyNull);
    Value *cmpLeft, *cmpRight;
    std::pair<llvm::Value **, llvm::Value **> cmpPair = std::make_pair(&cmpLeft, &cmpRight);
    bool supportedType = VisitBetweenExprHelper(*bExpr, val, lowerVal, upperVal, cmpPair);

    if (supportedType) {
        std::vector<Value *> andValues;
        andValues.push_back(isNeitherNull);
        andValues.push_back(cmpLeft);
        andValues.push_back(cmpRight);
        Value *result = builder->CreateAnd(andValues);
        this->value = make_shared<CodeGenValue>(result, isAnyNull);
        return;
    }

    LogError("Unsupported data type for between %d", valueTypeId);
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
    Type *phiType = llvmTypes->VectorToLLVMType(cExpr.GetReturnType());
    func->getBasicBlockList().push_back(mergeBlock);
    builder->SetInsertPoint(mergeBlock);
    PHINode *pn = builder->CreatePHI(phiType, numReservedValues, "iftmp");
    PHINode *pnNull = builder->CreatePHI(value1->isNull->getType(), numReservedValues, "iftmp");

    pn->addIncoming(value1Data, isNotNullBlock);
    pn->addIncoming(value2Data, isNullBlock);
    pnNull->addIncoming(value1->isNull, isNotNullBlock);
    pnNull->addIncoming(value2->isNull, isNullBlock);

    PHINode *lengthPhi = nullptr;
    if (TypeUtil::IsStringType(cExpr.GetReturnTypeId())) {
        lengthPhi = builder->CreatePHI(Type::getInt32Ty(*context), numReservedValues, "length");
        lengthPhi->addIncoming(value1Length, isNotNullBlock);
        lengthPhi->addIncoming(value2Length, isNullBlock);
    }

    if (TypeUtil::IsDecimalType(cExpr.GetReturnTypeId())) {
        HandleCoalesceDecimals(*value1.get(), *value2.get(), *isNotNullBlock, *isNullBlock, *pn, *pnNull);
        return;
    }

    this->value = make_shared<CodeGenValue>(pn, pnNull, lengthPhi);
}

void ExpressionCodeGen::HandleCoalesceDecimals(CodeGenValue &v1, CodeGenValue &v2, BasicBlock &isNotNullBlock,
    BasicBlock &isNullBlock, PHINode &pn, PHINode &pnNull)
{
    int32_t numReservedValues = 2;
    auto precisionPhi = builder->CreatePHI(Type::getInt32Ty(*context), numReservedValues, "precision");
    auto value1Precision = (Value *)static_cast<DecimalValue &>(v1).GetPrecision();
    auto value2Precision = (Value *)static_cast<DecimalValue &>(v2).GetPrecision();
    precisionPhi->addIncoming(value1Precision, &isNotNullBlock);
    precisionPhi->addIncoming(value2Precision, &isNullBlock);

    auto scalePhi = builder->CreatePHI(Type::getInt32Ty(*context), numReservedValues, "scale");
    auto value1Scale = (Value *)static_cast<DecimalValue &>(v1).GetScale();
    auto value2Scale = (Value *)static_cast<DecimalValue &>(v2).GetScale();
    scalePhi->addIncoming(value1Scale, &isNotNullBlock);
    scalePhi->addIncoming(value2Scale, &isNullBlock);

    this->value = make_shared<DecimalValue>(&pn, &pnNull, precisionPhi, scalePhi);
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

    Value *result = builder->CreateICmpEQ(isNullValue, llvmTypes->CreateConstantBool(true), "isNullCompare");
    this->value = make_shared<CodeGenValue>(result, llvmTypes->CreateConstantBool(false));
}

// Handles all functions
void ExpressionCodeGen::Visit(const FuncExpr &fExpr)
{
    std::vector<Value *> argVals;
    CodeGenValuePtr resultPtr;

    int numArgs = fExpr.arguments.size();
    Value *isAnyNull = llvmTypes->CreateConstantBool(false);
    bool isDecimalFunction = false;
    DataTypeId funcRetType = fExpr.GetReturnTypeId();

    // set execution context
    if (fExpr.function->IsExecutionContextSet()) {
        argVals.push_back(this->codegenContext->executionContext);
    }
    for (int i = 0; i < numArgs; i++) {
        Expr *argN = fExpr.arguments[i];
        resultPtr = VisitExpr(*argN);
        if (!resultPtr->IsValidValue()) {
            this->value = CreateInvalidCodeGenValue();
            return;
        }
        argVals.push_back(resultPtr->data);
        isAnyNull = builder->CreateOr(isAnyNull, resultPtr->isNull);
        if ((TypeUtil::IsStringType(fExpr.arguments[i]->GetReturnTypeId()))) {
            if (fExpr.arguments[i]->GetReturnTypeId() == OMNI_CHAR) {
                argVals.push_back(llvmTypes->CreateConstantInt(fExpr.arguments[i]->GetReturnType().GetWidth()));
            }
            argVals.push_back(this->value->length);
        }
        if (argN->GetReturnTypeId() == OMNI_DECIMAL128) {
            isDecimalFunction = true;
        }
        if (fExpr.funcName == mm3hashStr) {
            argVals.push_back(this->value->isNull);
        }
    }
    Value *ret = nullptr;
    Value *outputLen = nullptr;
    AllocaInst *outputLenPtr = nullptr;
    // Call Decimal IR Generator for decimal functions
    if (TypeUtil::IsDecimalType(funcRetType)) {
        auto outputValuePtr = decimalIRBuilder->BuildDecimalValue(nullptr, fExpr.GetReturnType());
        ret =
            decimalIRBuilder->CallDecimalFunction(fExpr.function->GetId(), llvmTypes->ToLLVMType(funcRetType), argVals);
        outputValuePtr->data = ret;
        outputValuePtr->isNull = isAnyNull;
        outputValuePtr->length = outputLen;
        this->value = std::move(outputValuePtr);
        return;
    } else {
        if (TypeUtil::IsStringType(funcRetType)) {
            outputLenPtr = builder->CreateAlloca(Type::getInt32Ty(*context), nullptr, "output_len");
            argVals.push_back(outputLenPtr);
        }

        auto f = module->getFunction(fExpr.function->GetId());
        if (f) {
            ret = isDecimalFunction ? decimalIRBuilder->CallDecimalFunction(fExpr.function->GetId(),
                llvmTypes->ToLLVMType(funcRetType), argVals) :
                                      codeGenUtils->CreateCall(f, argVals, fExpr.function->GetId());
            InlineFunctionInfo inlineFunctionInfo;
            llvm::InlineFunction(*((CallInst *)ret), inlineFunctionInfo);
            outputLen = (outputLenPtr == nullptr) ? nullptr : builder->CreateLoad(outputLenPtr);
        } else {
            LogWarn("Unable to generate function : %s", fExpr.funcName.c_str());
            this->value = make_shared<CodeGenValue>(nullptr, nullptr, nullptr);
            return;
        }
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

    codeGenUtils->RemoveUnusedFunctions();

    mpm.add(createFunctionInliningPass());
    mpm.add(createPruneEHPass());

    fpm->doInitialization();
    for (auto &F : *module) {
        fpm->run(F);
    }
    mpm.run(*module);
}

void ExpressionCodeGen::OptimizeModule()
{
    codeGenUtils->RemoveUnusedFunctions();

    mpm.add(createFunctionInliningPass());
    mpm.add(createPruneEHPass());

    mpm.run(*module);
}
