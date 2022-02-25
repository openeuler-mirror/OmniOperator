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

// TODO:: define size of a vector
std::vector<Type *> ExpressionCodeGen::GetFunctionArgTypeVector(std::vector<VecTypeId> &params, VecTypeId &retTypeId,
    bool needsContext)
{
    std::vector<Type *> outTypeVector;
    if (needsContext) {
        outTypeVector.push_back(llvmTypes->I64Type());
    }
    for (auto type : params) {
        outTypeVector.push_back(llvmTypes->ToLLVMType(type));
        if (TypeUtil::IsStringType(type)) {
            if (type == OMNI_VEC_TYPE_CHAR) {
                // Add Type for width support
                outTypeVector.push_back(llvmTypes->I32Type());
            }
            // Add Type for Length of the string
            outTypeVector.push_back(llvmTypes->I32Type());
        }
    }
    // return arguments
    if (TypeUtil::IsStringType(retTypeId)) {
        outTypeVector.push_back(llvmTypes->I32PtrType());
    }
    return outTypeVector;
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

ExpressionCodeGen::ExpressionCodeGen(std::string name, const Expr &cpExpr) : funcName(std::move(name)), expr(&cpExpr) {}

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
        std::vector<VecTypeId> params = func.GetParamTypes();
        VecTypeId retType = func.GetReturnType();
        std::vector<Type *> args = this->GetFunctionArgTypeVector(params, retType, func.IsExecutionContextSet());
        auto s = llvm::orc::absoluteSymbols({
            {
                mangle(func.GetId()),
                JITEvaluatedSymbol(pointerToJITTargetAddress(func.GetAddress()), JITSymbolFlags::Exported)
                }
            });
        auto ign = jd.define(s);
        if (ign) {
            LogError("Error while defining absolute symbol in jd");
        }
        llvm::Type *ret = llvmTypes->ToLLVMType(retType);
        llvm::FunctionType *ft = llvm::FunctionType::get(ret, args, false);
        auto linkage = llvm::Function::ExternalLinkage;
        llvm::Function *fn = llvm::Function::Create(ft, linkage, func.GetId(), *module);
        FunctionCallee callee = module->getOrInsertFunction(func.GetId(), ft);
    }
}

// Other operations which require externed functions
Value *ExpressionCodeGen::StringCmp(Value *lhs, Value *lLen, Value *rhs, Value *rLen)
{
    // call function
    std::vector<Value *> argVals { lhs, lLen, rhs, rLen };
    auto signature = FunctionSignature(strCompareStr,
        std::vector<VecTypeId> { OMNI_VEC_TYPE_VARCHAR, OMNI_VEC_TYPE_VARCHAR }, OMNI_VEC_TYPE_INT);
    auto f = module->getFunction(FunctionRegistry::LookupFunction(&signature)->GetId());
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
    auto signature = FunctionSignature(decimal128CompareStr,
        std::vector<VecTypeId> { OMNI_VEC_TYPE_DECIMAL128, OMNI_VEC_TYPE_DECIMAL128 }, OMNI_VEC_TYPE_INT);
    auto f = module->getFunction(FunctionRegistry::LookupFunction(&signature)->GetId());
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
        std::vector<Value *> argLeftVals { this->codegenContext->executionContext, left, left };
        std::vector<Value *> argRightVals { this->codegenContext->executionContext, right, right };
        incomingBlock = builder->GetInsertBlock();
        nullBlock = BasicBlock::Create(*context, "nullBlock", builder->GetInsertBlock()->getParent());
        nextInst = BasicBlock::Create(*context, "nextInst", builder->GetInsertBlock()->getParent());
        nullCond = builder->CreateOr(leftIsNull, rightIsNull);
        builder->CreateCondBr(nullCond, nullBlock, nextInst);
        builder->SetInsertPoint(nullBlock);
        switch (binaryExpr->left->GetReturnType().GetId()) {
            case OMNI_VEC_TYPE_INT:
            case OMNI_VEC_TYPE_DATE32:
            case OMNI_VEC_TYPE_LONG:
            case OMNI_VEC_TYPE_DECIMAL64:
                leftZero = builder->CreateSub(left, left);
                rightZero = builder->CreateSub(right, right);
                break;
            case OMNI_VEC_TYPE_DOUBLE:
                leftZero = builder->CreateFSub(left, left);
                rightZero = builder->CreateFSub(right, right);
                break;
            case OMNI_VEC_TYPE_DECIMAL128: {
                auto signature = FunctionSignature(subDec128Str,
                    std::vector<VecTypeId> { OMNI_VEC_TYPE_DECIMAL128, OMNI_VEC_TYPE_DECIMAL128 },
                    OMNI_VEC_TYPE_DECIMAL128);
                auto f = module->getFunction(FunctionRegistry::LookupFunction(&signature)->GetId());
                leftZero = builder->CreateCall(f, argLeftVals, subDec128Str);
                rightZero = builder->CreateCall(f, argRightVals, subDec128Str);
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
    if (op == DIV || op == MOD) {
        DivExprNullHelper(binaryExpr, left, right, leftIsNull, rightIsNull, leftPhi, rightPhi);
    }
}

void ExpressionCodeGen::DivExprNullHelper(const BinaryExpr *binaryExpr, Value *left, Value *right, Value *leftIsNull,
    Value *rightIsNull, PHINode **leftPhi, PHINode **rightPhi)
{
    BasicBlock *incomingBlock, *nullBlock, *nextInst;
    Value *nullCond, *leftZero, *rightOne;

    std::vector<Value *> argLeftVals { this->codegenContext->executionContext, left, left };
    std::vector<Value *> argRightVals { this->codegenContext->executionContext, right, right };
    incomingBlock = builder->GetInsertBlock();
    nullBlock = BasicBlock::Create(*context, "nullBlock", builder->GetInsertBlock()->getParent());
    nextInst = BasicBlock::Create(*context, "nextInst", builder->GetInsertBlock()->getParent());
    nullCond = builder->CreateOr(leftIsNull, rightIsNull);
    builder->CreateCondBr(nullCond, nullBlock, nextInst);
    builder->SetInsertPoint(nullBlock);
    switch (binaryExpr->left->GetReturnTypeId()) {
        case OMNI_VEC_TYPE_INT:
        case OMNI_VEC_TYPE_DATE32:
            leftZero = builder->CreateSub(left, left);
            rightOne = builder->CreateSub(builder->CreateAdd(right, llvmTypes->CreateConstantInt(1)), right);
            break;
        case OMNI_VEC_TYPE_LONG:
        case OMNI_VEC_TYPE_DECIMAL64:
            leftZero = builder->CreateSub(left, left);
            rightOne = builder->CreateSub(builder->CreateAdd(right, llvmTypes->CreateConstantLong(1)), right);
            break;
        case OMNI_VEC_TYPE_DOUBLE:
            leftZero = builder->CreateFSub(left, left);
            rightOne = builder->CreateFSub(builder->CreateFAdd(right, llvmTypes->CreateConstantDouble(1.0)), right);
            break;
        case OMNI_VEC_TYPE_DECIMAL128: {
            auto subSignature = FunctionSignature(subDec128Str,
                std::vector<VecTypeId> { OMNI_VEC_TYPE_DECIMAL128, OMNI_VEC_TYPE_DECIMAL128 },
                OMNI_VEC_TYPE_DECIMAL128);
            auto f = module->getFunction(FunctionRegistry::LookupFunction(&subSignature)->GetId());
            leftZero = builder->CreateCall(f, argLeftVals, "Sub_decimal128");

            auto divSignature = FunctionSignature(divDec128Str,
                std::vector<VecTypeId> { OMNI_VEC_TYPE_DECIMAL128, OMNI_VEC_TYPE_DECIMAL128 },
                OMNI_VEC_TYPE_DECIMAL128);
            f = module->getFunction(FunctionRegistry::LookupFunction(&subSignature)->GetId());
            rightOne = builder->CreateCall(f, argRightVals, "Div_decimal128");
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
            return builder->CreateAnd(isNeitherNull, builder->CreateICmpSLT(
                this->StringCmp(leftVal, leftLen, rightVal, rightLen), llvmTypes->CreateConstantInt(0)));
        case GT:
            return builder->CreateAnd(isNeitherNull, builder->CreateICmpSGT(
                this->StringCmp(leftVal, leftLen, rightVal, rightLen), llvmTypes->CreateConstantInt(0)));
        case LTE:
            return builder->CreateAnd(isNeitherNull, builder->CreateICmpSLE(
                this->StringCmp(leftVal, leftLen, rightVal, rightLen), llvmTypes->CreateConstantInt(0)));
        case GTE:
            return builder->CreateAnd(isNeitherNull, builder->CreateICmpSGE(
                this->StringCmp(leftVal, leftLen, rightVal, rightLen), llvmTypes->CreateConstantInt(0)));
        case EQ:
            return builder->CreateAnd(isNeitherNull, builder->CreateICmpEQ(
                this->StringCmp(leftVal, leftLen, rightVal, rightLen), llvmTypes->CreateConstantInt(0)));
        case NEQ:
            return builder->CreateAnd(isNeitherNull, builder->CreateICmpNE(
                this->StringCmp(leftVal, leftLen, rightVal, rightLen), llvmTypes->CreateConstantInt(0)));
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
    std::vector<Value *> argVals { this->codegenContext->executionContext, leftPhi, rightPhi };
    std::vector<VecTypeId> binaryFuncParamTypes { OMNI_VEC_TYPE_DECIMAL128, OMNI_VEC_TYPE_DECIMAL128 };

    switch (binaryExpr->op) {
        case LT:
            return builder->CreateAnd(isNeitherNull,
                builder->CreateICmpSLT(this->Decimal128Cmp(*left, *right), llvmTypes->CreateConstantInt(0)));
        case GT:
            return builder->CreateAnd(isNeitherNull,
                builder->CreateICmpSGT(this->Decimal128Cmp(*left, *right), llvmTypes->CreateConstantInt(0)));
        case LTE:
            return builder->CreateAnd(isNeitherNull,
                builder->CreateICmpSLE(this->Decimal128Cmp(*left, *right), llvmTypes->CreateConstantInt(0)));
        case GTE:
            return builder->CreateAnd(isNeitherNull,
                builder->CreateICmpSGE(this->Decimal128Cmp(*left, *right), llvmTypes->CreateConstantInt(0)));
        case EQ:
            return builder->CreateAnd(isNeitherNull,
                builder->CreateICmpEQ(this->Decimal128Cmp(*left, *right), llvmTypes->CreateConstantInt(0)));
        case NEQ:
            return builder->CreateAnd(isNeitherNull,
                builder->CreateICmpNE(this->Decimal128Cmp(*left, *right), llvmTypes->CreateConstantInt(0)));
        case ADD: {
            auto signature = FunctionSignature(addDec128Str, binaryFuncParamTypes, OMNI_VEC_TYPE_DECIMAL128);
            auto f = module->getFunction(FunctionRegistry::LookupFunction(&signature)->GetId());
            return builder->CreateCall(f, argVals, addDec128Str);
        }
        case SUB: {
            auto signature = FunctionSignature(subDec128Str, binaryFuncParamTypes, OMNI_VEC_TYPE_DECIMAL128);
            auto f = module->getFunction(FunctionRegistry::LookupFunction(&signature)->GetId());
            return builder->CreateCall(f, argVals, subDec128Str);
        }
        case MUL: {
            auto signature = FunctionSignature(mulDec128Str, binaryFuncParamTypes, OMNI_VEC_TYPE_DECIMAL128);
            auto f = module->getFunction(FunctionRegistry::LookupFunction(&signature)->GetId());
            return builder->CreateCall(f, argVals, mulDec128Str);
        }
        case DIV: {
            auto signature = FunctionSignature(divDec128Str, binaryFuncParamTypes, OMNI_VEC_TYPE_DECIMAL128);
            auto f = module->getFunction(FunctionRegistry::LookupFunction(&signature)->GetId());
            return builder->CreateCall(f, argVals, divDec128Str);
        }
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
    if (expr->GetReturnTypeId() == VecTypeId::OMNI_VEC_TYPE_VARCHAR) {
        result->data = builder->CreatePtrToInt(result->data, llvmTypes->I64Type());
    }
    // Return value
    builder->CreateRet(result->data);
    verifyFunction(*func);
    return func;
}

Value *ExpressionCodeGen::GetIntToPtr(omniruntime::vec::VecTypeId vecTypeId, Value *elementAddr)
{
    Value *elementPtr = nullptr;
    // Convert the column address to array of proper datatype.
    switch (vecTypeId) {
        case OMNI_VEC_TYPE_BOOLEAN:
            elementPtr = builder->CreateIntToPtr(elementAddr, llvmTypes->I1PtrType());
            break;
        case OMNI_VEC_TYPE_INT:
        case OMNI_VEC_TYPE_DATE32:
            elementPtr = builder->CreateIntToPtr(elementAddr, llvmTypes->I32PtrType());
            break;
        case OMNI_VEC_TYPE_LONG:
        case OMNI_VEC_TYPE_DECIMAL64:
            elementPtr = builder->CreateIntToPtr(elementAddr, llvmTypes->I64PtrType());
            break;
        case OMNI_VEC_TYPE_DOUBLE:
            elementPtr = builder->CreateIntToPtr(elementAddr, llvmTypes->DoublePtrType());
            break;
        case OMNI_VEC_TYPE_CHAR:
        case OMNI_VEC_TYPE_VARCHAR:
            elementPtr = builder->CreateIntToPtr(elementAddr, llvmTypes->I8PtrType());
            break;
        case OMNI_VEC_TYPE_DECIMAL128:
            elementPtr = builder->CreateIntToPtr(elementAddr, llvmTypes->I64PtrType());
            break;
        default:
            LLVM_DEBUG_LOG("Unsupported column data type %d", vecTypeId);
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
        case OMNI_VEC_TYPE_INT:
        case OMNI_VEC_TYPE_DATE32: {
            codeGenValue = new CodeGenValue(llvmTypes->CreateConstantInt(lExpr.intVal),
                llvmTypes->CreateConstantBool(isNullLiteral));
            break;
        }
        case OMNI_VEC_TYPE_LONG: {
            codeGenValue = new CodeGenValue(llvmTypes->CreateConstantLong(lExpr.longVal),
                llvmTypes->CreateConstantBool(isNullLiteral));
            break;
        }
        case OMNI_VEC_TYPE_DOUBLE: {
            codeGenValue = new CodeGenValue(llvmTypes->CreateConstantDouble(lExpr.doubleVal),
                llvmTypes->CreateConstantBool(isNullLiteral));
            break;
        }
        case OMNI_VEC_TYPE_CHAR:
        case OMNI_VEC_TYPE_VARCHAR: {
            Constant *strValConst =
                ConstantInt::get(*context, APInt(INT64_VALUE, reinterpret_cast<int64_t>(lExpr.stringVal->c_str())));
            Value *strValPtr = ConstantExpr::getIntToPtr(strValConst, llvmTypes->I8PtrType());
            Constant *strLenConst =
                ConstantInt::get(*context, APInt(INT32_VALUE, static_cast<int32_t>(lExpr.stringVal->length())));
            codeGenValue = new CodeGenValue(strValPtr, llvmTypes->CreateConstantBool(isNullLiteral), strLenConst);
            break;
        }
        case OMNI_VEC_TYPE_BOOLEAN: {
            codeGenValue = new CodeGenValue(llvmTypes->CreateConstantBool(lExpr.boolVal),
                llvmTypes->CreateConstantBool(isNullLiteral));
            break;
        }
        case OMNI_VEC_TYPE_DECIMAL64: {
            codeGenValue = new CodeGenValue(llvmTypes->CreateConstantLong(lExpr.longVal),
                llvmTypes->CreateConstantBool(isNullLiteral));
            break;
        }
        case OMNI_VEC_TYPE_DECIMAL128: {
            int32_t length = 2;
            Decimal128 decValue = lExpr.longVal;
            auto decimal = std::make_unique<int64_t[]>(length).release();

            decimal[0] = decValue.LowBits();
            decimal[1] = decValue.HighBits();

            Constant *addr = ConstantInt::get(*context, APInt(INT64_VALUE, reinterpret_cast<int64_t>(decimal)));
            codeGenValue = new CodeGenValue(addr, llvmTypes->CreateConstantBool(isNullLiteral));
            break;
        }
        case OMNI_VEC_TYPE_NONE: {
            codeGenValue =
                new CodeGenValue(llvmTypes->CreateConstantInt(lExpr.intVal), llvmTypes->CreateConstantBool(true));
            break;
        }
        default: {
            LLVM_DEBUG_LOG("Unsupported data type in Data Expr %d", lExpr.GetReturnTypeId());
            codeGenValue =
                new CodeGenValue(llvmTypes->CreateConstantBool(lExpr.boolVal), llvmTypes->CreateConstantBool(false));
            break;
        }
    }
    return codeGenValue;
}

Value *ExpressionCodeGen::GetDictionaryVectorValue(VecType vectorType, Value *rowIdx, Value *dictionaryVectorPtr,
    AllocaInst *&lengthAllocaInst)
{
    std::vector<VecTypeId> paramTypes = { OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_INT };
    VecTypeId typeId = vectorType.GetId();
    FunctionSignature dictionaryFuncSignature;
    switch (typeId) {
        case OMNI_VEC_TYPE_INT:
        case OMNI_VEC_TYPE_DATE32:
            dictionaryFuncSignature = FunctionSignature(dictionaryGetIntStr, paramTypes, OMNI_VEC_TYPE_INT);
            break;
        case OMNI_VEC_TYPE_LONG:
        case OMNI_VEC_TYPE_DECIMAL64:
            dictionaryFuncSignature = FunctionSignature(dictionaryGetLongStr, paramTypes, OMNI_VEC_TYPE_LONG);
            break;
        case OMNI_VEC_TYPE_DECIMAL128:
            dictionaryFuncSignature = FunctionSignature(dictionaryGetDecimalStr, paramTypes, OMNI_VEC_TYPE_DECIMAL128);
            break;
        case OMNI_VEC_TYPE_DOUBLE:
            dictionaryFuncSignature = FunctionSignature(dictionaryGetDoubleStr, paramTypes, OMNI_VEC_TYPE_DOUBLE);
            break;
        case OMNI_VEC_TYPE_BOOLEAN:
            dictionaryFuncSignature = FunctionSignature(dictionaryGetBooleanStr, paramTypes, OMNI_VEC_TYPE_BOOLEAN);
            break;
        case OMNI_VEC_TYPE_CHAR:
        case OMNI_VEC_TYPE_VARCHAR:
            dictionaryFuncSignature = FunctionSignature(dictionaryGetVarcharStr, paramTypes, OMNI_VEC_TYPE_VARCHAR);
            break;
        default:
            LLVM_DEBUG_LOG("Unsupported dictionary value type: %d", typeId);
            return nullptr;
    }
    auto dictionaryFunc = module->getFunction(FunctionRegistry::LookupFunction(&dictionaryFuncSignature)->GetId());
    std::vector<Value *> funcArgs;

    if (typeId == OMNI_VEC_TYPE_DECIMAL128) {
        funcArgs.push_back(this->codegenContext->executionContext);
    }

    funcArgs.push_back(dictionaryVectorPtr);
    funcArgs.push_back(rowIdx);
    if (TypeUtil::IsStringType(typeId)) {
        lengthAllocaInst = builder->CreateAlloca(llvmTypes->I32Type(), nullptr, "varchar_length");
        builder->CreateStore(llvmTypes->CreateConstantInt(0), lengthAllocaInst);
        funcArgs.push_back(lengthAllocaInst);
    }

    auto call = builder->CreateCall(dictionaryFunc, funcArgs, "get_dictionary_value");
    InlineFunctionInfo inlineFunctionInfo;
    auto inlinedFunction = llvm::InlineFunction(*call, inlineFunctionInfo);
    return call;
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

    this->value.reset(new CodeGenValue(phiValue, bitmapValue, phiLength));
    return;
}

CodeGenValuePtr CreateInvalidCodeGenValue()
{
    return make_shared<CodeGenValue>(nullptr, nullptr, nullptr);
}

void ExpressionCodeGen::Visit(const BinaryExpr &binaryExpr)
{
    const BinaryExpr *bExpr = &binaryExpr;

    auto leftId = static_cast<int32_t>(bExpr->left->GetReturnTypeId());
    auto rightId = static_cast<int32_t>(bExpr->right->GetReturnTypeId());
    if (bExpr->left->GetType() == ExprType::LITERAL_E || bExpr->right->GetType() == ExprType::LITERAL_E ||
        bExpr->left->GetType() == ExprType::FIELD_E || bExpr->right->GetType() == ExprType::FIELD_E) {
        if (leftId != rightId) {
            VecType &biggerType = leftId < rightId ? bExpr->right->GetReturnType() : bExpr->left->GetReturnType();
            bExpr->left->dataType = std::make_unique<VecType>(biggerType);
            bExpr->right->dataType = std::make_unique<VecType>(biggerType);
        }
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

    if (bExpr->left->GetReturnTypeId() == OMNI_VEC_TYPE_INT || bExpr->left->GetReturnTypeId() == OMNI_VEC_TYPE_LONG ||
        bExpr->left->GetReturnTypeId() == OMNI_VEC_TYPE_DATE32 ||
        bExpr->left->GetReturnTypeId() == OMNI_VEC_TYPE_DECIMAL64) {
        this->value =
            make_shared<CodeGenValue>(this->BinaryExprIntHelper(bExpr, leftValue, rightValue, leftNull, rightNull),
            builder->CreateOr(leftNull, rightNull));
        return;
    } else if (bExpr->left->GetReturnTypeId() == OMNI_VEC_TYPE_DOUBLE) {
        this->value =
            make_shared<CodeGenValue>(this->BinaryExprDoubleHelper(bExpr, leftValue, rightValue, leftNull, rightNull),
            builder->CreateOr(leftNull, rightNull));
        return;
    } else if (TypeUtil::IsStringType(bExpr->left->GetReturnTypeId())) {
        this->value = make_shared<CodeGenValue>(
            this->BinaryExprStringHelper(bExpr, leftValue, leftLen, rightValue, rightLen, leftNull, rightNull),
            builder->CreateOr(leftNull, rightNull));
        return;
    } else if (bExpr->left->GetReturnTypeId() == OMNI_VEC_TYPE_DECIMAL128) {
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
    Value *trueValue = llvmTypes->CreateConstantBool(true);
    Value *falseValue = llvmTypes->CreateConstantBool(false);

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


void ExpressionCodeGen::Visit(const SwitchExpr &switchExpr)
{
    Type *switchDataType = llvmTypes->VectorToLLVMType(switchExpr.GetReturnType());
    Expr *elseExpr = switchExpr.falseExpr;
    std::vector<std::pair<Expr*, Expr*>> whenClause = switchExpr.whenClause;
    const int size = whenClause.size();

    std::vector<BasicBlock*> condBlockList;
    std::vector<BasicBlock*> trueBlockList;
    BasicBlock *falseBlock = BasicBlock::Create(*context, "FALSE_BLOCK");
    BasicBlock *mergeBlock = BasicBlock::Create(*context, "ifcont");
    int32_t numReservedValues = 2;

    AllocaInst *resultValuePtr = builder->CreateAlloca(switchDataType, numReservedValues,
                                                       nullptr, "temp_result_value");
    AllocaInst *resultNullPtr = builder->CreateAlloca(Type::getInt1Ty(*context), numReservedValues,
                                                      nullptr,  "temp_result_null");
    AllocaInst *resultLengthPtr = builder->CreateAlloca(Type::getInt32Ty(*context), numReservedValues,
                                                        nullptr, "temp_result_length");
    condBlockList.push_back(BasicBlock::Create(*context, "Condition" + std::to_string(0), func));
    trueBlockList.push_back(BasicBlock::Create(*context, "TRUE_BLOCK" + std::to_string(0), func));

    for (int i = 1; i < size; i++) {  // generate block lists used in the next loop to evaluate conditions
        condBlockList.push_back(BasicBlock::Create(*context, "Condition" + std::to_string(i)));
        trueBlockList.push_back(BasicBlock::Create(*context, "TRUE_BLOCK" + std::to_string(i), func));
    }
    for (int i = 0; i < size; i++) {  // evaluate condition in the whenClause
        Expr *cond = whenClause[i].first;
        Expr *resExpr = whenClause[i].second;

        // if cond evaluates to true, control flow goes to trueBlock, save evTrue to temp value
        // Otherwise goes to next Block in the list and keeps evaluating next cond in the whenClause
        // If last cond evaluates to false, control flow goes to falseBlock and save evFalse to temp value
        if (i == 0) {  // Create the entry of the block
            builder->CreateBr(condBlockList[i]);
        }
        if (i > 0) {
            func->getBasicBlockList().push_back(condBlockList[i]);
        }
        if (i < size - 1) {
            builder->SetInsertPoint(condBlockList[i]);
            CodeGenValuePtr evCond = VisitExpr(*cond);
            if (!evCond->IsValidValue()) {
                this->value = CreateInvalidCodeGenValue();
                return;
            }
            builder->CreateCondBr(builder->CreateAnd(builder->CreateNot(evCond->isNull), evCond->data),
                                  trueBlockList[i],condBlockList[i + 1]);
        } else {
            builder->SetInsertPoint(condBlockList[i]);
            CodeGenValuePtr evCond = VisitExpr(*cond);
            if (!evCond->IsValidValue()) {
                this->value = CreateInvalidCodeGenValue();
                return;
            }
            builder->CreateCondBr(builder->CreateAnd(builder->CreateNot(evCond->isNull), evCond->data),
                                  trueBlockList[i], falseBlock);
        }

        builder->SetInsertPoint(trueBlockList[i]);
        auto evTrue = VisitExpr(*resExpr);
        if (!evTrue->IsValidValue()) {
            this->value = CreateInvalidCodeGenValue();
            return;
        }
        Value *evTrueValue = evTrue->data;
        Value *evTrueLength = evTrue->length;
        Value *evTrueNull = evTrue->isNull;
        builder->CreateStore(evTrueValue, resultValuePtr);
        builder->CreateStore(evTrueNull, resultNullPtr);
        if (TypeUtil::IsStringType(switchExpr.GetReturnTypeId())) {
            builder->CreateStore(evTrueLength, resultLengthPtr);
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
    Value *evFalseValue = evFalse->data;
    Value *evFalseLength = evFalse->length;
    Value *evFalseNull = evFalse->isNull;
    builder->CreateStore(evFalseValue, resultValuePtr);
    builder->CreateStore(evFalseNull, resultNullPtr);
    if (TypeUtil::IsStringType(switchExpr.GetReturnTypeId())) {
        builder->CreateStore(evFalseLength, resultLengthPtr);
    }
    builder->CreateBr(mergeBlock);

    func->getBasicBlockList().push_back(mergeBlock);
    builder->SetInsertPoint(mergeBlock);
    if (TypeUtil::IsStringType(switchExpr.GetReturnTypeId())) {
        this->value = make_shared<CodeGenValue>(builder->CreateLoad(resultValuePtr), builder->CreateLoad(resultNullPtr),
                                                builder->CreateLoad(resultLengthPtr));
    }
    this->value = make_shared<CodeGenValue>(builder->CreateLoad(resultValuePtr), builder->CreateLoad(resultNullPtr));
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
        Value *tmpCmpData = llvmTypes->CreateConstantBool(false);
        Value *tmpCmpNull = llvmTypes->CreateConstantBool(false);

        switch (iExpr->arguments[0]->GetReturnTypeId()) {
            case OMNI_VEC_TYPE_INT:
            case OMNI_VEC_TYPE_DATE32:
            case OMNI_VEC_TYPE_DECIMAL64:
            case OMNI_VEC_TYPE_LONG: {
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
            case OMNI_VEC_TYPE_DOUBLE: {
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
            case OMNI_VEC_TYPE_CHAR:
            case OMNI_VEC_TYPE_VARCHAR: {
                argiValue = VisitExpr(*(iExpr->arguments[i]));
                if (!argiValue->IsValidValue()) {
                    this->value = CreateInvalidCodeGenValue();
                    return;
                }
                tmpCmpData = builder->CreateAnd(builder->CreateNot(valueToCompare->isNull),
                    builder->CreateAnd(builder->CreateNot(argiValue->isNull), builder->CreateICmpEQ(
                    this->StringCmp(valueToCompare->data, valueToCompare->length, argiValue->data, this->value->length),
                    llvmTypes->CreateConstantInt(0))));
                tmpCmpNull = builder->CreateOr(valueToCompare->isNull, argiValue->isNull);
                break;
            }
            default: {
                LLVM_DEBUG_LOG("Unsupported data type in IN expr %d", iExpr->arguments[0]->GetReturnTypeId());
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
    int32_t biggerTypeId =
        std::max(std::max(bExpr->lowerBound->GetReturnTypeId(), bExpr->upperBound->GetReturnTypeId()),
            bExpr->value->GetReturnTypeId());
    VecType biggerType;
    if (biggerTypeId == static_cast<int32_t>(bExpr->lowerBound->GetReturnTypeId())) {
        biggerType = bExpr->lowerBound->GetReturnType();
    } else {
        if (biggerTypeId == static_cast<int32_t>(bExpr->upperBound->GetReturnTypeId())) {
            biggerType = bExpr->upperBound->GetReturnType();
        } else {
            biggerType = bExpr->value->GetReturnType();
        }
    }

    bExpr->lowerBound->dataType = std::make_unique<VecType>(biggerType);
    bExpr->upperBound->dataType = std::make_unique<VecType>(biggerType);
    bExpr->value->dataType = std::make_unique<VecType>(biggerType);

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
    if (bExpr->value->GetReturnTypeId() == OMNI_VEC_TYPE_INT || bExpr->value->GetReturnTypeId() == OMNI_VEC_TYPE_LONG ||
        bExpr->value->GetReturnTypeId() == OMNI_VEC_TYPE_DATE32 ||
        bExpr->value->GetReturnTypeId() == OMNI_VEC_TYPE_DECIMAL64) {
        cmpLeft = builder->CreateICmpSLE(lowerValData, valData, "between_cmpleft");
        cmpRight = builder->CreateICmpSLE(valData, upperValData, "between_cmpright");
        supportedType = true;
    } else if (bExpr->value->GetReturnTypeId() == OMNI_VEC_TYPE_DOUBLE) {
        cmpLeft = builder->CreateFCmpULE(lowerValData, valData, "between_cmpleft");
        cmpRight = builder->CreateFCmpULE(valData, upperValData, "between_cmpright");
        supportedType = true;
    } else if (TypeUtil::IsStringType(bExpr->value->GetReturnTypeId())) {
        cmpLeft = builder->CreateICmpSLE(this->StringCmp(lowerValData, lowerValLen, valData, valLen),
            llvmTypes->CreateConstantInt(0));
        cmpRight = builder->CreateICmpSLE(this->StringCmp(valData, valLen, upperValData, upperValLen),
            llvmTypes->CreateConstantInt(0));
        supportedType = true;
    } else if (bExpr->value->GetReturnTypeId() == OMNI_VEC_TYPE_DECIMAL128) {
        cmpLeft = builder->CreateICmpSLE(this->Decimal128Cmp(*lowerValData, *valData), llvmTypes->CreateConstantInt(0));
        cmpRight =
            builder->CreateICmpSLE(this->Decimal128Cmp(*valData, *upperValData), llvmTypes->CreateConstantInt(0));
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

    LLVM_DEBUG_LOG("Error: unsupported data type for between %d", bExpr->value->GetReturnTypeId());
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

    PHINode *lengthPhi = nullptr;
    if (TypeUtil::IsStringType(cExpr.GetReturnTypeId())) {
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

    Value *result = builder->CreateICmpEQ(isNullValue, llvmTypes->CreateConstantBool(true), "isNullCompare");
    this->value = make_shared<CodeGenValue>(result, llvmTypes->CreateConstantBool(false));
}

// Handles all functions
void ExpressionCodeGen::Visit(const FuncExpr &fExpr)
{
    std::vector<Value *> argVals;
    int numArgs = fExpr.arguments.size();
    CodeGenValuePtr resultPtr;
    Value *isAnyNull = llvmTypes->CreateConstantBool(false);
    // set execution context
    if (fExpr.function->IsExecutionContextSet()) {
        argVals.push_back(this->codegenContext->executionContext);
    }
    for (int i = 0; i < numArgs; i++) {
        resultPtr = VisitExpr(*(fExpr.arguments[i]));
        argVals.push_back(resultPtr->data);
        isAnyNull = builder->CreateOr(isAnyNull, resultPtr->isNull);
        if ((TypeUtil::IsStringType(fExpr.arguments[i]->GetReturnTypeId()))) {
            if (fExpr.arguments[i]->GetReturnTypeId() == OMNI_VEC_TYPE_CHAR) {
                argVals.push_back(llvmTypes->CreateConstantInt(fExpr.arguments[i]->GetReturnType().GetWidth()));
            }
            argVals.push_back(this->value->length);
        }

        if (fExpr.funcName == mm3hashStr) {
            argVals.push_back(this->value->isNull);
        }
    }
    Value *ret = nullptr;
    Value *outputLen = nullptr;
    AllocaInst *outputLenPtr = nullptr;
    if (TypeUtil::IsStringType(fExpr.GetReturnTypeId())) {
        outputLenPtr = builder->CreateAlloca(Type::getInt32Ty(*context), nullptr, "output_len");
        argVals.push_back(outputLenPtr);
    }
    auto f = module->getFunction(fExpr.function->GetId());
    if (f) {
        ret = builder->CreateCall(f, argVals, fExpr.function->GetId());
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
