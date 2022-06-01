/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Expression code generator
 */
#include "decimal_ir_builder.h"
#include <llvm/Transforms/Utils/Cloning.h>
#include "llvm_types.h"

using namespace omniruntime::type;

void DecimalIRBuilder::AddScaleMultiplier(llvm::IntegerType *integerType, llvm::Type *type, int32_t defaultPrecision,
    const std::string &multipliersName) const
{
    std::string value = "1";
    std::vector<llvm::Constant *> scaleMultipliers;
    int32_t radix = 10;
    for (int i = 0; i < defaultPrecision + 1; ++i) {
        auto multiplier = llvm::ConstantInt::get(integerType, value, radix);
        scaleMultipliers.push_back(multiplier);
        value.append("0");
    }
    auto arrayType = llvm::ArrayType::get(type, defaultPrecision + 1);
    auto initializer = llvm::ConstantArray::get(arrayType, llvm::ArrayRef<llvm::Constant *>(scaleMultipliers));

    auto globalScaleMultipliers = new llvm::GlobalVariable(*module, arrayType, true,
        llvm::GlobalValue::LinkOnceAnyLinkage, initializer, multipliersName);
    int32_t alignment = 16;
    globalScaleMultipliers->setAlignment(llvm::MaybeAlign(alignment));
}

void DecimalIRBuilder::AddGlobalVariables()
{
    LLVMTypes llvmTypes(*context);
    AddScaleMultiplier(llvm::Type::getInt128Ty(*context), llvmTypes.I128Type(), DECIMAL128_DEFAULT_PRECISION,
        this->scale128MultipliersName);
    AddScaleMultiplier(llvm::Type::getInt64Ty(*context), llvmTypes.I64Type(), DECIMAL64_DEFAULT_PRECISION,
        this->scale64MultipliersName);
}

void DecimalIRBuilder::ScaleValues(llvm::Value &leftValue, llvm::Value &leftScale, llvm::Value &rightValue,
    llvm::Value &rightScale, llvm::Value **scaledLeft, llvm::Value **scaledRight, DataTypeId typeId)
{
    llvm::Value *le = builder->CreateICmpSLE(&leftScale, &rightScale);
    auto higherScale = builder->CreateSelect(le, &rightScale, &leftScale);

    auto leftDelta = builder->CreateSub(higherScale, &leftScale);
    *scaledLeft = ScaleValue(leftValue, *leftDelta, typeId);

    auto rightDelta = builder->CreateSub(higherScale, &rightScale);
    *scaledRight = ScaleValue(rightValue, *rightDelta, typeId);
}

llvm::Value *DecimalIRBuilder::GetScaleMultiplier(llvm::Value &delta, const std::string &multipliersName)
{
    LLVMTypes llvmTypes(*context);
    auto constArray = module->getGlobalVariable(multipliersName);
    auto ptr = builder->CreateGEP(constArray, { llvmTypes.CreateConstantInt(0), &delta });
    return builder->CreateLoad(ptr);
}

llvm::Value *DecimalIRBuilder::ScaleValue(llvm::Value &value, llvm::Value &delta, DataTypeId typeId)
{
    auto multipliersName = GetMultipliersName(typeId);
    auto returnType = GetLLVMType(typeId);

    LLVMTypes llvmTypes(*context);
    auto lessEqual = builder->CreateICmpSLE(&delta, llvmTypes.CreateConstantInt(0));

    auto trueBranch = [&] { return &value; };
    auto falseBranch = [&] {
        auto multiplier = GetScaleMultiplier(delta, multipliersName);
        return builder->CreateMul(&value, multiplier);
    };

    return BuildIfElse(*lessEqual, *returnType, trueBranch, falseBranch);
}

llvm::Value *DecimalIRBuilder::BuildIfElse(llvm::Value &condition, llvm::Type &returnType,
    std::function<llvm::Value *()> then_func, std::function<llvm::Value *()> else_func)
{
    llvm::Function *function = builder->GetInsertBlock()->getParent();

    // Create blocks for the then, else and merge cases.
    llvm::BasicBlock *trueBlock = llvm::BasicBlock::Create(*context, "then", function);
    llvm::BasicBlock *falseBlock = llvm::BasicBlock::Create(*context, "else", function);
    llvm::BasicBlock *mergeBlock = llvm::BasicBlock::Create(*context, "merge", function);

    builder->CreateCondBr(&condition, trueBlock, falseBlock);

    builder->SetInsertPoint(trueBlock);
    auto thenValue = then_func();
    builder->CreateBr(mergeBlock);

    builder->SetInsertPoint(falseBlock);
    auto elseValue = else_func();
    builder->CreateBr(mergeBlock);

    builder->SetInsertPoint(mergeBlock);
    int32_t numReservedValues = 2;
    llvm::PHINode *result = builder->CreatePHI(&returnType, numReservedValues, "res_value");
    result->addIncoming(thenValue, trueBlock);
    result->addIncoming(elseValue, falseBlock);
    return result;
}

DecimalSplitValue DecimalIRBuilder::Split(llvm::Value *fullValue)
{
    LLVMTypes types(*context);
    const int32_t intValue = 64;
    auto high = builder->CreateLShr(fullValue, types.CreateConstant128(intValue), "split_high");
    high = builder->CreateTrunc(high, types.I64Type(), "split_high");
    auto low = builder->CreateTrunc(fullValue, types.I64Type(), "split_low");
    return DecimalSplitValue(high, low);
}

llvm::Value *DecimalIRBuilder::ToInt128(llvm::Value *high, llvm::Value *low) const
{
    LLVMTypes types(*context);
    auto value = builder->CreateSExt(high, types.I128Type());
    const int32_t intValue = 64;
    value = builder->CreateShl(value, types.CreateConstant128(intValue));
    value = builder->CreateAdd(value, builder->CreateZExt(low, types.I128Type()));
    return value;
}

llvm::Value *DecimalIRBuilder::CallDecimalFunction(const std::string &fnName, llvm::Type *retType,
    const std::vector<llvm::Value *> &args, llvm::Value *executionContextPtr)
{
    LLVMTypes llvmTypes(*context);
    std::vector<llvm::Value *> disassembledArgs;

    if (executionContextPtr != nullptr) {
        disassembledArgs.push_back(executionContextPtr);
    }

    for (auto &arg : args) {
        if (arg->getType() == llvmTypes.I128Type()) {
            // split i128 arg into two int64s.
            auto split = Split(arg);
            disassembledArgs.push_back(const_cast<llvm::Value *>(split.GetHigh()));
            disassembledArgs.push_back(const_cast<llvm::Value *>(split.GetLow()));
        } else {
            disassembledArgs.push_back(arg);
        }
    }
    auto f = module->getFunction(fnName);
    llvm::Value *result = nullptr;
    if (f) {
        if (retType == llvmTypes.I128Type()) {
            // for i128 ret, replace with two int64* args, and join them.
            auto outHighPtr = builder->CreateAlloca(llvmTypes.I64Type(), nullptr, "out_high");
            auto outLowPtr = builder->CreateAlloca(llvmTypes.I64Type(), nullptr, "out_low");
            disassembledArgs.push_back(outHighPtr);
            disassembledArgs.push_back(outLowPtr);

            // Make call to pre-compiled IR function.
            engine.CreateCall(f, disassembledArgs, fnName);

            auto outHigh = builder->CreateLoad(outHighPtr);
            auto outLow = builder->CreateLoad(outLowPtr);
            result = ToInt128(outHigh, outLow);
        } else {
            result = engine.CreateCall(f, disassembledArgs, fnName);
        }
        llvm::InlineFunctionInfo inlineFunctionInfo;
        llvm::InlineFunction(*((llvm::CallInst *)result), inlineFunctionInfo);
    } else {
        LogWarn("Unable to generate function : %s", fnName.c_str());
    }
    return result;
}

std::shared_ptr<DecimalValue> DecimalIRBuilder::BuildDecimalValue(llvm::Value *data,
    omniruntime::type::DataType &retType, llvm::Value *isNull)
{
    LLVMTypes llvmTypes(*context);
    llvm::Value *precision = llvmTypes.CreateConstantInt(retType.GetPrecision());
    llvm::Value *scale = llvmTypes.CreateConstantInt(retType.GetScale());
    return std::make_shared<DecimalValue>(data, isNull, precision, scale);
}
// Private methods

llvm::Type *DecimalIRBuilder::GetLLVMType(DataTypeId typeId)
{
    LLVMTypes llvmTypes(*context);
    switch (typeId) {
        case OMNI_DECIMAL128:
            return llvmTypes.I128Type();
        case OMNI_DECIMAL64:
            return llvmTypes.I64Type();
        default:
            LogWarn("%d Is not supported", typeId);
            return llvmTypes.I64Type();
    }
}

std::string DecimalIRBuilder::GetMultipliersName(DataTypeId typeId)
{
    LLVMTypes llvmTypes(*context);
    switch (typeId) {
        case OMNI_DECIMAL128:
            return this->scale128MultipliersName;
        case OMNI_DECIMAL64:
            return this->scale64MultipliersName;
        default:
            LogWarn("%d Is not supported", typeId);
            return this->scale64MultipliersName;
    }
}

std::vector<llvm::Value *> DecimalIRBuilder::BuildDecimalArgs(llvm::Value *left, omniruntime::type::DataType &leftType,
    llvm::Value *right, omniruntime::type::DataType &rightType, omniruntime::type::DataType &returnType,
    bool withOutputParam)
{
    LLVMTypes llvmTypes(*context);
    llvm::Value *leftPrecision = llvmTypes.CreateConstantInt(leftType.GetPrecision());
    llvm::Value *leftScale = llvmTypes.CreateConstantInt(leftType.GetScale());
    llvm::Value *rightPrecision = llvmTypes.CreateConstantInt(rightType.GetPrecision());
    llvm::Value *rightScale = llvmTypes.CreateConstantInt(rightType.GetScale());
    std::vector<llvm::Value *> argVals { left, leftPrecision, leftScale, right, rightPrecision, rightScale };
    if (withOutputParam) {
        llvm::Value *returnPrecision = llvmTypes.CreateConstantInt(returnType.GetPrecision());
        llvm::Value *returnScale = llvmTypes.CreateConstantInt(returnType.GetScale());
        argVals.push_back(returnPrecision);
        argVals.push_back(returnScale);
    }
    return argVals;
}
