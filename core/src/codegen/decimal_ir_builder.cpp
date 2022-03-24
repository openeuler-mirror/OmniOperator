/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Expression code generator
 */
#include <llvm/Transforms/Utils/Cloning.h>
#include "decimal_ir_builder.h"
#include "llvm_types.h"


void DecimalIRBuilder::AddScaleMultiplier() const
{
    std::string value = "1";
    int32_t decimal128Precision = 38;
    std::vector<llvm::Constant *> scale_multipliers;
    for (int i = 0; i < decimal128Precision + 1; ++i) { // 38 stands for the max precision
        int32_t radix = 10;
        auto multiplier = llvm::ConstantInt::get(llvm::Type::getInt128Ty(this->context), value, radix);
        scale_multipliers.push_back(multiplier);
        value.append("0");
    }
    LLVMTypes llvmTypes(context);
    auto arrayType = llvm::ArrayType::get(llvmTypes.I128Type(), decimal128Precision + 1);
    auto initializer = llvm::ConstantArray::get(arrayType, llvm::ArrayRef<llvm::Constant *>(scale_multipliers));

    auto globalScaleMultipliers = std::make_unique<llvm::GlobalVariable>(this->module, arrayType, true,
        llvm::GlobalValue::LinkOnceAnyLinkage, initializer, this->scaleMultipliersName).release();
    int32_t alignment = 16;
    globalScaleMultipliers->setAlignment(llvm::MaybeAlign(alignment));
}

void DecimalIRBuilder::ScaleValues(llvm::Value &leftValue, llvm::Value &leftScale, llvm::Value &rightValue,
    llvm::Value &rightScale, llvm::Value **scaledLeft, llvm::Value **scaledRight)
{
    llvm::Value *le = this->builder.CreateICmpSLE(&leftScale, &rightScale);
    auto higherScale = this->builder.CreateSelect(le, &rightScale, &leftScale);

    auto leftDelta = this->builder.CreateSub(higherScale, &leftScale);
    *scaledLeft = ScaleValue(leftValue, *leftDelta);

    auto rightDelta = this->builder.CreateSub(higherScale, &rightScale);
    *scaledRight = ScaleValue(rightValue, *rightDelta);
}

llvm::Value *DecimalIRBuilder::GetScaleMultiplier(llvm::Value &delta)
{
    LLVMTypes llvmTypes(context);
    auto constArray = this->module.getGlobalVariable(this->scaleMultipliersName);
    auto ptr = builder.CreateGEP(constArray, { llvmTypes.CreateConstantInt(0), &delta });
    return this->builder.CreateLoad(ptr);
}

llvm::Value *DecimalIRBuilder::ScaleValue(llvm::Value &value, llvm::Value &delta)
{
    LLVMTypes llvmTypes(context);
    auto lessEqual = this->builder.CreateICmpSLE(&delta, llvmTypes.CreateConstantInt(0));

    auto trueBranch = [&] { return &value; };
    auto falseBranch = [&] {
        auto multiplier = GetScaleMultiplier(delta);
        return this->builder.CreateMul(&value, multiplier);
    };

    return BuildIfElse(*lessEqual, *llvmTypes.I128Type(), trueBranch, falseBranch);
}


llvm::Value *DecimalIRBuilder::BuildIfElse(llvm::Value &condition, llvm::Type &return_type,
    std::function<llvm::Value *()> then_func, std::function<llvm::Value *()> else_func)
{
    llvm::Function *function = builder.GetInsertBlock()->getParent();

    // Create blocks for the then, else and merge cases.
    llvm::BasicBlock *trueBlock = llvm::BasicBlock::Create(this->context, "then", function);
    llvm::BasicBlock *falseBlock = llvm::BasicBlock::Create(this->context, "else", function);
    llvm::BasicBlock *mergeBlock = llvm::BasicBlock::Create(this->context, "merge", function);

    builder.CreateCondBr(&condition, trueBlock, falseBlock);

    builder.SetInsertPoint(trueBlock);
    auto thenValue = then_func();
    builder.CreateBr(mergeBlock);

    builder.SetInsertPoint(falseBlock);
    auto elseValue = else_func();
    builder.CreateBr(mergeBlock);

    builder.SetInsertPoint(mergeBlock);
    int32_t numReservedValues = 2;
    llvm::PHINode *result = builder.CreatePHI(&return_type, numReservedValues, "res_value");
    result->addIncoming(thenValue, trueBlock);
    result->addIncoming(elseValue, falseBlock);
    return result;
}


DecimalSplitValue DecimalIRBuilder::Split(llvm::Value *fullValue)
{
    LLVMTypes types(context);
    const int32_t intValue = 64;
    auto high = builder.CreateLShr(fullValue, types.CreateConstant128(intValue), "split_high");
    high = builder.CreateTrunc(high, types.I64Type(), "split_high");
    auto low = builder.CreateTrunc(fullValue, types.I64Type(), "split_low");
    return DecimalSplitValue(high, low);
}

llvm::Value *DecimalIRBuilder::ToInt128(llvm::Value *high, llvm::Value *low) const
{
    LLVMTypes types(context);
    auto value = builder.CreateSExt(high, types.I128Type());
    const int32_t intValue = 64;
    value = builder.CreateShl(value, types.CreateConstant128(intValue));
    value = builder.CreateAdd(value, builder.CreateZExt(low, types.I128Type()));
    return value;
}

llvm::Value *DecimalIRBuilder::CallDecimalFunction(const std::string &fnName, llvm::Type *retType,
    const std::vector<llvm::Value *> &args)
{
    LLVMTypes llvmTypes(context);
    std::vector<llvm::Value *> disassembledArgs;
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
    auto f = module.getFunction(fnName);
    llvm::Value *result = nullptr;
    if (f) {
        if (retType == llvmTypes.I128Type()) {
            // for i128 ret, replace with two int64* args, and join them.
            auto outHighPtr = builder.CreateAlloca(llvmTypes.I64Type(), nullptr, "out_high");
            auto outLowPtr = builder.CreateAlloca(llvmTypes.I64Type(), nullptr, "out_low");
            disassembledArgs.push_back(outHighPtr);
            disassembledArgs.push_back(outLowPtr);

            // Make call to pre-compiled IR function.
            codeGenUtils.CreateCall(f, disassembledArgs, fnName);

            auto outHigh = builder.CreateLoad(outHighPtr);
            auto outLow = builder.CreateLoad(outLowPtr);
            result = ToInt128(outHigh, outLow);
        } else {
            result = codeGenUtils.CreateCall(f, disassembledArgs, fnName);
        }
        llvm::InlineFunctionInfo inlineFunctionInfo;
        auto inlinedFunction = llvm::InlineFunction(*((llvm::CallInst *)result), inlineFunctionInfo);
    } else {
        std::cout << "Unable to generate function " << fnName << std::endl;
        LogWarn("Unable to generate function : %s", fnName.c_str());
    }
    return result;
}

std::shared_ptr<DecimalValue> DecimalIRBuilder::BuildDecimalValue(llvm::Value *data,
    omniruntime::type::DataType &retType, llvm::Value *isNull)
{
    LLVMTypes llvmTypes(context);
    llvm::Value *precision = llvmTypes.CreateConstantInt(retType.GetPrecision());
    llvm::Value *scale = llvmTypes.CreateConstantInt(retType.GetScale());
    return std::make_shared<DecimalValue>(data, isNull, precision, scale);
}