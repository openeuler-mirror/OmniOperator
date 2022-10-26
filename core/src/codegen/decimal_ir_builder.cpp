/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Expression code generator
 */
#include "decimal_ir_builder.h"
#include <llvm/Transforms/Utils/Cloning.h>
#include "llvm_types.h"

using namespace omniruntime::type;
using namespace llvm;

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
    const std::vector<llvm::Value *> &args, llvm::Value *executionContextPtr,
    omniruntime::op::OverflowConfig *overflowConfig, llvm::Value *overflowNull)
{
    LLVMTypes llvmTypes(*context);
    std::vector<llvm::Value *> disassembledArgs;

    if (executionContextPtr != nullptr) {
        if (overflowConfig != nullptr &&
            overflowConfig->getOverflowConfigId() == omniruntime::op::OVERFLOW_CONFIG_NULL) {
            disassembledArgs.push_back(overflowNull);
        } else {
            disassembledArgs.push_back(executionContextPtr);
        }
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
    llvm::Value *precision;
    llvm::Value *scale;
    if (TypeUtil::IsDecimalType(retType.GetId())) {
        precision = llvmTypes.CreateConstantInt(static_cast<DecimalDataType &>(retType).GetPrecision());
        scale = llvmTypes.CreateConstantInt(static_cast<DecimalDataType &>(retType).GetScale());
    } else {
        precision = llvmTypes.CreateConstantInt(0);
        scale = llvmTypes.CreateConstantInt(0);
    }
    return std::make_shared<DecimalValue>(data, isNull, precision, scale);
}
