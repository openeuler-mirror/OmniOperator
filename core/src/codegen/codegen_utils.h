/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Expression code generation utilities
 */
#ifndef OMNI_RUNTIME_CODEGEN_UTILS_H
#define OMNI_RUNTIME_CODEGEN_UTILS_H

#include <string>
#include <llvm/IR/IRBuilder.h>
#include <llvm/IR/Function.h>
#include <llvm/IR/Instructions.h>
#include <llvm/IR/Module.h>
#include <llvm/IR/Value.h>
#include <llvm/IR/LLVMContext.h>
#include <llvm/IR/LegacyPassManager.h>
#include <type/data_type.h>

class CodeGenUtils {
public:
    explicit CodeGenUtils(llvm::LLVMContext &context, llvm::Module &module, llvm::IRBuilder<> &builder)
        : context(context), module(module), builder(builder)
    {}
    virtual ~CodeGenUtils() = default;

    llvm::CallInst *CreateCall(llvm::Function *func, std::vector<llvm::Value *> argsVals, std::string name);
    llvm::Value *CallExternFunction(const std::string fn_name, std::vector<omniruntime::type::DataTypeId> params,
        const omniruntime::type::DataTypeId &returnType, std::vector<llvm::Value *> args, std::string msg = "");
    void RecordMainFunction(llvm::Function *func);
    void RemoveUnusedFunctions();
    friend class ExpressionCodeGen;

private:
    llvm::Function *function;
    llvm::LLVMContext &context;
    llvm::Module &module;
    llvm::IRBuilder<> &builder;
    llvm::legacy::PassManager mpm;
};

#endif // OMNI_RUNTIME_CODEGEN_UTILS_H
