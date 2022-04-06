/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Expression code generation utilities
 */
#ifndef OMNI_RUNTIME_CODEGEN_UTILS_H
#define OMNI_RUNTIME_CODEGEN_UTILS_H

#include <string>
#include "function.h"
#include "type/data_type.h"
#include <llvm/IR/IRBuilder.h>
#include <llvm/IR/Function.h>
#include <llvm/IR/Instructions.h>
#include <llvm/IR/Module.h>
#include <llvm/IR/Value.h>
#include <llvm/IR/LLVMContext.h>
#include <set>
#include <llvm/IR/LegacyPassManager.h>

class CodeGenUtils {
public:
    explicit CodeGenUtils(llvm::LLVMContext &context, llvm::Module &module, llvm::IRBuilder<> &builder)
        : context(context), module(module), builder(builder)
    {}
    virtual ~CodeGenUtils() = default;

    const omniruntime::Function *GetFunction(const std::string &functionName,
        const std::vector<omniruntime::type::DataTypeId> &params, omniruntime::type::DataTypeId returnType);
    llvm::CallInst *CreateCall(llvm::Function *func, const std::vector<llvm::Value *> &argsVals);
    llvm::CallInst *CreateCall(llvm::Function *func, const std::vector<llvm::Value *> &argsVals,
        const std::string &name);
    void RecordFunctions(llvm::Function *func);
    void RemoveUnusedFunctions();
    friend class ExpressionCodeGen;

private:
    std::set<std::string> visited;
    llvm::Function *function;
    llvm::LLVMContext &context;
    llvm::Module &module;
    llvm::IRBuilder<> &builder;
    llvm::legacy::PassManager mpm;
};

#endif // OMNI_RUNTIME_CODEGEN_UTILS_H
