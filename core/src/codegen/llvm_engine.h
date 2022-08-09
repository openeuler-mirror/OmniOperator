/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Expression code generation utilities
 */

#ifndef OMNI_RUNTIME_LLVM_ENGINE_H
#define OMNI_RUNTIME_LLVM_ENGINE_H

#include <memory>
#include <set>
#include <string>
#include <vector>
#include <utility>

#include "llvm/Transforms/Scalar.h"
#include "llvm/Transforms/IPO.h"
#include "llvm/Transforms/Utils.h"
#include "llvm/ADT/APInt.h"
#include "llvm/ADT/APFloat.h"
#include "llvm/ADT/STLExtras.h"
#include "llvm/Support/Error.h"
#include "llvm/IR/Function.h"
#include "llvm/IR/Instructions.h"
#include "llvm/IR/IRBuilder.h"
#include "llvm/IRReader/IRReader.h"
#include "llvm/IR/LegacyPassManager.h"
#include "llvm/IR/LLVMContext.h"
#include "llvm/IR/Module.h"
#include "llvm/IR/Type.h"
#include "llvm/Support/SourceMgr.h"
#include "llvm/Support/TargetSelect.h"
#include "llvm/ExecutionEngine/ExecutionEngine.h"
#include "llvm/ExecutionEngine/Orc/LLJIT.h"

#include "expression/expressions.h"
#include "llvm_types.h"

class LLVMEngine {
public:
    LLVMEngine(std::unique_ptr<llvm::LLVMContext> context, std::unique_ptr<llvm::orc::LLJIT> jit,
        std::unique_ptr<llvm::IRBuilder<>> builder, std::unique_ptr<llvm::Module> module,
        std::unique_ptr<LLVMTypes> llvmTypes, std::unique_ptr<llvm::legacy::FunctionPassManager> fpm)
        : context(std::move(context)),
          builder(std::move(builder)),
          module(std::move(module)),
          llvmTypes(std::move(llvmTypes)),
          jit(std::move(jit)),
          fpm(std::move(fpm))
    {}
    virtual ~LLVMEngine() = default;
    static void Create(std::unique_ptr<LLVMEngine> *out);
    void OptimizeFunctionsAndModule();
    void OptimizeModule();
    llvm::CallInst *CreateCall(llvm::Function *func, std::vector<llvm::Value *> argsVals, std::string name);
    llvm::Value *CallExternFunction(const std::string fn_name, std::vector<omniruntime::type::DataTypeId> params,
        const omniruntime::type::DataTypeId returnType, std::vector<llvm::Value *> args,
        llvm::Value *executionContextPtr, const std::string msg = "");
    static void InitializeCodegenTargets();
    void RegisterFunctions(const std::vector<omniruntime::Function> &func);
    void MakeThreadSafe(llvm::orc::ResourceTrackerSP *res);
    void RecordMainFunction(llvm::Function *func);
    void RemoveUnusedFunctions();

    llvm::IRBuilder<> *GetIRBuilder();
    llvm::Module *GetModule();
    llvm::LLVMContext *GetContext();
    llvm::orc::LLJIT *GetJit();
    llvm::ExitOnError GetEoe();
    LLVMTypes *GetTypes();

private:
    std::vector<llvm::Type *> GetFunctionArgTypeVector(std::vector<omniruntime::type::DataTypeId> &params,
        omniruntime::type::DataTypeId &retTypeId, bool needsContext);
    llvm::Function *function;
    std::unique_ptr<llvm::LLVMContext> context;
    std::unique_ptr<llvm::IRBuilder<>> builder;
    std::unique_ptr<llvm::Module> module;
    std::unique_ptr<LLVMTypes> llvmTypes;
    std::unique_ptr<llvm::orc::LLJIT> jit;
    llvm::ExitOnError eoe;
    llvm::legacy::PassManager mpm;
    std::unique_ptr<llvm::legacy::FunctionPassManager> fpm = nullptr;
};

#endif // OMNI_RUNTIME_LLVM_ENGINE_H