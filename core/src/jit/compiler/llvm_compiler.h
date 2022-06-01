/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
#ifndef __OMNI_JIT_LLVM_COMPILER_H__
#define __OMNI_JIT_LLVM_COMPILER_H__

#include "library_loader.h"
#include "./compiler.h"
#include "../config.h"
#include "llvm/ExecutionEngine/Orc/LLJIT.h"
#include "llvm/IR/IRBuilder.h"
#include "llvm/IR/LLVMContext.h"
#include "llvm/IR/Module.h"

#include <memory>
#include <vector>
#include <set>

namespace omniruntime {
namespace jit {
class LLVMCompiler : public Compiler {
public:
    LLVMCompiler();

    ~LLVMCompiler() override;

    bool LoadModule(const std::string &templatePath) override;

    bool SpecializeAndCompile(const std::vector<Optimization> &optimizations,
        const std::vector<ModuleOptimization> &moduleOptimizations) override;

    void AddSpecialization(const std::string &id, const Specialization &specialization) override;

    uint64_t GetJitedFunction(const std::string &functionName, bool isNameMangled) override;

private:
    Config *config;
    std::unique_ptr<llvm::LLVMContext> context;
    std::vector<std::unique_ptr<llvm::Module>> modules;
    std::vector<std::string> functionSymbols;

    llvm::orc::LLJIT *jitter = nullptr;

    static LibraryLoader ll;

    static void LoadExtraLibraries();

    std::unique_ptr<llvm::orc::LLJIT> CompileModules(std::map<std::string, std::set<std::string>> &specializedModules,
        const std::vector<Optimization> &optimizations, const std::vector<ModuleOptimization> &moduleOptimizations);

    std::set<std::string> SpecializeModule(const std::unique_ptr<llvm::Module> &module);

    bool HardenFunction(const std::string &specializationId, llvm::Function *function,
        const std::unique_ptr<llvm::Module> &module);

    llvm::Constant *ToLLVMValue(const std::string &name, ParamValue value, const std::unique_ptr<llvm::Module> &module);

    llvm::Constant *ToScalarLLVMValue(const ParamValue &value);

    llvm::Constant *ToArrayLLVMValue(const std::string &name, const ParamValue &value,
        const std::unique_ptr<llvm::Module> &module);

    llvm::Constant *ToInt32VectorLLVMValue(ParamValue value, std::vector<llvm::Constant *> &vecValues);

    llvm::Constant *ToInt32ArrayLlvmValue(const std::string &name, ParamValue value,
        const std::unique_ptr<llvm::Module> &module, std::vector<llvm::Constant *> &vecValues);

    llvm::Constant *ToVectorLLVMValue(const std::string &name, const ParamValue &value,
        const std::unique_ptr<llvm::Module> &module);

    llvm::Constant *To2dArrayLLVMValue(const std::string &name, ParamValue value,
        const std::unique_ptr<llvm::Module> &module);
};

bool OptimizeAttributes(llvm::Function *function);

std::map<std::string, llvm::Function *> GetAnnotatedFuncs(const std::unique_ptr<llvm::Module> &module);

std::string BuildParamKey(llvm::Function &func, int argPos);
}
}

#endif