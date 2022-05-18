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

    bool LoadModule(std::string templatePath) override;

    bool SpecializeAndCompile(const std::vector<Optimization> &optimizations,
        const std::vector<ModuleOptimization> &moduleOptimizations) override;

    void AddSpecialization(std::string id, Specialization specialization) override;

    uint64_t GetJitedFunction(std::string functionName, bool isNameMangled = false) override;

private:
    Config *config;
    std::unique_ptr<llvm::StringRef> layout;
    std::unique_ptr<llvm::IRBuilder<>> builder;
    std::unique_ptr<llvm::LLVMContext> context;
    std::vector<std::unique_ptr<llvm::Module>> modules;
    std::vector<std::string> functionSymbols;

    llvm::orc::ResourceTrackerSP rt;
    std::unique_ptr<llvm::orc::LLJIT> jitter;

    static LibraryLoader ll;

    static void LoadExtraLibraries();

    std::unique_ptr<llvm::orc::LLJIT> compileModules(std::map<std::string, std::set<std::string>> &specializedModules,
        const std::vector<Optimization> &optimizations, const std::vector<ModuleOptimization> &moduleOptimizations);

    std::set<std::string> specializeModule(const std::unique_ptr<llvm::Module> &module);

    bool harden_function(const std::string &specializationId, llvm::Function *function,
        const std::unique_ptr<llvm::Module> &module);

    llvm::Constant *to_llvm_value(const std::string &name, ParamValue value,
        const std::unique_ptr<llvm::Module> &module);

    llvm::Constant *to_scalar_llvm_value(ParamValue value);

    llvm::Constant *to_array_llvm_value(const std::string &name, ParamValue value,
        const std::unique_ptr<llvm::Module> &module);

    llvm::Constant *to_int32_vector_llvm_value(ParamValue value, std::vector<llvm::Constant *> &vecValues);

    llvm::Constant *ToInt32ArrayLlvmValue(const std::string &name, ParamValue value,
        const std::unique_ptr<llvm::Module> &module, std::vector<llvm::Constant *> &vecValues);

    llvm::Constant *to_vector_llvm_value(const std::string &name, ParamValue value,
        const std::unique_ptr<llvm::Module> &module);

    llvm::Constant *to_2darray_llvm_value(const std::string &name, ParamValue value,
        const std::unique_ptr<llvm::Module> &module);
};

bool optimizeAttributes(llvm::Function *function);

std::map<std::string, llvm::Function *> getAnnotatedFuncs(const std::unique_ptr<llvm::Module> &module);

std::string build_param_key(llvm::Function &func, int argPos);
}
}

#endif