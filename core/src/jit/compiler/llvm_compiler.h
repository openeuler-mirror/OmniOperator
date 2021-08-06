/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
#ifndef __OMNI_JIT_LLVM_COMPILER_H__
#define __OMNI_JIT_LLVM_COMPILER_H__

#include "./compiler.h"
#include "./library_loader.h"
#include "../config.h"
#include "llvm/ExecutionEngine/Orc/LLJIT.h"
#include "llvm/IR/IRBuilder.h"
#include "llvm/IR/LLVMContext.h"
#include "llvm/IR/Module.h"

#include <cstdlib>
#include <iostream>
#include <memory>
#include <vector>
#include <set>
#include <cstdlib>

namespace omniruntime {
    namespace jit {
        class LLVMCompiler : public Compiler {
        public:
            LLVMCompiler();

            ~LLVMCompiler();

            bool LoadOperatorTemplate(std::string operatorName, bool isDependency) override;

            uint64_t SpecializeAndCompile() override;

            void AddSpecialization(std::string id, Specialization specialization) override;

        private:
            const std::string templateFileSuffix = ".ll";

            std::unique_ptr<Config> config;
            std::unique_ptr<llvm::StringRef> layout;
            std::unique_ptr<llvm::IRBuilder<>> builder;
            std::unique_ptr<llvm::LLVMContext> context;
            std::vector<std::unique_ptr<llvm::Module>> modules;
            std::string createOperatorSymbol;

            llvm::orc::LLJIT *jitter;

            static LibraryLoader ll;
            static void LoadExtraLibraries();

            std::unique_ptr<llvm::orc::LLJIT> CompileModules(const std::set<std::string> &specializedModules);

            bool SpecializeModule(const std::unique_ptr<llvm::Module> &module);

            bool HardenFunction(const std::string &specializationId, llvm::Function *function,
                                 const std::unique_ptr<llvm::Module> &module);

            llvm::Constant *
            ToLlvmValue(const std::string &name, ParamValue value, const std::unique_ptr<llvm::Module> &module);

            llvm::Constant *ToScalarLlvmValue(ParamValue value);

            llvm::Constant *
            ToArrayLlvmValue(const std::string &name, ParamValue value, const std::unique_ptr<llvm::Module> &module);

            llvm::Constant *ToInt32VectorLlvmValue(ParamValue value, std::vector<llvm::Constant *> &vecValues);

            llvm::Constant *ToInt32ArrayLlvmValue(
                const std::string &name, ParamValue value,
                const std::unique_ptr<llvm::Module> &module, std::vector<llvm::Constant *> &vecValues);

            llvm::Constant *ToVectorLlvmValue(const std::string &name, ParamValue value,
                                                 const std::unique_ptr<llvm::Module> &module);

            llvm::Constant *To2darrayLlvmValue(const std::string &name, ParamValue value,
                                                  const std::unique_ptr<llvm::Module> &module);
        };

        bool OptimizeAttributes(llvm::Function *function);

        std::map<std::string, llvm::Function *> GetAnnotatedFuncs(const std::unique_ptr<llvm::Module> &module);

        std::string BuildParamKey(llvm::Function &func, int argPos);
    }
}

#endif