#ifndef __OMNI_JIT_LLVM_COMPILER_H__
#define __OMNI_JIT_LLVM_COMPILER_H__

#include "./compiler.h"
#include "../config.h"
#include "llvm/ExecutionEngine/Orc/LLJIT.h"
#include "llvm/IR/IRBuilder.h"
#include "llvm/IR/LLVMContext.h"
#include "llvm/IR/Module.h"

#include <memory>
#include <vector>

namespace omniruntime {
    namespace jit {
        class LLVMCompiler : public Compiler {
        public:
            LLVMCompiler();

            bool loadOperatorTemplate(std::string operatorName, bool isDependency) override;

            uint64_t specializeAndCompile() override;

            void addSpecialization(std::string id, Specialization specialization) override;

        private:
            const std::string templateFileSuffix = ".ll";

            Config *config;
            std::unique_ptr<llvm::StringRef> layout;
            std::unique_ptr<llvm::IRBuilder<>> builder;
            std::unique_ptr<llvm::LLVMContext> context;
            std::vector<std::unique_ptr<llvm::Module>> modules;
            std::string createOperatorSymbol;

            llvm::orc::LLJIT *jitter;

            static void loadExtraLibraries();

            std::unique_ptr<llvm::orc::LLJIT> compileModules();

            bool specializeModule(const std::unique_ptr<llvm::Module> &module);

            bool harden_function(const std::string &specializationId, llvm::Function *function,
                                 const std::unique_ptr<llvm::Module> &module);

            llvm::Constant *
            to_llvm_value(const std::string &name, ParamValue value, const std::unique_ptr<llvm::Module> &module);

            llvm::Constant *to_scalar_llvm_value(ParamValue value);

            llvm::Constant *
            to_array_llvm_value(const std::string &name, ParamValue value, const std::unique_ptr<llvm::Module> &module);

            llvm::Constant *to_vector_llvm_value(const std::string &name, ParamValue value,
                                                 const std::unique_ptr<llvm::Module> &module);

            llvm::Constant *to_2darray_llvm_value(const std::string &name, ParamValue value,
                                                  const std::unique_ptr<llvm::Module> &module);
        };

        bool optimizeAttributes(llvm::Function *function);

        map<string, llvm::Function *> getAnnotatedFuncs(const std::unique_ptr<llvm::Module> &module);

        string build_param_key(llvm::Function &func, int arg_pos);
    }
}

#endif