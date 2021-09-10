/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
#include "jit.h"
#include "compiler/llvm_compiler.h"
#include "../util/debug.h"

#include <iostream>
#include <utility>

namespace omniruntime {
    namespace jit {
        void Jit::InitCompile()
        {
            this->compiler = new LLVMCompiler();
        }

        Jit::Jit(std::vector<Context> contexts, CompilerType compilerType)
        {
            this->contexts = std::move(contexts);

            switch (compilerType) {
                case LLVM:
                    this->compiler = nullptr;
                    InitCompile();
                    break;
                default:
                    std::cerr << "Error: Compiler type not supported: " << compilerType << std::endl;
                    break;
            }
        }

        bool Jit::Specialize(
                const std::vector<Optimization> &optimizations,
                const std::vector<ModuleOptimization> &moduleOptimizations)
        {
            for (auto &context : this->contexts) {
                bool loaded = this->compiler->LoadModule(context.getJitTemplate());
                if (!loaded) {
                    std::cerr << "Error: Failed to load template: " + context.getJitTemplate() << std::endl;
                    return false;
                }
                LLVM_DEBUG_LOG("Loaded template: %s", context.getJitTemplate());

                for (auto &specializationPair : context.getSpecializations()) {
                    this->compiler->AddSpecialization(specializationPair.first, specializationPair.second);
                }
                LLVM_DEBUG_LOG("Added specializations");
            }
            return this->compiler->SpecializeAndCompile(optimizations, moduleOptimizations);
        }

        std::vector<std::string> Jit::GetAppliedOptimizations()
        {
            std::vector<std::string> temp;
            return temp;
        }

        uint64_t Jit::GetJitedFunction(std::string functionName, bool isNameMangled)
        {
            return this->compiler->GetJitedFunction(functionName, isNameMangled);
        }
    }
}