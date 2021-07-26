/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
#include "jit.h"
#include "compiler/llvm_compiler.h"

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
                    std::cout << "Error: Compiler type not supported: " << compilerType << std::endl;
                    break;
            }
        }

        uint64_t Jit::Specialize()
        {
            for (auto &context : this->contexts) {
                bool loaded = this->compiler->LoadOperatorTemplate(context.getJitTemplate(), context.IsDependency());
                if (!loaded) {
                    std::cout << "Error: Failed to load template: " + context.getJitTemplate() << std::endl;
                    return 0;
                }
                std::cout << "Loaded template: " << context.getJitTemplate() << std::endl;

                for (auto &specializationPair : context.getSpecializations()) {
                    this->compiler->AddSpecialization(specializationPair.first, specializationPair.second);
                }
                std::cout << "Added specializations" << std::endl;
            }
            return this->compiler->SpecializeAndCompile();
        }

        std::vector<std::string> Jit::getAppliedOptimizations()
        {
            std::vector<std::string> temp;
            return temp;
        }
    }
}