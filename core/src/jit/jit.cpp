#include "jit.h"
#include "compiler/llvm_compiler.h"

#include <iostream>
#include <utility>

namespace omniruntime {
    namespace jit {
        Jit::Jit(std::vector<Context> contexts, CompilerType compilerType) {
            this->contexts = std::move(contexts);

            switch (compilerType) {
                case llvm:
                    this->compiler = new LLVMCompiler();
                    break;
                default:
                    std::cout << "Error: Compiler type not supported: " << compilerType << std::endl;
                    break;
            }
        }

        uint64_t Jit::specialize() {
            for (auto &context : this->contexts) {
                bool loaded = this->compiler->loadOperatorTemplate(context.getJitTemplate(), context.isDependency());
                if (!loaded) {
                    std::cout << "Error: Failed to load template: " + context.getJitTemplate() << std::endl;
                }

                for (auto &specializationPair : context.getSpecializations()) {
                    this->compiler->addSpecialization(specializationPair.first, specializationPair.second);
                }
            }
            return this->compiler->specializeAndCompile();
        }

        std::vector<std::string> Jit::getAppliedOptimizations() {
        }
    }
}