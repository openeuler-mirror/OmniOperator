#ifndef __OMNI_JIT_H__
#define __OMNI_JIT_H__

#include "compiler/compiler.h"
#include "config.h"
#include "context.h"

#include <vector>
#include <map>
#include <string>

namespace omniruntime {
    namespace jit {
        enum CompilerType {
            llvm
        };

        class Jit {
        public:
            Jit(std::vector<Context> contexts, CompilerType compilerType = llvm);

            /// Specialize operator templates with values/stats in Context
            /// return pointer to createOperator method in the optimized code
            /// or 0 if specialization failed
            uint64_t specialize();

            std::vector<std::string> getAppliedOptimizations();

        private:
            Compiler *compiler;
            Config config;
            std::vector<Context> contexts;
        };
    }
}

#endif