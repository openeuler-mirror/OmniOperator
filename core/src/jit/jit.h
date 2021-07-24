/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
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
            LLVM
        };

        class Jit {
        public:
            explicit Jit(std::vector<Context> contexts, CompilerType compilerType = LLVM);

            ~Jit(){}

            /// Specialize operator templates with values/stats in Context
            /// return pointer to createOperator method in the optimized code
            /// or 0 if specialization failed
            uint64_t Specialize();

            std::vector<std::string> getAppliedOptimizations();

        private:
            Compiler *compiler;
            Config config;
            std::vector<Context> contexts;
            void InitCompile();
        };
    }
}

#endif