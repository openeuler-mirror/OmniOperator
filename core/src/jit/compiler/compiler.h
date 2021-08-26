/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
#ifndef __OMNI_JIT_COMPILER_H__
#define __OMNI_JIT_COMPILER_H__

#include "../specialization.h"
#include "../config.h"

#include <string>
#include <map>

namespace omniruntime {
    namespace jit {
        class Compiler {
        public:
            virtual bool LoadModule(std::string templatePath) = 0;

            virtual bool SpecializeAndCompile(const std::vector<Optimization> &optimizations, const std::vector<ModuleOptimization> &moduleOptimizations) = 0;

            virtual void AddSpecialization(std::string id, Specialization specialization) = 0;

            virtual uint64_t GetJitedFunction(std::string functionName) = 0;

        protected:
            std::map<std::string, Specialization> specializations;
        };
    }
}

#endif