/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
#ifndef __OMNI_JIT_COMPILER_H__
#define __OMNI_JIT_COMPILER_H__

#include "../specialization.h"
#include "../config.h"
#include "../../../libconfig.h"

#include <string>
#include <map>

namespace omniruntime {
    namespace jit {
        class Compiler {
        public:
            virtual bool LoadOperatorTemplate(std::string operatorName, bool isDependency) = 0;

            virtual uint64_t SpecializeAndCompile(const std::vector<Optimization> &optimizations, const std::vector<ModuleOptimization> &moduleOptimizations) = 0;

            virtual void AddSpecialization(std::string id, Specialization specialization) = 0;

        protected:
            const std::string operatorPath = G_LIB_PATH + "ir/";
            const std::string entryFuncName = "CreateOperator";
            std::map<std::string, Specialization> specializations;
        };
    }
}

#endif