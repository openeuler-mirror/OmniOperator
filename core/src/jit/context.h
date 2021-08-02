/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
#ifndef __OMNI_JIT_CONTEXT_H__
#define __OMNI_JIT_CONTEXT_H__

#include "specialization.h"
#include <map>
#include <string>
#include <vector>

namespace omniruntime {
    namespace jit {
        class Context {
        public:
            Context(std::string jitTemplate,
                    std::map<std::string, Specialization> specializations,
                    std::vector<std::string> libraryPaths,
                    std::vector<std::string> optimizations,
                    bool hasOperatorFactory = false)
                : jitTemplate(jitTemplate), specializations(specializations), libraryPaths(libraryPaths),
                  optimizations(optimizations), hasOperatorFactory(hasOperatorFactory) {
            }

            ~Context(){}

            std::string getJitTemplate()
            {
                return this->jitTemplate;
            }

            std::map<std::string, Specialization> getSpecializations()
            {
                return this->specializations;
            }

            std::vector<std::string> getLibraries()
            {
                return this->libraryPaths;
            }

            std::vector<std::string> getOptimizations()
            {
                return this->optimizations;
            }

            bool IsDependency()
            {
                return !this->hasOperatorFactory;
            }

        private:
            std::string jitTemplate;
            std::map<std::string, Specialization> specializations;
            std::vector<std::string> libraryPaths;
            std::vector<std::string> optimizations;
            bool hasOperatorFactory;
        };
    }
}

#endif