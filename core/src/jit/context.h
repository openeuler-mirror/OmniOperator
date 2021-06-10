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

            std::string getJitTemplate() {
                return this->jitTemplate;
            }

            std::map<std::string, Specialization> getSpecializations() {
                return this->specializations;
            }

            std::vector<std::string> getLibraries() {
                return this->libraryPaths;
            }

            std::vector<std::string> getOptimizations() {
                return this->optimizations;
            }

            bool isDependency() {
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