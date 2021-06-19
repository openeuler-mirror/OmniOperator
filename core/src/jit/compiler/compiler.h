#ifndef __OMNI_JIT_COMPILER_H__
#define __OMNI_JIT_COMPILER_H__

#include "../specialization.h"

#include <string>
#include <map>

namespace omniruntime {
    namespace jit {
        class Compiler {
        public:
            virtual bool loadOperatorTemplate(std::string operatorName, bool isDependency) = 0;

            virtual uint64_t specializeAndCompile() = 0;

            virtual void addSpecialization(std::string id, Specialization specialization) = 0;

        protected:
            const std::string operatorPath = "/opt/lib/ir/";
            const std::string entryFuncName = "createOperator";
            std::map<std::string, Specialization> specializations;
        };
    }
}

#endif