#ifndef __OMNI_JIT_SPECIALIZATION_H__
#define __OMNI_JIT_SPECIALIZATION_H__

#include "./param_value.h"

#include <string>
#include <map>

namespace omniruntime {
    namespace jit {
        class Stats {
        public:
        private:
        };

        class Specialization {
        public:
            Specialization();

            void addSpecializedParam(int paramIndex, ParamValue *paramValue);

            bool hasSpecializedParam(int paramIndex);

            ParamValue *getSpecializedParam(int paramIndex);

            void addSpecializedStats(Stats stats);

        private:
            std::string id;
            std::map<int, ParamValue *> specializedParams;
        };
    }
}

#endif