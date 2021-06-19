#include "specialization.h"

namespace omniruntime {
    namespace jit {
        Specialization::Specialization() = default;

        void Specialization::addSpecializedParam(int paramIndex, ParamValue *paramValue) {
            this->specializedParams.insert({paramIndex, paramValue});
        }

        bool Specialization::hasSpecializedParam(int paramIndex) {
            return (this->specializedParams.count(paramIndex) > 0);
        }

        ParamValue *Specialization::getSpecializedParam(int paramIndex) {
            return this->specializedParams.at(paramIndex);
        }

        void Specialization::addSpecializedStats(Stats stats) {
        }
    }
}