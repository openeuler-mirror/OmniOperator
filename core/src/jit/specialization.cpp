/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
#include "specialization.h"

namespace omniruntime {
    namespace jit {
        Specialization::Specialization() = default;
        Specialization::~Specialization() = default;

        void Specialization::AddSpecializedParam(int paramIndex, ParamValue *paramValue)
        {
            this->specializedParams.insert({paramIndex, paramValue});
        }

        bool Specialization::HasSpecializedParam(int paramIndex) const
        {
            return (this->specializedParams.count(paramIndex) > 0);
        }

        ParamValue *Specialization::GetSpecializedParam(int paramIndex) const
        {
            return this->specializedParams.at(paramIndex);
        }

        void Specialization::AddSpecializedStats(Stats stats) const
        {
        }
    }
}