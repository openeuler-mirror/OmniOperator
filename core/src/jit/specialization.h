/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
#ifndef __OMNI_JIT_SPECIALIZATION_H__
#define __OMNI_JIT_SPECIALIZATION_H__

#include <string>
#include <map>
#include "param_value.h"

namespace omniruntime {
namespace jit {

class Specialization {
public:
    Specialization();

    ~Specialization();

    void AddSpecializedParam(int32_t paramIndex, ParamValue *paramValue);

    bool HasSpecializedParam(int32_t paramIndex) const;

    ParamValue *GetSpecializedParam(int32_t paramIndex) const;

private:
    std::string id;
    std::map<int32_t, ParamValue *> specializedParams;
};
}
}

#endif