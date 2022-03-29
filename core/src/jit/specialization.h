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
class Stats {
public:
private:
};

class Specialization {
public:
    Specialization();

    ~Specialization();

    void AddSpecializedParam(int paramIndex, ParamValue *paramValue);

    bool HasSpecializedParam(int paramIndex) const;

    ParamValue *GetSpecializedParam(int paramIndex) const;

private:
    std::string id;
    std::map<int, ParamValue *> specializedParams;
};
}
}

#endif