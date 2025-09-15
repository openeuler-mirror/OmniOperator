/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * Description: Policy Class and Enumeration Classes of various rules
 */

#ifndef OMNI_RUNTIME_PROPERTY_H
#define OMNI_RUNTIME_PROPERTY_H

#include "policy.h"

class Properties {
public:
    Properties() : isEnableBatchExprEvaluate(false) {}

    ~Properties() = default;

    void SetEnableBatchExprEvaluate(bool isEnable)
    {
        isEnableBatchExprEvaluate = isEnable;
    }

    bool IsEnableBatchExprEvaluate()
    {
        return isEnableBatchExprEvaluate;
    }

    void SetHiveUdfPropertyFilePath(const std::string &udfPath)
    {
        hiveUdfPropertyFilePath = udfPath;
    }

    std::string &GetHiveUdfPropertyFilePath()
    {
        return hiveUdfPropertyFilePath;
    }

    void SetPolicy(Policy *inputPolicy)
    {
        policy = inputPolicy;
    }

    Policy *GetPolicy()
    {
        return policy;
    }

private:
    bool isEnableBatchExprEvaluate;
    std::string hiveUdfPropertyFilePath {};
    Policy *policy;
};

#endif // OMNI_RUNTIME_PROPERTY_H
