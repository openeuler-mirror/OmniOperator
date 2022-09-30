/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * Description: Omni config util header file.
 */

#ifndef OMNI_RUNTIME_CONFIG_UTIL_H
#define OMNI_RUNTIME_CONFIG_UTIL_H

#include <map>

class Properties {
public:
    Properties() : isEnableBatchExprEvaluate(false), isEnableHMPP(false) {}
    ~Properties() = default;

    void SetEnableBatchExprEvaluate(bool isEnable)
    {
        isEnableBatchExprEvaluate = isEnable;
    }

    bool IsEnableBatchExprEvaluate()
    {
        return isEnableBatchExprEvaluate;
    }

    void SetEnableHMPP(bool isEnable)
    {
        isEnableHMPP = isEnable;
    }

    bool IsEnableHMPP()
    {
        return isEnableHMPP;
    }

    void SetHiveUdfPropertyFilePath(const std::string &udfPath)
    {
        hiveUdfPropertyFilePath = udfPath;
    }

    std::string &GetHiveUdfPropertyFilePath()
    {
        return hiveUdfPropertyFilePath;
    }

private:
    bool isEnableBatchExprEvaluate;
    bool isEnableHMPP;
    std::string hiveUdfPropertyFilePath {};
};

class ConfigUtil {
public:
    static Properties CreateProperties();

    static bool IsEnableBatchExprEvaluate();

    static std::string &GetHiveUdfPropertyFilePath();

    // for test
    static void SetEnableBatchExprEvaluate(bool isEnable);

    static bool IsEnableHMPP();

    // for test
    static void SetEnableHMPP(bool isEnable);

private:
    static std::map<std::string, std::string> configMap;

    static void SetProperties(Properties &tmpProperties);
    template <typename T> static bool GetProperty(const char *key, T &value);
};

#endif // OMNI_RUNTIME_CONFIG_UTIL_H
