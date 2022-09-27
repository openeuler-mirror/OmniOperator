/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * Description: Omni config util source file.
 */

#include <iostream>
#include <fstream>
#include "config_util.h"

std::map<std::string, std::string> ConfigUtil::configMap;
Properties g_properties = ConfigUtil::CreateProperties();

static std::string GetConfigFilePath()
{
    std::string confFile = "/conf/omni.conf";
    auto omniHome = std::getenv("OMNI_HOME");
    std::string confDir = (omniHome == nullptr) ? "/opt" : std::string { omniHome };
    return confDir + confFile;
}

static void Trim(std::string &value)
{
    value.erase(0, value.find_first_not_of(' '));
    value.erase(value.find_last_not_of(' ') + 1);
}

static std::map<std::string, std::string> LoadConf()
{
    auto configFilePath = GetConfigFilePath();
    std::map<std::string, std::string> omniConfigMap;
    std::ifstream confInput(configFilePath);
    if (!confInput.is_open()) {
        return omniConfigMap;
    }

    std::string s;
    while (std::getline(confInput, s)) {
        Trim(s);
        if (s.empty() || s[0] == '#') {
            continue;
        }
        auto pos = s.find('=');
        if (pos == std::string::npos) {
            continue;
        }
        auto key = s.substr(0, pos);
        auto value = s.substr(pos + 1);
        Trim(key);
        Trim(value);

        omniConfigMap[key] = value;
    }
    confInput.close();
    return omniConfigMap;
}

Properties ConfigUtil::CreateProperties()
{
    // first, load omni.conf
    configMap = LoadConf();

    // second, set properties
    static Properties tmpProperties;
    SetProperties(tmpProperties);

    return tmpProperties;
}

void ConfigUtil::SetProperties(Properties &tmpProperties)
{
    // set the property enableBatchExprEvaluate
    bool isEnableBatchExprEvaluate = false;
    GetProperty<bool>("enableBatchExprEvaluate", isEnableBatchExprEvaluate);
    tmpProperties.SetEnableBatchExprEvaluate(isEnableBatchExprEvaluate);

    // set the property hiveUdfPath
    std::string hiveUdfPropertyFilePath;
    auto ret = GetProperty<std::string &>("hiveUdfPropertyFilePath", hiveUdfPropertyFilePath);
    if (ret) {
        tmpProperties.SetHiveUdfPropertyFilePath(hiveUdfPropertyFilePath);
    }

    // set the property enableHMPP
    bool isEnableHMPP = false;
    GetProperty<bool>("isEnableHMPP", isEnableHMPP);
    tmpProperties.SetEnableHMPP(isEnableHMPP);
}

static void Convert(const std::string &value, bool &property)
{
    if (value == "true") {
        property = true;
    } else {
        property = false;
    }
}

static void Convert(const std::string &value, std::string &property)
{
    property = value;
}

template <typename T> bool ConfigUtil::GetProperty(const char *key, T &value)
{
    const auto &iter = configMap.find(key);
    if (iter == configMap.end()) {
        return false;
    }
    Convert(iter->second, value);
    return true;
}

bool ConfigUtil::IsEnableBatchExprEvaluate()
{
    return g_properties.IsEnableBatchExprEvaluate();
}

std::string &ConfigUtil::GetHiveUdfPropertyFilePath()
{
    return g_properties.GetHiveUdfPropertyFilePath();
}

void ConfigUtil::SetEnableBatchExprEvaluate(bool isEnable)
{
    g_properties.SetEnableBatchExprEvaluate(isEnable);
}

bool ConfigUtil::IsEnableHMPP()
{
    return g_properties.IsEnableHMPP();
}

void ConfigUtil::SetEnableHMPP(bool isEnable)
{
    g_properties.SetEnableHMPP(isEnable);
}