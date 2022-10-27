/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * Description: Omni config util source file.
 */

#include <iostream>
#include <fstream>
#include <cstring>
#include "config_util.h"

std::map<std::string, std::string> ConfigUtil::configMap;
Properties g_properties = ConfigUtil::CreateProperties();

static void Trim(std::string &value)
{
    value.erase(0, value.find_first_not_of(' '));
    value.erase(value.find_last_not_of(' ') + 1);
}

static std::string GetOmniHome()
{
    auto omniHome = std::getenv("OMNI_HOME");
    if (omniHome != nullptr && omniHome[0] != '\0') {
        std::string confDir { omniHome };
        Trim(confDir);
        return confDir;
    } else {
        return "/opt";
    }
}

static std::string GetConfigFilePath()
{
    std::string confFile = "/conf/omni.conf";
    return GetOmniHome() + confFile;
}

static std::map<std::string, std::string> LoadConf()
{
    std::map<std::string, std::string> omniConfigMap;

    auto configFilePath = GetConfigFilePath();
    auto configFileRealPath = realpath(configFilePath.c_str(), nullptr);
    if (configFileRealPath == nullptr) {
        return omniConfigMap;
    }

    std::ifstream confInput(configFileRealPath);
    if (!confInput.is_open()) {
        free(configFileRealPath);
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
    free(configFileRealPath);
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
        if (hiveUdfPropertyFilePath[0] == '.') {
            hiveUdfPropertyFilePath = GetOmniHome() + "/" + hiveUdfPropertyFilePath;
        }
        auto realPathRes = realpath(hiveUdfPropertyFilePath.c_str(), nullptr);
        if (realPathRes != nullptr) {
            tmpProperties.SetHiveUdfPropertyFilePath(realPathRes);
            free(realPathRes);
        }
    }

    // set the property enableHMPP
    bool isEnableHMPP = false;
    GetProperty<bool>("enableHMPP", isEnableHMPP);
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