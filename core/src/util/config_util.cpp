/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * Description: Omni config util source file.
 */

#include <iostream>
#include <fstream>
#include <cstring>
#include <functional>
#include <memory>
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
    // set the property policy using omni.conf
    tmpProperties.SetPolicy(InitializePolicy());

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

void ConfigUtil::SetRoundingRule(RoundingRule rule)
{
    g_properties.GetPolicy()->SetRoundingRule(rule);
}

RoundingRule ConfigUtil::GetRoundingRule()
{
    return g_properties.GetPolicy()->GetRoundingRule();
}

void ConfigUtil::SetCheckReScaleRule(CheckReScaleRule rule)
{
    g_properties.GetPolicy()->SetCheckReScaleRule(rule);
}

CheckReScaleRule ConfigUtil::GetCheckReScaleRule()
{
    return g_properties.GetPolicy()->GetCheckReScaleRule();
}

void ConfigUtil::SetEmptySearchStrReplaceRule(EmptySearchStrReplaceRule rule)
{
    g_properties.GetPolicy()->SetEmptySearchStrReplaceRule(rule);
}

EmptySearchStrReplaceRule ConfigUtil::GetEmptySearchStrReplaceRule()
{
    return g_properties.GetPolicy()->GetEmptySearchStrReplaceRule();
}

void ConfigUtil::SetCastDecimalToDoubleRule(CastDecimalToDoubleRule rule)
{
    g_properties.GetPolicy()->SetCastDecimalToDoubleRule(rule);
}

CastDecimalToDoubleRule ConfigUtil::GetCastDecimalToDoubleRule()
{
    return g_properties.GetPolicy()->GetCastDecimalToDoubleRule();
}

void ConfigUtil::SetNegativeStartIndexOutOfBoundsRule(NegativeStartIndexOutOfBoundsRule rule)
{
    g_properties.GetPolicy()->SetNegativeStartIndexOutOfBoundsRule(rule);
}

NegativeStartIndexOutOfBoundsRule ConfigUtil::GetNegativeStartIndexOutOfBoundsRule()
{
    return g_properties.GetPolicy()->GetNegativeStartIndexOutOfBoundsRule();
}

void ConfigUtil::SetSupportContainerVecRule(SupportContainerVecRule rule)
{
    g_properties.GetPolicy()->SetSupportContainerVecRule(rule);
}

SupportContainerVecRule ConfigUtil::GetSupportContainerVecRule()
{
    return g_properties.GetPolicy()->GetSupportContainerVecRule();
}

void ConfigUtil::SetStringToDateFormatRule(StringToDateFormatRule rule)
{
    g_properties.GetPolicy()->SetStringToDateFormatRule(rule);
}

StringToDateFormatRule ConfigUtil::GetStringToDateFormatRule()
{
    return g_properties.GetPolicy()->GetStringToDateFormatRule();
}

Policy *ConfigUtil::GetPolicy()
{
    return g_properties.GetPolicy();
}

Policy *ConfigUtil::InitializePolicy()
{
    static Policy policy;
    std::map<const char *, std::function<void(Policy *, std::string)>> initFunctionMap = { { "RoundingRule",
                                                                                             InitRoundingRule },
                                                                                           { "CheckReScaleRule", InitCheckReScaleRule },
                                                                                           { "EmptySearchStrReplaceRule", InitEmptySearchStrReplaceRule },
                                                                                           { "CastDecimalToDoubleRule", InitCastDecimalToDoubleRule },
                                                                                           { "NegativeStartIndexOutOfBoundsRule", InitNegativeStartIndexOutOfBoundsRule },
                                                                                           { "SupportContainerVecRule", InitSupportContainerVecRule },
                                                                                           { "StringToDateFormatRule", InitStringToDateFormatRule } };
    std::string ruleValueStr;
    for (const auto &item : initFunctionMap) {
        const char *ruleKeyStr = item.first;
        if (GetProperty<std::string &>(ruleKeyStr, ruleValueStr)) {
            auto initFunc = item.second;
            initFunc(&policy, ruleValueStr);
        }
    }
    return &policy;
}

void ConfigUtil::InitRoundingRule(Policy *policy, const std::string &ruleValueStr)
{
    if (ruleValueStr == "DOWN") {
        policy->SetRoundingRule(RoundingRule::DOWN);
    }
}

void ConfigUtil::InitCheckReScaleRule(Policy *policy, const std::string &ruleValueStr)
{
    if (ruleValueStr == "CHECK_RESCALE") {
        policy->SetCheckReScaleRule(CheckReScaleRule::CHECK_RESCALE);
    }
}

void ConfigUtil::InitEmptySearchStrReplaceRule(Policy *policy, const std::string &ruleValueStr)
{
    if (ruleValueStr == "NOT_REPLACE") {
        policy->SetEmptySearchStrReplaceRule(EmptySearchStrReplaceRule::NOT_REPLACE);
    }
}

void ConfigUtil::InitCastDecimalToDoubleRule(Policy *policy, const std::string &ruleValueStr)
{
    if (ruleValueStr == "CONVERT_WITH_STRING") {
        policy->SetCastDecimalToDoubleRule(CastDecimalToDoubleRule::CONVERT_WITH_STRING);
    }
}

void ConfigUtil::InitNegativeStartIndexOutOfBoundsRule(Policy *policy, const std::string &ruleValueStr)
{
    if (ruleValueStr == "INTERCEPT_FROM_BEYOND") {
        policy->SetNegativeStartIndexOutOfBoundsRule(NegativeStartIndexOutOfBoundsRule::INTERCEPT_FROM_BEYOND);
    }
}

void ConfigUtil::InitSupportContainerVecRule(Policy *policy, const std::string &ruleValueStr)
{
    if (ruleValueStr == "NOT_SUPPORT") {
        policy->SetSupportContainerVecRule(SupportContainerVecRule::NOT_SUPPORT);
    }
}

void ConfigUtil::InitStringToDateFormatRule(Policy *policy, const std::string &ruleValueStr)
{
    if (ruleValueStr == "ALLOW_REDUCED_PRECISION") {
        policy->SetStringToDateFormatRule(StringToDateFormatRule::ALLOW_REDUCED_PRECISION);
    }
}
