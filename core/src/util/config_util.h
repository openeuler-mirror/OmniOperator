/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2024. All rights reserved.
 * Description: Omni config util header file.
 */

#ifndef OMNI_RUNTIME_CONFIG_UTIL_H
#define OMNI_RUNTIME_CONFIG_UTIL_H

#include <map>
#include <string>
#include "policy.h"
#include "property.h"

class ConfigUtil {
public:
    static Properties CreateProperties();

    static bool IsEnableBatchExprEvaluate();

    static std::string &GetHiveUdfPropertyFilePath();

    // for test
    static void SetRoundingRule(RoundingRule rule);

    static RoundingRule GetRoundingRule();

    static void SetCheckReScaleRule(CheckReScaleRule rule);

    static CheckReScaleRule GetCheckReScaleRule();

    static void SetEmptySearchStrReplaceRule(EmptySearchStrReplaceRule rule);

    static EmptySearchStrReplaceRule GetEmptySearchStrReplaceRule();

    static void SetCastDecimalToDoubleRule(CastDecimalToDoubleRule rule);

    static CastDecimalToDoubleRule GetCastDecimalToDoubleRule();

    static void SetNegativeStartIndexOutOfBoundsRule(NegativeStartIndexOutOfBoundsRule rule);

    static NegativeStartIndexOutOfBoundsRule GetNegativeStartIndexOutOfBoundsRule();

    static void SetZeroStartIndexSupportRule(ZeroStartIndexSupportRule rule);

    static ZeroStartIndexSupportRule GetZeroStartIndexSupportRule();

    static void SetSupportContainerVecRule(SupportContainerVecRule rule);

    static SupportContainerVecRule GetSupportContainerVecRule();

    static AggHashTableRule GetAggHashTableRule();

    static void SetAggHashTableRule(AggHashTableRule rule);

    static void SetStringToDateFormatRule(StringToDateFormatRule rule);

    static StringToDateFormatRule GetStringToDateFormatRule();

    static void SetSupportDecimalPrecisionImprovementRule(SupportDecimalPrecisionImprovementRule rule);

    static SupportDecimalPrecisionImprovementRule GetSupportDecimalPrecisionImprovementRule();

    static Policy *GetPolicy();

    // for test
    static void SetEnableBatchExprEvaluate(bool isEnable);

private:
    static std::map<std::string, std::string> configMap;

    static void SetProperties(Properties &tmpProperties);

    template <typename T> static bool GetProperty(const char *key, T &value);

    static void InitRoundingRule(Policy *policy, const std::string &ruleValueStr);

    static void InitCheckReScaleRule(Policy *policy, const std::string &ruleValueStr);

    static void InitEmptySearchStrReplaceRule(Policy *policy, const std::string &ruleValueStr);

    static void InitCastDecimalToDoubleRule(Policy *policy, const std::string &ruleValueStr);

    static void InitNegativeStartIndexOutOfBoundsRule(Policy *policy, const std::string &ruleValueStr);

    static void InitZeroStartIndexSupportRule(Policy *policy, const std::string &ruleValueStr);

    static void InitSupportContainerVecRule(Policy *policy, const std::string &ruleValueStr);

    static void InitStringToDateFormatRule(Policy *policy, const std::string &ruleValueStr);

    static void InitSupportExprFilterRule(Policy *policy, const std::string &ruleValueStr);

    static void InitSupportDecimalPrecisionImprovementRule(Policy *policy, const std::string &ruleValueStr);

    static void InitAggHashTableRule(Policy *policy, const std::string &ruleValueStr);

    static void InitStringToDecimalRule(Policy *policy, const std::string &ruleValueStr);

    static Policy *InitializePolicy();
};

static Properties g_properties = ConfigUtil::CreateProperties();
static Properties &GetProperties()
{
    return g_properties;
}

#endif // OMNI_RUNTIME_CONFIG_UTIL_H
