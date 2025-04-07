/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2024. All rights reserved.
 * Description: Policy Class and Enumeration Classes of various rules
 */

#ifndef OMNI_RUNTIME_POLICY_H
#define OMNI_RUNTIME_POLICY_H

/**
 * Defines the differences in Decimal-to-Int/Long conversion between different engines.
 */
enum class RoundingRule {
    // Round towards "nearest neighbor" unless both neighbors are equidistant, in which case round up.
    HALF_UP = 0,

    // Round towards zero.
    DOWN
};

/**
 * Defines the differences in Decimal math function between different engines.
 */
enum class CheckReScaleRule {
    /**
     * Whether to Check Overflow when decimal operations result is return.
     */
    NOT_CHECK_RESCALE = 0,
    CHECK_RESCALE
};

/**
 * Defines the differences that engines treat empty SearchStr while replacing string.
 */
enum class EmptySearchStrReplaceRule {
    /**
     * If SearchStr is "", each location between characters will be replaced with ReplaceStr.
     * e.g., InputStr="apple", ReplaceStr="*", SearchStr="", OutputStr="*a*p*p*l*e*"
     */
    REPLACE = 0,

    /**
     * If SearchStr is "", replace nothing.
     * e.g., InputStr="apple", ReplaceStr="*", SearchStr="", OutputStr="apple"
     */
    NOT_REPLACE
};

/**
 * Defines switch for array join for hash agg.
 */
enum class AggHashTableRule {
    // use array for hash table
    ARRAY = 0,

    NORMAL
};

/**
 * Defines the differences that engines cast Decimal to Double.
 */
enum class CastDecimalToDoubleRule {
    // Directly use input value to cast.
    CAST = 0,

    // Convert the input value to string first, then use the string to construct double.
    CONVERT_WITH_STRING
};

/**
 * Defines the difference that SubStr function returns the result if the negative startIndex exceeds the bounds.
 */
enum class NegativeStartIndexOutOfBoundsRule {
    /**
     * Directly return an empty string.
     * e.g., str="apple", strLength=5, startIndex=-7, subStringLength=3, Result="".
     * Note: The index from left to right starts from 1. The index from right to left starts from -1.
     */
    EMPTY_STRING = 0,

    /**
     * Intercept substring from beyond.
     * e.g., str="apple", strLength=5, startIndex=-7, subStringLength=3, Result="a".
     */
    INTERCEPT_FROM_BEYOND
};

/**
 * Defines the difference that SubStr function returns the result if the startIndex equals zero.
 */
enum class ZeroStartIndexSupportRule {
    /**
     * Directly return an empty string when startIndex = 0
     */
    IS_NOT_SUPPORT = 0,

    /**
     * It refers to the first element when startIndex = 0
     */
    IS_SUPPORT
};

/**
 * Defines whether the engine supports container vector.
 * If the engine supports container vector, it doesn't need to flat container vector in sum/avg operations, otherwise
 * flat.
 */
enum class SupportContainerVecRule {
    SUPPORT = 0,
    NOT_SUPPORT
};

/**
 * Defines whether the engine supports decimal process improvement.
 * If the engine supports decimal process improvement, it will improve precision based on the output type in avg
 * operations.
 * Currently, this configuration needs to be enabled for Hive.
 */
enum class SupportDecimalPrecisionImprovementRule {
    IS_NOT_SUPPORT = 0,
    IS_SUPPORT
};

/**
 * Defines the string format when the engine cast string to date.
 */
enum class StringToDateFormatRule {
    /**
     * This means the date string must be completely written in the extended format of ISO Calendar dates.
     * A two-digit mouth and a two-digit day cannot be omitted.
     * e.g., 1996-02-28.
     */
    NOT_ALLOW_REDUCED_PRECISION = 0,

    /**
     * This means the date string can be written with reduced precision in the extended format of ISO Calendar dates.
     * Mouth or day can be omitted.
     * e.g., 1996-02-28, 1996-02, 1996.
     */
    ALLOW_REDUCED_PRECISION
};

/**
 * Defines the string format when the engine cast string to decimal.
 */
enum class StringToDecimalRule {
    /**
     * This means if precision > 38/18 when cast string to decimal128/64, the result will return null or throw a error.
     * e.g. "312423423423542352333243423423.123412342" -> decimal(38,8) = null
     */
    OVERFLOW_AS_NULL = 0,

    /**
     * This means if fractional part is overflow when cast string to decimal128/64, the result will round up
     * e.g. "312423423423542352333243423423.123412342" -> decimal(38,8) = 312423423423542352333243423423.12341234
     */
    OVERFLOW_AS_ROUND_UP
};

class Policy {
public:
    // Default policy is spark policy
    Policy()
        : Policy(RoundingRule::DOWN, CheckReScaleRule::CHECK_RESCALE, EmptySearchStrReplaceRule::NOT_REPLACE,
        CastDecimalToDoubleRule::CONVERT_WITH_STRING, NegativeStartIndexOutOfBoundsRule::INTERCEPT_FROM_BEYOND,
        ZeroStartIndexSupportRule::IS_SUPPORT, SupportContainerVecRule::NOT_SUPPORT,
        StringToDateFormatRule::ALLOW_REDUCED_PRECISION, SupportDecimalPrecisionImprovementRule::IS_NOT_SUPPORT,
        StringToDecimalRule::OVERFLOW_AS_NULL, AggHashTableRule::NORMAL) {};

    Policy(RoundingRule roundingRule, CheckReScaleRule checkReScaleRule,
        EmptySearchStrReplaceRule emptySearchStrReplaceRule, CastDecimalToDoubleRule castDecimalToDoubleRule,
        NegativeStartIndexOutOfBoundsRule negativeStartIndexOutOfBoundsRule,
        ZeroStartIndexSupportRule zeroStartIndexSupportRule, SupportContainerVecRule supportContainerVecRule,
        StringToDateFormatRule stringToDateFormatRule,
        SupportDecimalPrecisionImprovementRule supportDecimalPrecisionImprovementRule,
        StringToDecimalRule stringToDecimalRule, AggHashTableRule aggHashTableRule)
        : roundingRule(roundingRule),
          checkReScaleRule(checkReScaleRule),
          emptySearchStrReplaceRule(emptySearchStrReplaceRule),
          castDecimalToDoubleRule(castDecimalToDoubleRule),
          negativeStartIndexOutOfBoundsRule(negativeStartIndexOutOfBoundsRule),
          zeroStartIndexSupportRule(zeroStartIndexSupportRule),
          supportContainerVecRule(supportContainerVecRule),
          stringToDateFormatRule(stringToDateFormatRule),
          supportDecimalPrecisionImprovementRule(supportDecimalPrecisionImprovementRule),
          stringToDecimalRule(stringToDecimalRule),
          aggHashTableRule(aggHashTableRule){};

    RoundingRule GetRoundingRule()
    {
        return roundingRule;
    }

    void SetRoundingRule(RoundingRule rule)
    {
        roundingRule = rule;
    }

    CheckReScaleRule GetCheckReScaleRule()
    {
        return checkReScaleRule;
    }

    void SetCheckReScaleRule(CheckReScaleRule rule)
    {
        checkReScaleRule = rule;
    }

    EmptySearchStrReplaceRule GetEmptySearchStrReplaceRule() const
    {
        return emptySearchStrReplaceRule;
    }

    void SetEmptySearchStrReplaceRule(EmptySearchStrReplaceRule rule)
    {
        emptySearchStrReplaceRule = rule;
    }

    CastDecimalToDoubleRule GetCastDecimalToDoubleRule() const
    {
        return castDecimalToDoubleRule;
    }

    void SetCastDecimalToDoubleRule(CastDecimalToDoubleRule rule)
    {
        castDecimalToDoubleRule = rule;
    }

    NegativeStartIndexOutOfBoundsRule GetNegativeStartIndexOutOfBoundsRule() const
    {
        return negativeStartIndexOutOfBoundsRule;
    }

    void SetNegativeStartIndexOutOfBoundsRule(NegativeStartIndexOutOfBoundsRule rule)
    {
        negativeStartIndexOutOfBoundsRule = rule;
    }

    ZeroStartIndexSupportRule GetZeroStartIndexSupportRule() const
    {
        return zeroStartIndexSupportRule;
    }

    void SetZeroStartIndexSupportRule(ZeroStartIndexSupportRule rule)
    {
        zeroStartIndexSupportRule = rule;
    }

    SupportContainerVecRule GetSupportContainerVecRule() const
    {
        return supportContainerVecRule;
    }

    void SetSupportContainerVecRule(SupportContainerVecRule rule)
    {
        supportContainerVecRule = rule;
    }

    AggHashTableRule GetAggHashTableRule() const
    {
        return aggHashTableRule;
    }

    void SetAggHashTableRule(AggHashTableRule rule)
    {
        aggHashTableRule = rule;
    }

    StringToDateFormatRule GetStringToDateFormatRule() const
    {
        return stringToDateFormatRule;
    }

    void SetStringToDateFormatRule(StringToDateFormatRule rule)
    {
        stringToDateFormatRule = rule;
    }

    void SetSupportDecimalPrecisionImprovementRule(SupportDecimalPrecisionImprovementRule rule)
    {
        supportDecimalPrecisionImprovementRule = rule;
    }

    SupportDecimalPrecisionImprovementRule GetSupportDecimalPrecisionImprovementRule() const
    {
        return supportDecimalPrecisionImprovementRule;
    }

    StringToDecimalRule GetStringToDecimalRule() const
    {
        return stringToDecimalRule;
    }

    void SetStringToDecimalRule(StringToDecimalRule rule)
    {
        stringToDecimalRule = rule;
    }

protected:
    RoundingRule roundingRule;
    CheckReScaleRule checkReScaleRule;
    EmptySearchStrReplaceRule emptySearchStrReplaceRule;
    CastDecimalToDoubleRule castDecimalToDoubleRule;
    NegativeStartIndexOutOfBoundsRule negativeStartIndexOutOfBoundsRule;
    ZeroStartIndexSupportRule zeroStartIndexSupportRule;
    SupportContainerVecRule supportContainerVecRule;
    StringToDateFormatRule stringToDateFormatRule;
    SupportDecimalPrecisionImprovementRule supportDecimalPrecisionImprovementRule;
    StringToDecimalRule stringToDecimalRule;
    AggHashTableRule aggHashTableRule;
};

#endif // OMNI_RUNTIME_POLICY_H
