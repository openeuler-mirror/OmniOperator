/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
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
 * Defines whether the engine supports container vector.
 * If the engine supports container vector, it doesn't need to flat container vector in sum/avg operations, otherwise
 * flat.
 */
enum class SupportContainerVecRule {
    SUPPORT = 0,
    NOT_SUPPORT
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

class Policy {
public:
    // Default policy is OLK policy
    Policy()
        : Policy(RoundingRule::HALF_UP, CheckReScaleRule::NOT_CHECK_RESCALE, EmptySearchStrReplaceRule::REPLACE,
        CastDecimalToDoubleRule::CAST, NegativeStartIndexOutOfBoundsRule::EMPTY_STRING,
        SupportContainerVecRule::SUPPORT, StringToDateFormatRule::NOT_ALLOW_REDUCED_PRECISION) {};

    Policy(RoundingRule roundingRule, CheckReScaleRule checkReScaleRule,
        EmptySearchStrReplaceRule emptySearchStrReplaceRule, CastDecimalToDoubleRule castDecimalToDoubleRule,
        NegativeStartIndexOutOfBoundsRule negativeStartIndexOutOfBoundsRule,
        SupportContainerVecRule supportContainerVecRule, StringToDateFormatRule stringToDateFormatRule)
        : roundingRule(roundingRule),
          checkReScaleRule(checkReScaleRule),
          emptySearchStrReplaceRule(emptySearchStrReplaceRule),
          castDecimalToDoubleRule(castDecimalToDoubleRule),
          negativeStartIndexOutOfBoundsRule(negativeStartIndexOutOfBoundsRule),
          supportContainerVecRule(supportContainerVecRule),
          stringToDateFormatRule(stringToDateFormatRule) {};

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

    SupportContainerVecRule GetSupportContainerVecRule() const
    {
        return supportContainerVecRule;
    }

    void SetSupportContainerVecRule(SupportContainerVecRule rule)
    {
        supportContainerVecRule = rule;
    }

    StringToDateFormatRule GetStringToDateFormatRule() const
    {
        return stringToDateFormatRule;
    }

    void SetStringToDateFormatRule(StringToDateFormatRule rule)
    {
        stringToDateFormatRule = rule;
    }

protected:
    RoundingRule roundingRule;
    CheckReScaleRule checkReScaleRule;
    EmptySearchStrReplaceRule emptySearchStrReplaceRule;
    CastDecimalToDoubleRule castDecimalToDoubleRule;
    NegativeStartIndexOutOfBoundsRule negativeStartIndexOutOfBoundsRule;
    SupportContainerVecRule supportContainerVecRule;
    StringToDateFormatRule stringToDateFormatRule;
};

#endif // OMNI_RUNTIME_POLICY_H
