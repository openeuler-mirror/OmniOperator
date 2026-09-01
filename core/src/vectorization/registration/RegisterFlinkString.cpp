/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Description: registration of string functions whose Flink semantics differ from Spark's
 *
 * Kept apart from RegisterString.cpp so that adapting a Flink expression never touches the
 * Spark registrations. Names are prefixed with "flink_" and are emitted by RexNodeUtil's
 * simpleFunctionNameMap; overloads that only differ in their return type reuse the Spark name.
 */

#include <string>
#include "../functions/String.h"
#include "RegistrationHelpers.h"

namespace omniruntime::vectorization {
void RegisterFlinkStringFunctions(const std::string &prefix)
{
    // Flink FROM_BASE64(string) -> STRING: decoded bytes are interpreted as a UTF-8 string.
    // A VARCHAR-returning overload makes the result marshallable to RowData and matches the
    // signature RexNodeUtil emits (returnType=VARCHAR). Same UnBase64Function body as the
    // VARBINARY-returning Spark overload.
    RegisterFunction<UnBase64Function, std::string, std::string_view>(
        prefix + "unbase64", {OMNI_VARCHAR}, OMNI_VARCHAR);

    // Flink REPLACE (SqlFunctionUtils.replace -> Java String.replace): an empty search string
    // inserts the replacement around every character, where Spark returns the input unchanged.
    RegisterFunction<FlinkReplaceFunction, std::string, std::string_view, std::string_view, std::string_view>(
        prefix + "flink_replace", {OMNI_VARCHAR, OMNI_VARCHAR, OMNI_VARCHAR}, OMNI_VARCHAR);

    // Flink SUBSTRING/SUBSTR (BinaryStringDataUtil.substringSQL): a negative length yields
    // NULL and an out-of-range negative position yields an empty string, where Spark yields
    // an empty string and a prefix respectively.
    RegisterFunction<FlinkSubstrFunction, std::string, std::string_view, int32_t>(
        prefix + "flink_substr", {OMNI_VARCHAR, OMNI_INT}, OMNI_VARCHAR);
    RegisterFunction<FlinkSubstrFunction, std::string, std::string_view, int32_t, int32_t>(
        prefix + "flink_substr", {OMNI_VARCHAR, OMNI_INT, OMNI_INT}, OMNI_VARCHAR);
    RegisterFunction<FlinkSubstrFunction, std::string, std::string_view, int32_t>(
        prefix + "flink_substr", {OMNI_CHAR, OMNI_INT}, OMNI_VARCHAR);
    RegisterFunction<FlinkSubstrFunction, std::string, std::string_view, int32_t, int32_t>(
        prefix + "flink_substr", {OMNI_CHAR, OMNI_INT, OMNI_INT}, OMNI_VARCHAR);

    // Flink LPAD (SqlFunctionUtils.lpad): a negative length or empty pad string
    // yields NULL, where Spark's "lpad" yields an empty string.
    RegisterFunction<FlinkLPadFunction, std::string, std::string_view, int64_t, std::string_view>(
        prefix + "flink_lpad", {OMNI_VARCHAR, OMNI_LONG, OMNI_VARCHAR}, OMNI_VARCHAR);
    RegisterFunction<FlinkLPadFunction, std::string, std::string_view, int32_t, std::string_view>(
        prefix + "flink_lpad", {OMNI_VARCHAR, OMNI_INT, OMNI_VARCHAR}, OMNI_VARCHAR);
    RegisterFunction<FlinkLPadFunction, std::string, std::string_view, int64_t, std::string_view>(
        prefix + "flink_lpad", {OMNI_CHAR, OMNI_LONG, OMNI_CHAR}, OMNI_VARCHAR);
    RegisterFunction<FlinkLPadFunction, std::string, std::string_view, int32_t, std::string_view>(
        prefix + "flink_lpad", {OMNI_CHAR, OMNI_INT, OMNI_CHAR}, OMNI_VARCHAR);
    RegisterFunction<FlinkLPadFunction, std::string, std::string_view, int64_t, std::string_view>(
        prefix + "flink_lpad", {OMNI_VARCHAR, OMNI_LONG, OMNI_CHAR}, OMNI_VARCHAR);
    RegisterFunction<FlinkLPadFunction, std::string, std::string_view, int32_t, std::string_view>(
        prefix + "flink_lpad", {OMNI_VARCHAR, OMNI_INT, OMNI_CHAR}, OMNI_VARCHAR);
    RegisterFunction<FlinkLPadFunction, std::string, std::string_view, int64_t, std::string_view>(
        prefix + "flink_lpad", {OMNI_CHAR, OMNI_LONG, OMNI_VARCHAR}, OMNI_VARCHAR);
    RegisterFunction<FlinkLPadFunction, std::string, std::string_view, int32_t, std::string_view>(
        prefix + "flink_lpad", {OMNI_CHAR, OMNI_INT, OMNI_VARCHAR}, OMNI_VARCHAR);

    // Flink RPAD (SqlFunctionUtils.rpad): same NULL boundaries as flink_lpad.
    RegisterFunction<FlinkRPadFunction, std::string, std::string_view, int64_t, std::string_view>(
        prefix + "flink_rpad", {OMNI_VARCHAR, OMNI_LONG, OMNI_VARCHAR}, OMNI_VARCHAR);
    RegisterFunction<FlinkRPadFunction, std::string, std::string_view, int32_t, std::string_view>(
        prefix + "flink_rpad", {OMNI_VARCHAR, OMNI_INT, OMNI_VARCHAR}, OMNI_VARCHAR);
    RegisterFunction<FlinkRPadFunction, std::string, std::string_view, int64_t, std::string_view>(
        prefix + "flink_rpad", {OMNI_CHAR, OMNI_LONG, OMNI_CHAR}, OMNI_VARCHAR);
    RegisterFunction<FlinkRPadFunction, std::string, std::string_view, int32_t, std::string_view>(
        prefix + "flink_rpad", {OMNI_CHAR, OMNI_INT, OMNI_CHAR}, OMNI_VARCHAR);
    RegisterFunction<FlinkRPadFunction, std::string, std::string_view, int64_t, std::string_view>(
        prefix + "flink_rpad", {OMNI_VARCHAR, OMNI_LONG, OMNI_CHAR}, OMNI_VARCHAR);
    RegisterFunction<FlinkRPadFunction, std::string, std::string_view, int32_t, std::string_view>(
        prefix + "flink_rpad", {OMNI_VARCHAR, OMNI_INT, OMNI_CHAR}, OMNI_VARCHAR);
    RegisterFunction<FlinkRPadFunction, std::string, std::string_view, int64_t, std::string_view>(
        prefix + "flink_rpad", {OMNI_CHAR, OMNI_LONG, OMNI_VARCHAR}, OMNI_VARCHAR);
    RegisterFunction<FlinkRPadFunction, std::string, std::string_view, int32_t, std::string_view>(
        prefix + "flink_rpad", {OMNI_CHAR, OMNI_INT, OMNI_VARCHAR}, OMNI_VARCHAR);
}
}
