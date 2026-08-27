/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Description: registration of date/time functions whose Flink semantics differ from Spark's
 *
 * Kept apart from RegisterDateTime.cpp so that adapting a Flink expression never touches the
 * Spark registrations. Names are prefixed with "flink_" and are emitted by RexNodeUtil's
 * simpleFunctionNameMap; current_timestamp deliberately reuses the Spark name (codegen ships
 * a namesake) and therefore stays in RegisterDateTime.cpp.
 */

#include <string>
#include "../functions/CurrentDateTimeFunctions.h"
#include "../functions/Floor.h"
#include "../functions/Ceil.h"
#include "RegistrationHelpers.h"

namespace omniruntime::vectorization {
void RegisterFlinkDateTimeFunctions(const std::string &prefix)
{
    // Flink LOCALTIME -> TIME(3). Milliseconds since midnight in the session timezone
    // (Flink falls back to ZoneId.systemDefault() when unset), evaluated once per query.
    RegisterFunction<LocalTimeFunction, int64_t>(prefix + "flink_localtime", {}, OMNI_LONG);

    // Flink LOCALTIMESTAMP -> TIMESTAMP(3). Session-local wall-clock time treated as UTC
    // millis, evaluated once per query.
    RegisterFunction<LocalTimestampFunction, int64_t>(prefix + "flink_localtimestamp", {}, OMNI_LONG);

    // Flink CURRENT_ROW_TIMESTAMP() / NOW() -> TIMESTAMP_LTZ(3). The true UTC instant is
    // re-evaluated on every call, where current_timestamp is fixed for the duration of a
    // query.
    RegisterFunction<CurrentRowTimestampFunction, int64_t>(prefix + "flink_current_row_timestamp", {}, OMNI_LONG);

    // Flink CURRENT_DATE -> DATE. Days since the Unix epoch for the session-local date,
    // evaluated once per query.
    RegisterFunction<CurrentDateFunction, int32_t>(prefix + "flink_current_date", {}, OMNI_INT);

    // Flink FLOOR(<temporal> TO <unit>) -> unit-truncated temporal. Registered as
    // flink_floor_time to keep it distinct from Spark's numeric floor.
    RegisterFloorFunction(prefix + "flink_floor_time");

    // Flink CEIL(<temporal> TO <unit>) -> unit-rounded-up temporal. Registered as
    // flink_ceil_time to keep it distinct from Spark's numeric ceil.
    RegisterCeilFunction(prefix + "flink_ceil_time");
}
}
