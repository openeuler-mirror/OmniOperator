/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2024. All rights reserved.
 * Description: Date Time Function Registry
 */

#include "func_registry_datetime.h"
#include "functions/datetime_functions.h"

namespace omniruntime::codegen {
using namespace omniruntime::type;
using namespace omniruntime::codegen::function;

std::vector<Function> DateTimeFunctionRegistry::GetFunctions()
{
    std::vector<Function> dateTimeFnRegistry = {
        Function(reinterpret_cast<void *>(GetHourFromTimestamp), "get_hour", {},
                 {OMNI_LONG}, OMNI_INT, INPUT_DATA),
        Function(reinterpret_cast<void *>(UnixTimestampFromStr), "unix_timestamp", {},
            { OMNI_VARCHAR, OMNI_VARCHAR, OMNI_VARCHAR, OMNI_VARCHAR }, OMNI_LONG, INPUT_DATA_AND_NULL_AND_RETURN_NULL),
        Function(reinterpret_cast<void *>(UnixTimestampFromDate), "unix_timestamp", {},
            { OMNI_DATE32, OMNI_VARCHAR, OMNI_VARCHAR, OMNI_VARCHAR }, OMNI_LONG, INPUT_DATA),
        Function(Function(reinterpret_cast<void *>(FromUnixTime), "from_unixtime", {},
            { OMNI_LONG, OMNI_VARCHAR, OMNI_VARCHAR }, OMNI_VARCHAR, INPUT_DATA, true)),
        Function(reinterpret_cast<void *>(FromUnixTimeRetNull), "from_unixtime_null", {},
            { OMNI_LONG, OMNI_VARCHAR, OMNI_VARCHAR }, OMNI_VARCHAR, INPUT_DATA_AND_OVERFLOW_NULL, true),
        Function(reinterpret_cast<void *>(FromUnixTimeWithoutTz), "from_unixtime_without_tz", {},
                 { OMNI_LONG, OMNI_VARCHAR }, OMNI_VARCHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(DateTrunc), "trunc_date", {}, { OMNI_DATE32, OMNI_VARCHAR },
            OMNI_DATE32, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(DateTruncRetNull), "trunc_date_null", {}, { OMNI_DATE32, OMNI_VARCHAR },
            OMNI_DATE32, INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(DateAdd), "date_add", {}, {OMNI_DATE32, OMNI_INT}, OMNI_DATE32, INPUT_DATA)
    };

    return dateTimeFnRegistry;
}
}
