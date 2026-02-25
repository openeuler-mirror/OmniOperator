/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Description: JSON function registration
 */

#include <string>
#include "../functions/ToJson.h"
#include "../functions/JsonObjectKeys.h"
#include "RegistrationHelpers.h"

namespace omniruntime::vectorization {
void RegisterJsonFunctions(const std::string &prefix)
{
    auto toJsonFunc = std::make_shared<ToJsonFunction>();
    VectorFunction::RegisterVectorFunction(prefix + "to_json", {OMNI_ROW}, OMNI_VARCHAR, toJsonFunc);
    VectorFunction::RegisterVectorFunction(prefix + "to_json", {OMNI_ARRAY}, OMNI_VARCHAR, toJsonFunc);
    VectorFunction::RegisterVectorFunction(prefix + "to_json", {OMNI_MAP}, OMNI_VARCHAR, toJsonFunc);

    VectorFunction::RegisterVectorFunction("json_object_keys", {OMNI_VARCHAR}, OMNI_ARRAY,
        std::make_shared<JsonObjectKeysFunction>());
}
}
