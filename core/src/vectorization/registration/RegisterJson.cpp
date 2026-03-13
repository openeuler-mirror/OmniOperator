/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Description: JSON function registration
 */

 #include <string>
 #include "../functions/ToJson.h"
 #include "../functions/JsonObjectKeys.h"
 #include "../functions/JsonArrayLength.h"
 #include "../functions/FromJson.h"
 #include "../functions/GetJsonObject.h"
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
 
     VectorFunction::RegisterVectorFunction("json_array_length", {OMNI_VARCHAR}, OMNI_INT,
         std::make_shared<JsonArrayLengthFunction>());
 
     VectorFunction::RegisterVectorFunction("from_json", {OMNI_VARCHAR}, OMNI_ROW,
         std::make_shared<FromJsonFunction>());
 
     // Register get_json_object function (lowercase version for standard SQL)
     VectorFunction::RegisterVectorFunction("get_json_object", {OMNI_VARCHAR, OMNI_VARCHAR}, OMNI_VARCHAR,
         std::make_shared<GetJsonObjectFunction>());
 
     // Also register CamelCase version for Gluten compatibility
     VectorFunction::RegisterVectorFunction("GetJsonObject", {OMNI_VARCHAR, OMNI_VARCHAR}, OMNI_VARCHAR,
         std::make_shared<GetJsonObjectFunction>());
 }
 }
 