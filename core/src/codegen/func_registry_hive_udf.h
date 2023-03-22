/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * Description: registry hive udf.
 */
#ifndef OMNI_RUNTIME_FUNC_REGISTRY_HIVE_UDF_H
#define OMNI_RUNTIME_FUNC_REGISTRY_HIVE_UDF_H

#include "function.h"
#include "func_registry_base.h"

namespace omniruntime::codegen {
class HiveUdfRegistry : public BaseFunctionRegistry {
public:
    std::vector<Function> GetFunctions() override;

    static void GenerateHiveUdfMap(std::unordered_map<std::string, std::string> &hiveUdfMap);
};
}
#endif // OMNI_RUNTIME_FUNC_REGISTRY_HIVE_UDF_H
