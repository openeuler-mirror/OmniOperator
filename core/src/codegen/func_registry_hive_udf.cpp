/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * Description: registry hive udf.
 */
#include <fstream>
#include <algorithm>
#include "util/debug.h"
#include "functions/udffunctions.h"
#include "func_registry_hive_udf.h"

namespace omniruntime {
using namespace omniruntime::type;

std::vector<Function> HiveUdfRegistry::GetFunctions()
{
    std::vector<Function> hiveUdfFunctions = { Function(reinterpret_cast<void *>(EvaluateHiveUdfSingle),
        "EvaluateHiveUdfSingle", {}, std::vector<DataTypeId> {}, OMNI_INT),
        Function(reinterpret_cast<void *>(EvaluateHiveUdfBatch), "EvaluateHiveUdfBatch", {}, std::vector<DataTypeId> {},
        OMNI_INT) };
    return hiveUdfFunctions;
}

static std::string TransEnv(const char *srcEnv)
{
    return std::string { srcEnv };
}

static std::string GetHiveUdfPropertyPath()
{
    auto omniHome = std::getenv("OMNI_HOME");
    if (omniHome == nullptr) {
        return "/opt/hive-udf/udf.properties";
    }
    return TransEnv(omniHome) + "/hive-udf/udf.properties";
}

static void Trim(std::string &value)
{
    value.erase(0, value.find_first_not_of(' '));
    value.erase(value.find_last_not_of(' ') + 1);
}

void HiveUdfRegistry::GenerateHiveUdfMap(std::unordered_map<std::string, std::string> &hiveUdfMap)
{
    std::string propertyFile = GetHiveUdfPropertyPath();
    std::ifstream file(propertyFile);
    if (!file.good()) {
        LogWarn("%s does not exist.", propertyFile.c_str());
        return;
    }

    std::string s;
    while (getline(file, s)) {
        Trim(s);
        auto pos = s.find(' ');
        if (pos == std::string::npos) {
            continue;
        }
        std::string udfName = s.substr(0, pos);
        std::string udfClass = s.substr(pos + 1);
        Trim(udfName);
        Trim(udfClass);
        std::transform(udfName.begin(), udfName.end(), udfName.begin(), ::tolower);
        hiveUdfMap.insert(std::make_pair(udfName, udfClass));
    }
    file.close();
}
}