/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 */

#include <string>
namespace omniruntime {
namespace LibConfig {
static const std::string DEFAULT_LIB_PATH =
    "/opt/lib/"; // ideally should be changed to something similar to ~/omni_home/lib
static const std::string IR_FOLDER = "ir/";
static const std::string IR_SUFFIX = ".ll";

static std::string TransEnv(const char *srcEnv)
{
    return std::string { srcEnv };
}

std::string GetLibPath()
{
    auto omniHome = std::getenv("OMNI_HOME");
    if (omniHome == nullptr) {
        return DEFAULT_LIB_PATH;
    }
    return TransEnv(omniHome) + "/lib/";
}

std::string GenerateOperatorTemplatePath(const std::string &operatorName)
{
    return GetLibPath() + IR_FOLDER + operatorName + IR_SUFFIX;
}
}
}
