/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

#ifndef __LIBCONFIG_H__
#define __LIBCONFIG_H__

#include <string>
const std::string DEFAULT_LIB_PATH = "/opt/lib/";
const std::string DEFAULT_FILE_PATH = "/etc/";
const std::string IR_FOLDER = "ir/";
const std::string IR_SUFFIX = ".ll";

static std::string GetLibPath()
{
    auto omniHome = std::getenv("OMNI_HOME");
    if (omniHome) {
        return std::string(omniHome) + "/lib/";
    } else {
        return DEFAULT_LIB_PATH;
    }
}

static std::string GetConfPath()
{
    auto omniHome = std::getenv("OMNI_HOME");
    if (omniHome) {
        return std::string(omniHome) + "/conf/";
    } else {
        return DEFAULT_FILE_PATH;
    }
}

static std::string GenerateOperatorTemplatePath(std::string operatorName)
{
    return GetLibPath() + IR_FOLDER + operatorName + IR_SUFFIX;
}

#endif