/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

#ifndef __LIBCONFIG_H__
#define __LIBCONFIG_H__

#include <string>
const std::string G_LIB_PATH = "/opt/lib/";
const std::string IR_FOLDER = "ir/";
const std::string IR_SUFFIX = ".ll";

static std::string GenerateOperatorTemplatePath(std::string operatorName)
{
    return G_LIB_PATH + IR_FOLDER + operatorName + IR_SUFFIX;
}

#endif