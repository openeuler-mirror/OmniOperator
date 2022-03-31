/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

#ifndef __LIBCONFIG_H__
#define __LIBCONFIG_H__

#include <string>
namespace omniruntime {
namespace LibConfig {
std::string GetLibPath();
std::string GenerateOperatorTemplatePath(const std::string &operatorName);
}
}

#endif