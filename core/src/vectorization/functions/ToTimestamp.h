/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: ToTimestamp and ToUnixTimestamp functions for expression system
 */

#pragma once
#include <string>
#include "vectorization/VectorFunction.h"

namespace omniruntime::vectorization {
void RegisterToTimestampFunction(const std::string &name);
void RegisterToUnixTimestampFunction(const std::string &name);
}
