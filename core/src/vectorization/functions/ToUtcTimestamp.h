/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: ToUtcTimestamp and FromUtcTimestamp functions for expression system
 */

#pragma once
#include <string>
#include "vectorization/VectorFunction.h"

namespace omniruntime::vectorization {
void RegisterToUtcTimestampFunction(const std::string &name);
void RegisterFromUtcTimestampFunction(const std::string &name);
}
