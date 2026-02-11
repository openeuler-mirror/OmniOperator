/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Description: MakeTimestamp function - make_timestamp(year, month, day, hour, minute, second) -> timestamp
 */

#pragma once
#include <string>
#include "vectorization/VectorFunction.h"

namespace omniruntime::vectorization {
void RegisterMakeTimestampFunction(const std::string &name);
}
