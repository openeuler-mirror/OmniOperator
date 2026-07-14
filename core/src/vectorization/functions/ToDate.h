/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Description: ToDate function for expression system
 */

#pragma once
#include <string>
#include "vectorization/VectorFunction.h"

namespace omniruntime::vectorization {
/// to_date(string, format) -> DATE32
/// Parses a date string with the specified (Java/SimpleDateFormat-style) format and
/// returns the date as days-since-epoch. Mirrors the vectorized get_timestamp parsing,
/// but converts the parsed UTC micros to a DATE32 value instead of a TIMESTAMP.
void RegisterToDateFunction(const std::string &name);
}
