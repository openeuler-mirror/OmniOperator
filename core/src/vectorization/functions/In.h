/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: In expression implementation for SimpleFunction framework (无空值版)
 */

#pragma once

namespace omniruntime::vectorization {
// Supported types:
//   - Bools
//   - Integer types (byte, short, int, long)
//   - String, Binary
//   - Float, Double
//   - Timestamp
//   - Date
//
// Unsupported:
//   - Decimal
//   - Datetime
//   - Structs, Arrays
//   - Maps

void registerIn(const std::string &prefix);
}
