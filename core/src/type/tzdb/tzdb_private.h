//===----------------------------------------------------------------------===//
//
// Part of the LLVM Project, under the Apache License v2.0 with LLVM Exceptions.
// See https://llvm.org/LICENSE.txt for license information.
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
//
//===----------------------------------------------------------------------===//

// For information see https://libcxx.llvm.org/DesignDocs/TimeZone.html

#pragma once

#include "types_private.h"
#include "tzdb.h"

namespace omniruntime::tzdb {

void __init_tzdb(tzdb& __tzdb, __rules_storage_type& __rules);

} // namespace facebook::velox::tzdb
