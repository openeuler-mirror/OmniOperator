/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * @Description: HMPP_hash_util implementations
 */

#ifndef OMNI_RUNTIME_HMPP_HASH_UTIL_H
#define OMNI_RUNTIME_HMPP_HASH_UTIL_H

#include <HMPP/hmpp.h>
#include "vector/vector_common.h"

using namespace omniruntime::vec;
namespace omniruntime {
namespace op {
class HmppHashUtil {
public:
    static HmppResult ComputeHash(Vector *vector, int64_t *combinedHash);
};
}
}
#endif // OMNI_RUNTIME_HMPP_HASH_UTIL_H