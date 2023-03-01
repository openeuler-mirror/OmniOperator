/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * @Description: HMPP_hash_util implementations
 */

#ifndef OMNI_RUNTIME_HMPP_HASH_UTIL_H
#define OMNI_RUNTIME_HMPP_HASH_UTIL_H

#include <HMPP/hmpp.h>
#include "vector/vector_helper.h"

namespace omniruntime {
namespace op {
class HmppHashUtil {
public:
    static HmppResult ComputeHash(vec::BaseVector *vector, type::DataTypeId omniId, int64_t *combinedHash,
                                  int64_t start, int64_t rowCount);
};
}
}
#endif // OMNI_RUNTIME_HMPP_HASH_UTIL_H