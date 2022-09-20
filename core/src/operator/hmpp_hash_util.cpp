/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * @Description: HMPP_hash_util implementations
 */
#include <HMPP/hmpp.h>
#include "hmpp_hash_util.h"

using namespace omniruntime::vec;
namespace omniruntime {
namespace op {
HmppResult HmppHashUtil::ComputeHash(Vector *vector, int64_t *combinedHash)
{
    auto rowCount = vector->GetSize();
    int32_t positionOffset = vector->GetPositionOffset();
    int64_t *resultHash = new int64_t[rowCount]();
    int8_t *nullAddr = nullptr;
    if (vector->MayHaveNull()) {
        nullAddr = (int8_t *)(vector->GetValueNulls()) + positionOffset;
    }
    HmppResult result;
    switch (vector->GetTypeId()) {
        case OMNI_SHORT:
            result = HMPPS_Hash_16s(reinterpret_cast<const int16_t *>(vector->GetValues()) + positionOffset, rowCount,
                nullAddr, resultHash);
            break;
        case OMNI_LONG:
            result = HMPPS_Hash_64s(reinterpret_cast<const int64_t *>(vector->GetValues()) + positionOffset, rowCount,
                nullAddr, resultHash);
            break;
        case OMNI_INT:
        case OMNI_DATE32:
            result = HMPPS_Hash_32s(reinterpret_cast<const int32_t *>(vector->GetValues()) + positionOffset, rowCount,
                nullAddr, resultHash);
            break;
        case OMNI_DOUBLE:
            result = HMPPS_Hash_64f(reinterpret_cast<const double *>(vector->GetValues()) + positionOffset, rowCount,
                nullAddr, resultHash);
            break;
        case OMNI_BOOLEAN:
            result = HMPPS_Hash_bool(reinterpret_cast<const bool *>(vector->GetValues()) + positionOffset, rowCount,
                nullAddr, resultHash);
            break;
        case OMNI_DECIMAL64:
            result = HMPPS_Hash_decimal64(reinterpret_cast<const int64_t *>(vector->GetValues()) + positionOffset,
                rowCount, nullAddr, resultHash);
            break;
        default:
            delete[] resultHash;
            LogWarn("Unsupported HMMPP TypeId  %d", vector->GetTypeId());
            return HMPP_STS_NULL_PTR_ERR;
    }
    if (result != HMPP_STS_NO_ERR) {
        return result;
    }
    result = HMPPS_CombineHash(combinedHash, resultHash, rowCount, combinedHash);
    delete[] resultHash;
    return result;
}
}
}
