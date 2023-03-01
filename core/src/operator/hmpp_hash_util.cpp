/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * @Description: HMPP_hash_util implementations
 */
#include <HMPP/hmpp.h>
#include "operator/aggregation/vector_getter.h"
#include "hmpp_hash_util.h"

namespace omniruntime {
namespace op {
HmppResult HmppHashUtil::ComputeHash(vec::BaseVector *vector, type::DataTypeId omniId, int64_t *combinedHash,
                                     int64_t start, int64_t rowCount)
{
    int64_t *resultHash = new int64_t[rowCount]();
    int8_t *nullAddr = nullptr;
    if (vector->HasNull()) {
        nullAddr = (int8_t *)(vec::unsafe::UnsafeBaseVector::GetNulls(vector)) + start;
    }
    HmppResult result;
    switch (omniId) {
        case OMNI_SHORT: {
            auto vectorValues = reinterpret_cast<int16_t *>(GetValuesFromVector<int16_t>(vector));
            result =
                HMPPS_Hash_16s(reinterpret_cast<const int16_t *>(vectorValues) + start, rowCount, nullAddr, resultHash);
            break;
        }
        case OMNI_LONG: {
            auto vectorValues = reinterpret_cast<int64_t *>(GetValuesFromVector<int64_t>(vector));
            result =
                HMPPS_Hash_64s(reinterpret_cast<const int64_t *>(vectorValues) + start, rowCount, nullAddr, resultHash);
            break;
        }
        case OMNI_INT:
        case OMNI_DATE32: {
            auto vectorValues = reinterpret_cast<int32_t *>(GetValuesFromVector<int32_t>(vector));
            result =
                HMPPS_Hash_32s(reinterpret_cast<const int32_t *>(vectorValues) + start, rowCount, nullAddr, resultHash);
            break;
        }
        case OMNI_DOUBLE: {
            auto vectorValues = reinterpret_cast<double *>(GetValuesFromVector<double>(vector));
            result =
                HMPPS_Hash_64f(reinterpret_cast<const double *>(vectorValues) + start, rowCount, nullAddr, resultHash);
            break;
        }
        case OMNI_BOOLEAN: {
            auto vectorValues = reinterpret_cast<int8_t *>(GetValuesFromVector<int8_t>(vector));
            result =
                HMPPS_Hash_bool(reinterpret_cast<const bool *>(vectorValues) + start, rowCount, nullAddr, resultHash);
            break;
        }
        case OMNI_DECIMAL64: {
            auto vectorValues = reinterpret_cast<int64_t *>(GetValuesFromVector<int64_t>(vector));
            result = HMPPS_Hash_decimal64(reinterpret_cast<const int64_t *>(vectorValues) + start, rowCount, nullAddr,
                resultHash);
            break;
        }
        default:
            delete[] resultHash;
            LogWarn("Unsupported HMMPP TypeId  %d", omniId);
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
