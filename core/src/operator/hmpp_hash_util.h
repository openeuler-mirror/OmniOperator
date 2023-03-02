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
using namespace omniruntime::type;
class HmppHashUtil {
public:
    template <type::DataTypeId OmniId>
    static HmppResult ComputeHash(vec::BaseVector *vector, int64_t *combinedHash, int64_t start, int64_t rowCount)
    {
        int64_t *resultHash = new int64_t[rowCount]();
        int8_t *nullAddr = nullptr;
        if (vector->HasNull()) {
            nullAddr = (int8_t *)(vec::unsafe::UnsafeBaseVector::GetNulls(vector)) + start;
        }
        HmppResult result;
        switch (OmniId) {
            case OMNI_SHORT: {
                auto vectorValues = reinterpret_cast<int16_t *>(vec::VectorHelper::GetValues(vector, OMNI_SHORT));
                result = HMPPS_Hash_16s(reinterpret_cast<const int16_t *>(vectorValues) + start, rowCount, nullAddr,
                    resultHash);
                break;
            }
            case OMNI_LONG: {
                auto vectorValues = reinterpret_cast<int64_t *>(vec::VectorHelper::GetValues(vector, OMNI_LONG));
                result = HMPPS_Hash_64s(reinterpret_cast<const int64_t *>(vectorValues) + start, rowCount, nullAddr,
                    resultHash);
                break;
            }
            case OMNI_INT:
            case OMNI_DATE32: {
                auto vectorValues = reinterpret_cast<int32_t *>(vec::VectorHelper::GetValues(vector, OMNI_INT));
                result = HMPPS_Hash_32s(reinterpret_cast<const int32_t *>(vectorValues) + start, rowCount, nullAddr,
                    resultHash);
                break;
            }
            case OMNI_DOUBLE: {
                auto vectorValues = reinterpret_cast<double *>(vec::VectorHelper::GetValues(vector, OMNI_DOUBLE));
                result = HMPPS_Hash_64f(reinterpret_cast<const double *>(vectorValues) + start, rowCount, nullAddr,
                    resultHash);
                break;
            }
            case OMNI_BOOLEAN: {
                auto vectorValues = reinterpret_cast<int8_t *>(vec::VectorHelper::GetValues(vector, OMNI_BOOLEAN));
                result = HMPPS_Hash_bool(reinterpret_cast<const bool *>(vectorValues) + start, rowCount, nullAddr,
                    resultHash);
                break;
            }
            case OMNI_DECIMAL64: {
                auto vectorValues = reinterpret_cast<int64_t *>(vec::VectorHelper::GetValues(vector, OMNI_DECIMAL64));
                result = HMPPS_Hash_decimal64(reinterpret_cast<const int64_t *>(vectorValues) + start, rowCount,
                    nullAddr, resultHash);
                break;
            }
            default:
                delete[] resultHash;
                LogWarn("Unsupported HMMPP TypeId  %d", OmniId);
                return HMPP_STS_NULL_PTR_ERR;
        }
        if (result != HMPP_STS_NO_ERR) {
            return result;
        }
        result = HMPPS_CombineHash(combinedHash, resultHash, rowCount, combinedHash);
        delete[] resultHash;
        return result;
    }
};
}
}
#endif // OMNI_RUNTIME_HMPP_HASH_UTIL_H