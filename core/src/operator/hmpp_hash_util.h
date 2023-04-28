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
    template <type::DataTypeId typeId>
    static HmppResult ComputeHash(vec::BaseVector *vector, int64_t *combinedHash, int64_t start, int64_t rowCount)
    {
        int64_t *resultHash = new int64_t[rowCount]();
        int8_t *nullAddr = nullptr;
        if (vector->HasNull()) {
            nullAddr = (int8_t *)(vec::unsafe::UnsafeBaseVector::GetNulls(vector)) + start;
        }
        HmppResult result = ComputeHmppHash(typeId, vector, start, rowCount, resultHash, nullAddr);
        if (result == HMPP_STS_NULL_PTR_ERR) {
            delete[] resultHash;
            return result;
        }
        if (result != HMPP_STS_NO_ERR) {
            delete[] resultHash;
            return result;
        }
        result = HMPPS_CombineHash(combinedHash, resultHash, rowCount, combinedHash);
        delete[] resultHash;
        return result;
    }

private:
    static HmppResult ComputeHmppHash(type::DataTypeId typeId, vec::BaseVector *vector, int64_t start, int64_t rowCount,
        int64_t *resultHash, int8_t *nullAddr)
    {
        switch (typeId) {
            case OMNI_SHORT: {
                auto vectorValues = reinterpret_cast<int16_t *>(vec::VectorHelper::UnsafeGetValues(vector, OMNI_SHORT));
                return HMPPS_Hash_16s(reinterpret_cast<const int16_t *>(vectorValues) + start, rowCount, nullAddr,
                    resultHash);
            }
            case OMNI_LONG: {
                auto vectorValues = reinterpret_cast<int64_t *>(vec::VectorHelper::UnsafeGetValues(vector, OMNI_LONG));
                return HMPPS_Hash_64s(reinterpret_cast<const int64_t *>(vectorValues) + start, rowCount, nullAddr,
                    resultHash);
            }
            case OMNI_INT:
            case OMNI_DATE32: {
                auto vectorValues = reinterpret_cast<int32_t *>(vec::VectorHelper::UnsafeGetValues(vector, OMNI_INT));
                return HMPPS_Hash_32s(reinterpret_cast<const int32_t *>(vectorValues) + start, rowCount, nullAddr,
                    resultHash);
            }
            case OMNI_DOUBLE: {
                auto vectorValues = reinterpret_cast<double *>(vec::VectorHelper::UnsafeGetValues(vector, OMNI_DOUBLE));
                return HMPPS_Hash_64f(reinterpret_cast<const double *>(vectorValues) + start, rowCount, nullAddr,
                    resultHash);
            }
            case OMNI_BOOLEAN: {
                auto vectorValues =
                    reinterpret_cast<int8_t *>(vec::VectorHelper::UnsafeGetValues(vector, OMNI_BOOLEAN));
                return HMPPS_Hash_bool(reinterpret_cast<const bool *>(vectorValues) + start, rowCount, nullAddr,
                    resultHash);
            }
            case OMNI_DECIMAL64: {
                auto vectorValues =
                    reinterpret_cast<int64_t *>(vec::VectorHelper::UnsafeGetValues(vector, OMNI_DECIMAL64));
                return HMPPS_Hash_decimal64(reinterpret_cast<const int64_t *>(vectorValues) + start, rowCount, nullAddr,
                    resultHash);
            }
            default:
                LogWarn("Unsupported HMMPP TypeId  %d", OmniId);
                return HMPP_STS_NULL_PTR_ERR;
        }
    }
};
}
}
#endif // OMNI_RUNTIME_HMPP_HASH_UTIL_H