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
                auto values = unsafe::UnsafeVector::GetRawValues(static_cast<Vector<int16_t> *>(vector));
                return HMPPS_Hash_16s(values + start, rowCount, nullAddr, resultHash);
            }
            case OMNI_LONG: {
                auto values = unsafe::UnsafeVector::GetRawValues(static_cast<Vector<int64_t> *>(vector));
                return HMPPS_Hash_64s(values + start, rowCount, nullAddr, resultHash);
            }
            case OMNI_INT:
            case OMNI_DATE32: {
                auto values = unsafe::UnsafeVector::GetRawValues(static_cast<Vector<int32_t> *>(vector));
                return HMPPS_Hash_32s(values + start, rowCount, nullAddr, resultHash);
            }
            case OMNI_DOUBLE: {
                auto values = unsafe::UnsafeVector::GetRawValues(static_cast<Vector<double> *>(vector));
                return HMPPS_Hash_64f(values + start, rowCount, nullAddr, resultHash);
            }
            case OMNI_BOOLEAN: {
                auto values = unsafe::UnsafeVector::GetRawValues(static_cast<Vector<bool> *>(vector));
                return HMPPS_Hash_bool(values + start, rowCount, nullAddr, resultHash);
            }
            case OMNI_DECIMAL64: {
                auto values = unsafe::UnsafeVector::GetRawValues(static_cast<Vector<int64_t> *>(vector));
                return HMPPS_Hash_decimal64(values + start, rowCount, nullAddr, resultHash);
            }
            default:
                LogWarn("Unsupported HMPP TypeId %d", typeId);
                return HMPP_STS_NULL_PTR_ERR;
        }
    }
};
}
}
#endif // OMNI_RUNTIME_HMPP_HASH_UTIL_H