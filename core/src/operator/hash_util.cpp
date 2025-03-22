/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * @Description: hash util implementations
 */

#include "hash_util.h"
#include <cmath>
#include "operator/util/mm3_util.h"

namespace omniruntime {
namespace op {

uint64_t HashUtil::NextPowerOfTwo(uint64_t x)
{
    if (x == 0) {
        return 1;
    } else {
        --x;
        x |= x >> ROTATE_DISTANCE_1;
        x |= x >> ROTATE_DISTANCE_2;
        x |= x >> ROTATE_DISTANCE_4;
        x |= x >> ROTATE_DISTANCE_8;
        x |= x >> ROTATE_DISTANCE_16;
        return (x | (x >> ROTATE_DISTANCE_32)) + 1;
    }
}

uint32_t HashUtil::HashArraySize(uint32_t expected, float f)
{
    double result = static_cast<double>(expected) / static_cast<double>(f);
    auto s = static_cast<uint64_t>(std::ceil(result));
    s = NextPowerOfTwo(s);
    if (s > MAX_ARRAY_SIZE) {
        return expected;
    } else {
        return static_cast<uint32_t>(s);
    }
}

std::unique_ptr<omniruntime::vec::Vector<int32_t>> HashUtil::ComputePartitionIds(
    std::vector<omniruntime::vec::BaseVector *> &vecs, int32_t partitionNum, int32_t rowCount)
{
    auto colCount = vecs.size();
    std::vector<uint32_t> partitionIds(rowCount, MM3HASH_SEED);
    for (auto col = 0; col < colCount; col++) {
        switch (vecs[col]->GetTypeId()) {
            case type::OMNI_INT:
            case type::OMNI_DATE32:
                Mm3Int(vecs[col], rowCount, partitionIds);
                break;
            case type::OMNI_LONG:
            case type::OMNI_TIMESTAMP:
            case type::OMNI_DECIMAL64:
            case type::OMNI_DOUBLE:
                Mm3Long(vecs[col], rowCount, partitionIds);
                break;
            case type::OMNI_CHAR:
            case type::OMNI_VARCHAR:
                Mm3String(vecs[col], rowCount, partitionIds);
                break;
            case type::OMNI_DECIMAL128:
                Mm3Decimal128(vecs[col], rowCount, partitionIds);
                break;
            case type::OMNI_BOOLEAN:
                Mm3Boolean(vecs[col], rowCount, partitionIds);
                break;
            default:
                std::string omniExceptionInfo =
                    "Error in shuffle hash, not support type: " +
                    std::to_string(vecs[col]->GetTypeId());
                throw omniruntime::exception::OmniException("UNSUPPORTED_ERROR", omniExceptionInfo);
        }
    }
    auto ret = std::make_unique<omniruntime::vec::Vector<int>>(rowCount, type::OMNI_INT);
    int32_t row = 0;
    for (; row + PTR_STEP_4 <= rowCount; row += PTR_STEP_4) {
        ret->SetValue(row, partitionIds[row] % partitionNum);
        ret->SetValue(row + PTR_STEP_1, partitionIds[row + PTR_STEP_1] % partitionNum);
        ret->SetValue(row + PTR_STEP_2, partitionIds[row + PTR_STEP_2] % partitionNum);
        ret->SetValue(row + PTR_STEP_3, partitionIds[row + PTR_STEP_3] % partitionNum);
    }
    for (; row < rowCount; row++) {
        ret->SetValue(row, partitionIds[row] % partitionNum);
    }
    return ret;
}

}
}
