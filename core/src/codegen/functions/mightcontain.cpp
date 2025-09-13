/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 * Description: registry  function  implementation
 */
#include "mightcontain.h"
#include "codegen/bloom_filter.h"

namespace omniruntime::codegen::function {
extern "C" DLLEXPORT bool MightContain(int64_t bloomFilterAddr, int64_t hashValue, bool isNull)
{
    /*
     * Limited by the current processing framework, bloomFilterAddr is set to null by the engine when the value of
     * bloomFilterAddr is 0.
     */
    if (!isNull) {
        auto bloomFilter = reinterpret_cast<omniruntime::op::BloomFilter *>(bloomFilterAddr);
        return bloomFilter->MightContainLong(hashValue);
    }
    return false;
}
}