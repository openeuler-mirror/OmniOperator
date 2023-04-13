/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 * Description: BloomFilter operator header
 */
#ifndef OMNI_RUNTIME_BLOOM_FILTER_H
#define OMNI_RUNTIME_BLOOM_FILTER_H

#include "util/bit_array.h"
#include "operator/operator_factory.h"

namespace omniruntime {
namespace op {
class BloomFilter {
public:
    explicit BloomFilter(int8_t *in, int32_t versionJava);

    ~BloomFilter();

    bool PutLong(int64_t item);

    bool MightContainLong(int64_t item);

    int32_t GetNumHashFunctions();

    BitArray *GetBits()
    {
        return bits;
    }

private:
    int32_t numHashFunctions;
    BitArray *bits;
    int32_t version;
};

class BloomFilterOperatorFactory : public OperatorFactory {
public:
    explicit BloomFilterOperatorFactory(int32_t version);

    ~BloomFilterOperatorFactory() override;

    static BloomFilterOperatorFactory *CreateBloomFilterOperatorFactory(int32_t version);

    Operator *CreateOperator() override;

private:
    int32_t version;
};

/*
 * BloomFilterOperator is only used by spark runtimeFilter Feature, It is used independently and cannot cooperate with
 * other operators. AddInput must be IntVector GetOutput must be LongVector
 */
class BloomFilterOperator : public Operator {
public:
    explicit BloomFilterOperator(int32_t version);

    ~BloomFilterOperator() override;

    int32_t AddInput(omniruntime::vec::VectorBatch *vecBatch) override;

    int32_t GetOutput(omniruntime::vec::VectorBatch **blOutPut) override;

    OmniStatus Close() override;

private:
    int32_t version;
    BloomFilter *bloomFilterAddress;
    VectorBatch *inputVecBatch;
};
} // end of op
} // end of omniruntime
#endif // OMNI_RUNTIME_BLOOM_FILTER_H