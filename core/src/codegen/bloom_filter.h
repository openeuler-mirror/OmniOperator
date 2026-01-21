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

    explicit BloomFilter(int32_t size, int32_t version);

    explicit BloomFilter(char *serialized, bool isRelease = false);

    void Serialize(char *serialized);

    void Merge(char *in);

    uint64_t GetSerializedSize();

    ~BloomFilter();

    bool PutLong(int64_t item);

    bool MightContainLong(int64_t item);

    int32_t GetNumHashFunctions();

    BitArray *GetBits()
    {
        return bits;
    }

    std::unique_ptr<mem::AlignedBuffer<int8_t>> WriteData(char *serialized);


    /**
     * Write a 32-bit integer into the specified buffer using big-endian byte order.
     *
     * @param val: 32-bit integer to be written.
     * @param buf: A pointer to the tartget buffer for storing the written bytes.
     */
    void WriteInt(int32_t val, int8_t *buf)
    {
        *buf = (val>>24) & 0xff;
        *(buf+1) = (val>>16) & 0xff;
        *(buf+2) = (val>>8) & 0xff;
        *(buf+3) = val & 0xff;
    }

    /**
     * Write a 64-bit integer into the specified buffer using big-endian byte order.
     *
     * @param val: 64-bit integer to be written.
     * @param buf: A pointer to the tartget buffer for storing the written bytes.
     */
    void WriteLong(int64_t val, int8_t *buf)
    {
        *buf = (val>>56) & 0xff;
        *(buf+1) = (val>>48) & 0xff;
        *(buf+2) = (val>>40) & 0xff;
        *(buf+3) = (val>>32) & 0xff;
        *(buf+4) = (val>>24) & 0xff;
        *(buf+5) = (val>>16) & 0xff;
        *(buf+6) = (val>>8) & 0xff;
        *(buf+7) = val & 0xff;
    }

private:
    // todo Most of the values of numHashFunctions calculated by Spark are 5 to 6. In this example, the value is 5 temporarily.
    int32_t numHashFunctions = 5;
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