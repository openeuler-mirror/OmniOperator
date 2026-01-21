/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 * Description: BloomFilter operator source file
 */

#include "bloom_filter.h"
#include "util/type_util.h"
#include "codegen/functions/murmur3_hash.h"

namespace omniruntime {
namespace op {
using namespace std;
using namespace omniruntime::codegen::function;
using namespace omniruntime::type;

/**
 * Build a BloomFilter object and store the input data into the BloomFilter structure after reversing the byte order.
 *
 * @param in: Pointer to input data.
 * @param versionJava: The version of the BloomFilter.
 */
BloomFilter::BloomFilter(int8_t *in, int32_t versionJava) : version(versionJava)
{
    // Flip the data endianness
    auto buf = WriteData(reinterpret_cast<char *>(in));

    int32_t versionIn = (reinterpret_cast<int32_t *>(buf->GetBuffer()))[0]; // version 4 Bytes
    if (version != versionIn) {
        throw omniruntime::exception::OmniException("ILLEGAL_INPUT", "wrong version for bloom filter");
    }

    numHashFunctions = (reinterpret_cast<int32_t *>(buf->GetBuffer()))[1]; // numHashFunctions 4 Bytes
    bits = new BitArray(buf->GetBuffer() + 8);                    // offset is 8 Bytes
}

/**
 * Build a BloomFilter object and initialize its internal BitArray based on the input size.
 *
 * @param size: Size of input data bits.
 * @param version: The version of the BloomFilter.
 */
BloomFilter::BloomFilter(int32_t size, int32_t version)
{
    bits = new BitArray(size);
    this->version = version;
}

/**
 * Build a BloomFilter object and initialize its internal BitArray based on the serialized data.
   This function will reverse the endianness of the input serialized data,
   then extract the BloomFilter`s version ,numHashFunctions, and the content of the BitArray.
 *
 * @param serialized: Pointer to the input serialized data, The structure of the data should be:
                      version(4 bytes) + numHashFunctions(4 bytes) + BitArray length(4 bytes) + BitArray content(length determined by the BitArray length).
 * @param isRelease: Release serialized or not.
 */
BloomFilter::BloomFilter(char *serialized, bool isRelease)
{
    // Flip the data endianness
    auto buf = WriteData(serialized);
    if (isRelease && serialized != nullptr) {
        delete[] serialized;
    }

    int offset = 0;
    // Get version
    version = *reinterpret_cast<int32_t *>(buf->GetBuffer() + offset);
    offset += sizeof(int32_t);

    // Get numHashFunctions
    numHashFunctions = *reinterpret_cast<int32_t *>(buf->GetBuffer() + offset);
    offset += sizeof(int32_t);

    // Get bits size
    int32_t size = *reinterpret_cast<int32_t *>(buf->GetBuffer() + offset);
    offset += sizeof(int32_t);
    bits = new BitArray(size);
    // Get bits datas by serialized
    auto bitsdata = reinterpret_cast<uint64_t *>(buf->GetBuffer() + offset);
    // Build bits
    for (auto i = 0; i < size; i++) {
        reinterpret_cast<uint64_t *>(bits->GetData())[i] = bitsdata[i];
    }
}

/**
 * Serialize the BloomFilter object into a byte stream.
   version(4 bytes) + numHashFunctions(4 bytes) + BitArray length(4 bytes) + BitArray content(length determined by the BitArray length).
 *
 * @param serialized: Pointer to the input serialized data.
 */
void BloomFilter::Serialize(char *serialized)
{
    int offset = 0;
    // Write version in reverse order into serialized
    WriteInt(version, reinterpret_cast<int8_t *>(serialized + offset));
    offset += sizeof(int32_t);

    // Write numHashFunctions in reverse order into serialized
    WriteInt(numHashFunctions, reinterpret_cast<int8_t *>(serialized + offset));
    offset += sizeof(int32_t);

    // Write wordsNum in reverse order into serialized
    WriteInt(bits->GetWordsNum(), reinterpret_cast<int8_t *>(serialized + offset));
    offset += sizeof(int32_t);

    // Write the datas in reverse order into serialized
    auto data = reinterpret_cast<uint64_t *>(bits->GetData());
    for (auto i = 0; i < bits->GetWordsNum(); i++) {
        WriteLong(data[i], reinterpret_cast<int8_t *>(serialized + offset));
        offset += sizeof(uint64_t);
    }
}

/**
 * Merge the serialized data of another BloomFIlter object into the current BloomFilter object.
 *
 * @param serialized: Pointer to the input serialized data.
 * @exception: If the version, numHashFunctions, or BitArray length of the input BloomFIlter object does not match that of the current object, an OmniException will be thrown.
 */
void BloomFilter::Merge(char *serialized)
{
    // Get the positive sequence data
    auto buf = WriteData(serialized);

    int32_t offset = 0;
    if (*(reinterpret_cast<int32_t *>(buf->GetBuffer() + offset)) != version) {
        throw omniruntime::exception::OmniException("ILLEGAL_INPUT", "BloomFilter version must be the same");
    }
    offset += sizeof(int32_t);

    if (*(reinterpret_cast<int32_t *>(buf->GetBuffer() + offset)) != numHashFunctions) {
        throw omniruntime::exception::OmniException("ILLEGAL_INPUT", "BloomFilter numHashFunctions must be the same");
    }
    offset += sizeof(int32_t);

    if (*(reinterpret_cast<int32_t *>(buf->GetBuffer() + offset)) != bits->GetWordsNum()) {
        throw omniruntime::exception::OmniException("ILLEGAL_INPUT", "BloomFilter numHashFunctions must be the same");
    }
    offset += sizeof(int32_t);

    auto bitsdata = reinterpret_cast<const uint64_t *>(buf->GetBuffer() + offset);
    bits->Merge(bitsdata);
}

/**
 * Reverse the endianness of the input serialized data and return an AlignedBuffer object containing the data in the original order.
   This function extracts the BloomFIlter`s version, numHashFunctions, the length of the BigArray,
   and the content of the BitArray form the input serialized data, and vonoverts them into a properly orderd storage format.
 *
 * @param serialized: Pointer to the input serialized data.
                      version(4 bytes) + numHashFunctions(4 bytes) + BitArray length(4 bytes) + BitArray content(length determined by the BitArray length).
 * @return: Return a std::unique_ptr<mem::AlignedBuffer<int8_t>> object containing the serialized data in forward order.
 * @example: Version reverse order:                        16777216 -> 0000 0001 0000 0000 0000 0000 0000 0000
 *           Version positive order:                              1 <- 0000 0000 0000 0000 0000 0000 0000 0001
 *           numHashFunctions reverse order:               83886080 -> 0000 0101 0000 0000 0000 0000 0000 0000
 *           numHashFunctions positive order:                     6 <- 0000 0000 0000 0000 0000 0000 0000 0101
 *           numWords reverse order:                         262144 -> 0000 0000 0000 0100 0000 0000 0000 0000
 *           numWords positive order:                          1024 <- 0000 0000 0000 0000 0000 0100 0000 0000
 */
std::unique_ptr<mem::AlignedBuffer<int8_t>> BloomFilter::WriteData(char *serialized)
{
    int32_t offset = 0;

    // Get the reversed versionIn
    int32_t versionIn = *reinterpret_cast<int32_t *>(serialized + offset);
    offset += sizeof(int32_t);

    // Get the reversed numHashFunctionsIn
    int32_t numHashFunctionsIn = *reinterpret_cast<int32_t *>(serialized + offset);
    offset += sizeof(int32_t);

    // Get the reversed wordsNumIn
    int32_t wordsNumIn = *reinterpret_cast<int32_t *>(serialized + offset);
    offset += sizeof(int32_t);

    // reverse wordsNum to obtain the correct length for constructing the buffer int the correct order.
    std::unique_ptr<mem::AlignedBuffer<int8_t>> intBuf = std::make_unique<mem::AlignedBuffer<int8_t>>(sizeof(int32_t));
    WriteInt(wordsNumIn, intBuf->GetBuffer());
    int32_t wordsNum = *reinterpret_cast<int32_t *>(intBuf->GetBuffer());

    // Build a positive sequence buffer
    int32_t size = offset + sizeof(uint64_t) * wordsNum;
    std::unique_ptr<mem::AlignedBuffer<int8_t>> buf = std::make_unique<mem::AlignedBuffer<int8_t>>(size);

    // reset offset
    offset = 0;
    // Write version into the buffer.
    WriteInt(versionIn, buf->GetBuffer() + offset);
    offset += sizeof(int32_t);
    // Write numHashFunctions into the buffer.
    WriteInt(numHashFunctionsIn, buf->GetBuffer() + offset);
    offset += sizeof(int32_t);
    // Write wordsNum into the buffer.
    WriteInt(wordsNumIn, buf->GetBuffer() + offset);
    offset += sizeof(int32_t);

    // Write data into the buffer.
    auto data = reinterpret_cast<uint64_t *>(serialized + offset);
    for (auto i = 0; i < wordsNum; i++) {
        WriteLong(data[i], buf->GetBuffer() + offset);
        offset += sizeof(uint64_t);
    }

    return buf;
}

/**
 * Calculate the size of the BloomFilter object after serialization.
   version(4 bytes) + numHashFunctions(4 bytes) + BitArray length(4 bytes) + BitArray content(length determined by the BitArray length).
 *
 * @return: Return the total number of bytes after the BloomFilter object is serialized.
 */
uint64_t BloomFilter::GetSerializedSize()
{
    return 12 + bits->GetWordsNum() * 8;
}

BloomFilter::~BloomFilter()
{
    delete bits;
}

/*
 * @Func : put long data into BloomFilter struct
 * @param item : long type data
 * @return : Returns true if the bit slot is reversed after insertion, Otherwise, false is returned.
 */
bool BloomFilter::PutLong(int64_t item)
{
    if (version == 1) {
        int32_t h1 = Mm3Int64(item, false, 0, false);
        int32_t h2 = Mm3Int64(item, false, h1, false);

        uint64_t bitSize = bits->GetBitSize();
        bool bitsChanged = false;
        for (int32_t i = 1; i <= numHashFunctions; i++) {
            int32_t combinedHash = h1 + (i * h2);
            // Flip all the bits if it's negative (guaranteed positive number)
            if (combinedHash < 0) {
                combinedHash = ~combinedHash;
            }
            bitsChanged |= bits->Set(combinedHash % bitSize);
        }
        return bitsChanged;
    } else {
        throw omniruntime::exception::OmniException("ILLEGAL_INPUT", "BloomFilter only support version1(version == 1) of PutLong.");
    }
}

/*
 * @Func : Check whether a item is in the filter.
 * @param item : long type data
 * @return : Returns true if the item in the filter, Otherwise, false is returned.
 */
bool BloomFilter::MightContainLong(int64_t item)
{
    if (version == 1) {
        int32_t h1 = Mm3Int64(item, false, 0, false);
        int32_t h2 = Mm3Int64(item, false, h1, false);

        uint64_t bitSize = bits->GetBitSize();
        for (int32_t i = 1; i <= numHashFunctions; i++) {
            int32_t combinedHash = h1 + (i * h2);
            // Flip all the bits if it's negative (guaranteed positive number)
            if (combinedHash < 0) {
                combinedHash = ~combinedHash;
            }
            if (!bits->Get(combinedHash % bitSize)) {
                return false;
            }
        }
        return true;
    } else {
        throw omniruntime::exception::OmniException("ILLEGAL_INPUT", "BloomFilter only support version1(version == 1) of MightContainLong.");
    }
}

int32_t BloomFilter::GetNumHashFunctions()
{
    return numHashFunctions;
}

// function implements for class BloomFilterOperatorFactory
BloomFilterOperatorFactory::BloomFilterOperatorFactory(int32_t version) : version(version) {}

BloomFilterOperatorFactory::~BloomFilterOperatorFactory() = default;

BloomFilterOperatorFactory *BloomFilterOperatorFactory::CreateBloomFilterOperatorFactory(int32_t version)
{
    auto pOperatorFactory = new BloomFilterOperatorFactory(version);
    return pOperatorFactory;
}

Operator *BloomFilterOperatorFactory::CreateOperator()
{
    auto pSortOperator = new BloomFilterOperator(version);
    return pSortOperator;
}

// function implements for class BloomFilterOperator
BloomFilterOperator::BloomFilterOperator(int32_t version) : version(version) {}

BloomFilterOperator::~BloomFilterOperator()
{
    VectorHelper::FreeVecBatch(inputVecBatch);
    delete bloomFilterAddress;
}

int32_t BloomFilterOperator::AddInput(VectorBatch *vecBatch)
{
    if (vecBatch == nullptr) {
        throw omniruntime::exception::OmniException("ILLEGAL_INPUT", "BloomFilterOperator AddInput can't be nullptr!");
    }

    inputVecBatch = vecBatch;
    int32_t vectorCount = inputVecBatch->GetVectorCount();
    if (vectorCount != 1) {
        throw omniruntime::exception::OmniException("ILLEGAL_INPUT", "vecBatch col should be 1 for bloom filter");
    }
    BaseVector *colVec = inputVecBatch->Get(0);
    auto valuesAddress = reinterpret_cast<int64_t>(VectorHelper::UnsafeGetValues(colVec));

    // init BloomFilter
    bloomFilterAddress = new BloomFilter(reinterpret_cast<int8_t *>((uintptr_t)valuesAddress), version);
    return 0;
}

int32_t BloomFilterOperator::GetOutput(VectorBatch **blOutPut)
{
    auto outPut = new VectorBatch(1);
    auto *col = new Vector<int64_t>(1);
    col->SetValue(0, reinterpret_cast<int64_t>(bloomFilterAddress));
    outPut->Append(col);
    *blOutPut = outPut;
    SetStatus(OMNI_STATUS_FINISHED);
    return 0;
}

OmniStatus BloomFilterOperator::Close()
{
    return OMNI_STATUS_NORMAL;
}
} // end of op
} // end of omniruntime