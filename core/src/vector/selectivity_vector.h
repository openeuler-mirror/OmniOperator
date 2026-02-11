/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef SELECTIVITY_VECTOR_H
#define SELECTIVITY_VECTOR_H

#include <optional>
#include "util/bits_selectivity_vector.h"
#include "util/omni_exception.h"
#include "util/compiler_util.h"
#include "vector/vector.h"
#include "vector_helper.h"
#include "vector/vector_batch.h"
#include "vector/unsafe_vector.h"

namespace omniruntime::vec {
enum class GetSelectivityVectorMethod {
    AND = 0,
    OR = 1
};
class SelectivityVector {
public:
    SelectivityVector() {}

    explicit SelectivityVector(size_t length, bool allSelected = true)
    {
        bits.resize(omniruntime::Nwords(length), allSelected ? ~0ULL : 0);
        elementSize = length;
        begin = 0;
        end = allSelected ? elementSize : 0;
        this->allSelected = allSelected;
    }
    explicit SelectivityVector(uint8_t *nulls, size_t arrayLength, size_t bitNum)
    {
        bits.resize(omniruntime::Nwords(bitNum), 0);
        memcpy(bits.data(), nulls, sizeof(uint8_t) * arrayLength);
        elementSize = bitNum;
        begin = 0;
        end = elementSize;
        allSelected.reset();
    }

    void UpdateBounds()
    {
        begin = omniruntime::FindFirstBit(bits.data(), 0, elementSize);
        if (begin == -1) {
            begin = 0;
            end = 0;
            allSelected = false;
            return;
        }
        end = omniruntime::FindLastBit(bits.data(), begin, elementSize) + 1;
        allSelected.reset();
    }

    bool IsValid(size_t idx) const
    {
        // idx must less than elementSize.
        return omniruntime::IsBitSet(bits.data(), idx);
    }

    size_t BeginPos() const
    {
        return begin;
    }

    size_t EndPos() const
    {
        return end;
    }

    void SetAll()
    {
        omniruntime::FillBits(bits.data(), 0, elementSize, true);
        begin = 0;
        end = elementSize;
        allSelected = true;
    }

    size_t Size() const
    {
        return elementSize;
    }

    bool IsAllSelected()
    {
        if (allSelected.has_value()) {
            return allSelected.value();
        }
        allSelected = begin == 0 && end == elementSize && omniruntime::IsAllSet(bits.data(), 0, elementSize, true);
        return allSelected.value();
    }

    void Resize(int32_t size, bool value = true)
    {
        auto numWords = omniruntime::Nwords(size);
        // Set bits from size_ to end of the word.
        if (size > elementSize && !bits.empty()) {
            const auto start = elementSize % NUMBER_OF_BITS;
            if (start) {
                omniruntime::FillBits(&bits.back(), start, NUMBER_OF_BITS, value);
            }
        }

        bits.resize(numWords, value ? -1 : 0);
        elementSize = size;
        UpdateBounds();
    }

    void And(const SelectivityVector &other)
    {
        omniruntime::AndBits(bits.data(), other.bits.data(), begin, std::min(end, other.Size()));
        UpdateBounds();
    }

    void Or(const SelectivityVector &other)
    {
        if (elementSize < other.Size()) {
            Resize(other.Size(), false);
        }
        omniruntime::OrBits(bits.data(), other.bits.data(), 0, std::min(elementSize, other.Size()));
        UpdateBounds();
    }

    size_t CountSelected() const
    {
        if (allSelected.has_value() && *allSelected) {
            return Size();
        }
        auto count = CountBits(bits.data(), begin, end);
        allSelected = count == Size();
        return count;
    }

    void SetBit(size_t idx, bool flag)
    {
        if (UNLIKELY(idx >= elementSize)) {
            throw omniruntime::exception::OmniException("OPERATOR_RUNTIME_ERROR", "idx out of range, idx is: " +
                std::to_string(idx) + ", elementSize is: " + std::to_string(elementSize) + ".");
        }
        omniruntime::SetBit(bits.data(), idx, flag);
        // Some bit positions may be set to false, so allSelected needs to be reset,allSelected.has_value() is false.
        allSelected.reset();
    }

    uint64_t *GetBits()
    {
        return bits.data();
    }

    void ClearAll()
    {
        omniruntime::FillBits(bits.data(), 0, elementSize, false);
        begin = 0;
        end = 0;
        allSelected = false;
    }

    static SelectivityVector GetSelectivityVectorFromBaseVector(omniruntime::vec::BaseVector *baseVector);

    template <GetSelectivityVectorMethod type>
    static void GetSelectivityVectorFromVectorBatch(omniruntime::vec::VectorBatch *vectorBatch,
        const std::vector<size_t> &selectedColumns, SelectivityVector &selectivityVector);

    template <GetSelectivityVectorMethod type>
    static void GetSelectivityVectorFromVectorBatch(omniruntime::vec::VectorBatch *vectorBatch,
        SelectivityVector &selectivityVector);

    static bool GetFlatBaseVectorsFromSelectivityVector(std::vector<BaseVector *> &baseVectors,
        SelectivityVector &selectivityVector, std::vector<BaseVector *> &result);
    void Or(const SelectivityVector &a, const SelectivityVector &b)
    {
        this->Or(a);
        this->Or(b);
    }
    void And(const SelectivityVector &a, const SelectivityVector &b)
    {
        if (elementSize < a.Size()) {
            Resize(a.Size(), true);
        }
        this->And(a);
        this->And(b);
    }

    void Not()
    {
        Negate(reinterpret_cast<char *>(bits.data()), elementSize);
        UpdateBounds();
    }

private:
    std::vector<uint64_t> bits;
    size_t elementSize = 0;
    size_t begin = 0;
    size_t end = 0;

    mutable std::optional<bool> allSelected;
};
}


#endif // SELECTIVITY_VECTOR_H
