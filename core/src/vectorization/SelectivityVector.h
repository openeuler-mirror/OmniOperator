/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: visitor class for expressions
 */

#pragma once
#include <cstring>
#include <optional>
#include <vector>
#include <string>
#include <securec.h>
#include "util/bit_util.h"

namespace omniruntime {
using vector_size_t = int32_t;
using ByteCount = int32_t;

/**
 * useage of SelectivityVector
 * there are two exmpale of SelectivityVector for vectors or vector with const comparison
 * the usage is also explain in class EqualStringFunction.h
 *
 * 1. vectors comparison
 * auto leftVector = static_cast<Vector<LargeStringContainer<std::string_view>> *>(leftArg);
 * auto rightVector = static_cast<Vector<LargeStringContainer<std::string_view>> *>(rightArg);
 * auto leftSelectivity = std::make_shared<SelectivityVector>(rowSize);
 * const auto leftNullBits = reinterpret_cast<uint64_t *>(unsafe::UnsafeBaseVector::GetNulls(leftVector));
 * leftSelectivity->setFromBitsNegate(leftNullBits, rowSize); // bits is used identify which row is null or not
 * auto rightSelectivity = std::make_shared<SelectivityVector>(rowSize);
 * const auto rightNullBits = reinterpret_cast<uint64_t *>(unsafe::UnsafeBaseVector::GetNulls(leftVector));
 * leftSelectivity->setFromBitsNegate(rightNullBits, rowSize);
 * leftSelectivity->intersect(*rightSelectivity); // acquire the not null bits which will compare laterly using intersect method
 * leftSelectivity->applyToSelected([&](vector_size_t i) {
 *                   comparedResult->SetValue(i, leftVector->GetValue(i) == rightVector->GetValue(i));
 *              }); // the compared reuslt is stored in comparedResult
 *
 * 2. vector and constant
 * auto leftVector = static_cast<Vector<DictionaryContainer<std::string_view>> *>(leftArg);
 * auto leftSelectivity = std::make_shared<SelectivityVector>(rowSize);
 * const auto leftNullBits = reinterpret_cast<uint64_t *>(unsafe::UnsafeBaseVector::GetNulls(leftVector));
 * leftSelectivity->setFromBitsNegate(leftNullBits, rowSize);
 * leftSelectivity->applyToSelected([&](vector_size_t i) {
 *                   comparedResult->SetValue(i, leftVector->GetValue(i) == constant);
 *              });
 */
class SelectivityVector {
public:
    SelectivityVector() {}

    explicit SelectivityVector(vector_size_t length, bool allSelected = true)
    {
        bits_.resize(BitUtil::Nwords(length), allSelected ? ~0ULL : 0);
        size_ = length;
        begin_ = 0;
        end_ = allSelected ? size_ : 0;
        allSelected_ = allSelected;
    }

    // Returns a statically allocated reference to an empty selectivity vector
    // (size zero).
    static const SelectivityVector &empty();

    // Returns a new allocated selectivity vector of size `size`, where all bits
    // are set to false.
    static SelectivityVector empty(vector_size_t size);

    /// Return a summary of all selected rows and row numbers of the first few
    /// selected rows. To be used for debugging.
    /// @param maxSelectedRowsToPrint Maximum number of the first selected rows to
    /// include in the output.
    std::string toString(vector_size_t maxSelectedRowsToPrint = 10) const;

    /// Resizes the vector to new size and sets the new bits with value `value`.
    void resize(int32_t size, bool value = true)
    {
        auto numWords = BitUtil::Nwords(size);
        // Set bits from size_ to end of the word.
        if (size > size_ && !bits_.empty()) {
            if (const auto start = size_ % 64) {
                BitUtil::FillBits(&bits_.back(), start, 64, value);
            }
        }

        bits_.resize(numWords, value ? -1 : 0);
        size_ = size;

        updateBounds();
    }

    /// Resizes the vector to new size and sets all bits to `value`.
    void resizeFill(int32_t size, bool value = true)
    {
        auto numWords = BitUtil::Nwords(size);
        bits_.resize(numWords);
        std::fill(bits_.begin(), bits_.end(), value ? ~0LL : 0);
        size_ = size;
        begin_ = 0;
        end_ = value ? size_ : 0;
        allSelected_ = value;
    }

    /**
     * Set whether given index is selected. updateBounds() need to be called
     * explicitly after setValid() call, it can be called only once after multiple
     * setValid() calls in a row.
     */
    void setValid(vector_size_t idx, bool valid)
    {
        BitUtil::SetBit(bits_.data(), idx, valid);
        allSelected_.reset();
    }

    /**
     * If range is not empty, set a range of values to valid from [start, end).
     * updateBounds() need to be called explicitly after setValidRange() call, it
     * can be called only once after multiple setValidRange() calls in a row.
     */
    void setValidRange(vector_size_t begin, vector_size_t end, bool valid)
    {
        if (begin == end) {
            return;
        }
        BitUtil::FillBits(bits_.data(), begin, end, valid);
        allSelected_.reset();
    }

    const uint64_t *allBits() const
    {
        return bits_.data();
    }

    vector_size_t begin() const
    {
        return begin_;
    }

    vector_size_t end() const
    {
        return end_;
    }

    /**
     * @return true if the vector has anything selected, false otherwise
     */
    bool hasSelections() const
    {
        return begin_ < end_;
    }

    /**
     * Sets the vector to all not selected.
     */
    void clearAll()
    {
        BitUtil::FillBits(bits_.data(), 0, size_, false);
        begin_ = 0;
        end_ = 0;
    }

    /**
     * Sets the vector to all selected.
     */
    void setAll()
    {
        BitUtil::FillBits(bits_.data(), 0, size_, true);
        begin_ = 0;
        end_ = size_;
        allSelected_ = true;
    }

    void setFromBits(const uint64_t *bits, int32_t size)
    {
        auto numWords = BitUtil::Nwords(size);
        if (numWords > bits_.size()) {
            bits_.resize(numWords);
        }
        memcpy_s(bits_.data(), numWords * 8, bits, numWords * 8);
        size_ = size;
        end_ = size;
        begin_ = 0;
        updateBounds();
    }

    void setFromBitsNegate(const uint64_t *bits, int32_t size)
    {
        auto numWords = BitUtil::Nwords(size);
        if (numWords > bits_.size()) {
            bits_.resize(numWords);
        }
        memcpy_s(bits_.data(), numWords * 8, bits, numWords * 8);
        BitUtil::Negate(bits_.data(), size);
        size_ = size;
        end_ = size;
        begin_ = 0;
        updateBounds();
    }

    /**
     * Removes rows that are not present in the 'other' vector.
     */
    void intersect(const SelectivityVector &other)
    {
        BitUtil::AndBits(bits_.data(), other.bits_.data(), begin_, std::min(end_, other.size()));
        updateBounds();
    }

    /**
     * Merges the valid vector of another SelectivityVector by !AND'ing them
     * together. This is used to support logical deletes where
     * any keys passing should actually be inverted
     */
    void deselect(const SelectivityVector &other)
    {
        BitUtil::AndWithNegatedBits(bits_.data(), other.bits_.data(), begin_, std::min(end_, other.size()));
        updateBounds();
    }

    void deselect(const uint64_t *bits, int32_t begin, int32_t end)
    {
        BitUtil::AndWithNegatedBits(bits_.data(), reinterpret_cast<const uint64_t *>(bits),
            std::max<int32_t>(begin_, begin), std::min<int32_t>(end_, end));
        updateBounds();
    }

    void deselectNulls(const uint64_t *bits, int32_t begin, int32_t end)
    {
        BitUtil::AndBits(bits_.data(), reinterpret_cast<const uint64_t *>(bits), std::max<int32_t>(begin_, begin),
            std::min<int32_t>(end_, end));
        updateBounds();
    }

    void deselectNonNulls(const uint64_t *bits, int32_t begin, int32_t end)
    {
        BitUtil::AndWithNegatedBits(bits_.data(), reinterpret_cast<const uint64_t *>(bits),
            std::max<int32_t>(begin_, begin), std::min<int32_t>(end_, end));
        updateBounds();
    }

    void clearNulls(uint64_t *rawNulls) const
    {
        if (rawNulls) {
            BitUtil::OrBits(rawNulls, bits_.data(), begin_, end_);
        }
    }

    void setNulls(uint64_t *rawNulls) const
    {
        BitUtil::AndWithNegatedBits(rawNulls, bits_.data(), begin_, end_);
    }

    /// Copy null bits from 'src' to 'dest' for active rows.
    void copyNulls(uint64_t *dest, const uint64_t *src) const;

    /// Merges the valid vector of another SelectivityVector by or'ing
    /// them together. This is used to support memoization where a state
    /// may acquire new values over time. Grows 'this' if size of 'other' exceeds
    /// this size.
    void select(const SelectivityVector &other)
    {
        if (size_ < other.size()) {
            resize(other.size(), false);
        }
        BitUtil::OrBits(bits_.data(), other.bits_.data(), 0, std::min(size_, other.size()));
        updateBounds();
    }

    /**
 * Updates the begin_ and end_ values to match the
 * current bounds of the minimum selected index and the maximum selected
 * index (noting that the range in between may contain not selected indices).
 */
    void updateBounds()
    {
        begin_ = BitUtil::FindFirstBit(bits_.data(), 0, size_);
        if (begin_ == -1) {
            begin_ = 0;
            end_ = 0;
            allSelected_ = false;
            return;
        }
        end_ = BitUtil::FindLastBit(bits_.data(), begin_, size_) + 1;
        allSelected_.reset();
    }

    bool isAllSelected() const
    {
        if (allSelected_.has_value()) {
            return allSelected_.value();
        }
        allSelected_ = begin_ == 0 && end_ == size_ && BitUtil::IsAllSet(bits_.data(), 0, size_, true);
        return allSelected_.value();
    }

    /**
     * Iterate and count the number of selected values in this SelectivityVector
     */
    vector_size_t countSelected() const
    {
        if (allSelected_.has_value() && *allSelected_) {
            return size();
        }
        auto count = BitUtil::CountBits(bits_.data(), begin_, end_);
        allSelected_ = count == size();
        return count;
    }

    vector_size_t size() const
    {
        return size_;
    }

    bool operator==(const SelectivityVector &other) const
    {
        return begin_ == other.begin_ && end_ == other.end_ && BitUtil::testWords(begin_, end_,
            [&](int32_t index, uint64_t mask) {
                return (bits_[index] & mask) == (other.bits_[index] & mask);
            }, [&](int32_t index) {
                return bits_[index] == other.bits_[index];
            });
    }

    bool operator!=(const SelectivityVector &other) const
    {
        return !(*this == other);
    }

    /// Invokes a function on each selected row. The function must take a single
    /// "row" argument of type vector_size_t and return void.
    template <typename Callable>
    void applyToSelected(Callable func) const;

private:
    // The vector of bits for what is selected vs not (1 is selected).
    std::vector<uint64_t> bits_;

    // The number of leading bits used in 'bits_'.
    vector_size_t size_ = 0;

    // The minimum index of a selected value, if there are any selected.
    vector_size_t begin_ = 0;

    // One past the last selected value, if there are any selected.
    vector_size_t end_ = 0;

    mutable std::optional<bool> allSelected_;

    friend class SelectivityIterator;
};

template <typename Callable>
inline void SelectivityVector::applyToSelected(Callable func) const
{
    if (isAllSelected()) {
        const auto end = end_;
        for (vector_size_t row = begin_; row < end; ++row) {
            func(row);
        }
    } else {
        BitUtil::ForEachSetBit(bits_.data(), begin_, end_, func);
    }
}
}
