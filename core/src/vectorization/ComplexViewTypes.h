/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: visitor class for expressions
 */

#pragma once
#include "VectorReaders.h"

namespace omniruntime::vectorization {
// Pointer wrapper used to convert r-values to valid return type for operator->.
template <typename T>
class PointerWrapper {
public:
    explicit PointerWrapper(T &&t) : t_(t) {}

    const T *operator->() const
    {
        return &t_;
    }

    T *operator->()
    {
        return &t_;
    }

private:
    T t_;
};

// Base class for ArrayView::Iterator, MapView::Iterator, and
// VariadicView::Iterator.
// TElementAccessor is a class that supplies the following:
// index_t: the type to use for indexes into the container. It should be signed
// to allow, for example, loops to iterate backwards and end at a point before
// the beginning of the container.
// element_t: the type returned when the iterator is dereferenced.
// element_t operator()(index_t index): returns the element at the provided
// index in the container being iterated over.
template <typename TElementAccessor>
class IndexBasedIterator {
public:
    using Iterator = IndexBasedIterator<TElementAccessor>;
    using index_t = typename TElementAccessor::index_t;
    using element_t = typename TElementAccessor::element_t;
    using iterator_category = std::random_access_iterator_tag;
    using value_type = element_t;
    using difference_type = index_t;
    using pointer = PointerWrapper<element_t>;
    using reference = element_t;

    explicit IndexBasedIterator(index_t index, index_t containerStartIndex, index_t containerEndIndex,
        const TElementAccessor &elementAccessor)
        : index_(index), containerStartIndex_(containerStartIndex), containerEndIndex_(containerEndIndex),
        elementAccessor_(elementAccessor) {}

    PointerWrapper<element_t> operator->() const
    {
        validateBounds(index_);

        return PointerWrapper(elementAccessor_(index_));
    }

    element_t operator*() const
    {
        validateBounds(index_);

        return elementAccessor_(index_);
    }

    element_t operator[](difference_type n) const
    {
        return elementAccessor_(index_ + n);
    }

    bool operator!=(const Iterator &rhs) const
    {
        return index_ != rhs.index_;
    }

    bool operator==(const Iterator &rhs) const
    {
        return index_ == rhs.index_;
    }

    bool operator<(const Iterator &rhs) const
    {
        return index_ < rhs.index_;
    }

    bool operator>(const Iterator &rhs) const
    {
        return index_ > rhs.index_;
    }

    bool operator<=(const Iterator &rhs) const
    {
        return index_ <= rhs.index_;
    }

    bool operator>=(const Iterator &rhs) const
    {
        return index_ >= rhs.index_;
    }

    // Implement post increment.
    Iterator operator++(int)
    {
        Iterator old = *this;
        ++*this;
        return old;
    }

    // Implement pre increment.
    Iterator &operator++()
    {
        index_++;
        return *this;
    }

    // Implement post decrement.
    Iterator operator--(int)
    {
        Iterator old = *this;
        --*this;
        return old;
    }

    // Implement pre decrement.
    Iterator &operator--()
    {
        index_--;
        return *this;
    }

    Iterator &operator+=(const difference_type &rhs)
    {
        index_ += rhs;
        return *this;
    }

    Iterator &operator-=(const difference_type &rhs)
    {
        index_ -= rhs;
        return *this;
    }

    // Iterators +/- ints.
    Iterator operator+(difference_type rhs) const
    {
        return Iterator(this->index_ + rhs, containerStartIndex_, containerEndIndex_, elementAccessor_);
    }

    Iterator operator-(difference_type rhs) const
    {
        return Iterator(this->index_ - rhs, containerStartIndex_, containerEndIndex_, elementAccessor_);
    }

    // Subtract iterators.
    difference_type operator-(const Iterator &rhs) const
    {
        return this->index_ - rhs.index_;
    }

    friend Iterator operator+(typename Iterator::difference_type lhs, const Iterator &rhs)
    {
        return rhs + lhs;
    }

protected:
    index_t index_;

    // Every instance of Iterator for the same container should have the same
    // values for these two fields.
    // The first index in the container. When begin() is called on a container
    // the returned iterator should have index_ == containerStartIndex_.
    index_t containerStartIndex_;

    // Last index in the container + 1. When end() is called on a
    // container the returned iterator should have index_ == containerEndIndex_.
    index_t containerEndIndex_;
    TElementAccessor elementAccessor_;

    inline void validateBounds(index_t index) const
    {
    }
};

// Implements an iterator for values that skips nulls and provides direct access
// to those values by wrapping another iterator of type BaseIterator.
//
// BaseIterator must implement the following functions:
//   hasValue() : Returns whether the current value pointed at by the iterator
//                is a null.
//   value()    : Returns the non-null value pointed at by the iterator.
template <typename BaseIterator>
class SkipNullsIterator {
    using Iterator = SkipNullsIterator<BaseIterator>;

    // SkipNullsIterator cannot meet the requirements of
    // random_access_iterator_tag as moving between elements is a linear time
    // operation.
    using iterator_category = std::input_iterator_tag;
    using value_type = typename std::invoke_result<decltype(&BaseIterator::value), BaseIterator>::type;
    using difference_type = int;
    using pointer = PointerWrapper<value_type>;
    using reference = value_type;

public:
    SkipNullsIterator(const BaseIterator &begin, const BaseIterator &end)
        : iter_(begin), end_(end) {}

    // Given an element, return an iterator to the first not-null element starting
    // from the element itself.
    static Iterator initialize(const BaseIterator &begin, const BaseIterator &end)
    {
        auto it = Iterator{begin, end};

        // The container is empty.
        if (begin >= end) {
            return it;
        }

        if (begin.hasValue()) {
            return it;
        }

        // Move to next not null.
        it++;
        return it;
    }

    value_type operator*() const
    {
        // Always return a copy, it's guaranteed to be cheap object.
        return iter_.value();
    }

    PointerWrapper<value_type> operator->() const
    {
        return PointerWrapper(iter_.value());
    }

    bool operator<(const Iterator &rhs) const
    {
        return iter_ < rhs.iter_;
    }

    bool operator!=(const Iterator &rhs) const
    {
        return iter_ != rhs.iter_;
    }

    bool operator==(const Iterator &rhs) const
    {
        return iter_ == rhs.iter_;
    }

    // Implement post increment.
    Iterator operator++(int)
    {
        Iterator old = *this;
        ++*this;
        return old;
    }

    // Implement pre increment.
    Iterator &operator++()
    {
        iter_++;
        while (iter_ != end_) {
            if (iter_.hasValue()) {
                break;
            }
            iter_++;
        }
        return *this;
    }

private:
    BaseIterator iter_;
    // Iterator pointing just beyond the range to expose.
    const BaseIterator end_;
};

// Given a type T, this class represents a lazy access optional wrapper
// around an element in the VectorReader<T> with interface similar to
// std::optional<VectorReader<T>::exec_in_t>. This is used to represent elements
// of ArrayView and values of MapView. OptionalAccessor can be
// compared with and assigned to std::optional.
template <typename T>
class OptionalAccessor {
public:
    using element_t = typename VectorReader<T>::exec_in_t;

    explicit operator bool() const
    {
        return has_value();
    }

    // Enable to be assigned to std::optional<element_t>.
    operator std::optional<element_t>() const
    {
        if (!has_value()) {
            return std::nullopt;
        }
        return {value()};
    }

    // Disable all other implicit casts to avoid odd behaviors.
    template <typename B>
    operator B() const = delete;

    bool operator==(const OptionalAccessor &other) const
    {
        if (other.has_value() != has_value()) {
            return false;
        }

        if (has_value()) {
            return value() == other.value();
        }
        // Both are nulls.
        return true;
    }

    bool operator!=(const OptionalAccessor &other) const
    {
        return !(*this == other);
    }

    bool has_value() const
    {
        return reader_->isSet(index_);
    }

    element_t value() const
    {
        OMNI_CHECK(has_value(), "has_value()");
        return (*reader_)[index_];
    }

    element_t value_or(const element_t &defaultValue) const
    {
        return has_value() ? value() : defaultValue;
    }

    element_t operator*() const
    {
        OMNI_CHECK(has_value(), "has_value()");
        return value();
    }

    PointerWrapper<element_t> operator->() const
    {
        return PointerWrapper(value());
    }

    OptionalAccessor(const VectorReader<T> *reader, int64_t index)
        : reader_(reader), index_(index) {}

private:
    const VectorReader<T> *reader_;
    // Index of element within the reader.
    int64_t index_;

    template <bool nullable, typename V>
    friend class ArrayView;
};

// Helper function that calls materialize on element if it's not primitive.
template <typename VeloxType, typename T>
auto materializeElement(const T &element)
{
    if constexpr (MaterializeType<VeloxType>::requiresMaterialization) {
        return element.materialize();
    } else {
        using unwrapped_type = typename UnwrapCustomType<VeloxType>::type;
        if constexpr (is_shared_ptr<unwrapped_type>::value) {
            return *element;
        } else {
            return element;
        }
    }
}

// Represents an array of elements with an interface similar to std::vector.
// When returnsOptionalValues is true, the interface is like
// std::vector<std::optional<V>>.
// When returnsOptionalValues is false, the interface is like std::vector<V>.
template <bool returnsOptionalValues, typename V>
class ArrayView {
public:
    using reader_t = VectorReader<V>;
    using element_t = V;

    ArrayView(const reader_t *reader, int32_t offset, int32_t size)
        : reader_(reader), offset_(offset), size_(size) {}

    using Element = typename std::conditional<returnsOptionalValues, OptionalAccessor<V>, element_t>::type;

    class ElementAccessor {
    public:
        using element_t = Element;
        using index_t = int32_t;

        explicit ElementAccessor(const reader_t *reader) : reader_(reader) {}

        Element operator()(int32_t index) const
        {
            if constexpr (returnsOptionalValues) {
                return Element{reader_, index};
            } else {
                return reader_->readNullFree(index);
            }
        }

    private:
        const reader_t *reader_;
    };

    using Iterator = IndexBasedIterator<ElementAccessor>;

    Iterator begin() const
    {
        return Iterator{offset_, offset_, offset_ + size_, ElementAccessor(reader_)};
    }

    Iterator end() const
    {
        return Iterator{offset_ + size_, offset_, offset_ + size_, ElementAccessor(reader_)};
    }

    bool empty() const
    {
        return size() == 0;
    }

    struct SkipNullsContainer {
        class SkipNullsBaseIterator : public Iterator {
        public:
            SkipNullsBaseIterator(const reader_t *reader, int32_t index, int32_t startIndex, int32_t endIndex)
                : Iterator(index, startIndex, endIndex, ElementAccessor(reader)), reader_(reader) {}

            bool hasValue() const
            {
                return reader_->isSet(this->index_);
            }

            element_t value() const
            {
                return (*reader_)[this->index_];
            }

        private:
            const reader_t *reader_;
        };

        explicit SkipNullsContainer(const ArrayView *array_) : array_(array_) {}

        SkipNullsIterator<SkipNullsBaseIterator> begin()
        {
            return SkipNullsIterator<SkipNullsBaseIterator>::initialize(
                SkipNullsBaseIterator{
                    array_->reader_,
                    array_->offset_,
                    array_->offset_,
                    array_->offset_ + array_->size_
                }, SkipNullsBaseIterator{
                    array_->reader_,
                    array_->offset_ + array_->size_,
                    array_->offset_,
                    array_->offset_ + array_->size_
                });
        }

        SkipNullsIterator<SkipNullsBaseIterator> end()
        {
            return SkipNullsIterator<SkipNullsBaseIterator>{
                SkipNullsBaseIterator{
                    array_->reader_,
                    array_->offset_ + array_->size_,
                    array_->offset_,
                    array_->offset_ + array_->size_
                },
                SkipNullsBaseIterator{
                    array_->reader_,
                    array_->offset_ + array_->size_,
                    array_->offset_,
                    array_->offset_ + array_->size_
                }
            };
        }

    private:
        const ArrayView *array_;
    };

    // Returns true if any of the arrayViews in the vector might have null
    // element.
    bool mayHaveNulls() const
    {
        if constexpr (returnsOptionalValues) {
            return reader_->mayHaveNulls();
        } else {
            return false;
        }
    }

    using materialize_t = typename std::conditional<returnsOptionalValues, typename MaterializeType<Array<
        V>>::nullable_t, typename MaterializeType<Array<V>>::null_free_t>::type;

    materialize_t materialize() const
    {
        materialize_t result;

        for (const auto &element : *this) {
            if constexpr (returnsOptionalValues) {
                if (element.has_value()) {
                    result.push_back({materializeElement<V>(element.value())});
                } else {
                    result.push_back(std::nullopt);
                }
            } else {
                result.push_back(materializeElement<V>(element));
            }
        }
        return result;
    }

    Element operator[](int32_t index) const
    {
        if constexpr (returnsOptionalValues) {
            return Element{reader_, index + offset_};
        } else {
            return reader_->readNullFree(index + offset_);
        }
    }

    Element at(int32_t index) const
    {
        return (*this)[index];
    }

    int32_t size() const
    {
        return size_;
    }

    SkipNullsContainer skipNulls() const
    {
        if constexpr (returnsOptionalValues) {
            return SkipNullsContainer{this};
        }

        OMNI_THROW("Runtime error:",
            "ArrayViews over NULL-free data do not support skipNulls().  It's "
            "already been checked that this object contains no NULLs, it's more "
            "efficient to use the standard iterator interface.");
    }

    const BaseVector *elementsVectorBase() const
    {
        return reader_->baseVector();
    }

    bool isFlatElements() const
    {
        return reader_->decoded_.isIdentityMapping();
    }

    int32_t offset() const
    {
        return offset_;
    }

private:
    const reader_t *reader_;
    int32_t offset_;
    int32_t size_;
};

template <typename T>
struct is_array_view : std::false_type {};

template <bool B, typename V>
struct is_array_view<ArrayView<B, V>> : std::true_type {};

template <typename T>
constexpr bool is_array_view_v = is_array_view<T>::value;
}
