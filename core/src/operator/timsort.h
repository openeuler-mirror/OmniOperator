/*
    timsort.h - TimSort, a stable adaptive merge sort.

    Derived from cpp-TimSort: https://github.com/timsort/cpp-TimSort

    Copyright (c) 2011 Fuji Goro (gfx) <gfuji@cpan.org>.

    Permission is hereby granted, free of charge, to any person obtaining a copy of this
    software and associated documentation files (the "Software"), to deal in the Software
    without restriction, including without limitation the rights to use, copy, modify,
    merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
    permit persons to whom the Software is furnished to do so, subject to the following
    conditions:

    The above copyright notice and this permission notice shall be included in all copies
    or substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
    INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
    PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
    HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF
    CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR
    THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/

#ifndef TIMSORT_HPP
#define TIMSORT_HPP

#ifdef OMNI_ENABLE_EXPERIMENTAL_SORT

#include <algorithm>
#include <functional>
#include <iterator>
#include <utility>
#include <vector>

namespace timsort {

// ---------------------------------------
// Implementation details
// ---------------------------------------

namespace detail {

// Equivalent to C++20 std::identity
struct Identity {
    template <typename T>
    constexpr T&& operator()(T&& value) const noexcept {
        return std::forward<T>(value);
    }
};

// Merge a predicate and a projection function
template <typename Compare, typename Projection>
struct ProjectionCompare {
    ProjectionCompare(Compare comp, Projection proj)
        : compare_(std::move(comp)), projection_(std::move(proj)) {
    }

    template <typename T, typename U>
    bool operator()(T&& lhs, U&& rhs) {
#ifdef __cpp_lib_invoke
        return static_cast<bool>(std::invoke(
            compare_, std::invoke(projection_, std::forward<T>(lhs)),
            std::invoke(projection_, std::forward<U>(rhs))));
#else
        return static_cast<bool>(
            compare_(projection_(std::forward<T>(lhs)), projection_(std::forward<U>(rhs))));
#endif
    }

    Compare compare_;
    Projection projection_;
};

template <typename Iterator>
struct Run {
    using DiffT = typename std::iterator_traits<Iterator>::difference_type;

    Iterator base;
    DiffT len;

    Run(Iterator b, DiffT l) : base(b), len(l) {
    }
};

template <typename RandomAccessIterator, typename Compare>
class TimSort {
    using IterT = RandomAccessIterator;
    using ValueT = typename std::iterator_traits<IterT>::value_type;
    using RefT = typename std::iterator_traits<IterT>::reference;
    using DiffT = typename std::iterator_traits<IterT>::difference_type;

    static constexpr int kMinMerge = 32;
    static constexpr int kMinGallop = 7;

    int min_gallop_;

    std::vector<ValueT> tmp_;
    using TmpIterT = typename std::vector<ValueT>::iterator;

    std::vector<Run<RandomAccessIterator>> pending_;

    static void BinarySort(const IterT lo, const IterT hi, IterT start, Compare compare) {
        if (start == lo) {
            ++start;
        }
        for (; start < hi; ++start) {
            ValueT pivot = std::move(*start);

            const IterT pos = std::upper_bound(lo, start, pivot, compare);
            for (IterT p = start; p > pos; --p) {
                *p = std::move(*std::prev(p));
            }
            *pos = std::move(pivot);
        }
    }

    static DiffT CountRunAndMakeAscending(const IterT lo, const IterT hi, Compare compare) {

        auto run_hi = std::next(lo);
        if (run_hi == hi) {
            return 1;
        }

        if (compare(*run_hi, *lo)) {  // decreasing
            do {
                ++run_hi;
            } while (run_hi < hi && compare(*run_hi, *std::prev(run_hi)));
            std::reverse(lo, run_hi);
        } else {  // non-decreasing
            do {
                ++run_hi;
            } while (run_hi < hi && !compare(*run_hi, *std::prev(run_hi)));
        }

        return run_hi - lo;
    }

    static DiffT MinRunLength(DiffT n) {

        DiffT r = 0;
        while (n >= 2 * kMinMerge) {
            r |= (n & 1);
            n >>= 1;
        }
        return n + r;
    }

    TimSort() : min_gallop_(kMinGallop) {
    }

    // Silence GCC -Winline warning
    ~TimSort() {
    }

    void PushRun(const IterT run_base, const DiffT run_len) {
        pending_.emplace_back(run_base, run_len);
    }

    void MergeCollapse(Compare compare) {
        while (pending_.size() > 1) {
            DiffT n = static_cast<DiffT>(pending_.size()) - 2;

            if ((n > 0 && pending_[n - 1].len <= pending_[n].len + pending_[n + 1].len) ||
                (n > 1 && pending_[n - 2].len <= pending_[n - 1].len + pending_[n].len)) {
                if (pending_[n - 1].len < pending_[n + 1].len) {
                    --n;
                }
                MergeAt(n, compare);
            } else if (pending_[n].len <= pending_[n + 1].len) {
                MergeAt(n, compare);
            } else {
                break;
            }
        }
    }

    void MergeForceCollapse(Compare compare) {
        while (pending_.size() > 1) {
            DiffT n = static_cast<DiffT>(pending_.size()) - 2;

            if (n > 0 && pending_[n - 1].len < pending_[n + 1].len) {
                --n;
            }
            MergeAt(n, compare);
        }
    }

    void MergeAt(const DiffT i, Compare compare) {
        const DiffT stack_size = static_cast<DiffT>(pending_.size());

        IterT base1 = pending_[i].base;
        DiffT len1 = pending_[i].len;
        IterT base2 = pending_[i + 1].base;
        DiffT len2 = pending_[i + 1].len;

        pending_[i].len = len1 + len2;

        if (i == stack_size - 3) {
            pending_[i + 1] = pending_[i + 2];
        }

        pending_.pop_back();

        MergeConsecutiveRuns(base1, len1, base2, len2, std::move(compare));
    }

    void MergeConsecutiveRuns(IterT base1, DiffT len1, IterT base2, DiffT len2, Compare compare) {

        const DiffT k = GallopRight(*base2, base1, len1, 0, compare);

        base1 += k;
        len1 -= k;

        if (len1 == 0) {
            return;
        }

        len2 = GallopLeft(*(base1 + (len1 - 1)), base2, len2, len2 - 1, compare);
        if (len2 == 0) {
            return;
        }

        if (len1 <= len2) {
            MergeLo(base1, len1, base2, len2, compare);
        } else {
            MergeHi(base1, len1, base2, len2, compare);
        }
    }

    static void RotateLeft(IterT first, IterT last) {
        ValueT tmp = std::move(*first);
        auto last_1 = std::move(std::next(first), last, first);
        *last_1 = std::move(tmp);
    }

    static void RotateRight(IterT first, IterT last) {
        auto last_1 = std::prev(last);
        ValueT tmp = std::move(*last_1);
        std::move_backward(first, last_1, last);
        *first = std::move(tmp);
    }

    void MergeLo(const IterT base1, DiffT len1, const IterT base2, DiffT len2, Compare compare) {

        if (len1 == 1) {
            RotateLeft(base1, base2 + len2);
            return;
        }
        if (len2 == 1) {
            RotateRight(base1, base2 + len2);
            return;
        }

        CopyToTmp(base1, len1);

        TmpIterT cursor1 = tmp_.begin();
        IterT cursor2 = base2;
        IterT dest = base1;

        *dest = std::move(*cursor2);
        ++cursor2;
        ++dest;
        --len2;

        int min_gallop = min_gallop_;
        bool is_done = false;

        while (!is_done) {
            DiffT count1 = 0;
            DiffT count2 = 0;

            do {

                if (compare(*cursor2, *cursor1)) {
                    *dest = std::move(*cursor2);
                    ++cursor2;
                    ++dest;
                    ++count2;
                    count1 = 0;
                    if (--len2 == 0) {
                        is_done = true;
                        break;
                    }
                } else {
                    *dest = std::move(*cursor1);
                    ++cursor1;
                    ++dest;
                    ++count1;
                    count2 = 0;
                    if (--len1 == 1) {
                        is_done = true;
                        break;
                    }
                }
            } while ((count1 | count2) < min_gallop);

            if (is_done) {
                break;
            }

            do {

                count1 = GallopRight(*cursor2, cursor1, len1, 0, compare);
                if (count1 != 0) {
                    std::move_backward(cursor1, cursor1 + count1, dest + count1);
                    dest += count1;
                    cursor1 += count1;
                    len1 -= count1;

                    if (len1 <= 1) {
                        is_done = true;
                        break;
                    }
                }
                *dest = std::move(*cursor2);
                ++cursor2;
                ++dest;
                if (--len2 == 0) {
                    is_done = true;
                    break;
                }

                count2 = GallopLeft(*cursor1, cursor2, len2, 0, compare);
                if (count2 != 0) {
                    std::move(cursor2, cursor2 + count2, dest);
                    dest += count2;
                    cursor2 += count2;
                    len2 -= count2;
                    if (len2 == 0) {
                        is_done = true;
                        break;
                    }
                }
                *dest = std::move(*cursor1);
                ++cursor1;
                ++dest;
                if (--len1 == 1) {
                    is_done = true;
                    break;
                }

                --min_gallop;
            } while ((count1 >= kMinGallop) | (count2 >= kMinGallop));

            if (is_done) {
                break;
            }

            if (min_gallop < 0) {
                min_gallop = 0;
            }
            min_gallop += 2;
        }

        min_gallop_ = (std::min)(min_gallop, 1);

        if (len1 == 1) {
            std::move(cursor2, cursor2 + len2, dest);
            *(dest + len2) = std::move(*cursor1);
        } else {
            std::move(cursor1, cursor1 + len1, dest);
        }
    }

    void MergeHi(const IterT base1, DiffT len1, const IterT base2, DiffT len2, Compare compare) {

        if (len1 == 1) {
            RotateLeft(base1, base2 + len2);
            return;
        }
        if (len2 == 1) {
            RotateRight(base1, base2 + len2);
            return;
        }

        CopyToTmp(base2, len2);

        IterT cursor1 = base1 + len1;
        TmpIterT cursor2 = tmp_.begin() + (len2 - 1);
        IterT dest = base2 + (len2 - 1);

        *dest = std::move(*(--cursor1));
        --dest;
        --len1;

        int min_gallop = min_gallop_;
        bool is_done = false;

        while (!is_done) {
            DiffT count1 = 0;
            DiffT count2 = 0;

            --cursor1;

            do {

                if (compare(*cursor2, *cursor1)) {
                    *dest = std::move(*cursor1);
                    --dest;
                    ++count1;
                    count2 = 0;
                    if (--len1 == 0) {
                        is_done = true;
                        break;
                    }
                    --cursor1;
                } else {
                    *dest = std::move(*cursor2);
                    --cursor2;
                    --dest;
                    ++count2;
                    count1 = 0;
                    if (--len2 == 1) {
                        ++cursor1;
                        is_done = true;
                        break;
                    }
                }
            } while ((count1 | count2) < min_gallop);

            if (is_done) {
                break;
            }
            ++cursor1;

            do {

                count1 = len1 - GallopRight(*cursor2, base1, len1, len1 - 1, compare);
                if (count1 != 0) {
                    dest -= count1;
                    cursor1 -= count1;
                    len1 -= count1;
                    std::move_backward(cursor1, cursor1 + count1, dest + (1 + count1));

                    if (len1 == 0) {
                        is_done = true;
                        break;
                    }
                }
                *dest = std::move(*cursor2);
                --cursor2;
                --dest;
                if (--len2 == 1) {
                    is_done = true;
                    break;
                }

                count2 =
                    len2 - GallopLeft(*std::prev(cursor1), tmp_.begin(), len2, len2 - 1, compare);
                if (count2 != 0) {
                    dest -= count2;
                    cursor2 -= count2;
                    len2 -= count2;
                    std::move(std::next(cursor2), cursor2 + (1 + count2), std::next(dest));
                    if (len2 <= 1) {
                        is_done = true;
                        break;
                    }
                }
                *dest = std::move(*(--cursor1));
                --dest;
                if (--len1 == 0) {
                    is_done = true;
                    break;
                }

                --min_gallop;
            } while ((count1 >= kMinGallop) | (count2 >= kMinGallop));

            if (is_done) {
                break;
            }

            if (min_gallop < 0) {
                min_gallop = 0;
            }
            min_gallop += 2;
        }

        min_gallop_ = (std::min)(min_gallop, 1);

        if (len2 == 1) {
            dest -= len1;
            std::move_backward(cursor1 - len1, cursor1, dest + (1 + len1));
            *dest = std::move(*cursor2);
        } else {
            std::move(tmp_.begin(), tmp_.begin() + len2, dest - (len2 - 1));
        }
    }

    void CopyToTmp(const IterT begin, DiffT len) {
        tmp_.assign(std::make_move_iterator(begin), std::make_move_iterator(begin + len));
    }

public:
    static void Merge(const IterT lo, const IterT mid, const IterT hi, Compare compare) {

        if (lo == mid || mid == hi) {
            return;
        }

        TimSort ts;
        ts.MergeConsecutiveRuns(lo, mid - lo, mid, hi - mid, std::move(compare));
    }

    static void Sort(const IterT lo, const IterT hi, Compare compare) {

        DiffT n_remaining = (hi - lo);
        if (n_remaining < 2) {
            return;
        }

        if (n_remaining < kMinMerge) {
            const DiffT init_run_len = CountRunAndMakeAscending(lo, hi, compare);
            BinarySort(lo, hi, lo + init_run_len, compare);
            return;
        }

        TimSort ts;
        const DiffT min_run = MinRunLength(n_remaining);
        IterT cur = lo;
        do {
            DiffT run_len = CountRunAndMakeAscending(cur, hi, compare);

            if (run_len < min_run) {
                const DiffT force = (std::min)(n_remaining, min_run);
                BinarySort(cur, cur + force, cur + run_len, compare);
                run_len = force;
            }

            ts.PushRun(cur, run_len);
            ts.MergeCollapse(compare);

            cur += run_len;
            n_remaining -= run_len;
        } while (n_remaining != 0);

        ts.MergeForceCollapse(compare);
    }

    template <typename Iter>
    static DiffT GallopLeft(
        RefT key, const Iter base, const DiffT len, const DiffT hint, Compare compare) {

        DiffT last_ofs = 0;
        DiffT ofs = 1;

        if (compare(*(base + hint), key)) {
            const DiffT max_ofs = len - hint;
            while (ofs < max_ofs && compare(*(base + (hint + ofs)), key)) {
                last_ofs = ofs;
                ofs = (ofs << 1) + 1;

                if (ofs <= 0) {  // int overflow
                    ofs = max_ofs;
                }
            }
            if (ofs > max_ofs) {
                ofs = max_ofs;
            }

            last_ofs += hint;
            ofs += hint;
        } else {
            const DiffT max_ofs = hint + 1;
            while (ofs < max_ofs && !compare(*(base + (hint - ofs)), key)) {
                last_ofs = ofs;
                ofs = (ofs << 1) + 1;

                if (ofs <= 0) {
                    ofs = max_ofs;
                }
            }
            if (ofs > max_ofs) {
                ofs = max_ofs;
            }

            const DiffT tmp = last_ofs;
            last_ofs = hint - ofs;
            ofs = hint - tmp;
        }

        return std::lower_bound(base + (last_ofs + 1), base + ofs, key, compare) - base;
    }

    template <typename Iter>
    static DiffT GallopRight(
        RefT key, const Iter base, const DiffT len, const DiffT hint, Compare compare) {

        DiffT ofs = 1;
        DiffT last_ofs = 0;

        if (compare(key, *(base + hint))) {
            const DiffT max_ofs = hint + 1;
            while (ofs < max_ofs && compare(key, *(base + (hint - ofs)))) {
                last_ofs = ofs;
                ofs = (ofs << 1) + 1;

                if (ofs <= 0) {
                    ofs = max_ofs;
                }
            }
            if (ofs > max_ofs) {
                ofs = max_ofs;
            }

            const DiffT tmp = last_ofs;
            last_ofs = hint - ofs;
            ofs = hint - tmp;
        } else {
            const DiffT max_ofs = len - hint;
            while (ofs < max_ofs && !compare(key, *(base + (hint + ofs)))) {
                last_ofs = ofs;
                ofs = (ofs << 1) + 1;

                if (ofs <= 0) {  // int overflow
                    ofs = max_ofs;
                }
            }
            if (ofs > max_ofs) {
                ofs = max_ofs;
            }

            last_ofs += hint;
            ofs += hint;
        }

        return std::upper_bound(base + (last_ofs + 1), base + ofs, key, compare) - base;
    }
};

}  // namespace detail

// ---------------------------------------
// Public interface implementation
// ---------------------------------------

/**
 * Stably merges two consecutive sorted ranges [first, middle) and [middle, last) into one
 * sorted range [first, last) with a comparison function and a projection function.
 */
// NOLINTNEXTLINE(readability-identifier-naming)
template <
    typename RandomAccessIterator,
    typename Compare = std::less<typename std::iterator_traits<RandomAccessIterator>::value_type>,
    typename Projection = detail::Identity>
void timmerge(
    RandomAccessIterator first, RandomAccessIterator middle, RandomAccessIterator last,
    Compare compare = {}, Projection projection = {}) {
    using CompareT = detail::ProjectionCompare<Compare, Projection>;
    CompareT comp(std::move(compare), std::move(projection));
    detail::TimSort<RandomAccessIterator, CompareT>::Merge(first, middle, last, comp);
}

/**
 * Stably sorts a range with a comparison function and a projection function.
 */
// NOLINTNEXTLINE(readability-identifier-naming)
template <
    typename RandomAccessIterator,
    typename Compare = std::less<typename std::iterator_traits<RandomAccessIterator>::value_type>,
    typename Projection = detail::Identity>
void timsort(
    const RandomAccessIterator first, const RandomAccessIterator last, Compare compare = {},
    Projection projection = {}) {
    using CompareT = detail::ProjectionCompare<Compare, Projection>;
    CompareT comp(std::move(compare), std::move(projection));
    detail::TimSort<RandomAccessIterator, CompareT>::Sort(first, last, comp);
}

/**
 * bool columns use std::vector<bool> in TimSort merge buffers; bit iterators do not
 * produce bool&, so GallopLeft/GallopRight cannot compile. Fall back to std::sort.
 */
// NOLINTNEXTLINE(readability-identifier-naming)
template <
    typename Compare = std::less<bool>,
    typename Projection = detail::Identity>
void timsort(
    bool* first, bool* last, Compare compare = {}, Projection projection = {}) {
    using CompareT = detail::ProjectionCompare<Compare, Projection>;
    CompareT comp(std::move(compare), std::move(projection));
    std::sort(first, last, comp);
}

/**
 * Stably sorts a range with a comparison function and a projection function.
 */
// NOLINTNEXTLINE(readability-identifier-naming)
template <
    typename RandomAccessRange,
    typename Compare = std::less<typename std::iterator_traits<decltype(std::begin(
        std::declval<RandomAccessRange>()))>::value_type>,
    typename Projection = detail::Identity>
void timsort(RandomAccessRange& range, Compare compare = {}, Projection projection = {}) {
    timsort(std::begin(range), std::end(range), compare, projection);
}

}  // namespace timsort

#endif  // OMNI_ENABLE_EXPERIMENTAL_SORT

#endif  // TIMSORT_HPP
