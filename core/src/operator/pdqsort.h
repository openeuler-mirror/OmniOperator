/*
    pdqsort.h - Pattern-defeating quicksort.

    Copyright (c) 2021 Orson Peters

    Altered from the original: sorts parallel value/address index arrays, is templated on a
    sort-ascending flag instead of an arbitrary comparator, and adds Decimal128 support.
    This is not the original pdqsort.

    This software is provided 'as-is', without any express or implied warranty. In no event will the
    authors be held liable for any damages arising from the use of this software.

    Permission is granted to anyone to use this software for any purpose, including commercial
    applications, and to alter it and redistribute it freely, subject to the following restrictions:

    1. The origin of this software must not be misrepresented; you must not claim that you wrote the
       original software. If you use this software in a product, an acknowledgment in the product
       documentation would be appreciated but is not required.

    2. Altered source versions must be plainly marked as such, and must not be misrepresented as
       being the original software.

    3. This notice may not be removed or altered from any source distribution.
*/


#ifndef PDQSORT_H
#define PDQSORT_H

#ifdef OMNI_ENABLE_EXPERIMENTAL_SORT

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <type_traits>
#include <utility>
#include "type/decimal128.h"

namespace pdqsort {

namespace detail {

using omniruntime::type::Decimal128;

enum {
    // Partitions below this size are sorted using insertion sort.
    insertion_sort_threshold = 16,

    // Partitions above this size use Tukey's ninther to select the pivot.
    ninther_threshold = 128,

    // When we detect an already sorted partition, attempt an insertion sort that allows this
    // amount of element moves before giving up.
    partial_insertion_sort_limit = 8,

    // Must be multiple of 8 due to loop unrolling, and < 256 to fit in unsigned char.
    block_size = 64,

    // Cacheline size, assumes power of two.
    cacheline_size = 64
};

constexpr int8_t kCompareLess = -1;
constexpr int8_t kCompareEqual = 0;
constexpr int8_t kCompareGreater = 1;

inline void swap_indices(int64_t* values, uint64_t* addresses, int32_t a, int32_t b) {
    if (a == b) return;
    std::swap(values[a], values[b]);
    std::swap(addresses[a], addresses[b]);
}

template<class T>
inline int log2(T n) {
    int log = 0;
    while (n >>= 1) ++log;
    return log;
}

template<typename RawType>
inline auto sort_key_from_slot(int64_t slotVal) {
    if constexpr (std::is_same_v<RawType, Decimal128>) {
        return *reinterpret_cast<Decimal128*>(slotVal);
    } else if constexpr (std::is_same_v<RawType, double>) {
        return *reinterpret_cast<const double*>(&slotVal);
    } else if constexpr (std::is_same_v<RawType, float>) {
        return *reinterpret_cast<const float*>(&slotVal);
    } else {
        return static_cast<RawType>(slotVal);
    }
}

inline bool double_equal(double left, double right) {
    if (left == right) return true;
    const double diff = std::fabs(left - right);
    if (diff < __DBL_EPSILON__) return true;
    const double max_val = std::max(std::fabs(left), std::fabs(right));
    return diff < max_val * __DBL_EPSILON__;
}

template<int32_t sortAscending>
inline int8_t compare_double(double left, double right) {
    if (double_equal(left, right)) {
        return kCompareEqual;
    }
    if constexpr (sortAscending == 1) {
        return left > right ? kCompareGreater : kCompareLess;
    } else {
        return right > left ? kCompareGreater : kCompareLess;
    }
}

inline bool float_equal(float left, float right) {
    if (left == right) return true;
    const float diff = std::fabs(left - right);
    if (diff < __FLT_EPSILON__) return true;
    const float max_val = std::max(std::fabs(left), std::fabs(right));
    return diff < max_val * __FLT_EPSILON__;
}

template<int32_t sortAscending>
inline int8_t compare_float(float left, float right) {
    if (float_equal(left, right)) {
        return kCompareEqual;
    }
    if constexpr (sortAscending == 1) {
        return left > right ? kCompareGreater : kCompareLess;
    } else {
        return right > left ? kCompareGreater : kCompareLess;
    }
}

template<int32_t sortAscending>
inline int8_t compare_decimal128(const Decimal128& left, const Decimal128& right) {
    if constexpr (sortAscending == 1) {
        return left > right ? kCompareGreater :
               left < right ? kCompareLess : kCompareEqual;
    } else {
        return right > left ? kCompareGreater :
               right < left ? kCompareLess : kCompareEqual;
    }
}

template<typename RawType, int32_t sortAscending>
inline bool less(int64_t* values, int32_t ia, int32_t ib) {
    if constexpr (std::is_same_v<RawType, double>) {
        const double ka = sort_key_from_slot<RawType>(values[ia]);
        const double kb = sort_key_from_slot<RawType>(values[ib]);
        return compare_double<sortAscending>(ka, kb) == kCompareLess;
    } else if constexpr (std::is_same_v<RawType, float>) {
        const float ka = sort_key_from_slot<RawType>(values[ia]);
        const float kb = sort_key_from_slot<RawType>(values[ib]);
        return compare_float<sortAscending>(ka, kb) == kCompareLess;
    } else if constexpr (std::is_same_v<RawType, Decimal128>) {
        Decimal128 ka = sort_key_from_slot<RawType>(values[ia]);
        Decimal128 kb = sort_key_from_slot<RawType>(values[ib]);
        return compare_decimal128<sortAscending>(ka, kb) == kCompareLess;
    } else {
        const RawType ka = sort_key_from_slot<RawType>(values[ia]);
        const RawType kb = sort_key_from_slot<RawType>(values[ib]);
        if constexpr (sortAscending == 1) {
            return ka < kb;
        } else {
            return kb < ka;
        }
    }
}

template<typename RawType, int32_t sortAscending>
struct enable_branchless : std::false_type {};

template<int32_t sortAscending>
struct enable_branchless<int8_t, sortAscending> : std::true_type {};
template<int32_t sortAscending>
struct enable_branchless<int16_t, sortAscending> : std::true_type {};
template<int32_t sortAscending>
struct enable_branchless<int32_t, sortAscending> : std::true_type {};
template<int32_t sortAscending>
struct enable_branchless<int64_t, sortAscending> : std::true_type {};
template<int32_t sortAscending>
struct enable_branchless<uint8_t, sortAscending> : std::true_type {};
template<int32_t sortAscending>
struct enable_branchless<uint16_t, sortAscending> : std::true_type {};
template<int32_t sortAscending>
struct enable_branchless<uint32_t, sortAscending> : std::true_type {};
template<int32_t sortAscending>
struct enable_branchless<uint64_t, sortAscending> : std::true_type {};

template<typename RawType, int32_t sortAscending>
inline void insertion_sort(int64_t* values, uint64_t* addresses, int32_t from, int32_t to) {
    if (from >= to) return;

    for (int32_t cur = from + 1; cur < to; ++cur) {
        int32_t sift = cur;
        while (sift > from && less<RawType, sortAscending>(values, sift, sift - 1)) {
            swap_indices(values, addresses, sift, sift - 1);
            --sift;
        }
    }
}

template<typename RawType, int32_t sortAscending>
inline void unguarded_insertion_sort(int64_t* values, uint64_t* addresses, int32_t from, int32_t to) {
    if (from >= to) return;

    for (int32_t cur = from + 1; cur < to; ++cur) {
        int32_t sift = cur;
        while (less<RawType, sortAscending>(values, sift, sift - 1)) {
            swap_indices(values, addresses, sift, sift - 1);
            --sift;
        }
    }
}

template<typename RawType, int32_t sortAscending>
inline bool partial_insertion_sort(int64_t* values, uint64_t* addresses, int32_t from, int32_t to) {
    if (from >= to) return true;

    std::size_t limit = 0;
    for (int32_t cur = from + 1; cur < to; ++cur) {
        int32_t sift = cur;
        if (less<RawType, sortAscending>(values, sift, sift - 1)) {
            do {
                swap_indices(values, addresses, sift, sift - 1);
                --sift;
            } while (sift > from && less<RawType, sortAscending>(values, sift, sift - 1));
            limit += static_cast<std::size_t>(cur - sift);
        }

        if (limit > partial_insertion_sort_limit) return false;
    }

    return true;
}

template<typename RawType, int32_t sortAscending>
inline void sort2(int64_t* values, uint64_t* addresses, int32_t a, int32_t b) {
    if (less<RawType, sortAscending>(values, b, a)) swap_indices(values, addresses, a, b);
}

template<typename RawType, int32_t sortAscending>
inline void sort3(int64_t* values, uint64_t* addresses, int32_t a, int32_t b, int32_t c) {
    sort2<RawType, sortAscending>(values, addresses, a, b);
    sort2<RawType, sortAscending>(values, addresses, b, c);
    sort2<RawType, sortAscending>(values, addresses, a, b);
}

template<class T>
inline T* align_cacheline(T* p) {
    std::uintptr_t ip = reinterpret_cast<std::uintptr_t>(p);
    ip = (ip + cacheline_size - 1) & -cacheline_size;
    return reinterpret_cast<T*>(ip);
}

template<typename RawType, int32_t sortAscending>
inline void swap_offsets(int64_t* values, uint64_t* addresses,
                         int32_t first, int32_t last,
                         unsigned char* offsets_l, unsigned char* offsets_r,
                         size_t num, bool use_swaps) {
    if (use_swaps) {
        for (size_t i = 0; i < num; ++i) {
            swap_indices(values, addresses, first + offsets_l[i], last - offsets_r[i]);
        }
    } else if (num > 0) {
        int32_t l = first + offsets_l[0];
        int32_t r = last - offsets_r[0];
        int64_t tmp_val = values[l];
        uint64_t tmp_addr = addresses[l];
        values[l] = values[r];
        addresses[l] = addresses[r];
        for (size_t i = 1; i < num; ++i) {
            l = first + offsets_l[i];
            values[r] = values[l];
            addresses[r] = addresses[l];
            r = last - offsets_r[i];
            values[l] = values[r];
            addresses[l] = addresses[r];
        }
        values[r] = tmp_val;
        addresses[r] = tmp_addr;
    }
}

template<typename RawType, int32_t sortAscending>
inline void sift_down(int64_t* values, uint64_t* addresses,
                      int32_t from, int32_t heap_end, int32_t i) {
    int32_t pos = i - from;
    int32_t child = from + 2 * pos + 1;
    while (child < heap_end) {
        int32_t right = child + 1;
        if (right < heap_end && less<RawType, sortAscending>(values, child, right)) {
            ++child;
        }
        if (!less<RawType, sortAscending>(values, i, child)) {
            break;
        }
        swap_indices(values, addresses, i, child);
        i = child;
        pos = i - from;
        child = from + 2 * pos + 1;
    }
}

template<typename RawType, int32_t sortAscending>
inline void make_heap_indices(int64_t* values, uint64_t* addresses, int32_t from, int32_t to) {
    if (from >= to) return;
    for (int32_t i = from + (to - from) / 2 - 1; i >= from; --i) {
        sift_down<RawType, sortAscending>(values, addresses, from, to, i);
    }
}

template<typename RawType, int32_t sortAscending>
inline void sort_heap_indices(int64_t* values, uint64_t* addresses, int32_t from, int32_t to) {
    for (int32_t end = to; end > from + 1; ) {
        swap_indices(values, addresses, from, --end);
        sift_down<RawType, sortAscending>(values, addresses, from, end, from);
    }
}

template<typename RawType, int32_t sortAscending>
inline std::pair<int32_t, bool> partition_right_branchless(int64_t* values, uint64_t* addresses,
                                                           int32_t from, int32_t to) {
    int32_t first = from;
    int32_t last = to;

    while (less<RawType, sortAscending>(values, ++first, from));

    if (first - 1 == from) while (first < last) {
        --last;
        if (less<RawType, sortAscending>(values, last, from)) {
            break;
        };
    }
    else                   while (                !less<RawType, sortAscending>(values, --last, from));

    bool already_partitioned = first >= last;
    if (!already_partitioned) {
        swap_indices(values, addresses, first, last);
        ++first;

        unsigned char offsets_l_storage[block_size + cacheline_size];
        unsigned char offsets_r_storage[block_size + cacheline_size];
        unsigned char* offsets_l = align_cacheline(offsets_l_storage);
        unsigned char* offsets_r = align_cacheline(offsets_r_storage);

        int32_t offsets_l_base = first;
        int32_t offsets_r_base = last;
        size_t num_l, num_r, start_l, start_r;
        num_l = num_r = start_l = start_r = 0;

        while (first < last) {
            size_t num_unknown = static_cast<size_t>(last - first);
            size_t left_split = num_l == 0 ? (num_r == 0 ? num_unknown / 2 : num_unknown) : 0;
            size_t right_split = num_r == 0 ? (num_unknown - left_split) : 0;

            if (left_split >= block_size) {
                for (size_t i = 0; i < block_size;) {
                    offsets_l[num_l] = static_cast<unsigned char>(i++);
                    num_l += !less<RawType, sortAscending>(values, first, from);
                    ++first;
                    offsets_l[num_l] = static_cast<unsigned char>(i++);
                    num_l += !less<RawType, sortAscending>(values, first, from);
                    ++first;
                    offsets_l[num_l] = static_cast<unsigned char>(i++);
                    num_l += !less<RawType, sortAscending>(values, first, from);
                    ++first;
                    offsets_l[num_l] = static_cast<unsigned char>(i++);
                    num_l += !less<RawType, sortAscending>(values, first, from);
                    ++first;
                    offsets_l[num_l] = static_cast<unsigned char>(i++);
                    num_l += !less<RawType, sortAscending>(values, first, from);
                    ++first;
                    offsets_l[num_l] = static_cast<unsigned char>(i++);
                    num_l += !less<RawType, sortAscending>(values, first, from);
                    ++first;
                    offsets_l[num_l] = static_cast<unsigned char>(i++);
                    num_l += !less<RawType, sortAscending>(values, first, from);
                    ++first;
                    offsets_l[num_l] = static_cast<unsigned char>(i++);
                    num_l += !less<RawType, sortAscending>(values, first, from);
                    ++first;
                }
            } else {
                for (size_t i = 0; i < left_split;) {
                    offsets_l[num_l] = static_cast<unsigned char>(i++);
                    num_l += !less<RawType, sortAscending>(values, first, from);
                    ++first;
                }
            }

            if (right_split >= block_size) {
                for (size_t i = 0; i < block_size;) {
                    offsets_r[num_r] = static_cast<unsigned char>(++i);
                    num_r += less<RawType, sortAscending>(values, --last, from);
                    offsets_r[num_r] = static_cast<unsigned char>(++i);
                    num_r += less<RawType, sortAscending>(values, --last, from);
                    offsets_r[num_r] = static_cast<unsigned char>(++i);
                    num_r += less<RawType, sortAscending>(values, --last, from);
                    offsets_r[num_r] = static_cast<unsigned char>(++i);
                    num_r += less<RawType, sortAscending>(values, --last, from);
                    offsets_r[num_r] = static_cast<unsigned char>(++i);
                    num_r += less<RawType, sortAscending>(values, --last, from);
                    offsets_r[num_r] = static_cast<unsigned char>(++i);
                    num_r += less<RawType, sortAscending>(values, --last, from);
                    offsets_r[num_r] = static_cast<unsigned char>(++i);
                    num_r += less<RawType, sortAscending>(values, --last, from);
                    offsets_r[num_r] = static_cast<unsigned char>(++i);
                    num_r += less<RawType, sortAscending>(values, --last, from);
                }
            } else {
                for (size_t i = 0; i < right_split;) {
                    offsets_r[num_r] = static_cast<unsigned char>(++i);
                    num_r += less<RawType, sortAscending>(values, --last, from);
                }
            }

            size_t num = std::min(num_l, num_r);
            swap_offsets<RawType, sortAscending>(values, addresses, offsets_l_base, offsets_r_base,
                                                 offsets_l + start_l, offsets_r + start_r,
                                                 num, num_l == num_r);
            num_l -= num; num_r -= num;
            start_l += num; start_r += num;

            if (num_l == 0) {
                start_l = 0;
                offsets_l_base = first;
            }

            if (num_r == 0) {
                start_r = 0;
                offsets_r_base = last;
            }
        }

        if (num_l) {
            offsets_l += start_l;
            while (num_l > 0) {
                --num_l;
                --last;
                swap_indices(values, addresses, offsets_l_base + offsets_l[num_l], last);
            }
            first = last;
        }
        if (num_r) {
            offsets_r += start_r;
            while (num_r > 0) {
                --num_r;
                swap_indices(values, addresses, offsets_r_base - offsets_r[num_r], first);
                ++first;
            }
            last = first;
        }
    }

    int32_t pivot_pos = first - 1;
    swap_indices(values, addresses, from, pivot_pos);

    return std::make_pair(pivot_pos, already_partitioned);
}

template<typename RawType, int32_t sortAscending>
inline std::pair<int32_t, bool> partition_right(int64_t* values, uint64_t* addresses, int32_t from, int32_t to) {
    int32_t first = from;
    int32_t last = to;

    while (less<RawType, sortAscending>(values, ++first, from));

    if (first - 1 == from) 
        while (first < last) {
            --last;
            if (less<RawType, sortAscending>(values, last, from)) {
                break;
            }
        }
    else                   while (                !less<RawType, sortAscending>(values, --last, from));

    bool already_partitioned = first >= last;

    while (first < last) {
        swap_indices(values, addresses, first, last);
        while (less<RawType, sortAscending>(values, ++first, from));
        while (!less<RawType, sortAscending>(values, --last, from));
    }

    int32_t pivot_pos = first - 1;
    swap_indices(values, addresses, from, pivot_pos);

    return std::make_pair(pivot_pos, already_partitioned);
}

template<typename RawType, int32_t sortAscending>
inline int32_t partition_left(int64_t* values, uint64_t* addresses, int32_t from, int32_t to) {
    int32_t first = from;
    int32_t last = to;

    while (less<RawType, sortAscending>(values, from, --last));

    if (last + 1 == to) 
        while (first < last) {
            ++first;
            if (less<RawType, sortAscending>(values, from, first)) {
                break;
            }
        }
    else                while (                !less<RawType, sortAscending>(values, from, ++first));

    while (first < last) {
        swap_indices(values, addresses, first, last);
        while (less<RawType, sortAscending>(values, from, --last));
        while (!less<RawType, sortAscending>(values, from, ++first));
    }

    int32_t pivot_pos = last;
    swap_indices(values, addresses, from, pivot_pos);

    return pivot_pos;
}

template<typename RawType, int32_t sortAscending, bool Branchless>
inline void pdqsort_loop(int64_t* values, uint64_t* addresses,
                         int32_t from, int32_t to, int bad_allowed, bool leftmost = true) {
    while (to - from >= insertion_sort_threshold) {
        int32_t size = to - from;

        int32_t s2 = from + size / 2;
        if (size > ninther_threshold) {
            sort3<RawType, sortAscending>(values, addresses, from, s2, to - 1);
            sort3<RawType, sortAscending>(values, addresses, from + 1, from + (size / 2 - 1), to - 2);
            sort3<RawType, sortAscending>(values, addresses, from + 2, from + (size / 2 + 1), to - 3);
            sort3<RawType, sortAscending>(values, addresses, from + (size / 2 - 1), s2, from + (size / 2 + 1));
            swap_indices(values, addresses, from, s2);
        } else {
            sort3<RawType, sortAscending>(values, addresses, s2, from, to - 1);
        }

        if (!leftmost && !less<RawType, sortAscending>(values, from - 1, from)) {
            from = partition_left<RawType, sortAscending>(values, addresses, from, to) + 1;
            continue;
        }

        std::pair<int32_t, bool> part_result =
            Branchless ? partition_right_branchless<RawType, sortAscending>(values, addresses, from, to)
                       : partition_right<RawType, sortAscending>(values, addresses, from, to);
        int32_t pivot_pos = part_result.first;
        bool already_partitioned = part_result.second;

        int32_t l_size = pivot_pos - from;
        int32_t r_size = to - (pivot_pos + 1);
        bool highly_unbalanced = l_size < size / 8 || r_size < size / 8;

        if (highly_unbalanced) {
            if (--bad_allowed == 0) {
                make_heap_indices<RawType, sortAscending>(values, addresses, from, to);
                sort_heap_indices<RawType, sortAscending>(values, addresses, from, to);
                return;
            }

            if (l_size >= insertion_sort_threshold) {
                swap_indices(values, addresses, from, from + l_size / 4);
                swap_indices(values, addresses, pivot_pos - 1, pivot_pos - l_size / 4);

                if (l_size > ninther_threshold) {
                    swap_indices(values, addresses, from + 1, from + (l_size / 4 + 1));
                    swap_indices(values, addresses, from + 2, from + (l_size / 4 + 2));
                    swap_indices(values, addresses, pivot_pos - 2, pivot_pos - (l_size / 4 + 1));
                    swap_indices(values, addresses, pivot_pos - 3, pivot_pos - (l_size / 4 + 2));
                }
            }

            if (r_size >= insertion_sort_threshold) {
                swap_indices(values, addresses, pivot_pos + 1, pivot_pos + (1 + r_size / 4));
                swap_indices(values, addresses, to - 1, to - r_size / 4);

                if (r_size > ninther_threshold) {
                    swap_indices(values, addresses, pivot_pos + 2, pivot_pos + (2 + r_size / 4));
                    swap_indices(values, addresses, pivot_pos + 3, pivot_pos + (3 + r_size / 4));
                    swap_indices(values, addresses, to - 2, to - (1 + r_size / 4));
                    swap_indices(values, addresses, to - 3, to - (2 + r_size / 4));
                }
            }
        } else {
            if (already_partitioned
                && partial_insertion_sort<RawType, sortAscending>(values, addresses, from, pivot_pos)
                && partial_insertion_sort<RawType, sortAscending>(values, addresses, pivot_pos + 1, to)) {
                return;
            }
        }

        pdqsort_loop<RawType, sortAscending, Branchless>(values, addresses, from, pivot_pos, bad_allowed, leftmost);
        from = pivot_pos + 1;
        leftmost = false;
    }
    
    if (leftmost) {
        insertion_sort<RawType, sortAscending>(values, addresses, from, to);
    } else {
        unguarded_insertion_sort<RawType, sortAscending>(values, addresses, from, to);
    }
}

} // namespace detail

template<typename RawType, int32_t sortAscending>
inline void pdqsort(int64_t* values, uint64_t* addresses, int32_t from, int32_t to) {
    if (from >= to) return;
    detail::pdqsort_loop<RawType, sortAscending,
        detail::enable_branchless<RawType, sortAscending>::value>(
        values, addresses, from, to, detail::log2(to - from));
}

template<typename RawType, int32_t sortAscending>
inline void pdqsort_branchless(int64_t* values, uint64_t* addresses, int32_t from, int32_t to) {
    if (from >= to) return;
    detail::pdqsort_loop<RawType, sortAscending, true>(
        values, addresses, from, to, detail::log2(to - from));
}


} // namespace pdqsort

#endif  // OMNI_ENABLE_EXPERIMENTAL_SORT

#endif
