#ifndef OMNI_RUNTIME_STRING_VIEW_H
#define OMNI_RUNTIME_STRING_VIEW_H

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <ostream>
#include <string>
#include <string_view>

namespace omniruntime::vec {

/**
 *
 * 16-byte fixed-size struct stored directly in a values array.
 *  - Inline storage for strings <= 12 bytes (zero heap allocation)
 *  - Prefix caching (first 4 bytes) for strings > 12 bytes, enabling fast
 *    comparison short-circuits
 *
 * Memory layout (16 bytes total):
 *   [ size_ (4B) | prefix_[4] (4B) | value_ (8B) ]
 *
 * Inline  (size <= 12): prefix_[0..3] + value_.inlined[0..7] hold the string bytes
 * Non-inline (size > 12): prefix_[0..3] = first 4 chars; value_.data = pointer
 */
struct StringView {
    using value_type = char;

    static constexpr size_t kPrefixSize =  4 * sizeof(char);;
    static constexpr size_t kInlineSize = 12;

    StringView()
    {
        static_assert(sizeof(StringView) == 16, "StringView must be exactly 16 bytes");
        memset(this, 0, sizeof(StringView));
    }

    StringView(const char* data, int32_t len)
    {
        static_assert(sizeof(StringView) == 16, "StringView must be exactly 16 bytes");
        size_ = static_cast<uint32_t>(len);
        if (isInline()) {
            // Zero out then copy all bytes into prefix_ + inlined
            memset(prefix_, 0, kPrefixSize);
            memset(value_.inlined, 0, sizeof(value_.inlined));
            memcpy(prefix_, data, len);
        } else {
            memcpy(prefix_, data, kPrefixSize);
            value_.data = data;
        }
    }

    /* implicit */ StringView(const char* data) : StringView(data, static_cast<int32_t>(strlen(data))) {}

    explicit StringView(std::string_view sv)
        : StringView(sv.data(), static_cast<int32_t>(sv.size())) {}

    explicit StringView(const std::string& s)
        : StringView(s.data(), static_cast<int32_t>(s.size())) {}

    // Prevent dangling references from temporary strings/string_views
    StringView(std::string&&) = delete;
    StringView(std::string_view&&) = delete;

    bool isInline() const
    {
        return size_ <= kInlineSize;
    }

    const char* data() const&
    {
        return isInline() ? prefix_ : value_.data;
    }

    // Deleted rvalue overload — prevents dangling pointer from a temporary StringView
    const char* data() const&& = delete;

    size_t size() const
    {
        return size_;
    }

    bool empty() const
    {
        return size_ == 0;
    }

    const char* begin() const&
    {
        return data();
    }

    const char* end() const&
    {
        return data() + size_;
    }

    const char* begin() const&& = delete;
    const char* end() const&& = delete;

    bool operator==(const StringView& other) const
    {
        //std::cout << "--> operator== " << std::endl;
        // Compare lengths and first 4 characters.
        if (sizeAndPrefixAsInt64() != other.sizeAndPrefixAsInt64()) {
            return false;
        }
        if (isInline()) {
            // The inline part is zeroed at construction, so we can compare
            // a word at a time if data extends past 'prefix_'.
            return size_ <= kPrefixSize || inlinedAsInt64() == other.inlinedAsInt64();
        }
        // if (size_ != other.size_) {
        //     return false;
        // }
        // // Compare size+prefix as a single int64 for speed
        // int64_t sizeAndPrefix;
        // int64_t otherSizeAndPrefix;
        // memcpy(&sizeAndPrefix, this, sizeof(int64_t));
        // memcpy(&otherSizeAndPrefix, &other, sizeof(int64_t));
        // if (sizeAndPrefix != otherSizeAndPrefix) {
        //     return false;
        // }
        // if (isInline()) {
        //     // All 16 bytes are self-contained; compare second 8 bytes too
        //     int64_t v1, v2;
        //     memcpy(&v1, reinterpret_cast<const char*>(this) + sizeof(int64_t), sizeof(int64_t));
        //     memcpy(&v2, reinterpret_cast<const char*>(&other) + sizeof(int64_t), sizeof(int64_t));
        //     return v1 == v2;
        // }
        // Non-inline: prefix already matched; compare remaining bytes
        return memcmp(value_.data + kPrefixSize, other.value_.data + kPrefixSize,
                      size_ - kPrefixSize) == 0;
    }

    bool operator!=(const StringView& other) const
    {
        //std::cout << "--> operator != " << std::endl;
        return !(*this == other);
    }

    int32_t compare(const StringView& other) const
    {
        const size_t minLen = size_ < other.size_ ? size_ : other.size_;

        // Fast path: compare prefix bytes first
        const size_t prefixLen = minLen < kPrefixSize ? minLen : kPrefixSize;
        int cmp = memcmp(prefix_, other.prefix_, prefixLen);
        if (cmp != 0) {
            return cmp;
        }
        if (minLen <= kPrefixSize) {
            // Strings are equal up to minLen; shorter one is less
            if (size_ < other.size_) {
                return -1;
            }
            if (size_ > other.size_) {
                return 1;
            }
            return 0;
        }

        // Compare remainder beyond prefix
        cmp = memcmp(data() + kPrefixSize, other.data() + kPrefixSize, minLen - kPrefixSize);
        if (cmp != 0) {
            return cmp;
        }
        if (size_ < other.size_) {
            return -1;
        }
        if (size_ > other.size_) {
            return 1;
        }
        return 0;
    }

    bool operator<(const StringView& other) const { return compare(other) < 0; }
    bool operator>(const StringView& other) const { return compare(other) > 0; }
    bool operator<=(const StringView& other) const { return compare(other) <= 0; }
    bool operator>=(const StringView& other) const { return compare(other) >= 0; }

    /* implicit */ operator std::string_view() const&
    {
        return std::string_view(data(), size_);
    }

    /* implicit */ operator std::string_view() const&& = delete;

    explicit operator std::string() const
    {
        return std::string(data(), size_);
    }

    std::string str() const
    {
        return std::string(*this);
    }

    std::string getString() const
    {
        return str();
    }

    std::string materialize() const
    {
        return str();
    }

private:
    int64_t sizeAndPrefixAsInt64() const {
        return reinterpret_cast<const int64_t*>(this)[0];
    }

    int64_t inlinedAsInt64() const {
        return reinterpret_cast<const int64_t*>(this)[1];
    }

    int32_t prefixAsInt() const {
        return *reinterpret_cast<const int32_t*>(&prefix_);
    }
    uint32_t size_;
    char prefix_[kPrefixSize];
    union {
        char inlined[8];
        const char* data;
    } value_;
};

static_assert(sizeof(StringView) == 16, "StringView must be exactly 16 bytes");
static_assert(alignof(StringView) == alignof(const char*),
              "StringView alignment must match pointer alignment");

inline StringView operator""_sv(const char* str, size_t len)
{
    return StringView(str, static_cast<int32_t>(len));
}

inline std::ostream& operator<<(std::ostream& os, const StringView& sv)
{
    return os.write(sv.data(), static_cast<std::streamsize>(sv.size()));
}

} // namespace omniruntime::vec

namespace std {

template <>
struct hash<omniruntime::vec::StringView> {
    size_t operator()(const omniruntime::vec::StringView& v) const noexcept
    {
        // FNV-1a hash
        size_t hash = 14695981039346656037ULL;
        const char* p = v.data();
        for (size_t i = 0; i < v.size(); ++i) {
            hash ^= static_cast<unsigned char>(p[i]);
            hash *= 1099511628211ULL;
        }
        return hash;
    }
};

} // namespace std

#endif // OMNI_RUNTIME_STRING_VIEW_H
