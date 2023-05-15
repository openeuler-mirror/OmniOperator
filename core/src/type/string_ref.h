/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */
#ifndef OMNI_RUNTIME_STRING_REF_H
#define OMNI_RUNTIME_STRING_REF_H
#include <string>
#include <cstring>
namespace omniruntime {
namespace type {
// replace stringRef with std::string_view
struct StringRef {
    const char *data = nullptr;
    size_t size = 0;

    StringRef() = default;

    template <typename T, typename = std::enable_if_t<sizeof(T) == 1>>
    StringRef(const T *d, size_t s) : data(reinterpret_cast<char *>(d)), size(s)
    {}

    StringRef(const std::string &s) : data(const_cast<char *>(s.data())), size(s.size()) {}

    explicit StringRef(char *data_) : data(data_), size(strlen(data_)) {}

    friend bool operator == (const StringRef &lhs, const StringRef &rhs)
    {
        auto leftSize = lhs.size;
        auto rightSize = rhs.size;
        if (leftSize != rightSize) {
            return false;
        }
        if (leftSize == 0) {
            return true;
        }
        return (0 == memcmp(lhs.data, rhs.data, leftSize));
    }

    friend bool operator != (const StringRef &lhs, const StringRef &rhs)
    {
        return !(lhs == rhs);
    }

    friend bool operator < (const StringRef &lhs, const StringRef &rhs)
    {
        auto leftSize = lhs.size;
        auto rightSize = rhs.size;
        int cmp = memcmp(lhs.data, rhs.data, std::min(leftSize, rightSize));
        return cmp < 0 || (cmp == 0 && leftSize < rightSize);
    }

    friend bool operator > (const StringRef &lhs, const StringRef &rhs)
    {
        auto leftSize = lhs.size;
        auto rightSize = rhs.size;
        int cmp = memcmp(lhs.data, rhs.data, std::min(leftSize, rightSize));
        return cmp > 0 || (cmp == 0 && leftSize > rightSize);
    }

    std::string ToString() const
    {
        return std::string(data, size);
    }
};
}
}
#endif // OMNI_RUNTIME_STRING_REF_H
