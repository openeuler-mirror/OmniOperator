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
    StringRef(const T *d, size_t s) : data(reinterpret_cast<const char *>(d)), size(s)
    {}

    StringRef(const std::string &s) : data(s.data()), size(s.size()) {}

    explicit StringRef(const char *data_) : data(data_), size(strlen(data_)) {}

    bool operator == (StringRef rhs) const
    {
        if (size != rhs.size) {
            return false;
        }
        if (size == 0) {
            return true;
        }
        return (0 == memcmp(data, rhs.data, size));
    }

    bool operator != (StringRef rhs) const
    {
        return !(this->operator == (rhs));
    }

    bool operator < (StringRef rhs) const
    {
        int cmp = memcmp(data, rhs.data, std::min(size, rhs.size));
        return cmp < 0 || (cmp == 0 && size < rhs.size);
    }

    bool operator > (StringRef rhs) const
    {
        int cmp = memcmp(data, rhs.data, std::min(size, rhs.size));
        return cmp > 0 || (cmp == 0 && size > rhs.size);
    }

    std::string toString() const
    {
        return std::string(data, size);
    }
};
}
}
#endif // OMNI_RUNTIME_STRING_REF_H
