/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2023. All rights reserved.
 */
#ifndef OMNI_RUNTIME_NULL_KEY_TRAITS_H
#define OMNI_RUNTIME_NULL_KEY_TRAITS_H

namespace omniruntime {
namespace op {
template <typename T> struct NullValueTypeTraits {
    static bool IsNullValue(const T &value)
    {
        return false;
    }

    static T GetNullKey()
    {
        return T();
    }
};

// handle primitive type
template <typename T> struct NullValueTypeTraits<T *> {
    static bool IsNullValue(const T *&value)
    {
        return value == nullptr;
    }

    static T GetNullKey()
    {
        return T();
    }
};

template <> struct NullValueTypeTraits<type::StringRef> {
    static bool IsNullValue(const type::StringRef &value)
    {
        return value.size == 0;
    }

    static type::StringRef GetNullKey()
    {
        return type::StringRef();
    }
};

// Only for unit test, we think value is null when uint32_t value equal to zero .
template <typename T> struct NullValueTypeTraits<std::unique_ptr<T>> {
    static bool IsNullValue(const std::unique_ptr<T> &value)
    {
        return NullValueTypeTraits<T>::IsNullValue(*value);
    }

    static std::unique_ptr<T> GetNullKey()
    {
        return std::make_unique<T>();
    }
};

// Only for in unit test, we think string is null when string.length() equal to zero .
template <> struct NullValueTypeTraits<std::string> {
    static bool IsNullValue(const std::string &str)
    {
        return str.empty();
    }

    static std::string GetNullKey()
    {
        return std::string();
    }
};

// Only for in unit test, we think string is null when unsigned int equal to zero .
template <> struct NullValueTypeTraits<unsigned int> {
    static bool IsNullValue(const unsigned int &key)
    {
        return key == 0;
    }

    static unsigned int GetNullKey()
    {
        return 0;
    }
};
}
}
#endif // OMNI_RUNTIME_NULL_KEY_TRAITS_H
