/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
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
};

template <typename T> struct NullValueTypeTraits<T *> {
    static bool IsNullValue(const T *&value)
    {
        return value == nullptr;
    }
};

template <> struct NullValueTypeTraits<type::StringRef> {
    static bool IsNullValue(const type::StringRef &value)
    {
        return value.size == 0;
    }
};
}
}
#endif // OMNI_RUNTIME_NULL_KEY_TRAITS_H
