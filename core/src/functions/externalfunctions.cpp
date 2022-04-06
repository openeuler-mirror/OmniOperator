/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: registry function name
 */
#include "common.h"

extern "C" {
// Example functions
INLINE int32_t StringLength(char *str, int32_t length)
{
    return length;
}

#define INCREMENT(TYPE)                  \
    INLINE TYPE Increment_##TYPE(TYPE x) \
    {                                    \
        return x + 1;                    \
    }
INCREMENT(int32)
INCREMENT(int64)
#undef INCREMENT

// Add your functions below, following the format above
}