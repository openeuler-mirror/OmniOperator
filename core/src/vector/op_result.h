/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

#ifndef OMNI_RUNTIME_OP_RESULT_H
#define OMNI_RUNTIME_OP_RESULT_H

namespace omniruntime {
namespace vec {
enum class OpStatus {
    SUCCESS = 0,
    OP_OVERFLOW = 1,
    DIVIDE_BY_ZERO = 2,
    FAIL = 3
};

template <typename T> class OpResult {
public:
public:
    OpResult(OpStatus status, T data) : status(status), data(data) {};

    ~OpResult() {}

    OpStatus getStatus()
    {
        return status;
    }

    T get()
    {
        return data;
    }

private:
    T data;
    OpStatus status;
};
}
}

#endif // OMNI_RUNTIME_OP_RESULT_H
