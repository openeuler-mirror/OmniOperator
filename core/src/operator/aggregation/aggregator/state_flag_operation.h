/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 * Description: state flag operation
 */
#ifndef OMNI_RUNTIME_STATE_FLAG_OPERATION_H
#define OMNI_RUNTIME_STATE_FLAG_OPERATION_H

#include <cstdint>

namespace omniruntime {
namespace op {
enum class AggValueState : int8_t {
    EMPTY_VALUE,
    NORMAL,
    OVERFLOWED
};

#pragma pack(push, 1)

template <typename ResultType> struct BaseState {
    ResultType value;
    AggValueState valueState;

    bool IsValid() const
    {
        return valueState != AggValueState::OVERFLOWED;
    }

    bool IsOverFlowed() const
    {
        return valueState == AggValueState::OVERFLOWED;
    }

    bool IsEmpty() const
    {
        return valueState == AggValueState::EMPTY_VALUE;
    }

    void SetOverFlow()
    {
        valueState = AggValueState::OVERFLOWED;
    }
};

template <typename ResultType> struct BaseCountState {
    ResultType value;
    int64_t count;

    bool IsValid() const
    {
        return count >= 0;
    }

    bool IsOverFlowed() const
    {
        return count == -1;
    }

    bool IsEmpty() const
    {
        return count == 0;
    }

    void SetOverFlow()
    {
        count = -1;
    }
};

struct BaseStdDevState {
    double mean;
    double m2;
    double count;

    bool IsEmpty() const
    {
        return count == 0;
    }
};

#pragma pack(pop)

/*
 * Different types have distinct flag handlers.
 * A flag handler has three main methods:
 * 1. isValid() - Checks if the flag is valid, e.g., whether an overflow occurred.
 * 2. overflowed() - Indicates if the flag has changed due to an overflow.
 * 3. update(T&, const T&) - Updates the flag after the value is accumulated.
 * For sum operations: the flag uses int8_t to indicate overflow.
 * For average operations: the flag uses a counter to track the number of elements.
 */
struct StateValueHandler {
    static bool IsValid(AggValueState state)
    {
        return state != AggValueState::OVERFLOWED;
    }

    /*
     * state has two value now: NORMAL | EMPTY_VALUE
     */
    template <typename T> static void Update(AggValueState &state, const T count)
    {
        state = count > 0 ? AggValueState::NORMAL : state;
    }

    static AggValueState Overflowed()
    {
        return AggValueState::OVERFLOWED;
    }

    static void SetOverFlowed(AggValueState &state)
    {
        state = AggValueState::OVERFLOWED;
    }
};

struct StateCountHandler {
    using CountType = int64_t;

    static bool IsValid(CountType state)
    {
        return state >= 0;
    }

    template <typename T> static void Update(CountType &state, const T count)
    {
        state += count;
    }

    static CountType Overflowed()
    {
        return -1;
    }

    static void SetOverFlowed(CountType &state)
    {
        state = -1;
    }
};
}
}
#endif // OMNI_RUNTIME_STATE_FLAG_OPERATION_H
