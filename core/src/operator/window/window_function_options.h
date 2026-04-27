#ifndef __WINDOW_FUNCTION_OPTIONS_H__
#define __WINDOW_FUNCTION_OPTIONS_H__

#include <cstdint>

namespace omniruntime {
namespace op {

struct WindowFunctionOptions {
    static const int32_t IGNORE_NULLS = 1;
    static const int32_t INVALID_CHANNEL = -1;
    static const int32_t PENDING_CHANNEL = -2;

    int32_t flags = 0;
    int32_t nthValueOffset = 1;
    int32_t nthValueOffsetChannel = INVALID_CHANNEL;

    bool IsIgnoreNulls() const
    {
        return (flags & IGNORE_NULLS) != 0;
    }
};

} // namespace op
} // namespace omniruntime

#endif
