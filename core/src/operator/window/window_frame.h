/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * @Description: window operator implementations
 */

#ifndef __WINDOW_FRAME_H__
#define __WINDOW_FRAME_H__

namespace omniruntime {
namespace op {
using FrameType = enum FrameType {
    OMNI_FRAME_TYPE_RANGE = 0,
    OMNI_FRAME_TYPE_ROWS = 1
};

using FrameBoundType = enum FrameBoundType {
    OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING = 0,
    OMNI_FRAME_BOUND_PRECEDING = 1,
    OMNI_FRAME_BOUND_CURRENT_ROW = 2,
    OMNI_FRAME_BOUND_FOLLOWING = 3,
    OMNI_FRAME_BOUND_UNBOUNDED_FOLLOWING = 4
};

class WindowFrameInfo {
public:
    WindowFrameInfo()
        : type(OMNI_FRAME_TYPE_RANGE),
          startType(OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING),
          startChannel(INVALID_BOUND_CHANNEL),
          endType(OMNI_FRAME_BOUND_CURRENT_ROW),
          endChannel(INVALID_BOUND_CHANNEL)
    {}

    WindowFrameInfo(FrameType framType, FrameBoundType frameStartType, int32_t frameStartCol,
            FrameBoundType frameEndType, int32_t frameEndCol)
        : type(framType),
          startType(frameStartType),
          startChannel(frameStartCol),
          endType(frameEndType),
          endChannel(frameEndCol)
    {}

    ~WindowFrameInfo() {}

    FrameType GetType()
    {
        return type;
    }

    FrameBoundType GetStartType()
    {
        return startType;
    }

    int32_t GetStartChannel()
    {
        return startChannel;
    }

    FrameBoundType GetEndType()
    {
        return endType;
    }

    int32_t GetEndChannel()
    {
        return endChannel;
    }

public:
    static const int32_t INVALID_BOUND_CHANNEL = -1;

private:
    FrameType type;
    FrameBoundType startType;
    int32_t startChannel;
    FrameBoundType endType;
    int32_t endChannel;
};
}
}
#endif