/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: common
 */

#ifndef __COMMON_JOIN_H__
#define __COMMON_JOIN_H__

#include <cstdint>
#include "plannode/planFragment.h"

namespace omniruntime {
namespace op {
constexpr int32_t DEFAULT_ROW_SIZE = sizeof(int32_t);

enum class SortMergeJoinAddInputCode {
    SMJ_NEED_STREAM_TBL_INFO = 0,
    SMJ_NEED_BUFFER_TBL_INFO = 1,
    SMJ_NEED_ADD_STREAM_TBL_DATA = 2,
    SMJ_NEED_ADD_BUFFER_TBL_DATA = 3,
    SMJ_SCAN_FINISH = 4,
    SMJ_FETCH_JOIN_DATA = 5
};
}
}


#endif // __COMMON_JOIN_H__
