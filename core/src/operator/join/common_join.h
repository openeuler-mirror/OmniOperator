/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: sort merge join implementations
 */

#ifndef __COMMON_JOIN_H__
#define __COMMON_JOIN_H__

namespace omniruntime {
namespace op {
using JoinType = enum JoinType {
    OMNI_JOIN_TYPE_INNER = 0,
    OMNI_JOIN_TYPE_LEFT,
    OMNI_JOIN_TYPE_RIGHT,
    OMNI_JOIN_TYPE_FULL,
};


enum SortMergeJoinAddInputCode {
    SMJ_NEED_STREAM_TBL_INFO = 0,
    SMJ_NEED_BUFFER_TBL_INFO = 1,
    SMJ_NEED_ADD_STREAM_TBL_DATA = 2,
    SMJ_NEED_ADD_BUFFER_TBL_DATA = 3,
    SMJ_NO_RESULT = 4,
    SMJ_FETCH_JOIN_DATA = 5
};
}
}


#endif // __COMMON_JOIN_H__
