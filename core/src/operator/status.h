/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2022-2023. All rights reserved.
 * @Description: OmniStatus declaring
 */

#ifndef OMNI_RUNTIME_STATUS_H
#define OMNI_RUNTIME_STATUS_H

using OmniStatus = enum OmniStatus {
    OMNI_STATUS_NORMAL = 0,
    OMNI_STATUS_FINISHED = 1,
    OMNI_STATUS_ERROR = -1,
};

#endif // OMNI_RUNTIME_STATUS_H
