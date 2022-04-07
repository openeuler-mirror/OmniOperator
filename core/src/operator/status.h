//
// Created by root on 5/31/21.
//

#ifndef OMNI_RUNTIME_STATUS_H
#define OMNI_RUNTIME_STATUS_H

using OmniStatus = enum OmniStatus {
    OMNI_STATUS_NORMAL = 0,
    OMNI_STATUS_FINISHED = 1,
    OMNI_STATUS_ERROR = -1,
};

#endif // OMNI_RUNTIME_STATUS_H
