/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * @Description: vector batch spiller implements
 */
#ifndef OMNI_RUNTIME_ERROR_CODE_H
#define OMNI_RUNTIME_ERROR_CODE_H

#include <string>

namespace omniruntime {
namespace op {
enum class ErrorCode {
    SUCCESS = 0,
    EMPTY_PATH,
    INVALID_PATH,
    PATH_EXIST,
    MKDIR_FAILED,
    DISK_STAT_FAILED,
    DISK_SPACE_NOT_ENOUGH,
    MKSTEMP_FAILED,
    UNLINK_FAILED,
    WRITE_FAILED,
    EXCEED_SPILL_THRESHOLD,
    READ_FAILED,
    LOAD_LAZY_VECTOR_FAILED,
    MEM_CAP_EXCEEDED,
    JVM_FAILED,
    UNSUPPORTED
};

std::string &GetErrorCode(const ErrorCode &errorCode);

std::string &GetErrorMessage(const ErrorCode &errorCode);
}
}
#endif // OMNI_RUNTIME_ERROR_CODE_H
