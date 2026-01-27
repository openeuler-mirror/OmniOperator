/*
* Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */

#include "execution_context.h"

namespace omniruntime::op {
namespace {
auto throwError(const std::exception_ptr &exceptionPtr)
{
    std::rethrow_exception(toOmniException(exceptionPtr));
}

std::exception_ptr toOmniUserError(const std::string &message)
{
    return std::make_exception_ptr(OmniException("USER_ERROR", "INVALID_ARGUMENT"));
}
}

void ExecutionContext::SetError(int32_t index, const std::exception_ptr &exceptionPtr)
{
    if (throwOnError_) {
        throwError(exceptionPtr);
    }

    if (captureErrorDetails_) {
        AddError(index, toOmniException(exceptionPtr), errors_);
    } else {
        AddError(index, errors_);
    }
}

void ExecutionContext::SetStatus(vector_size_t index, const vectorization::Status &status)
{
    OMNI_CHECK(!status.ok(), "Status must be an error");

    if (status.isUserError()) {
        if (throwOnError_) {
            OMNI_USER_FAIL(status.message());
        }
        if (captureErrorDetails_) {
            AddError(index, toOmniUserError(status.message()), errors_);
        } else {
            AddError(index, errors_);
        }
    } else {
        OMNI_FAIL(status.message());
    }
}
}
