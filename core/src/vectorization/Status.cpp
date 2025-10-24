//
// Created by root on 11/3/25.
//

#include "Status.h"

namespace omniruntime::vectorization {
Status::Status(StatusCode code, std::string msg)
{
    if (UNLIKELY(code == StatusCode::kOK)) {
        throw std::invalid_argument("Cannot construct ok status with message");
    }
    state_ = new State;
    state_->code = code;
    state_->msg = std::move(msg);
}
}
