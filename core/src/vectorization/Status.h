/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: visitor class for expressions
 */

#pragma once
#include <string>
#include <utility>

#include <fmt/format.h>
#include <fmt/ostream.h>
#include <util/compiler_util.h>

namespace omniruntime::vectorization {
/// This enum represents common categories of errors found in the library. These
/// are not meant to cover every specific error situation, but rather cover
/// broader categories of errors. Therefore, additions to this list should be
/// infrequent.
///
/// Errors should be further described by attaching an error message to the
/// Status object.
///
/// The error classes are loosely defined as follows:
///
/// - kOk: A successful operation. No errors.
///
/// - kUserError: An error triggered by bad input from an API user. The user
///   in this context usually means the program using Velox (or its end users).
///
/// - kTypeError: An error triggered by a logical type mismatch (e.g. expecting
///   BIGINT but REAL provided).
///
/// - kIndexError: An error triggered by the index of something being invalid or
///   out-of-bounds.
///
/// - kKeyError: An error triggered by the key of something in a map/set being
///   invalid or not found.
///
/// - kAlreadyExists: An error triggered by an operation meant to create some
///   form of resource which already exists.
///
/// - kOutOfMemory: A failure triggered by a lack of available memory to
///   complete the operation.
///
/// - kIOError: An error triggered by IO failures (e.g: network or disk/SSD read
///   error).
///
/// - kCancelled: An error triggered because a certain resource required has
///   been stopped or cancelled.
///
/// - kInvalid: An error triggered by an invalid program state. Usually
///   triggered by bugs.
///
/// - kUnknownError: An error triggered by an unknown cause. Also usually
///   triggered by bugs. Should be used scarcely, favoring a more specific error
///   class above.
///
/// - kNotImplemented: An error triggered by a feature not being implemented
///   yet.
///
enum class StatusCode : int8_t {
    kOK = 0,
    kUserError = 1,
    kTypeError = 2,
    kIndexError = 3,
    kKeyError = 4,
    kAlreadyExists = 5,
    kOutOfMemory = 6,
    kIOError = 7,
    kCancelled = 8,
    kInvalid = 9,
    kUnknownError = 10,
    kNotImplemented = 11,
};

class Status {
public:
    // Create a success status.
    constexpr Status() noexcept : state_(nullptr) {}

    Status(StatusCode code, std::string msg);

    /// Return an error status for user errors.
    template <typename... Args>
    static Status UserError(Args &&... args)
    {
        return Status::FromArgs(StatusCode::kUserError, std::forward<Args>(args)...);
    }

    /// Return a success status.
    static Status OK()
    {
        return Status();
    }

    /// Return true iff the status indicates success.
    constexpr bool ok() const
    {
        return (state_ == nullptr);
    }

private:
    template <typename... Args>
    static Status
    FromArgs(StatusCode code, fmt::string_view fmt, Args &&... args)
    {
        return Status(code, fmt::vformat(fmt, fmt::make_format_args(args...)));
    }

    struct State {
        StatusCode code;
        std::string msg;
    };

    // OK status has a `nullptr` state_.  Otherwise, `state_` points to
    // a `State` structure containing the error code and message(s)
    State *state_;
};
}
