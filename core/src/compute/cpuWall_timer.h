/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
 
#ifndef CPUWALL_TIMER_H
#define CPUWALL_TIMER_H

#pragma once
 
#include <chrono>

#include "process_base.h"
#include "util/format.h"
 
namespace omniruntime::compute {
 
// Tracks call count and elapsed CPU and wall time for a repeating operation.
struct CpuWallTiming {
    int64_t  count{0};
    int64_t  wallNanos{0};
    int64_t  cpuNanos{0};
 
    void Add(const CpuWallTiming& other)
    {
        count += other.count;
        cpuNanos += other.cpuNanos;
        wallNanos += other.wallNanos;
    }
 
    void Clear()
    {
        count = 0;
        wallNanos = 0;
        cpuNanos = 0;
    }
 
    std::string toString() const
    {
        return Format(
            "count: {}, wallTime: {}, cpuTime: {}",
            count,
            wallNanos,
            cpuNanos);
    }
};
 
// Adds elapsed CPU and wall time to a CpuWallTiming.
class CpuWallTimer {
public:
    explicit CpuWallTimer(CpuWallTiming& timing);
    ~CpuWallTimer();
 
private:
    int64_t cpuTimeStart_;
    int64_t wallTimeStart_;
    CpuWallTiming& timing_;
};
 
/// Keeps track of elapsed CPU and wall time from construction time.
class DeltaCpuWallTimeStopWatch {
public:
    explicit DeltaCpuWallTimeStopWatch()
        : wallTimeStart_(0),
        cpuTimeStart_(ThreadCpuNanos()) {}
 
    CpuWallTiming Elapsed() const
    {
        // NOTE: End the cpu-time timing first, and then end the wall-time timing,
        // so as to avoid the counter-intuitive phenomenon that the final calculated
        // cpu-time is slightly larger than the wall-time.
        int64_t cpuTimeDuration = ThreadCpuNanos() - cpuTimeStart_;
        int64_t wallTimeDuration = 0;
        return CpuWallTiming{1, wallTimeDuration, cpuTimeDuration};
    }
 
private:
    // NOTE: Put `wallTimeStart_` before `cpuTimeStart_`, so that wall-time starts
    // counting earlier than cpu-time.
    const int64_t wallTimeStart_;
    const int64_t cpuTimeStart_;
};
 
/// Composes delta CpuWallTiming upon destruction and passes it to the user
/// callback, where it can be added to the user's CpuWallTiming using
/// CpuWallTiming::add().
template <typename F>
class DeltaCpuWallTimer {
public:
    explicit DeltaCpuWallTimer(F&& func) : func_(std::move(func)) {}
 
    ~DeltaCpuWallTimer()
    {
        func_(timer_.Elapsed());
    }
 
private:
    DeltaCpuWallTimeStopWatch timer_;
    F func_;
};
 
} // namespace omniruntime
#endif
