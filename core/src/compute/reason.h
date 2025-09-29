/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
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
#ifndef __REASON_H__
#define __REASON_H__

#include <chrono>
#include <future>

namespace omniruntime {
namespace compute {

using ContinueFuture = std::future<void>;

class OmniFuture {
public:
    /// Create an invalid Future
    /// RESULT.valid() == false
    static ContinueFuture makeEmpty()
    {
        ContinueFuture emptyFuture;
        return std::move(emptyFuture);
    }

    static bool isReady(const ContinueFuture& future)
    {
        try {
            return future.wait_for(std::chrono::seconds(0)) == std::future_status::ready;
        } catch (const std::future_error& e) {
            return false;
        }
    }

    static ContinueFuture collectAll(std::vector<ContinueFuture> futures)
    {
        for (auto& future : futures) {
            future.get();
        }

        ContinueFuture future;
        return std::move(future);
    }

    /// create valid Futures
    /// RESULT.valid == true
    /// RESULT.isReady == true
    static std::vector<ContinueFuture> createValidFutures(uint32_t count)
    {
        std::vector<ContinueFuture> futures;
        futures.reserve(count);

        for (auto i = 0; i < count; ++i) {
            std::promise<void> promise;
            promise.set_value();
            futures.emplace_back(promise.get_future());
        }

        return std::move(futures);
    }
};

 
enum class StopReason {
    /// Keep running.
    kNone,
    /// Go off thread and do not schedule more activity.
    kPause,
    /// Stop and free all. This is returned once and the thread that gets this
    /// value is responsible for freeing the state associated with the thread.
    /// Other threads will get kAlreadyTerminated after the first thread has
    /// received kTerminate.
    kTerminate,
    kAlreadyTerminated,
    /// Go off thread and then enqueue to the back of the runnable queue.
    kYield,
    /// Must wait for external events.
    kBlock,
    /// No more data to produce.
    kAtEnd,
    kAlreadyOnThread
};
 
enum class BlockingReason {
    kNotBlocked,
    kWaitForConsumer,
    kWaitForSplit,
    /// Some operators can get blocked due to the producer(s) (they are
    /// currently waiting data from) not having anything produced. Used by
    /// LocalExchange, LocalMergeExchange, Exchange and MergeExchange operators.
    kWaitForProducer,
    kWaitForJoinBuild,
    /// For a build operator, it is blocked waiting for the probe operators to
    /// finish probing before build the next hash table from one of the
    /// previously spilled partition data. For a probe operator, it is blocked
    /// waiting for all its peer probe operators to finish probing before
    /// notifying the build operators to build the next hash table from the
    /// previously spilled data.
    kWaitForJoinProbe,
    /// Used by MergeJoin operator, indicating that it was blocked by the right
    /// side input being unavailable.
    kWaitForMergeJoinRightSide,
    kWaitForMemory,
    kWaitForConnector,
    /// Build operator is blocked waiting for all its peers to stop to run group
    /// spill on all of them.
    kWaitForSpill,
    /// Some operators (like Table Scan) may run long loops and can 'voluntarily'
    /// exit them because Task requested to yield or stop or after a certain time.
    /// This is the blocking reason used in such cases.
    kYield,
    /// Operator is blocked waiting for its associated query memory arbitration to
    /// finish.
    kWaitForArbitration,
    kWaitForUnionBuild,
};
} // end of compute
} // end of omniruntime
#endif