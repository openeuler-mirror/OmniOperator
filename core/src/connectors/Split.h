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
 *
 * We modify this part of the code based on Velox to enable native execution of the Omni Spark operator,
 * modify the corresponding code.
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 */

#pragma once

#include <memory>
#include "connectors/Connector.h"

namespace omniruntime {
namespace connector {

struct Split {
    std::shared_ptr <omniruntime::connector::ConnectorSplit> connectorSplit = nullptr;
    int32_t groupId{-1}; // Bucketed group id (-1 means 'none').

    /// Indicates if this is a barrier split. A barrier split is used by task
    /// barrier processing which adds one barrier split to each leaf source node
    /// to signal the output drain processing.
    bool barrier{false};

    Split() = default;

    explicit Split(
        std::shared_ptr <omniruntime::connector::ConnectorSplit> &&connectorSplit_,
        int32_t groupId_ = -1)
        : connectorSplit(std::move(connectorSplit_)), groupId(groupId_) {}

    /// Called by the task barrier to create a special barrier split.
    static Split createBarrier()
    {
        static Split barrierSplit;
        barrierSplit.barrier = true;
        return barrierSplit;
    }

    Split(Split &&other) = default;

    Split(const Split &other) = default;

    Split &operator=(Split &&other) = default;

    Split &operator=(const Split &other) = default;

    bool isBarrier() const
    {
        return barrier;
    }

    inline bool hasConnectorSplit() const
    {
        return connectorSplit != nullptr;
    }

    inline bool hasGroup() const
    {
        return groupId != -1;
    }

    std::string toString() const
    {
        if (barrier) {
            return "BarrierSplit";
        } else {
            return "NULL";
        }
    }
};

/// Structure to accumulate splits for distribution.
struct SplitsStore {
    /// Arrived (added), but not distributed yet, splits.
    std::deque <Split> splits;
    /// Signal, that no more splits will arrive.
    bool noMoreSplits{false};
};

/// Structure contains the current info on splits for a particular plan node.
struct SplitsState {
    /// True if the source node is a table scan.
    bool sourceIsTableScan{false};

    /// Plan node-wide 'no more splits'.
    bool noMoreSplits{false};

    /// Keep the max added split's sequence id to deduplicate incoming splits.
    long maxSequenceId{std::numeric_limits<long>::min()};

    /// Map split group id -> split store.
    std::unordered_map <uint32_t, SplitsStore> groupSplitsStores;

    /// We need these due to having promises in the structure.
    SplitsState() = default;

    SplitsState(SplitsState const &) = delete;

    SplitsState &operator=(SplitsState const &) = delete;
};

}
}
