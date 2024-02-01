/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2022-2024. All rights reserved.
 * @Description: abstract class spiller
 */

#ifndef OMNI_RUNTIME_SPILLER_H
#define OMNI_RUNTIME_SPILLER_H

#include "util/error_code.h"
#include "spill_iterator.h"
#include "spill_tracker.h"

namespace omniruntime {
namespace op {
class Spiller {
public:
    Spiller() = default;

    virtual ~Spiller() = default;

    virtual void SetSpillTracker(SpillTracker *tracker) = 0;

    virtual ErrorCode Spill(SpillUnitIter &iterator) = 0;

    virtual void MergeFromDiskAndMemory(SpillUnitIter &memoryIter) = 0;

    virtual bool HasNext() = 0;

    virtual SpillUnit *Next() = 0;

    virtual uint64_t GetSpilledBytes() = 0;
};
}
}

#endif // OMNI_RUNTIME_SPILLER_H
