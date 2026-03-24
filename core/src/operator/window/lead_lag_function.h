/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Description: Lead and Lag window function implementations
 */

#ifndef __LEAD_LAG_FUNCTION_H__
#define __LEAD_LAG_FUNCTION_H__

#include <memory>
#include "window_function.h"
#include "vector/vector.h"
#include "vector/vector_helper.h"
#include "operator/pages_index.h"

namespace omniruntime {
namespace op {

/// Lead/Lag window function base class
/// Implements the common logic for lead() and lag() window functions.
/// Lead returns the value at offset rows after the current row within the partition.
/// Lag returns the value at offset rows before the current row within the partition.
/// Both support optional default value when the offset goes out of partition bounds.
template <bool isLag>
class LeadLagFunction : public WindowFunction {
public:
    /// Constructor
    /// @param frame Window frame information
    /// @param inputType Data type of the input value column
    /// @param outputType Data type of the output column (same as input)
    /// @param valueChannel The column index for the value argument
    /// @param offset The offset value (default is 1)
    /// @param hasDefaultValue Whether a default value is specified
    /// @param defaultValueChannel The column index for default value (-1 if constant or not specified)
    LeadLagFunction(std::unique_ptr<WindowFrameInfo> frame, DataTypePtr inputType, DataTypePtr outputType,
        int32_t valueChannel, int64_t offset = 1, bool hasDefaultValue = false, int32_t defaultValueChannel = -1)
        : WindowFunction(std::move(frame), std::move(inputType), std::move(outputType)),
          valueChannel_(valueChannel),
          offset_(offset),
          hasDefaultValue_(hasDefaultValue),
          defaultValueChannel_(defaultValueChannel),
          windowIndex_(nullptr),
          currentPosition_(0)
    {}

    ~LeadLagFunction() override = default;

    void Reset(WindowIndex *pWindowIndex) override
    {
        windowIndex_ = pWindowIndex;
        currentPosition_ = 0;
    }

    /// frameStart and frameEnd are partition-relative offsets (0-based) computed by
    /// WindowPartition::GetFrameRange. For Lead/Lag with UNBOUNDED frame they span
    /// the whole partition: frameStart=0, frameEnd=partitionSize-1.
    void ProcessRow(VectorBatch *inputVecBatchForAgg, BaseVector *outputColumn, int32_t outputIndex,
        int32_t peerGroupStart, int32_t peerGroupEnd, int32_t frameStart, int32_t frameEnd) override
    {
        int32_t currentRowInPartition = currentPosition_;

        int32_t targetRowInPartition;
        if constexpr (isLag) {
            targetRowInPartition = currentRowInPartition - static_cast<int32_t>(offset_);
        } else {
            targetRowInPartition = currentRowInPartition + static_cast<int32_t>(offset_);
        }

        if (targetRowInPartition >= frameStart && targetRowInPartition <= frameEnd) {
            int32_t absoluteRow = windowIndex_->GetStart() + targetRowInPartition;
            CopyValueFromPartition(outputColumn, outputIndex, absoluteRow);
        } else {
            SetDefaultOrNull(outputColumn, outputIndex);
        }

        currentPosition_++;
    }

private:
    void SetDefaultOrNull(BaseVector *outputColumn, int32_t outputIndex)
    {
        if (hasDefaultValue_ && defaultValueChannel_ >= 0) {
            int32_t absoluteRow = windowIndex_->GetStart() + currentPosition_;
            CopyDefaultValueFromPartition(outputColumn, outputIndex, absoluteRow);
        } else {
            outputColumn->SetNull(outputIndex);
        }
    }

    void CopyValueFromPartition(BaseVector *outputColumn, int32_t outputIndex, int32_t sourceRow)
    {
        if (windowIndex_ == nullptr || windowIndex_->GetPagesIndex() == nullptr) {
            outputColumn->SetNull(outputIndex);
            return;
        }

        PagesIndex *pagesIndex = windowIndex_->GetPagesIndex();
        uint64_t *valueAddresses = pagesIndex->GetValueAddresses();
        BaseVector **vectors = pagesIndex->GetColumns()[valueChannel_];

        uint64_t sliceAddress = valueAddresses[sourceRow];
        uint32_t vectorIndex = DecodeSliceIndex(sliceAddress);
        uint32_t vectorPosition = DecodePosition(sliceAddress);

        BaseVector *sourceVector = vectors[vectorIndex];
        
        if (sourceVector->IsNull(vectorPosition)) {
            outputColumn->SetNull(outputIndex);
        } else {
            vec::VectorHelper::CopyValue(sourceVector, vectorPosition, outputColumn, outputIndex);
        }
    }

    /// Copy default value from partition at given row to output
    void CopyDefaultValueFromPartition(BaseVector *outputColumn, int32_t outputIndex, int32_t sourceRow)
    {
        if (windowIndex_ == nullptr || windowIndex_->GetPagesIndex() == nullptr || defaultValueChannel_ < 0) {
            outputColumn->SetNull(outputIndex);
            return;
        }

        PagesIndex *pagesIndex = windowIndex_->GetPagesIndex();
        uint64_t *valueAddresses = pagesIndex->GetValueAddresses();
        BaseVector **vectors = pagesIndex->GetColumns()[defaultValueChannel_];

        uint64_t sliceAddress = valueAddresses[sourceRow];
        uint32_t vectorIndex = DecodeSliceIndex(sliceAddress);
        uint32_t vectorPosition = DecodePosition(sliceAddress);

        BaseVector *sourceVector = vectors[vectorIndex];
        
        if (sourceVector->IsNull(vectorPosition)) {
            outputColumn->SetNull(outputIndex);
        } else {
            vec::VectorHelper::CopyValue(sourceVector, vectorPosition, outputColumn, outputIndex);
        }
    }

    int32_t valueChannel_;
    int64_t offset_;
    bool hasDefaultValue_;
    int32_t defaultValueChannel_;
    WindowIndex *windowIndex_;
    int32_t currentPosition_;
};

// Type aliases for convenience
using LagFunction = LeadLagFunction<true>;
using LeadFunction = LeadLagFunction<false>;

} // namespace op
} // namespace omniruntime

#endif // __LEAD_LAG_FUNCTION_H__
