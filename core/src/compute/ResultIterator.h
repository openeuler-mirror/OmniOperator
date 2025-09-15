/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#pragma once

#include "ColumnarBatchIterator.h"
#include "metrics/omni_metrics.h"

namespace omniruntime {

class ResultIterator {
public:
    explicit ResultIterator(std::unique_ptr<ColumnarBatchIterator> iter)
        : iter_(std::move(iter)), next_(nullptr) {}

    // copy constructor and copy assignment (deleted)
    ResultIterator(const ResultIterator &in) = delete;

    ResultIterator &operator=(const ResultIterator &) = delete;

    // move constructor and move assignment
    ResultIterator(ResultIterator &&in) = default;

    ResultIterator &operator=(ResultIterator &&in) = default;

    bool HasNext()
    {
        CheckValid();
        GetNext();
        return next_ != nullptr;
    }

    vec::VectorBatch *Next()
    {
        CheckValid();
        GetNext();
        auto tmp = next_;
        next_ = nullptr;
        return tmp;
    }

    // For testing and benchmarking.
    ColumnarBatchIterator *GetInputIter()
    {
        return iter_.get();
    }

    void SetExportNanos(int64_t exportNanos)
    {
        exportNanos_ = exportNanos;
    }

    int64_t GetExportNanos() const
    {
        return exportNanos_;
    }

    OmniMetrics* getMetrics();

private:
    void CheckValid() const
    {
        if (iter_ == nullptr) {
            throw std::runtime_error("ResultIterator: the underlying iterator has expired.");
        }
    }

    void GetNext()
    {
        if (next_ == nullptr) {
            next_ = iter_->Next();
        }
    }

    std::unique_ptr<ColumnarBatchIterator> iter_;
    vec::VectorBatch *next_;
    int64_t exportNanos_{0};
};
}
