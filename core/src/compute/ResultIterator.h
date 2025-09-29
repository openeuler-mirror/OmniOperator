/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
