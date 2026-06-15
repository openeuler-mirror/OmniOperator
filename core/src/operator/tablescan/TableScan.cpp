/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#include <cstdint>
#include <chrono>
#include "TableScan.h"
#include "vector/vector_helper.h"
#include "vector/vector_batch.h"
#include "connectors/Split.h"
#include "plannode/planNode.h"
#include "compute/task.h"
#include "codegen/time_util.h"
#include "type/data_type.h"

namespace omniruntime {
namespace op {
using namespace omniruntime::vec;
using namespace omniruntime::connector;
using namespace std;

TableScanOperatorFactory::TableScanOperatorFactory(const std::shared_ptr<const TableScanNode> planNode,
    const uint64_t maxRowCount, const common::ReadMode readMode, std::shared_ptr <SplitsStore> splitsStore)
    : planNode_(planNode),
    maxRowCount_(maxRowCount),
    readMode_(readMode),
    splitsStore_(splitsStore) {}

TableScanOperatorFactory::~TableScanOperatorFactory() = default;

TableScanOperatorFactory *TableScanOperatorFactory::CreateTableScanOperatorFactory(
    const std::shared_ptr<const TableScanNode> planNode,
    const config::QueryConfig &queryConfig,
    std::shared_ptr <SplitsStore> tableScanSplitsStore)
{
    uint64_t maxRowCount = queryConfig.MaxBatchSize();
    common::ReadMode readMode = queryConfig.HdfsReadMode();
    auto pOperatorFactory = new TableScanOperatorFactory(planNode, maxRowCount, readMode, tableScanSplitsStore);
    return pOperatorFactory;
}

Operator *TableScanOperatorFactory::CreateOperator()
{
    auto pTableScanOperator = new TableScanOperator(planNode_, maxRowCount_, readMode_, splitsStore_);
    return pTableScanOperator;
}

TableScanOperator::TableScanOperator(
    const std::shared_ptr<const TableScanNode> tableScanNode, const uint64_t maxRowCount, const common::ReadMode readMode,
    std::shared_ptr <SplitsStore> splitsStore)
    : tableHandle_(tableScanNode->tableHandle()),
    columnHandles_(tableScanNode->assignments()),
    outputType_(tableScanNode->getRowTypePtr()),
    splitsStore_(splitsStore),
    maxReadBatchSize_(maxRowCount),
    readMode_(readMode),
    connector_(connector::getConnector(tableHandle_->connectorId())) {}

TableScanOperator::~TableScanOperator() {}

int32_t TableScanOperator::AddInput(omniruntime::vec::VectorBatch *vecBatch)
{
    return 0;
}

int32_t TableScanOperator::GetOutput(VectorBatch **outputVecBatch)
{
    if (noMoreSplits_) {
        SetStatus(OMNI_STATUS_FINISHED);
        return 0;
    } else {
        SetStatus(OMNI_STATUS_NORMAL);
    }

    for (;;) {
        if (noMoreSplits_) {
            SetStatus(OMNI_STATUS_FINISHED);
            return 0;
        } else {
            SetStatus(OMNI_STATUS_NORMAL);
        }

        if (needNewSplit_) {
            const auto hasNewSplit = getSplit();
            if (!hasNewSplit) {
                continue;
            }
        }

        std::optional < VectorBatch * > dataOptional;
        uint64_t nextWallNanos = 0;
        {
            const auto t0 = std::chrono::steady_clock::now();
            dataOptional = dataSource_->next(maxReadBatchSize_);
            const auto t1 = std::chrono::steady_clock::now();
            nextWallNanos = static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count());
        }
        if (dataSource_ != nullptr) {
            stats().totalScanTimeNanos += nextWallNanos;
        }
        if (!dataOptional.has_value()) {
            return 0;
        }

        VectorBatch *data = std::move(dataOptional).value();
        if (data != nullptr) {
            if (data->GetRowCount() == 0) {
                needNewSplit_ = true;
                continue;
            }
            *outputVecBatch = data;
            stats().AddInputVector(
                data->CalculateTotalSize(), 1, static_cast<uint64_t>(data->GetRowCount()));
            return 0;
        }
        needNewSplit_ = true;
    }
}

bool TableScanOperator::getSplit()
{
    if (splitsStore_ == nullptr) {
        noMoreSplits_ = true;
        return false;
    }

    if (splitsStore_->splits.empty()) {
        if (splitsStore_->noMoreSplits) {
            noMoreSplits_ = true;
        }
        return false;
    }

    Split split;

    split = std::move(splitsStore_->splits.front());
    splitsStore_->splits.pop_front();

    if (splitsStore_->splits.empty()) {
        splitsStore_->noMoreSplits = true;
    }

    if (!split.hasConnectorSplit()) {
        noMoreSplits_ = true;
        return false;
    }

    const auto &connectorSplit = split.connectorSplit;
    needNewSplit_ = false;

    if (dataSource_ == nullptr) {
        dataSource_ = connector_->createDataSource(outputType_, tableHandle_, columnHandles_);
    }

    dataSource_->addSplit(connectorSplit, maxReadBatchSize_, readMode_);

    return true;
}

bool TableScanOperator::isFinished()
{
    return noMoreSplits_;
}

OmniStatus TableScanOperator::Close()
{
    if (dataSource_ != nullptr) {
        dataSource_->cancel();
    }

    return OMNI_STATUS_NORMAL;
}

} // namespace op
} // namespace omniruntime
