/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * @Description: vector batch spiller implements
 */
#include "vector_batch_spiller.h"
#include <sys/types.h>
#include "../vector/vector_helper.h"

namespace omniruntime {
namespace op {
using namespace omniruntime::type;
using namespace omniruntime::vec;

ErrorCode VectorBatchSpiller::Spill(SpillUnitIter &vecBatches)
{
    auto writer = new VectorBatchWriter(tracker, sourceTypes);
    auto result = writer->CreateTempFile(path);
    if (result != ErrorCode::SUCCESS) {
        return result;
    }

    writers.push_back(writer);
    return writer->WriteVecBatches(static_cast<VectorBatchUnitIter &>(vecBatches));
}

void VectorBatchSpiller::MergeFromDiskAndMemory(SpillUnitIter &memoryIter)
{
    std::vector<VecBatchWithPositionIterator *> diskIterators;
    auto writerSize = writers.size();
    for (size_t i = 0; i < writerSize; i++) {
        auto writer = writers[i];
        auto reader = new VectorBatchReader(writer->GetFd(), writer->GetFileLength(), tracker);
        reader->ReadFileTailAndHead();
        totalRowCount += reader->GetRowCount();
        auto spillIterator = new SpillIterator(reader);
        diskIterators.push_back(spillIterator);
    }

    std::vector<VectorBatch *> vecBatches;
    while (memoryIter.HasNext()) {
        auto vecBatch = static_cast<VectorBatchUnit *>(memoryIter.Next())->GetVectorBatch();
        vecBatches.push_back(vecBatch);
        totalRowCount += vecBatch->GetRowCount();
    }
    this->remainingRowCount = totalRowCount;

    if (vecBatches.size() > 0) {
        auto vecBatchUnitIter = new VectorBatchUnitIter(vecBatches);
        auto memoryIterator = new SpillIterator(vecBatchUnitIter);

        merger = new VectorBatchMerger(comparator);
        merger->MergeFromDiskAndMemory(diskIterators, memoryIterator);
    } else {
        merger = new VectorBatchMerger(comparator);
        merger->MergeFromDiskAndMemory(diskIterators, nullptr);
    }
}

bool VectorBatchSpiller::HasNext()
{
    return merger->HasNext();
}

template <typename V>
void SetValue(vec::BaseVector *inputVector, int32_t inputPosition, vec::BaseVector *outputVector,
              int32_t outputPosition)
{
    auto resultVector = static_cast<vec::Vector<V> *>(outputVector);
    if (inputVector->IsNull(inputPosition)) {
        resultVector->SetNull(outputPosition);
    } else {
        resultVector->SetValue(outputPosition, static_cast<vec::Vector<V> *>(inputVector)->GetValue(inputPosition));
    }
}

void SetVarcharValue(vec::BaseVector *inputVector, int32_t inputPosition, vec::BaseVector *outputVector,
                     int32_t outputPosition)
{
    using VarcharVector = vec::Vector<vec::LargeStringContainer<std::string_view>>;
    auto resultVector = static_cast<VarcharVector *>(outputVector);
    if (inputVector->IsNull(inputPosition)) {
        resultVector->SetNull(outputPosition);
    } else {
        std::string_view value = static_cast<VarcharVector *>(inputVector)->GetValue(inputPosition);
        resultVector->SetValue(outputPosition, value);
    }
}

VectorBatchUnit *VectorBatchSpiller::Next()
{
    auto rowCount = static_cast<int32_t>(std::min((int64_t)maxRowCountPerVecBatch, remainingRowCount));
    auto colCount = static_cast<int32_t>(outputCols.size());
    auto output = new vec::VectorBatch(rowCount);
    VectorHelper::AppendVectors(output, DataTypes(outputTypes), rowCount);
    int32_t rowIndex = 0;

    do {
        VecBatchWithPosition *vecBatchWithPosition = merger->Next();
        auto vectorBatch = vecBatchWithPosition->GetVectorBatch();
        auto position = vecBatchWithPosition->GetPosition();
        for (int32_t i = 0; i < colCount; i++) {
            int32_t outputCol = outputCols[i];
            switch (outputTypes[i]->GetId()) {
                case OMNI_INT:
                case OMNI_DATE32:
                    SetValue<int32_t>(vectorBatch->Get(outputCol), position, output->Get(i), rowIndex);
                    break;
                case OMNI_SHORT:
                    SetValue<int16_t>(vectorBatch->Get(outputCol), position, output->Get(i), rowIndex);
                    break;
                case OMNI_LONG:
                case OMNI_DECIMAL64:
                    SetValue<int64_t>(vectorBatch->Get(outputCol), position, output->Get(i), rowIndex);
                    break;
                case OMNI_BOOLEAN:
                    SetValue<bool>(vectorBatch->Get(outputCol), position, output->Get(i), rowIndex);
                    break;
                case OMNI_DOUBLE:
                    SetValue<double>(vectorBatch->Get(outputCol), position, output->Get(i), rowIndex);
                    break;
                case OMNI_CHAR:
                case OMNI_VARCHAR:
                    SetVarcharValue(vectorBatch->Get(outputCol), position, output->Get(i), rowIndex);
                    break;
                case OMNI_DECIMAL128:
                    SetValue<Decimal128>(vectorBatch->Get(outputCol), position, output->Get(i), rowIndex);
                    break;
                default:
                    break;
            }
        }
        delete vecBatchWithPosition;
        rowIndex++;
    } while (rowIndex < rowCount && merger->HasNext());

    remainingRowCount -= rowCount;
    return new VectorBatchUnit(output);
}
}
}
