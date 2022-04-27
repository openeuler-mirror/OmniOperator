/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * @Description: vector batch spiller implements
 */
#include "vector_batch_spiller.h"
#include <sys/types.h>

namespace omniruntime {
namespace op {
using namespace omniruntime::type;

ErrorCode VectorBatchSpiller::Spill(SpillUnitIter &vecBatches)
{
    auto writer = new VectorBatchWriter(tracker);
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
        auto reader = new VectorBatchReader(writer->GetFd(), writer->GetFileLength(), tracker, vectorAllocator);
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

    auto vecBatchUnitIter = new VectorBatchUnitIter(vecBatches);
    auto memoryIterator = new SpillIterator(vecBatchUnitIter);

    merger = new VectorBatchMerger(comparator);
    merger->MergeFromDiskAndMemory(diskIterators, memoryIterator);
}

bool VectorBatchSpiller::HasNext()
{
    return merger->HasNext();
}

template <typename V>
void SetValue(Vector *inputVector, int32_t inputPosition, Vector *outputVector, int32_t outputPosition)
{
    auto resultVector = static_cast<V *>(outputVector);
    if (inputVector->IsValueNull(inputPosition)) {
        resultVector->SetValueNull(outputPosition);
    } else {
        resultVector->SetValue(outputPosition, static_cast<V *>(inputVector)->GetValue(inputPosition));
    }
}

void SetVarcharValue(Vector *inputVector, int32_t inputPosition, Vector *outputVector, int32_t outputPosition)
{
    auto resultVector = static_cast<VarcharVector *>(outputVector);
    if (inputVector->IsValueNull(inputPosition)) {
        resultVector->SetValueNull(outputPosition);
    } else {
        uint8_t *value = nullptr;
        int32_t length = static_cast<VarcharVector *>(inputVector)->GetValue(inputPosition, &value);
        resultVector->SetValue(outputPosition, value, length);
    }
}

VectorBatchUnit *VectorBatchSpiller::Next()
{
    auto rowCount = static_cast<int32_t>(std::min((int64_t)maxRowCountPerVecBatch, remainingRowCount));
    auto colCount = static_cast<int32_t>(outputCols.size());
    auto output = new VectorBatch(colCount, rowCount);
    output->NewVectors(VectorAllocator::GetGlobalAllocator(), outputTypes);
    int32_t rowIndex = 0;

    do {
        VecBatchWithPosition *vecBatchWithPosition = merger->Next();
        auto vectorBatch = vecBatchWithPosition->GetVectorBatch();
        auto position = vecBatchWithPosition->GetPosition();
        for (int32_t i = 0; i < colCount; i++) {
            int32_t outputCol = outputCols[i];
            switch (outputTypes[i].GetId()) {
                case OMNI_INT:
                case OMNI_DATE32:
                    SetValue<IntVector>(vectorBatch->GetVector(outputCol), position, output->GetVector(i), rowIndex);
                    break;
                case OMNI_LONG:
                case OMNI_DECIMAL64:
                    SetValue<LongVector>(vectorBatch->GetVector(outputCol), position, output->GetVector(i), rowIndex);
                    break;
                case OMNI_BOOLEAN:
                    SetValue<BooleanVector>(vectorBatch->GetVector(outputCol), position, output->GetVector(i),
                        rowIndex);
                    break;
                case OMNI_DOUBLE:
                    SetValue<DoubleVector>(vectorBatch->GetVector(outputCol), position, output->GetVector(i), rowIndex);
                    break;
                case OMNI_CHAR:
                case OMNI_VARCHAR:
                    SetVarcharValue(vectorBatch->GetVector(outputCol), position, output->GetVector(i), rowIndex);
                    break;
                case OMNI_DECIMAL128:
                    SetValue<Decimal128Vector>(vectorBatch->GetVector(outputCol), position, output->GetVector(i),
                        rowIndex);
                    break;
                default:
                    break;
            }
        }
        delete vecBatchWithPosition;
    } while (rowIndex++ < rowCount && merger->HasNext());

    remainingRowCount -= rowCount;
    return new VectorBatchUnit(output);
}
}
}
