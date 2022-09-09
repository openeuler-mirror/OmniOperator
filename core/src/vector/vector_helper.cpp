/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

#include "vector_helper.h"
#include <iomanip>

namespace omniruntime {
namespace vec {
void VectorHelper::FreeVecBatch(VectorBatch *vecBatch)
{
    vecBatch->ReleaseAllVectors();
    delete vecBatch;
}

void VectorHelper::FreeVecBatches(VectorBatch **vecBatches, int32_t vecBatchCount)
{
    for (int i = 0; i < vecBatchCount; ++i) {
        vecBatches[i]->ReleaseAllVectors();
        delete vecBatches[i];
    }
    delete[] vecBatches;
}

void VectorHelper::FreeVecBatches(std::vector<VectorBatch *> &vecBatches)
{
    for (auto &vecBatch : vecBatches) {
        vecBatch->ReleaseAllVectors();
        delete vecBatch;
    }
}

void VectorHelper::PrintVectorValue(Vector *vector, int32_t rowIndex)
{
    int32_t originalRowIndex;
    vector = ExpandVectorAndIndex(vector, rowIndex, originalRowIndex);
    auto dataTypeId = vector->GetTypeId();
    if (vector->IsValueNull(originalRowIndex)) {
        std::cout << "NULL"
                  << "\t";
        return;
    }
    switch (dataTypeId) {
        case OMNI_INT:
        case OMNI_DATE32: {
            std::cout << std::dec << static_cast<IntVector *>(vector)->GetValue(originalRowIndex) << "\t";
            break;
        }
        case OMNI_SHORT: {
            std::cout << std::dec << static_cast<ShortVector *>(vector)->GetValue(originalRowIndex) << "\t";
            break;
        }
        case OMNI_LONG:
        case OMNI_DECIMAL64: {
            std::cout << std::dec << static_cast<LongVector *>(vector)->GetValue(originalRowIndex) << "\t";
            break;
        }
        case OMNI_DOUBLE: {
            std::cout << static_cast<DoubleVector *>(vector)->GetValue(originalRowIndex) << "\t";
            break;
        }
        case OMNI_BOOLEAN: {
            std::cout << static_cast<BooleanVector *>(vector)->GetValue(originalRowIndex) << "\t";
            break;
        }
        case OMNI_VARCHAR:
        case OMNI_CHAR: {
            uint8_t *value = nullptr;
            int32_t len = static_cast<VarcharVector *>(vector)->GetValue(originalRowIndex, &value);
            std::string valueString(value, value + len);
            std::cout << valueString << "\t";
            break;
        }
        case OMNI_DECIMAL128: {
            Decimal128 result = static_cast<Decimal128Vector *>(vector)->GetValue(originalRowIndex);
            std::cout << result.HighBits() << " " << result.LowBits() << "\t";
            break;
        }
        case OMNI_CONTAINER: {
            auto *containerVector = static_cast<ContainerVector *>(vector);
            int32_t fieldCount = containerVector->GetVectorCount();
            for (int32_t colIdx = 0; colIdx < fieldCount; colIdx++) {
                PrintVectorValue(reinterpret_cast<Vector *>(containerVector->GetValue(colIdx)), rowIndex);
            }
            break;
        }
        default:
            LogError("Error vector type %d", dataTypeId);
    }
}

void VectorHelper::PrintVecBatch(VectorBatch *vecBatch)
{
    int32_t vectorCount = vecBatch->GetVectorCount();
    int32_t rowCount = vecBatch->GetRowCount();
    for (int32_t rowIdx = 0; rowIdx < rowCount; ++rowIdx) {
        for (int32_t colIdx = 0; colIdx < vectorCount; ++colIdx) {
            auto vector = vecBatch->GetVector(colIdx);
            PrintVectorValue(vector, rowIdx);
        }
        std::cout << std::endl;
    }
}

VectorBatch *VectorHelper::ConcatVectorBatches(std::vector<VectorBatch *> &vecBatches)
{
    if (vecBatches.empty()) {
        return nullptr;
    }
    int32_t rowCount = 0;
    int32_t vectorCount = vecBatches[0]->GetVectorCount();
    auto allocator = vecBatches[0]->GetVector(0)->GetAllocator();
    auto types = vecBatches[0]->GetVectorTypeIds();
    for (auto pV : vecBatches) {
        rowCount += pV->GetRowCount();
    }

    auto *result = new VectorBatch(vectorCount, rowCount);

    for (int32_t i = 0; i < vectorCount; ++i) {
        switch (types[i]) {
            case OMNI_INT:
            case OMNI_DATE32: {
                auto *vector = new IntVector(allocator, rowCount);
                result->SetVector(i, vector);
                break;
            }
            case OMNI_SHORT: {
                auto *vector = new ShortVector(allocator, rowCount);
                result->SetVector(i, vector);
                break;
            }
            case OMNI_LONG:
            case OMNI_DECIMAL64: {
                auto *vector = new LongVector(allocator, rowCount);
                result->SetVector(i, vector);
                break;
            }
            case OMNI_DOUBLE: {
                auto *vector = new DoubleVector(allocator, rowCount);
                result->SetVector(i, vector);
                break;
            }
            case OMNI_BOOLEAN: {
                auto *vector = new BooleanVector(allocator, rowCount);
                result->SetVector(i, vector);
                break;
            }
            case OMNI_VARCHAR:
            case OMNI_CHAR: {
                auto *vector = new VarcharVector(allocator, 50 * rowCount, rowCount);
                result->SetVector(i, vector);
                break;
            }
            case OMNI_DECIMAL128: {
                break;
            }
            case OMNI_CONTAINER: {
                break;
            }
            default:
                LogError("Error vector type %d", types[i]);
        }
    }
    SetVectorBatchVector(result, vecBatches);
    return result;
}

VectorBatch *VectorHelper::SetVectorBatchVector(VectorBatch *result, std::vector<VectorBatch *> &vecBatches)
{
    int32_t offset = 0;
    int32_t vectorCount = vecBatches[0]->GetVectorCount();
    auto types = vecBatches[0]->GetVectorTypeIds();
    for (auto pV : vecBatches) {
        int32_t rc = pV->GetRowCount();
        for (int32_t i = 0; i < vectorCount; ++i) {
            auto vector = pV->GetVector(i);
            auto resVec = result->GetVector(i);
            switch (types[i]) {
                case OMNI_INT:
                case OMNI_DATE32: {
                    auto rValues = static_cast<int32_t *>(static_cast<IntVector *>(vector)->GetValues());
                    static_cast<IntVector *>(resVec)->SetValues(offset, rValues, rc);
                    break;
                }
                case OMNI_SHORT: {
                    auto rValues = static_cast<int16_t *>(static_cast<ShortVector *>(vector)->GetValues());
                    static_cast<ShortVector *>(resVec)->SetValues(offset, rValues, rc);
                    break;
                }
                case OMNI_LONG:
                case OMNI_DECIMAL64: {
                    auto rValues = static_cast<int64_t *>(static_cast<IntVector *>(vector)->GetValues());
                    static_cast<LongVector *>(resVec)->SetValues(offset, rValues, rc);
                    break;
                }
                case OMNI_DOUBLE: {
                    auto rValues = static_cast<double *>(static_cast<DoubleVector *>(vector)->GetValues());
                    static_cast<DoubleVector *>(resVec)->SetValues(offset, rValues, rc);
                    break;
                }
                case OMNI_BOOLEAN: {
                    auto rValues = static_cast<bool *>(static_cast<BooleanVector *>(vector)->GetValues());
                    static_cast<BooleanVector *>(resVec)->SetValues(offset, rValues, rc);
                    break;
                }
                case OMNI_VARCHAR:
                case OMNI_CHAR: {
                    for (int32_t j = 0; j < rc; ++j) {
                        uint8_t *data = nullptr;
                        int32_t len = static_cast<VarcharVector *>(vector)->GetValue(j, &data);
                        std::string val(reinterpret_cast<char *>(data), len);
                        static_cast<VarcharVector *>(resVec)->SetValue(offset + j, data, len);
                    }
                    break;
                }
                case OMNI_DECIMAL128: {
                    break;
                }
                case OMNI_CONTAINER: {
                    break;
                }
                default:
                    LogError("Error vector type %d", types[i]);
            }
        }
        offset += rc;
    }
    return result;
}
} // namespace vec
} // namespace omniruntime