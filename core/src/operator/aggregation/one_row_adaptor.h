/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * @Description: Trans row value to one row VectorBatch
 */

#ifndef OMNI_RUNTIME_ONE_ROW_ADAPTOR_H
#define OMNI_RUNTIME_ONE_ROW_ADAPTOR_H
#include <vector/vector_batch.h>
#include <functional>
#include <vector/fixed_width_vector.h>
#include <vector/variable_width_vector.h>

namespace omniruntime {
namespace op {
template <typename RawDataType, typename VectorType>
static void SetValueIntoVector(vec::VectorBatch *vecBatch, int32_t columnIndex, uintptr_t dataPtr, int32_t len)
{
    auto *vec = reinterpret_cast<VectorType *>(vecBatch->GetVector(columnIndex));
    if (len == -1) {
        vec->SetValueNull(0);
        return;
    }
    auto value = *(reinterpret_cast<RawDataType *>(dataPtr));
    vec->SetValue(0, value);
    // the null will be reAssign if SetValueIntoVector call again
    vec->SetValueNull(0, false);
}
using SetValueFunction =
    std::function<void(vec::VectorBatch *vecBatch, int32_t columnIndex, uintptr_t dataPtr, int32_t len)>;

template <>
void SetValueIntoVector<uint8_t *, vec::VarcharVector>(vec::VectorBatch *vecBatch, int32_t columnIndex,
    uintptr_t dataPtr, int32_t len)
{
    auto *vec = reinterpret_cast<vec::VarcharVector *>(vecBatch->GetVector(columnIndex));
    if (len == -1) {
        vec->SetValueNull(0);
        return;
    }
    vec->SetValue(0, reinterpret_cast<const uint8_t *>(dataPtr), len);
    // the null will be reAssign if SetValueIntoVector call again
    vec->SetValueNull(0, false);
}

static std::vector<SetValueFunction> setValueFunctions{
    nullptr,                                                     // OMNI_NONE = 0
    SetValueIntoVector<int32_t, vec::IntVector>,                 // OMNI_INT = 1
    SetValueIntoVector<int64_t, vec::LongVector>,                // OMNI_LONG = 2
    SetValueIntoVector<double, vec::DoubleVector>,               // OMNI_DOUBLE = 3
    SetValueIntoVector<uint8_t, vec::BooleanVector>,             // OMNI_BOOLEAN = 4
    SetValueIntoVector<int16_t, vec::ShortVector>,               // OMNI_SHORT = 5
    SetValueIntoVector<int64_t, vec::LongVector>,                // OMNI_DECIMAL64 = 6
    SetValueIntoVector<type::Decimal128, vec::Decimal128Vector>, // OMNI_DECIMAL128 = 7
    SetValueIntoVector<int32_t, vec::IntVector>,                 // OMNI_DATE32 = 8
    SetValueIntoVector<int64_t, vec::LongVector>,                // OMNI_DATE64 = 9
    SetValueIntoVector<int32_t, vec::IntVector>,                 // OMNI_TIME32 = 10
    SetValueIntoVector<int64_t, vec::LongVector>,                // OMNI_TIME64 = 11
    SetValueIntoVector<int64_t, vec::LongVector>,                // OMNI_TIMESTAMP = 12
    SetValueIntoVector<int64_t, vec::LongVector>,                // OMNI_INTERVAL_MONTHS = 13
    SetValueIntoVector<int64_t, vec::LongVector>,                // OMNI_INTERVAL_DAY_TIME = 14
    SetValueIntoVector<uint8_t *, vec::VarcharVector>,           // OMNI_VARCHAR = 15
    SetValueIntoVector<uint8_t *, vec::VarcharVector>,           // OMNI_CHAR = 16
    nullptr,                                                     // OMNI_CONTAINER = 17
};
/**
 * handle resource by RAII
 */
class OneRowAdaptor {
public:
    OneRowAdaptor()
    {
        vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("adaptor_header");
    };

    void Init(const std::vector<type::DataTypeId> &inputTypes)
    {
        types.reserve(inputTypes.size());
        for (auto type : inputTypes) {
            types.push_back(type);
        }
        totalTypeSize = types.size();
        InitFunc();
        InitMem();
    }

    OneRowAdaptor(const OneRowAdaptor &oneRowAdaptor) = delete;

    OneRowAdaptor &operator = (const OneRowAdaptor &oneRowAdaptor) = delete;

    OneRowAdaptor &operator = (OneRowAdaptor &&oneRowAdaptor) = delete;

    OneRowAdaptor(OneRowAdaptor &&oneRowAdaptor) = delete;

    ~OneRowAdaptor()
    {
        if (rowVectorBatch != nullptr) {
            VectorHelper::FreeVecBatch(rowVectorBatch);
            rowVectorBatch = nullptr;
        }
        delete vecAllocator;
    }

    vec::VectorBatch *Trans2VectorBatch(uintptr_t data[], int32_t len[])
    {
        for (int32_t i = 0; i < totalTypeSize; ++i) {
            setFuncs[i](rowVectorBatch, i, data[i], len[i]);
        }
        return rowVectorBatch;
    }

private:
    void InitFunc()
    {
        // init function to set value depend on different omni type
        setFuncs.reserve(totalTypeSize);
        for (int i = 0; i < totalTypeSize; ++i) {
            setFuncs.emplace_back(setValueFunctions.at(types.at(i)));
        }
    }

    // init memory of one row vector batch, only init once in operator
    void InitMem()
    {
        if (rowVectorBatch != nullptr) {
            return;
        }

        rowVectorBatch = new vec::VectorBatch(totalTypeSize, 1);
        // use typeAdaptors to trans std::vector<OmniId> , the NewVectors need this structure
        std::vector<DataTypePtr> typeAdaptors;
        typeAdaptors.reserve(totalTypeSize);
        for (int32_t i = 0; i < totalTypeSize; ++i) {
            auto id = types[i];
            typeAdaptors.push_back(std::make_shared<DataType>(id));
        }
        rowVectorBatch->NewVectors(vecAllocator, typeAdaptors);
    }

private:
    std::vector<SetValueFunction> setFuncs;
    std::vector<type::DataTypeId> types;
    int totalTypeSize = 0;
    vec::VectorAllocator *vecAllocator;
    vec::VectorBatch *rowVectorBatch = nullptr;
};
}
}
#endif // OMNI_RUNTIME_ONE_ROW_ADAPTOR_H
