/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * @Description: Trans row value to one row VectorBatch
 */

#ifndef OMNI_RUNTIME_ONE_ROW_ADAPTOR_H
#define OMNI_RUNTIME_ONE_ROW_ADAPTOR_H
#include <vector/vector_batch.h>
#include <functional>
#include "memory/allocator.h"
#include "type/decimal128.h"
#include "vector/vector_helper.h"

namespace omniruntime {
using namespace vec;
using namespace type;
namespace op {
template <typename T>
static void SetValueIntoVector(VectorBatch *vecBatch, int32_t columnIndex, uintptr_t dataPtr, int32_t len)
{
    auto *vec = reinterpret_cast<Vector<T> *>(vecBatch->Get(columnIndex));
    if (len == -1) {
        vec->SetNull(0);
        return;
    }
    auto value = *(reinterpret_cast<T *>(dataPtr));
    vec->SetValue(0, value);
    // the null will be reAssign if SetValueIntoVector call again
    vec->SetNotNull(0);
}
using SetValueFunction =
    std::function<void(VectorBatch *vecBatch, int32_t columnIndex, uintptr_t dataPtr, int32_t len)>;

template <>
void SetValueIntoVector<uint8_t *>(VectorBatch *vecBatch, int32_t columnIndex, uintptr_t dataPtr, int32_t len)
{
    auto *vec = reinterpret_cast<Vector<LargeStringContainer<std::string_view>> *>(vecBatch->Get(columnIndex));
    if (len == -1) {
        vec->SetNull(0);
        return;
    }
    std::string_view str(reinterpret_cast<const char *>(dataPtr), len);
    vec->SetValue(0, str);
    // the null will be reAssign if SetValueIntoVector call again
    vec->SetNotNull(0);
}

static std::vector<SetValueFunction> setValueFunctions{
    nullptr,                        // OMNI_NONE = 0
    SetValueIntoVector<int32_t>,    // OMNI_INT = 1
    SetValueIntoVector<int64_t>,    // OMNI_LONG = 2
    SetValueIntoVector<double>,     // OMNI_DOUBLE = 3
    SetValueIntoVector<uint8_t>,    // OMNI_BOOLEAN = 4
    SetValueIntoVector<int16_t>,    // OMNI_SHORT = 5
    SetValueIntoVector<int64_t>,    // OMNI_DECIMAL64 = 6
    SetValueIntoVector<Decimal128>, // OMNI_DECIMAL128 = 7
    SetValueIntoVector<int32_t>,    // OMNI_DATE32 = 8
    SetValueIntoVector<int64_t>,    // OMNI_DATE64 = 9
    SetValueIntoVector<int32_t>,    // OMNI_TIME32 = 10
    SetValueIntoVector<int64_t>,    // OMNI_TIME64 = 11
    SetValueIntoVector<int64_t>,    // OMNI_TIMESTAMP = 12
    SetValueIntoVector<int64_t>,    // OMNI_INTERVAL_MONTHS = 13
    SetValueIntoVector<int64_t>,    // OMNI_INTERVAL_DAY_TIME = 14
    SetValueIntoVector<uint8_t *>,  // OMNI_VARCHAR = 15
    SetValueIntoVector<uint8_t *>,  // OMNI_CHAR = 16
    nullptr,                        // OMNI_CONTAINER = 17
};
/**
 * handle resource by RAII
 */
class OneRowAdaptor {
public:
    OneRowAdaptor() {}

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
    }

    VectorBatch *Trans2VectorBatch(uintptr_t data[], int32_t len[])
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

        rowVectorBatch = new VectorBatch(1);
        // use typeAdaptors to trans std::vector<OmniId> , the NewVectors need this structure
        std::vector<DataTypePtr> typeAdaptors;
        typeAdaptors.reserve(totalTypeSize);
        for (int32_t i = 0; i < totalTypeSize; ++i) {
            auto id = types[i];
            typeAdaptors.push_back(std::make_shared<DataType>(id));
        }
        type::DataTypes outDataTypes(typeAdaptors);
        VectorHelper::AppendVectors(rowVectorBatch, outDataTypes, rowVectorBatch->GetRowCount());
    }

private:
    std::vector<SetValueFunction> setFuncs;
    std::vector<DataTypeId> types;
    int totalTypeSize = 0;
    VectorBatch *rowVectorBatch = nullptr;
};
}
}
#endif // OMNI_RUNTIME_ONE_ROW_ADAPTOR_H
