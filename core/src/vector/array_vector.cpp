/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: arrayVector  implementation
 */

#include "array_vector.h"
#include "vector_helper.h"

namespace omniruntime::vec {

void ArrayVector::SetValue(int index, BaseVector *value)
{
    if (value == nullptr) {
        SetNull(index);
        SetSize(index, 1);
        int elementVectorSize = elements->GetSize();
        elements->Expand(elementVectorSize + 1);
        elements->SetNull(elementVectorSize);
        return;
    }

    int valueSize = value->GetSize();
    int elementVectorSize = GetOffset(index);
    elements->Expand(elementVectorSize + valueSize);
    VectorHelper::AppendVector(elements.get(), elementVectorSize, value, valueSize);
    SetSize(index, valueSize);
}

ALWAYS_INLINE BaseVector* ArrayVector::GetValue(int index)
{
    if (UNLIKELY(index < 0 || index >= size)) {
        std::string message("slice vector out of range(needed size:%d, real size:%d).", index,
            size);
        throw OmniException("OPERATOR_RUNTIME_ERROR", message);
    }

    int64_t startOffset = GetOffset(index);
    int64_t arraySize = GetSize(index);

    return GetElementVector()->Slice(startOffset, arraySize, false);
}
}