/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: visitor class for expressions
 */

#pragma once
#include "vectorization/VectorFunction.h"
#include "vectorization/SelectivityVector.h"
#include "vector/array_vector.h"
#include "vectorization/VectorReaders.h"
#include "util/debug.h"
#include <vector>
#include <string_view>

namespace omniruntime::vectorization {
using namespace omniruntime::type;
using namespace omniruntime::vec;
using namespace omniruntime::op;

class SplitFunction final : public VectorFunction {
public:
    explicit SplitFunction() {}

    void Apply(std::stack<BaseVector *> &args, const DataTypePtr &outputType, BaseVector *&result,
        ExecutionContext *context) const override
    {
        auto limitArg = args.top(); // The limited number of segments
        args.pop();
        auto delimiterArg = args.top(); // The separator
        args.pop();
        auto inputArg = args.top(); // The input string vector to be split
        args.pop();

        int32_t rowSize = inputArg->GetSize();
        result = new ArrayVector(rowSize);

        auto *arrayResult = dynamic_cast<ArrayVector *>(result);
        if (!arrayResult) {
            OMNI_THROW("SplitFunction Error:", "Result vector is not an ArrayVector");
        }

        auto *constVec = dynamic_cast<ConstVector<std::string_view> *>(delimiterArg);
        std::string_view delimiter = constVec->GetConstValue();

        ConstVectorReader<int32_t> limitReader(limitArg);
        int32_t limit = limitReader[0];

        ProcessAllRows(arrayResult, rowSize, inputArg, delimiter, limit);

        // Clean up arguments. Do not delete limitArg: ConstVectorReader's destructor owns and
        // will delete it when limitReader goes out of scope.
        if (delimiterArg != nullptr) {
            delete delimiterArg;
        }
        if (inputArg != nullptr) {
            delete inputArg;
        }
    }

private:
    /*
     * Process all rows in batches:
     *   1. Traverse each input string row
     *   2. Perform the splitting operation on each non-empty string
     *   3. Collect all the split elements
     *   4. Build the final result array
     */
    void ProcessAllRows(ArrayVector *arrayResult, int32_t rowSize, BaseVector *inputVector,
        const std::string_view &delimiter, const int32_t limit) const
    {
        // limit <= 0 means no limit
        int32_t effectiveLimit = (limit <= 0) ? -1 : limit;

        // Pre-calculate the offsets, the total number of elements, and the total number of bytes
        std::vector<int64_t> offsets;
        offsets.reserve(rowSize + 1);
        offsets.push_back(0);

        int64_t totalElements = 0;
        size_t totalBytesNeeded = 0;

        for (int32_t row = 0; row < rowSize; ++row) {
            if (inputVector->IsNull(row)) {
                arrayResult->SetNull(row);
                offsets.push_back(offsets.back());
                continue;
            }

            std::string_view inputStr = GetStringValue(inputVector, row);

            int32_t count = 0;
            size_t bytes = 0;
            CalcSplitInfo(inputStr, delimiter, effectiveLimit, count, bytes);

            totalElements += count;
            totalBytesNeeded += bytes;
            offsets.push_back(offsets.back() + count);
        }

        // Add a small amount of redundancy (64 bytes) to prevent boundary calculation errors
        int32_t estimatedCapacity = static_cast<int32_t>(std::min(totalBytesNeeded + 64, static_cast<size_t>(INT32_MAX)));

        // Allocate the element vector in one go to avoid resizing
        auto elementVector = std::make_shared<Vector<LargeStringContainer<std::string_view>>>(totalElements, estimatedCapacity);

        int64_t currentInsertIndex = 0;
        for (int32_t row = 0; row < rowSize; ++row) {
            if (inputVector->IsNull(row)) {
                continue;
            }

            std::string_view inputStr = GetStringValue(inputVector, row);
            FillSplitParts(elementVector.get(), currentInsertIndex, inputStr, delimiter, effectiveLimit);

            currentInsertIndex = offsets[row + 1];
        }

        SetupArrayVector(arrayResult, offsets, elementVector, rowSize, inputVector);
    }

    // Calculate the total number of elements and the total number of bytes
    void CalcSplitInfo(std::string_view str, std::string_view delimiter, int32_t limit,
        int32_t &outCount, size_t &outBytes) const
    {
        outCount = 0;
        outBytes = 0;

        if (str.empty()) {
            outCount = 1;
            outBytes = 0;
            return;
        }

        if (delimiter.empty()) {
            int32_t len = static_cast<int32_t>(str.length());
            if (limit == -1 || limit >= len) {
                outCount = len;
                outBytes = len;
            } else {
                outCount = limit;
                outBytes = limit;
            }
            return;
        }

        size_t start = 0;
        size_t end = str.find(delimiter);
        size_t limitSize = static_cast<size_t>(limit);

        while (end != std::string_view::npos && (limit == -1 || outCount < static_cast<int32_t>(limitSize) - 1)) {
            outBytes += (end - start);
            outCount++;
            start = end + delimiter.length();
            end = str.find(delimiter, start);
        }

        if (start <= str.length() && (limit == -1 || outCount < static_cast<int32_t>(limitSize))) {
            outBytes += (str.length() - start);
            outCount++;
        }

        if (outCount == 0) outCount = 1;
    }

    void FillSplitParts(Vector<LargeStringContainer<std::string_view>> *targetVec, int64_t startIndex,
        std::string_view str, std::string_view delimiter, int32_t limit) const
    {
        if (str.empty()) {
            std::string_view emptyStr("", 0);
            targetVec->SetValue(static_cast<int>(startIndex), emptyStr);
            return;
        }

        if (delimiter.empty()) {
            int32_t len = static_cast<int32_t>(str.length());
            int32_t countToProcess = (limit == -1 || limit >= len) ? len : limit;

            for (int32_t i = 0; i < countToProcess; ++i) {
                std::string_view singleChar = str.substr(i, 1);
                targetVec->SetValue(static_cast<int>(startIndex + i), singleChar);
            }
            return;
        }

        int32_t count = 0;
        size_t start = 0;
        size_t end = str.find(delimiter);
        size_t limitSize = static_cast<size_t>(limit);

        auto setElement = [&](std::string_view part) {
            targetVec->SetValue(startIndex + count, part);
            count++;
        };

        while (end != std::string_view::npos && (limit == -1 || count < limitSize - 1)) {
            setElement(str.substr(start, end - start));
            start = end + delimiter.length();
            end = str.find(delimiter, start);
        }

        if (start <= str.length() && (limit == -1 || count < limitSize)) {
            setElement(str.substr(start));
        }
    }


    std::string_view GetStringValue(BaseVector *vector, int32_t row) const
    {
        switch (vector->GetEncoding()) {
            case OMNI_FLAT: {
                auto *stringVector = static_cast<Vector<LargeStringContainer<std::string_view>> *>(vector);
                return stringVector->GetValue(row);
            }
            case OMNI_DICTIONARY: {
                auto *dictVector = static_cast<Vector<DictionaryContainer<std::string_view, LargeStringContainer>> *>(
                        vector);
                return dictVector->GetValue(row);
            }
            case OMNI_ENCODING_CONST: {
                auto *constVector = static_cast<ConstVector<std::string_view> *>(vector);
                return constVector->GetConstValue();
            }
            default: OMNI_THROW("SplitFunction Error:", "Unsupported encoding type");
        }
    }

    void SetupArrayVector(ArrayVector *arrayResult, const std::vector<int64_t> &offsets,
        const std::shared_ptr<BaseVector> &elementVector, int32_t rowSize, BaseVector *inputVector) const
    {
        for (size_t i = 0; i < offsets.size(); ++i) {
            arrayResult->SetOffset(i, offsets[i]);
        }
        arrayResult->SetElementVector(elementVector);
        for (int32_t row = 0; row < rowSize; ++row) {
            if (inputVector->IsNull(row)) {
                arrayResult->SetNull(row);
            } else {
                arrayResult->SetNotNull(row);
            }
        }
    }
};
}
