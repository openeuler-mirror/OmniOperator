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

class SplitFunction : public VectorFunction {
public:
    explicit SplitFunction() {}

    void apply(std::stack<VectorPtr> &args, const type::DataTypePtr &outputType,
               vec::BaseVector *&result, op::ExecutionContext *context) const override
    {
        auto limitArg = args.top();
        args.pop();
        auto delimiterArg = args.top();
        args.pop();
        auto inputArg = args.top();
        args.pop();

        auto* arrayResult = dynamic_cast<vec::ArrayVector*>(result);
        if (!arrayResult) {
            OMNI_THROW("SplitFunction Error:", "Result vector is not an ArrayVector");
        }

        int32_t rowSize = context->GetResultRowSize();
        
        StringVectorReader inputReader(inputArg);
        StringVectorReader delimiterReader(delimiterArg);
        FlatVectorReader<int32_t> limitReader(limitArg);

        ProcessAllRows(arrayResult, rowSize, inputReader, delimiterReader, limitReader);
    }

private:
    void ProcessAllRows(vec::ArrayVector* arrayResult, int32_t rowSize,
                       const StringVectorReader& inputReader,
                       const StringVectorReader& delimiterReader,
                       const FlatVectorReader<int32_t>& limitReader) const
    {
        std::vector<std::string_view> allElements;
        std::vector<int64_t> offsets = {0};

        // Process each row and collect split results
        for (int32_t row = 0; row < rowSize; ++row) {
            if (HasNullInput(row, inputReader, delimiterReader, limitReader)) {
                arrayResult->SetNull(row);
                offsets.push_back(offsets.back());
                continue;
            }

            std::string_view inputStr = inputReader[row];
            std::string_view delimiter = delimiterReader[row];
            int32_t limit = limitReader[row];

            if (limit < -1 || limit == 0) {
                OMNI_THROW("SplitFunction Error:", "Limit must be positive or -1 (for no limit)");
            }

            std::vector<std::string_view> splitParts = SplitString(inputStr, delimiter, limit);
            int64_t currentOffset = offsets.back();
            offsets.push_back(currentOffset + splitParts.size());
            allElements.insert(allElements.end(), splitParts.begin(), splitParts.end());
        }

        // Create and setup the result array vector
        auto elementVector = CreateElementVector(allElements);
        SetupArrayVector(arrayResult, offsets, elementVector, rowSize, inputReader, delimiterReader, limitReader);
    }

    bool HasNullInput(int32_t row, const StringVectorReader& inputReader,
                     const StringVectorReader& delimiterReader,
                     const FlatVectorReader<int32_t>& limitReader) const
    {
        return inputReader.containsNull(row) ||
               delimiterReader.containsNull(row) ||
               limitReader.containsNull(row, row + 1);
    }

    std::shared_ptr<vec::Vector<vec::LargeStringContainer<std::string_view>>>
    CreateElementVector(const std::vector<std::string_view>& allElements) const
    {
        auto elementVector = std::make_shared<vec::Vector<vec::LargeStringContainer<std::string_view>>>(allElements.size());
        
        for (size_t i = 0; i < allElements.size(); ++i) {
            auto element = allElements[i];
            if (element.empty() || element.data()[0] == '\0') {
                elementVector->SetNull(i);
            } else {
                elementVector->SetValue(i, element);
            }
        }
        
        return elementVector;
    }

    void SetupArrayVector(vec::ArrayVector* arrayResult,
                         const std::vector<int64_t>& offsets,
                         const std::shared_ptr<vec::Vector<vec::LargeStringContainer<std::string_view>>>& elementVector,
                         int32_t rowSize,
                         const StringVectorReader& inputReader,
                         const StringVectorReader& delimiterReader,
                         const FlatVectorReader<int32_t>& limitReader) const
    {
        // Set offsets
        for (size_t i = 0; i < offsets.size(); ++i) {
            arrayResult->SetOffset(i, offsets[i]);
        }
        
        // Set element vector
        arrayResult->SetElementVector(elementVector);
        
        // Set null information
        for (int32_t row = 0; row < rowSize; ++row) {
            if (HasNullInput(row, inputReader, delimiterReader, limitReader)) {
                arrayResult->SetNull(row);
            } else {
                arrayResult->SetNotNull(row);
            }
        }
    }

    std::vector<std::string_view> SplitString(std::string_view str, std::string_view delimiter, int32_t limit) const
    {
        if (delimiter.empty()) {
            return SplitSingleCharacters(str, limit);
        }
        return SplitWithDelimiter(str, delimiter, limit);
    }

    std::vector<std::string_view> SplitSingleCharacters(std::string_view str, int32_t limit) const
    {
        std::vector<std::string_view> result;
        if (str.empty()) return result;
        
        if (limit == -1 || limit > static_cast<int32_t>(str.length())) {
            for (size_t i = 0; i < str.length(); ++i) {
                result.push_back(str.substr(i, 1));
            }
        } else {
            for (int32_t i = 0; i < limit - 1 && i < static_cast<int32_t>(str.length()); ++i) {
                result.push_back(str.substr(i, 1));
            }
            if (limit - 1 < static_cast<int32_t>(str.length())) {
                result.push_back(str.substr(limit - 1));
            }
        }
        return result;
    }

    std::vector<std::string_view> SplitWithDelimiter(std::string_view str, std::string_view delimiter, int32_t limit) const
    {
        std::vector<std::string_view> result;
        size_t start = 0;
        size_t end = str.find(delimiter);
        size_t limitSize = static_cast<size_t>(limit);
        
        while (end != std::string_view::npos && (limit == -1 || result.size() < limitSize - 1)) {
            result.push_back(str.substr(start, end - start));
            start = end + delimiter.length();
            end = str.find(delimiter, start);
        }
        
        if (start <= str.length() && (limit == -1 || result.size() < limitSize)) {
            result.push_back(str.substr(start));
        }

        // If no splits occurred but string is not empty, return the whole string
        if (result.empty() && !str.empty()) {
            result.push_back(str);
        }
        
        return result;
    }
};
}