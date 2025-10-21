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

            int32_t rowSize = inputArg->GetSize();
            result = new ArrayVector(rowSize);
            std::cout << "using vectorization split" << std::endl;

            auto* arrayResult = dynamic_cast<vec::ArrayVector*>(result);
            if (!arrayResult) {
                OMNI_THROW("SplitFunction Error:", "Result vector is not an ArrayVector");
            }

            auto* constVec = dynamic_cast<vec::ConstVector<std::string>*>(delimiterArg);
            std::string_view delimiter = constVec->GetConstValue();
            std::cout << "Delimiter: '" << delimiter << "'" << std::endl;

            ConstVectorReader<int32_t> limitReader(limitArg);
            int32_t limit = limitReader[0];
            std::cout << "Limit: " << limit << std::endl;

            ProcessAllRows(arrayResult, rowSize, inputArg, delimiter, limit);
        }

    private:
        void ProcessAllRows(vec::ArrayVector* arrayResult, int32_t rowSize,
                            vec::BaseVector* inputVector,
                            const std::string_view& delimiter,
                            const int32_t limit) const
        {
            std::vector<std::string_view> allElements;
            std::vector<int64_t> offsets = {0};

            if (limit < -1) {
                OMNI_THROW("SplitFunction Error:", "Limit must be positive or 0/-1 (got " + std::to_string(limit) + ")");
            }

            for (int32_t row = 0; row < rowSize; ++row) {
                if (inputVector->IsNull(row)) {
                    arrayResult->SetNull(row);
                    offsets.push_back(offsets.back());
                    continue;
                }

                std::string_view inputStr = GetStringValue(inputVector, row);
                std::vector<std::string_view> splitParts = SplitString(inputStr, delimiter, limit);

                int64_t currentOffset = offsets.back();
                offsets.push_back(currentOffset + splitParts.size());
                allElements.insert(allElements.end(), splitParts.begin(), splitParts.end());
            }

            auto elementVector = CreateElementVector(allElements);
            SetupArrayVector(arrayResult, offsets, elementVector, rowSize, inputVector);
        }

        std::string_view GetStringValue(vec::BaseVector* vector, int32_t row) const
        {
            switch (vector->GetEncoding()) {
                case Encoding::OMNI_FLAT: {
                    auto* stringVector = static_cast<vec::Vector<vec::LargeStringContainer<std::string_view>>*>(vector);
                    return stringVector->GetValue(row);
                }
                case Encoding::OMNI_DICTIONARY: {
                    auto* dictVector = static_cast<vec::Vector<vec::DictionaryContainer<std::string_view, vec::LargeStringContainer>>*>(vector);
                    return dictVector->GetValue(row);
                }
                case Encoding::OMNI_ENCODING_CONST: {
                    auto* constVector = static_cast<vec::ConstVector<std::string>*>(vector);
                    return constVector->GetConstValue();
                }
                default:
                    OMNI_THROW("SplitFunction Error:", "Unsupported encoding type");
            }
        }

        std::shared_ptr<vec::Vector<vec::LargeStringContainer<std::string_view>>>
        CreateElementVector(const std::vector<std::string_view>& allElements) const
        {
            auto elementVector = std::make_shared<vec::Vector<vec::LargeStringContainer<std::string_view>>>(allElements.size());
            for (size_t i = 0; i < allElements.size(); ++i) {
                auto element = allElements[i];
                if (element.data() == nullptr || element.data()[0] == '\0') {
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
                              vec::BaseVector* inputVector) const
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

            if (limit == -1 || result.size() < static_cast<size_t>(limit)) {
                result.push_back(std::string_view(""));
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
                std::string_view remaining = str.substr(start);
                result.push_back(remaining);
            }

            return result;
        }
    };
}