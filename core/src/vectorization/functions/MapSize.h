/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: Map Size function implementation
 */

#pragma once
#include "vectorization/VectorFunction.h"
#include "vectorization/SelectivityVector.h"
#include "vector/map_vector.h"
#include "vectorization/VectorReaders.h"
#include "util/debug.h"
#include <vector>

namespace omniruntime::vectorization {
    using namespace omniruntime::type;
    using namespace omniruntime::vec;
    using namespace omniruntime::op;

    class MapSizeFunction : public VectorFunction {
    public:
        explicit MapSizeFunction() {}

        void apply(std::stack<VectorPtr> &args, const type::DataTypePtr &outputType,
                   vec::BaseVector *result, op::ExecutionContext *context) const override
        {
            std::cout << "using vectorization split" << std::endl;
            if (args.empty()) {
                OMNI_THROW("MapSizeFunction Error:", "No arguments provided");
            }

            auto mapArg = args.top();
            args.pop();

            auto* flatResult = dynamic_cast<vec::Vector<int32_t>*>(result);
            if (!flatResult) {
                OMNI_THROW("MapSizeFunction Error:", "Result vector is not a FlatVector<int32_t>");
            }

            int32_t rowSize = context->GetResultRowSize();

            MapVectorReader mapReader(mapArg);

            ProcessAllRows(flatResult, rowSize, mapReader);
        }

    private:
        void ProcessAllRows(vec::Vector<int32_t>* result, int32_t rowSize,
                            const MapVectorReader& mapReader) const
        {
            for (int32_t row = 0; row < rowSize; ++row) {
                if (mapReader.containsNull(row)) {
                    // If input map is NULL, output is -1
                    result->SetValue(row, -1);
                } else {
                    // Get map size and set result
                    int64_t mapSize = mapReader.GetSize(row);
                    if (mapSize > std::numeric_limits<int32_t>::max()) {
                        OMNI_THROW("MapSizeFunction Error:",
                                   "Map size " + std::to_string(mapSize) + " exceeds int32_t range");
                    }
                    result->SetValue(row, static_cast<int32_t>(mapSize));
                }
            }
        }
    };
}