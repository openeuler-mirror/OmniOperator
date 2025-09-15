/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * Description: Batch Hash Function Registry
 */
#include "batch_func_registry_hash.h"
#include "batch_functions/batch_murmur3_hash.h"

namespace omniruntime::codegen {
using namespace omniruntime::type;
using namespace codegen::function;

std::vector<Function> BatchHashFunctionRegistry::GetFunctions()
{
    DataTypeId retType = OMNI_INT;
    std::string batchMm3fnStr = "batch_mm3hash";
    std::vector<Function> batchHashFunctions = { Function(reinterpret_cast<void *>(BatchCombineHash),
        "batch_combine_hash", {}, { OMNI_LONG, OMNI_LONG }, OMNI_LONG, INPUT_DATA_AND_NULL),
        Function(reinterpret_cast<void *>(BatchMm3Int32), batchMm3fnStr, {}, { OMNI_INT, OMNI_INT }, retType,
            INPUT_DATA_AND_NULL),
        Function(reinterpret_cast<void *>(BatchMm3Int32), batchMm3fnStr, {}, { OMNI_DATE32, OMNI_INT }, retType,
            INPUT_DATA_AND_NULL),
        Function(reinterpret_cast<void *>(BatchMm3Int64), batchMm3fnStr, {}, { OMNI_LONG, OMNI_INT }, retType,
            INPUT_DATA_AND_NULL),
        Function(reinterpret_cast<void *>(BatchMm3Int64), batchMm3fnStr, {}, { OMNI_TIMESTAMP, OMNI_INT }, retType,
            INPUT_DATA_AND_NULL),
        Function(reinterpret_cast<void *>(BatchMm3Double), batchMm3fnStr, {}, { OMNI_DOUBLE, OMNI_INT }, retType,
            INPUT_DATA_AND_NULL),
        Function(reinterpret_cast<void *>(BatchMm3String), batchMm3fnStr, {}, { OMNI_VARCHAR, OMNI_INT }, retType,
            INPUT_DATA_AND_NULL),
        Function(reinterpret_cast<void *>(BatchMm3Decimal64), batchMm3fnStr, {}, { OMNI_DECIMAL64, OMNI_INT }, retType,
            INPUT_DATA_AND_NULL),
        Function(reinterpret_cast<void *>(BatchMm3Decimal128), batchMm3fnStr, {}, { OMNI_DECIMAL128, OMNI_INT },
            retType, INPUT_DATA_AND_NULL),
        Function(reinterpret_cast<void *>(BatchMm3Boolean), batchMm3fnStr, {}, { OMNI_BOOLEAN, OMNI_INT }, retType,
            INPUT_DATA_AND_NULL) };

    return batchHashFunctions;
}
}
