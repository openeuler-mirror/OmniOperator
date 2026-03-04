/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: visitor class for expressions
 */

#include "RegistrationHelpers.h"
#include "vectorization/functions/ArrayFlattenFunction.h"
#include "vectorization/functions/SubscriptUtil.h"
#include "vectorization/functions/Slice.h"
#include "vectorization/functions/SizeFunction.h"
#include "vectorization/functions/ReverseFunction.h"
#include "vectorization/functions/ArrayMinMaxFunction.h"
#include "vectorization/functions/ArrayCompactFunction.h"
#include "vectorization/functions/ArrayContainsFunction.h"
#include "vectorization/functions/ArrayRepeatFunction.h"
#include "vectorization/functions/ArrayRemoveFunction.h"
#include "vectorization/functions/ArrayDistinctFunction.h"
#include "vectorization/functions/ArrayExceptFunction.h"

namespace omniruntime::vectorization {

void RegisterArrayFunctions(const std::string &prefix)
{
    VectorFunction::RegisterVectorFunction("get_array_item", {OMNI_ARRAY, OMNI_INT}, OMNI_INT,
        std::make_shared<SubscriptImpl>());
    VectorFunction::RegisterVectorFunction("get_array_item", {OMNI_ARRAY, OMNI_INT}, OMNI_VARCHAR,
        std::make_shared<SubscriptImpl>());
    VectorFunction::RegisterVectorFunction("slice", {OMNI_ARRAY, OMNI_INT, OMNI_INT}, OMNI_ARRAY,
        std::make_shared<SliceImpl>());
    VectorFunction::RegisterVectorFunction("slice", {OMNI_ARRAY, OMNI_LONG, OMNI_LONG}, OMNI_ARRAY,
        std::make_shared<SliceImpl>());

    // Register size function for Array type (Collection Functions)
    // Note: SizeFunction handles both Array and Map types with proper legacySizeOfNull support
    VectorFunction::RegisterVectorFunction("size", {OMNI_ARRAY, OMNI_BOOLEAN}, OMNI_INT,
        std::make_shared<SizeFunction>());

    // Register reverse function for Array type
    // reverse(array<T>) -> array<T>: Returns the array with elements in reverse order
    VectorFunction::RegisterVectorFunction("reverse", {OMNI_ARRAY}, OMNI_ARRAY,
        std::make_shared<ReverseFunction>());
    VectorFunction::RegisterVectorFunction("flatten", {OMNI_ARRAY}, OMNI_ARRAY,
        std::make_shared<ArrayFlattenFunction>());
    
    // Register array_max function for all supported element types
    // array_max(array<T>) -> T: Returns the maximum value in the array
    VectorFunction::RegisterVectorFunction("array_max", {OMNI_ARRAY}, OMNI_BYTE,
        std::make_shared<ArrayMaxFunction>());
    VectorFunction::RegisterVectorFunction("array_max", {OMNI_ARRAY}, OMNI_SHORT,
        std::make_shared<ArrayMaxFunction>());
    VectorFunction::RegisterVectorFunction("array_max", {OMNI_ARRAY}, OMNI_INT,
        std::make_shared<ArrayMaxFunction>());
    VectorFunction::RegisterVectorFunction("array_max", {OMNI_ARRAY}, OMNI_LONG,
        std::make_shared<ArrayMaxFunction>());
    VectorFunction::RegisterVectorFunction("array_max", {OMNI_ARRAY}, OMNI_FLOAT,
        std::make_shared<ArrayMaxFunction>());
    VectorFunction::RegisterVectorFunction("array_max", {OMNI_ARRAY}, OMNI_DOUBLE,
        std::make_shared<ArrayMaxFunction>());
    VectorFunction::RegisterVectorFunction("array_max", {OMNI_ARRAY}, OMNI_BOOLEAN,
        std::make_shared<ArrayMaxFunction>());
    VectorFunction::RegisterVectorFunction("array_max", {OMNI_ARRAY}, OMNI_DECIMAL128,
        std::make_shared<ArrayMaxFunction>());
    
    // Register array_min function for all supported element types
    // array_min(array<T>) -> T: Returns the minimum value in the array
    VectorFunction::RegisterVectorFunction("array_min", {OMNI_ARRAY}, OMNI_BYTE,
        std::make_shared<ArrayMinFunction>());
    VectorFunction::RegisterVectorFunction("array_min", {OMNI_ARRAY}, OMNI_SHORT,
        std::make_shared<ArrayMinFunction>());
    VectorFunction::RegisterVectorFunction("array_min", {OMNI_ARRAY}, OMNI_INT,
        std::make_shared<ArrayMinFunction>());
    VectorFunction::RegisterVectorFunction("array_min", {OMNI_ARRAY}, OMNI_LONG,
        std::make_shared<ArrayMinFunction>());
    VectorFunction::RegisterVectorFunction("array_min", {OMNI_ARRAY}, OMNI_FLOAT,
        std::make_shared<ArrayMinFunction>());
    VectorFunction::RegisterVectorFunction("array_min", {OMNI_ARRAY}, OMNI_DOUBLE,
        std::make_shared<ArrayMinFunction>());
    VectorFunction::RegisterVectorFunction("array_min", {OMNI_ARRAY}, OMNI_BOOLEAN,
        std::make_shared<ArrayMinFunction>());
    VectorFunction::RegisterVectorFunction("array_min", {OMNI_ARRAY}, OMNI_DECIMAL128,
        std::make_shared<ArrayMinFunction>());

    // Register array_compact function
    // array_compact(array<T>) -> array<T>: Removes all null elements from the array
    VectorFunction::RegisterVectorFunction("array_compact", {OMNI_ARRAY}, OMNI_ARRAY,
        std::make_shared<ArrayCompactFunction>());

    // Register array_contains function for all supported element types
    // array_contains(array<T>, T) -> boolean: Checks if array contains the given value
    VectorFunction::RegisterVectorFunction("array_contains", {OMNI_ARRAY, OMNI_BYTE}, OMNI_BOOLEAN,
        std::make_shared<ArrayContainsFunction>());
    VectorFunction::RegisterVectorFunction("array_contains", {OMNI_ARRAY, OMNI_SHORT}, OMNI_BOOLEAN,
        std::make_shared<ArrayContainsFunction>());
    VectorFunction::RegisterVectorFunction("array_contains", {OMNI_ARRAY, OMNI_INT}, OMNI_BOOLEAN,
        std::make_shared<ArrayContainsFunction>());
    VectorFunction::RegisterVectorFunction("array_contains", {OMNI_ARRAY, OMNI_LONG}, OMNI_BOOLEAN,
        std::make_shared<ArrayContainsFunction>());
    VectorFunction::RegisterVectorFunction("array_contains", {OMNI_ARRAY, OMNI_FLOAT}, OMNI_BOOLEAN,
        std::make_shared<ArrayContainsFunction>());
    VectorFunction::RegisterVectorFunction("array_contains", {OMNI_ARRAY, OMNI_DOUBLE}, OMNI_BOOLEAN,
        std::make_shared<ArrayContainsFunction>());
    VectorFunction::RegisterVectorFunction("array_contains", {OMNI_ARRAY, OMNI_BOOLEAN}, OMNI_BOOLEAN,
        std::make_shared<ArrayContainsFunction>());

    // Register array_repeat function for all supported element types
    // array_repeat(element T, count INT) -> array<T>
    VectorFunction::RegisterVectorFunction("array_repeat", {OMNI_BYTE, OMNI_INT}, OMNI_ARRAY,
        std::make_shared<ArrayRepeatFunction>());
    VectorFunction::RegisterVectorFunction("array_repeat", {OMNI_SHORT, OMNI_INT}, OMNI_ARRAY,
        std::make_shared<ArrayRepeatFunction>());
    VectorFunction::RegisterVectorFunction("array_repeat", {OMNI_INT, OMNI_INT}, OMNI_ARRAY,
        std::make_shared<ArrayRepeatFunction>());
    VectorFunction::RegisterVectorFunction("array_repeat", {OMNI_LONG, OMNI_INT}, OMNI_ARRAY,
        std::make_shared<ArrayRepeatFunction>());
    VectorFunction::RegisterVectorFunction("array_repeat", {OMNI_FLOAT, OMNI_INT}, OMNI_ARRAY,
        std::make_shared<ArrayRepeatFunction>());
    VectorFunction::RegisterVectorFunction("array_repeat", {OMNI_DOUBLE, OMNI_INT}, OMNI_ARRAY,
        std::make_shared<ArrayRepeatFunction>());
    VectorFunction::RegisterVectorFunction("array_repeat", {OMNI_BOOLEAN, OMNI_INT}, OMNI_ARRAY,
        std::make_shared<ArrayRepeatFunction>());
    VectorFunction::RegisterVectorFunction("array_repeat", {OMNI_DECIMAL128, OMNI_INT}, OMNI_ARRAY,
        std::make_shared<ArrayRepeatFunction>());
    VectorFunction::RegisterVectorFunction("array_repeat", {OMNI_VARCHAR, OMNI_INT}, OMNI_ARRAY,
        std::make_shared<ArrayRepeatFunction>());
    VectorFunction::RegisterVectorFunction("array_repeat", {OMNI_CHAR, OMNI_INT}, OMNI_ARRAY,
        std::make_shared<ArrayRepeatFunction>());
    VectorFunction::RegisterVectorFunction("array_repeat", {OMNI_VARBINARY, OMNI_INT}, OMNI_ARRAY,
        std::make_shared<ArrayRepeatFunction>());
    VectorFunction::RegisterVectorFunction("array_repeat", {OMNI_DATE32, OMNI_INT}, OMNI_ARRAY,
        std::make_shared<ArrayRepeatFunction>());
    VectorFunction::RegisterVectorFunction("array_repeat", {OMNI_TIMESTAMP, OMNI_INT}, OMNI_ARRAY,
        std::make_shared<ArrayRepeatFunction>());
    VectorFunction::RegisterVectorFunction("array_repeat", {OMNI_DECIMAL64, OMNI_INT}, OMNI_ARRAY,
        std::make_shared<ArrayRepeatFunction>());
    VectorFunction::RegisterVectorFunction("array_repeat", {OMNI_NONE, OMNI_INT}, OMNI_ARRAY,
        std::make_shared<ArrayRepeatFunction>());

    // Register array_repeat with OMNI_BOOLEAN count type for NULL literal handling
    // When Substrait encodes NULL count (kNothing type), it maps to OMNI_BOOLEAN
    VectorFunction::RegisterVectorFunction("array_repeat", {OMNI_BYTE, OMNI_BOOLEAN}, OMNI_ARRAY,
        std::make_shared<ArrayRepeatFunction>());
    VectorFunction::RegisterVectorFunction("array_repeat", {OMNI_SHORT, OMNI_BOOLEAN}, OMNI_ARRAY,
        std::make_shared<ArrayRepeatFunction>());
    VectorFunction::RegisterVectorFunction("array_repeat", {OMNI_INT, OMNI_BOOLEAN}, OMNI_ARRAY,
        std::make_shared<ArrayRepeatFunction>());
    VectorFunction::RegisterVectorFunction("array_repeat", {OMNI_LONG, OMNI_BOOLEAN}, OMNI_ARRAY,
        std::make_shared<ArrayRepeatFunction>());
    VectorFunction::RegisterVectorFunction("array_repeat", {OMNI_FLOAT, OMNI_BOOLEAN}, OMNI_ARRAY,
        std::make_shared<ArrayRepeatFunction>());
    VectorFunction::RegisterVectorFunction("array_repeat", {OMNI_DOUBLE, OMNI_BOOLEAN}, OMNI_ARRAY,
        std::make_shared<ArrayRepeatFunction>());
    VectorFunction::RegisterVectorFunction("array_repeat", {OMNI_BOOLEAN, OMNI_BOOLEAN}, OMNI_ARRAY,
        std::make_shared<ArrayRepeatFunction>());
    VectorFunction::RegisterVectorFunction("array_repeat", {OMNI_DECIMAL128, OMNI_BOOLEAN}, OMNI_ARRAY,
        std::make_shared<ArrayRepeatFunction>());
    VectorFunction::RegisterVectorFunction("array_repeat", {OMNI_VARCHAR, OMNI_BOOLEAN}, OMNI_ARRAY,
        std::make_shared<ArrayRepeatFunction>());
    VectorFunction::RegisterVectorFunction("array_repeat", {OMNI_CHAR, OMNI_BOOLEAN}, OMNI_ARRAY,
        std::make_shared<ArrayRepeatFunction>());
    VectorFunction::RegisterVectorFunction("array_repeat", {OMNI_VARBINARY, OMNI_BOOLEAN}, OMNI_ARRAY,
        std::make_shared<ArrayRepeatFunction>());
    VectorFunction::RegisterVectorFunction("array_repeat", {OMNI_DATE32, OMNI_BOOLEAN}, OMNI_ARRAY,
        std::make_shared<ArrayRepeatFunction>());
    VectorFunction::RegisterVectorFunction("array_repeat", {OMNI_TIMESTAMP, OMNI_BOOLEAN}, OMNI_ARRAY,
        std::make_shared<ArrayRepeatFunction>());
    VectorFunction::RegisterVectorFunction("array_repeat", {OMNI_DECIMAL64, OMNI_BOOLEAN}, OMNI_ARRAY,
        std::make_shared<ArrayRepeatFunction>());
    VectorFunction::RegisterVectorFunction("array_repeat", {OMNI_NONE, OMNI_BOOLEAN}, OMNI_ARRAY,
        std::make_shared<ArrayRepeatFunction>());

    // Register array_remove function for all supported element types
    // array_remove(array<T>, T) -> array<T>: Remove all elements equal to a given value from an array
    VectorFunction::RegisterVectorFunction("array_remove", {OMNI_ARRAY, OMNI_BYTE}, OMNI_ARRAY,
        std::make_shared<ArrayRemoveFunction>());
    VectorFunction::RegisterVectorFunction("array_remove", {OMNI_ARRAY, OMNI_SHORT}, OMNI_ARRAY,
        std::make_shared<ArrayRemoveFunction>());
    VectorFunction::RegisterVectorFunction("array_remove", {OMNI_ARRAY, OMNI_INT}, OMNI_ARRAY,
        std::make_shared<ArrayRemoveFunction>());
    VectorFunction::RegisterVectorFunction("array_remove", {OMNI_ARRAY, OMNI_LONG}, OMNI_ARRAY,
        std::make_shared<ArrayRemoveFunction>());
    VectorFunction::RegisterVectorFunction("array_remove", {OMNI_ARRAY, OMNI_FLOAT}, OMNI_ARRAY,
        std::make_shared<ArrayRemoveFunction>());
    VectorFunction::RegisterVectorFunction("array_remove", {OMNI_ARRAY, OMNI_DOUBLE}, OMNI_ARRAY,
        std::make_shared<ArrayRemoveFunction>());
    VectorFunction::RegisterVectorFunction("array_remove", {OMNI_ARRAY, OMNI_BOOLEAN}, OMNI_ARRAY,
        std::make_shared<ArrayRemoveFunction>());
    VectorFunction::RegisterVectorFunction("array_remove", {OMNI_ARRAY, OMNI_DECIMAL128}, OMNI_ARRAY,
        std::make_shared<ArrayRemoveFunction>());
    VectorFunction::RegisterVectorFunction("array_remove", {OMNI_ARRAY, OMNI_VARCHAR}, OMNI_ARRAY,
        std::make_shared<ArrayRemoveFunction>());
    VectorFunction::RegisterVectorFunction("array_remove", {OMNI_ARRAY, OMNI_CHAR}, OMNI_ARRAY,
        std::make_shared<ArrayRemoveFunction>());
    VectorFunction::RegisterVectorFunction("array_remove", {OMNI_ARRAY, OMNI_VARBINARY}, OMNI_ARRAY,
        std::make_shared<ArrayRemoveFunction>());
    VectorFunction::RegisterVectorFunction("array_remove", {OMNI_ARRAY, OMNI_DATE32}, OMNI_ARRAY,
        std::make_shared<ArrayRemoveFunction>());
    VectorFunction::RegisterVectorFunction("array_remove", {OMNI_ARRAY, OMNI_TIMESTAMP}, OMNI_ARRAY,
        std::make_shared<ArrayRemoveFunction>());
    VectorFunction::RegisterVectorFunction("array_remove", {OMNI_ARRAY, OMNI_DECIMAL64}, OMNI_ARRAY,
        std::make_shared<ArrayRemoveFunction>());
    VectorFunction::RegisterVectorFunction("array_remove", {OMNI_ARRAY, OMNI_NONE}, OMNI_ARRAY,
        std::make_shared<ArrayRemoveFunction>());

    // Register array_distinct function
    // array_distinct(array<T>) -> array<T>: Removes duplicate elements from the array
    VectorFunction::RegisterVectorFunction("array_distinct", {OMNI_ARRAY}, OMNI_ARRAY,
        std::make_shared<ArrayDistinctFunction>());

    // Register array_except function
    // array_except(array<T>, array<T>) -> array<T>: Returns elements in first array but not in second
    VectorFunction::RegisterVectorFunction("array_except", {OMNI_ARRAY, OMNI_ARRAY}, OMNI_ARRAY,
        std::make_shared<ArrayExceptFunction>());
}
}
