//
// Created by root on 10/26/21.
//
#include <stdint.h>
#ifndef OMNI_RUNTIME_DEFINITIONS_H
#define OMNI_RUNTIME_DEFINITIONS_H
namespace omniruntime {
namespace op {

constexpr int32_t MAX_TABLE_SIZE_IN_BYTES = 1024 * 1024;
constexpr int32_t DEFAULT_HASHTABLE_SIZE = 512;
constexpr int32_t DEFAULT_TEMP_MEM_SIZE = 8192;
constexpr int32_t AVG_VECTOR_COUNT = 2;

}
}
#endif // OMNI_RUNTIME_DEFINITIONS_H
