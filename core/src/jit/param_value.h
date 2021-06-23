#ifndef __PARAM_H__
#define __PARAM_H__

#include <list>
#include <assert.h>
#include <vector>

using namespace std;

namespace omniruntime {
    namespace jit {

        typedef enum ParamType {
            INT32,
            INT64,
            FP32,
            FP64,
            ARRAY2D,
        } ParamType;

        // Use this struct to encapsulate information to be hardened
        class ParamValue {
        public:
            void *value;
            int size; //the length if it's an array
            ParamType type;
            bool vector = false;

            ParamValue(int *v, int size) : value(v), size(size), type(ParamType::INT32) {}

            ParamValue(int *v) : value(v), size(1), type(ParamType::INT32) {}

            ParamValue(long *v, int size) : value(v), size(size), type(ParamType::INT64) {}

            ParamValue(long *v) : value(v), size(1), type(ParamType::INT64) {}

            ParamValue(double *v, int size) : value(v), size(size), type(ParamType::FP64) {}

            ParamValue(double *v) : value(v), size(1), type(ParamType::FP64) {}

            ParamValue(std::vector<int> *v) : value(v), size(v->size()), type(ParamType::INT32), vector(true) {}

            // FIXME: check each item (ParamValue) size>1 and all items have the same size
            ParamValue(std::list<ParamValue> *v, int size) : value(v), size(size), type(ParamType::ARRAY2D) {}

            ParamValue(void *v, int size, ParamType type) : value(v), size(size), type(type) {}

            std::vector<int> *to_int32_vec() {
                assert(size > 1 && type == INT32 && vector);
                return (std::vector<int> *) value;
            }

            int *to_int32_array() {
                assert(size > 1 && type == INT32);
                return (int *) value;
            };

            int to_int32() {
                assert(size == 1 && type == INT32);
                return *(int *) value;
            };

            long *to_int64_array() {
                assert(size > 1 && type == INT64);
                return (long *) value;
            };

            long to_int64() {
                assert(size == 1 && type == INT64);
                return *(long *) value;
            };

            double *to_fp64_array() {
                assert(size > 1 && type == FP64);
                return (double *) value;
            };

            double to_fp64() {
                assert(size == 1 && type == FP64);
                return *(double *) value;
            };

            std::list<ParamValue> *to_param_list() {
                return (std::list<ParamValue> *) value;
            }
        };
    }
};
#endif