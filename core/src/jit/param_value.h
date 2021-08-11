/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
#ifndef __PARAM_H__
#define __PARAM_H__

#include <list>
#include <vector>
#include "../util/debug.h"

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
            const void *value;
            int size; // the length if it's an array
            ParamType type;
            bool vector = false;

            ParamValue(const int *v, int size) : value(v), size(size), type(ParamType::INT32) {}

            explicit ParamValue(int *v) : value(v), size(-1), type(ParamType::INT32) {}

            ParamValue(const long *v, int size) : value(v), size(size), type(ParamType::INT64) {}

            explicit ParamValue(long *v) : value(v), size(-1), type(ParamType::INT64) {}

            ParamValue(const double *v, int size) : value(v), size(size), type(ParamType::FP64) {}

            explicit ParamValue(double *v) : value(v), size(-1), type(ParamType::FP64) {}

            explicit ParamValue(std::vector<int> *v) : value(v), size(v->size()), type(ParamType::INT32), vector(true)
            {}

            // FIXME: check each item (ParamValue) size>1 and all items have the same size
            ParamValue(std::list<ParamValue> *v, int size) : value(v), size(size), type(ParamType::ARRAY2D) {}

            ParamValue(void *v, int size, ParamType type) : value(v), size(size), type(type) {}

            ~ParamValue(){}

            std::vector<int> *ToInt32Vec()
            {
                ASSERT(size >= 0 && type == INT32 && vector);
                return (std::vector<int> *) value;
            }

            int *ToInt32Array()
            {
                ASSERT(size >= 0 && type == INT32);
                return (int *) value;
            };

            int ToInt32() const
            {
                ASSERT(size == -1 && type == INT32);
                return *(int *) value;
            };

            long *ToInt64Array() const
            {
                ASSERT(size >= 0 && type == INT64);
                return (long *) value;
            };

            long ToInt64() const
            {
                ASSERT(size == -1 && type == INT64);
                return *(long *) value;
            };

            double *ToFp64Array() const
            {
                ASSERT(size >= 0 && type == FP64);
                return (double *) value;
            };

            double ToFp64() const
            {
                ASSERT(size == -1 && type == FP64);
                return *(double *) value;
            };

            std::list<ParamValue> *ToParamList()
            {
                return (std::list<ParamValue> *) value;
            }

            bool IsScalar()
            {
                return size == -1;
            }
        };
    }
};
#endif