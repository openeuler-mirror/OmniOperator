/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
#ifndef __PARAM_H__
#define __PARAM_H__

#include <list>
#include <assert.h>
#include <vector>

namespace omniruntime {
    namespace jit {

        using namespace std;

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
            int size; // the length if it's an array
            ParamType type;
            bool vector = false;

            ParamValue(int *v, int size) : value(v), size(size), type(ParamType::INT32) {}

            explicit ParamValue(int *v) : value(v), size(-1), type(ParamType::INT32) {}

            ParamValue(long *v, int size) : value(v), size(size), type(ParamType::INT64) {}

            explicit ParamValue(long *v) : value(v), size(-1), type(ParamType::INT64) {}

            ParamValue(double *v, int size) : value(v), size(size), type(ParamType::FP64) {}

            explicit ParamValue(double *v) : value(v), size(-1), type(ParamType::FP64) {}

            explicit ParamValue(std::vector<int> *v) : value(v), size(v->size()), type(ParamType::INT32), vector(true)
            {}

            // FIXME: check each item (ParamValue) size>1 and all items have the same size
            ParamValue(std::list<ParamValue> *v, int size) : value(v), size(size), type(ParamType::ARRAY2D) {}

            ParamValue(void *v, int size, ParamType type) : value(v), size(size), type(type) {}

            ~ParamValue(){}

            std::vector<int> *ToInt32Vec()
            {
#ifdef DEBUG
#define ASSERT(f) assert(size >= 0 && type == INT32 && vector);
#else
#define ASSERT(f) ((void)0)
#endif
                return (std::vector<int> *) value;
            }

            int *ToInt32Array()
            {
#ifdef DEBUG
#define ASSERT(f) assert(size >= 0 && type == INT32);
#else
#define ASSERT(f) ((void)0)
#endif
                return (int *) value;
            };

            int ToInt32() const
            {
#ifdef DEBUG
#define ASSERT(f) assert(size == -1 && type == INT32);
#else
#define ASSERT(f) ((void)0)
#endif
                return *(int *) value;
            };

            long *ToInt64Array() const
            {
#ifdef DEBUG
#define ASSERT(f) assert(size >= 0 && type == INT64);
#else
#define ASSERT(f) ((void)0)
#endif
                return (long *) value;
            };

            long ToInt64() const
            {
#ifdef DEBUG
#define ASSERT(f) assert(size == -1 && type == INT64);
#else
#define ASSERT(f) ((void)0)
#endif
                return *(long *) value;
            };

            double *ToFp64Array() const
            {
#ifdef DEBUG
#define ASSERT(f) assert(size >= 0 && type == FP64);
#else
#define ASSERT(f) ((void)0)
#endif
                return (double *) value;
            };

            double ToFp64() const
            {
#ifdef DEBUG
#define ASSERT(f) assert(size == -1 && type == FP64);
#else
#define ASSERT(f) ((void)0)
#endif
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