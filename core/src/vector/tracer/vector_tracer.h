/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

#ifndef OMNI_RUNTIME_VECTOR_TRACER_H
#define OMNI_RUNTIME_VECTOR_TRACER_H

#include <vector>
#include <string>

namespace omniruntime {
namespace vec {
enum VecOpType {
    NEW = 0,
    SLICE,
    FREE,
    JNI_NEW,
    JNI_SLICE,
    JNI_SLICE_SRC,
    JNI_COPY_POSITIONS,
    JNI_COPY_POSITIONS_SRC,
    JNI_COPY_REGION,
    JNI_COPY_REGION_SRC,
    JNI_FREE,
    JNI_ADD_INPUT,
    JNI_GET_OUTPUT,
    MAX_OP_TYPE
};

class VecOpTypeName {
public:
    static std::string GetName(int vecOpType)
    {
        static std::string nameMap[MAX_OP_TYPE + 1] = {
            "NEW",
            "SLICE",
            "FREE",
            "JNI_NEW",
            "JNI_SLICE",
            "JNI_SLICE_SRC",
            "JNI_COPY_POSITIONS",
            "JNI_COPY_POSITIONS_SRC",
            "JNI_COPY_REGION",
            "JNI_COPY_REGION_SRC",
            "JNI_FREE",
            "JNI_ADD_INPUT",
            "JNI_GET_OUTPUT",
            "INVALID_TYPE"
        };
        return nameMap[vecOpType];
    }
};

class Vector;
class VectorTracer {
public:
    explicit VectorTracer(const std::string &scope, const Vector *vec);

    ~VectorTracer();

    void Record(const std::string &stack, VecOpType vecOpType);

    void Print(const char *message);

    const Vector *GetVec()
    {
        return vec;
    }

    void Close()
    {
        closed = true;
    }

    bool IsClosed()
    {
        return closed;
    }

    VectorTracer *next;

private:
    const std::string &scope;
    const Vector *vec;
    bool closed;

    void PrepareTracerHeaderLog(std::stringstream &ss, const char *message) const;
};
}
}
#endif // OMNI_RUNTIME_VECTOR_TRACER_H
