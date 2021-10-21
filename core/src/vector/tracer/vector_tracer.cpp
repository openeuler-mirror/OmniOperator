/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

#include "vector_tracer.h"

#include <iostream>
#include <sstream>

#include "../vector.h"

namespace omniruntime {
namespace vec {
VectorTracer::VectorTracer(const std::string &scope, const Vector *vec) : scope(scope), vec(vec), closed(false) {}

VectorTracer::~VectorTracer() {}

void VectorTracer::Record(std::string stack, VecOpType vecOpType)
{
    std::string opStack = stack + "(" + VecOpTypeName::GetName(vecOpType) + ")";
#ifdef TRACE
    std::stringstream ss;
    PrepareTracerHeaderLog(ss, "Normal");
    LogTrace("%s %s", ss.str().c_str(), opStack.c_str());
#endif
    bool insertToPath = true;
    switch (vecOpType) {
        case NEW:
        case SLICE:
        case JNI_SLICE_SRC:
        case JNI_COPY_POSITIONS_SRC:
        case JNI_COPY_REGION_SRC:
            break;
        case JNI_NEW:
        case JNI_SLICE:
        case JNI_COPY_POSITIONS:
        case JNI_COPY_REGION:
            // pop native stack, just record JNI_XXX.
            path.pop_back();
            break;
        case JNI_FREE:
        case JNI_ADD_INPUT:
        case JNI_GET_OUTPUT:
            if (closed) {
                Print("Vector be used after free");
            }
            break;
        case FREE:
            // if path already recorded JNI_FREE, don't need record FREE again.
            if (-1 != path.back().find(VecOpTypeName::GetName(VecOpType::JNI_FREE))) {
                insertToPath = false;
            }
            if (closed) {
                Print("Double Free");
            }
            closed = true;
            break;
        default:
            std::cerr << "error operator type: " << vecOpType << std::endl;
            break;
    }
    if (insertToPath) {
        path.push_back(opStack);
    }
}

void VectorTracer::Print(const char *message)
{
    std::stringstream ss;
    PrepareTracerHeaderLog(ss, message);
    int pathSize = path.size();
    for (int i = 0; i < pathSize; ++i) {
        ss << path[i];
        if (i != pathSize - 1) {
            ss << "\n\t->";
        }
    }
    LogError("%s", ss.str().c_str());
}

void VectorTracer::PrepareTracerHeaderLog(std::stringstream &ss, const char *message) const
{
    ss << "[scope:" << scope << "][" << message << "][vecAddr:" << vec << "][vecTypeId:" << vec->GetTypeId() <<
        "][vecSize:" << vec->GetSize() << "]";
    ss << "\n\t";
}
}
}