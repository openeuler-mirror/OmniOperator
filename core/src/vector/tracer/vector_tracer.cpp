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
    if (closed) {
        std::string opStack = stack + "(" + VecOpTypeName::GetName(vecOpType) + ")";
        std::stringstream ss;
        PrepareTracerHeaderLog(ss, "Already Released");
        LogError("%s %s", ss.str().c_str(), opStack.c_str());
    }
#ifdef TRACE
    std::string opStack = stack + "(" + VecOpTypeName::GetName(vecOpType) + ")";
    std::stringstream ss;
    PrepareTracerHeaderLog(ss, "Normal");
    LogTrace("%s %s", ss.str().c_str(), opStack.c_str());
#endif
}

void VectorTracer::Print(const char *message)
{
    std::stringstream ss;
    PrepareTracerHeaderLog(ss, message);
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