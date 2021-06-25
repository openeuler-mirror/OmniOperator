//
// Created by root on 6/1/21.
//

#ifndef __VECTOR_ALLOCATOR_H__
#define __VECTOR_ALLOCATOR_H__

#include <string>
#include <list>
#include <atomic>
#include "chunk.h"
#include "vector_reference.h"
#include "vector_type.h"

using namespace std;

class VectorAllocator {
public:
    VectorAllocator(string scope);

    VectorReference *newVector(int capacityInBytes, int size, VecType type);

    void freeAllVectors();

    string getScope();

    int64_t getAllocatedBytes();

private:
    bool isVariableWidthType(int type);

private:
    string scope;
    // TODO: per list per CPU core.
    list<VectorReference *> vectorList;
    atomic<int64_t> allocatedBytes;
};


#endif //__VECTOR_ALLOCATOR_H__
