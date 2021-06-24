//
// Created by root on 6/1/21.
//

#ifndef __VECTOR_ALLOCATOR_MANAGER_H__
#define __VECTOR_ALLOCATOR_MANAGER_H__

#include <map>
#include "vector_allocator.h"

using namespace std;

const static string GLOBAL_SCOPE_NAME = "___GLOBAL_SCOPE___";

class VectorAllocatorManager {
public:
    VectorAllocatorManager();

    VectorAllocator *getOrCreateAllocator(string scope);

    void deleteAllocator(VectorAllocator **allocator);

    static const VectorAllocatorManager &getInstance();
public:
private:
    std::map<string, VectorAllocator *> allocatorList;
};


#endif //__VECTOR_ALLOCATOR_MANAGER_H__
