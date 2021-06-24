//
// Created by root on 6/1/21.
//

#include <memory.h>
#include "chunk.h"
#include "../memory/memory_pool.h"

Chunk::Chunk(int64_t sizeInBytes) : sizeInBytes(sizeInBytes) {
    address = omni_allocate(sizeInBytes);
    memset(address, 0, sizeInBytes);
}

Chunk::~Chunk() {
    omni_release((int64_t) address);
}

void *Chunk::getAddress() {
    return address;
}

int64_t Chunk::getSizeInBytes() {
    return sizeInBytes;
}