//
// Created by root on 6/1/21.
//

#ifndef __CHUNK_H__
#define __CHUNK_H__

#include <stdint.h>

class Chunk {
public:
    Chunk(int64_t sizeInBytes);

    ~Chunk();

    void *getAddress();

    int64_t getSizeInBytes();

private:
    void *address;
    int64_t sizeInBytes;
};


#endif //__CHUNK_H__
