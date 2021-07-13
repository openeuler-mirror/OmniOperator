/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */


#ifndef __CHUNK_H__
#define __CHUNK_H__

#include <iostream>

class Chunk {
public:
    explicit Chunk(int64_t sizeInBytes);

    ~Chunk();

    void *GetAddress() const;

    int64_t GetSizeInBytes();

private:
    void *address;
    int64_t sizeInBytes;
};


#endif // __CHUNK_H__
