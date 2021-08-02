/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */


#ifndef __CHUNK_H__
#define __CHUNK_H__

#include <iostream>

namespace omniruntime {
namespace vec {
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
} // namespace vec
} // namespace omniruntime
#endif // __CHUNK_H__
