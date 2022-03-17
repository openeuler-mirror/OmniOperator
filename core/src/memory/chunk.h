/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */


#ifndef CHUNK_H
#define CHUNK_H

#include <iostream>

namespace omniruntime {
namespace mem {
class Chunk {
public:
    explicit Chunk(int64_t sizeInBytes);

    ~Chunk();

    void *GetAddress() const;

    int64_t GetSizeInBytes();

private:
    void *address = nullptr;
    int64_t sizeInBytes;
};
} // namespace mem
} // namespace omniruntime
#endif // CHUNK_H
