/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * @Description: abstract class memory builder
 */

#ifndef OMNI_RUNTIME_MEMORY_BUILDER_H
#define OMNI_RUNTIME_MEMORY_BUILDER_H

namespace omniruntime {
namespace op {
class MemoryBuilder {
public:
    MemoryBuilder() = default;
    virtual ~MemoryBuilder() = default;

    virtual int64_t GetRowCount() = 0;
};
}
}
#endif // OMNI_RUNTIME_MEMORY_BUILDER_H
