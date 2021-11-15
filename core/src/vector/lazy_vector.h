/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

#ifndef OMNI_RUNTIME_LAZY_VECTOR_H
#define OMNI_RUNTIME_LAZY_VECTOR_H

#include "fixed_width_vector.h"
#include "loader/vector_loader.h"

namespace omniruntime {
namespace vec {
class LazyVector : public FixedWidthVector<int64_t> {
public:
    LazyVector(VectorAllocator *allocator, int32_t size)
        : FixedWidthVector<int64_t>(allocator, sizeof(int64_t), size, LazyVecType::Instance()), loader(nullptr), loadedVector(nullptr)
    {}

    void SetLoader(VectorLoader *loader)
    {
        this->loader = loader;
    }

    VectorLoader *GetLoader()
    {
        return this->loader;
    }

    void AssureLoaded()
    {
        if (loadedVector == nullptr) {
            loadedVector = loader->Load();
        }
    }

    Vector *GetLoadedVector()
    {
        AssureLoaded();
        return loadedVector;
    }

    void SetValues(int startIndex, const int64_t *values, int length) override {
    };

    Vector *Slice(int positionOffset, int length) override
    {
        AssureLoaded();
        return loadedVector->Slice(positionOffset, length);
    };

    Vector *CopyPositions(const int *positions, int offset, int length) override
    {
        AssureLoaded();
        return loadedVector->CopyPositions(positions, offset, length);
    };

    Vector *CopyRegion(int positionOffset, int length) override
    {
        AssureLoaded();
        return loadedVector->CopyRegion(positionOffset, length);
    };

    void Append(Vector *other, int positionOffset, int length) override
    {
        AssureLoaded();
        loadedVector->Append(other, positionOffset, length);
    };

    ~LazyVector()
    {
        if (loader != nullptr) {
            delete loader;
        }
        if (loadedVector != nullptr) {
            delete loadedVector;
        }
    }

private:
    VectorLoader *loader;
    Vector *loadedVector;
};
}
}

#endif // OMNI_RUNTIME_LAZY_VECTOR_H
