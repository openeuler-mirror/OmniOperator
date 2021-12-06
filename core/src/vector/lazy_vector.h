/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

#ifndef OMNI_RUNTIME_LAZY_VECTOR_H
#define OMNI_RUNTIME_LAZY_VECTOR_H

#include "fixed_width_vector.h"
#include "loader/vector_loader.h"

namespace omniruntime {
namespace vec {
class LazyVector : public Vector {
public:
    LazyVector(VectorAllocator *allocator, int32_t size)
        : Vector(allocator, -1, size, OMNI_VEC_TYPE_LAZY), loader(nullptr), loadedVector(nullptr)
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
