//
// Created by root on 6/17/21.
//
#include <long_vector.h>
#include "gtest/gtest.h"
#include "vector_batch.h"

TEST(VectorBatch, constructVectorBatchWithVectorCount) {
    VectorBatch *vectorBatch = new VectorBatch(4);
    LongVector *vector0 = new LongVector(nullptr, 1024);
    LongVector *vector1 = new LongVector(nullptr, 1024);
    LongVector *vector2 = new LongVector(nullptr, 1024);
    LongVector *vector3 = new LongVector(nullptr, 1024);

    vectorBatch->setVector(0, vector0);
    vectorBatch->setVector(1, vector1);
    vectorBatch->setVector(2, vector2);
    vectorBatch->setVector(3, vector3);

    for (int i = 0; i < 4; ++i) {
        EXPECT_EQ(vectorBatch->getVector(0)->getSize(), 1024);
    }
}

TEST(VectorBatch, constructVectorBatchWithTypes) {
    int32_t types[] = {OMNI_VEC_TYPE_INT, OMNI_VEC_TYPE_DOUBLE, OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_DOUBLE};
    VectorBatch *vectorBatch = new VectorBatch(types, 4, 1024);

    for (int i = 0; i < 4; ++i) {
        EXPECT_EQ(vectorBatch->getVector(0)->getSize(), 1024);
    }
}

TEST(VectorBatch, getVectorCount) {
    int32_t types[] = {OMNI_VEC_TYPE_INT, OMNI_VEC_TYPE_DOUBLE, OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_DOUBLE};
    VectorBatch *vectorBatch = new VectorBatch(types, 4, 1024);

    EXPECT_EQ(4, vectorBatch->getVectorCount());
    EXPECT_EQ(1024, vectorBatch->getRowCount());
}

TEST(VectorBatch, getVectorTypes) {
    int32_t types[] = {OMNI_VEC_TYPE_INT, OMNI_VEC_TYPE_DOUBLE, OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_DOUBLE};
    VectorBatch *vectorBatch = new VectorBatch(types, 4, 1024);

    VecType *vectorTypes = vectorBatch->getVectorTypes();
    for (int i = 0; i < 4; ++i) {
        EXPECT_EQ(types[i], vectorTypes[i]);
    }
}

TEST(VectorBatch, freeAllVectors) {
    int32_t types[] = {OMNI_VEC_TYPE_INT, OMNI_VEC_TYPE_DOUBLE, OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_DOUBLE};
    VectorBatch *vectorBatch = new VectorBatch(types, 4, 1024);

    vectorBatch->freeAllVectors();
    for (int i = 0; i < 4; ++i) {
        EXPECT_EQ(nullptr, vectorBatch->getVector(i));
    }
}