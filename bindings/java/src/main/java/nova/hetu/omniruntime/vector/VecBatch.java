/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
package nova.hetu.omniruntime.vector;

import nova.hetu.omniruntime.constants.VecType;

import java.io.Closeable;
import java.util.List;

import static nova.hetu.omniruntime.constants.VecType.OMNI_VEC_TYPE_128_DECIMAL;
import static nova.hetu.omniruntime.constants.VecType.OMNI_VEC_TYPE_256_DECIMAL;
import static nova.hetu.omniruntime.constants.VecType.OMNI_VEC_TYPE_CONTAINER;
import static nova.hetu.omniruntime.constants.VecType.OMNI_VEC_TYPE_DICTIONARY;
import static nova.hetu.omniruntime.constants.VecType.OMNI_VEC_TYPE_DOUBLE;
import static nova.hetu.omniruntime.constants.VecType.OMNI_VEC_TYPE_INT;
import static nova.hetu.omniruntime.constants.VecType.OMNI_VEC_TYPE_LONG;
import static nova.hetu.omniruntime.constants.VecType.OMNI_VEC_TYPE_SHORT;
import static nova.hetu.omniruntime.constants.VecType.OMNI_VEC_TYPE_VARCHAR;
import static nova.hetu.omniruntime.vector.Vec.getTypeNative;

/**
 * vec batch
 *
 * @since 2021-07-17
 */
public class VecBatch
        implements Closeable {
    private final Vec[] vectors;

    private final int rowCount;

    private final long nativeVectorBatch;

    public VecBatch(Vec[] vectors, int size) {
        this.vectors = vectors;
        this.rowCount = size;
        this.nativeVectorBatch = newVectorBatchNative(vectors.length);
        int index = 0;
        for (Vec vector : vectors) {
            setVectorNative(this.nativeVectorBatch, index++, vector.getNativeVector());
        }
    }

    public VecBatch(Vec[] vectors) {
        this(vectors, vectors[0].getSize());
    }

    public VecBatch(List<Vec> vectors, int size) {
        this(vectors.toArray(new Vec[vectors.size()]), size);
    }

    public VecBatch(List<Vec> vectors) {
        this(vectors.toArray(new Vec[vectors.size()]));
    }

    /**
     * This constructor is for native to call
     *
     * @param nativeVectorBatch
     */
    public VecBatch(long nativeVectorBatch, int size) {
        this.nativeVectorBatch = nativeVectorBatch;
        int vectorCount = getVectorCountNative(nativeVectorBatch);
        vectors = new Vec[vectorCount];
        for (int idx = 0; idx < vectorCount; idx++) {
            Vec vector;
            long nativeVector = getVectorNative(nativeVectorBatch, idx);
            VecType type = new VecType(getTypeNative(nativeVector));
            if (OMNI_VEC_TYPE_INT.equals(type)) {
                vector = new IntVec(nativeVector);
            }
            else if (OMNI_VEC_TYPE_LONG.equals(type)) {
                vector = new LongVec(nativeVector);
            }
            else if (OMNI_VEC_TYPE_DOUBLE.equals(type)) {
                vector = new DoubleVec(nativeVector);
            }
            else if (OMNI_VEC_TYPE_SHORT.equals(type)) {
                vector = new ShortVec(nativeVector);
            }
            else if (OMNI_VEC_TYPE_VARCHAR.equals(type)) {
                vector = new VarcharVec(nativeVector);
            }
            else if (OMNI_VEC_TYPE_128_DECIMAL.equals(type)) {
                vector = new Decimal128Vec(nativeVector);
            }
            else if (OMNI_VEC_TYPE_256_DECIMAL.equals(type)) {
                vector = new Decimal256Vec(nativeVector);
            }
            else if (OMNI_VEC_TYPE_DICTIONARY.equals(type)) {
                vector = new DictionaryVec(nativeVector);
            }
            else if (OMNI_VEC_TYPE_CONTAINER.equals(type)) {
                vector = new ContainerVec(nativeVector);
            }
            else {
                throw new IllegalArgumentException(String.format("Not Support Vec Type %s", type));
            }
            vectors[idx] = vector;
        }
        this.rowCount = size;
    }

    public VecBatch(long nativeVectorBatch) {
        this.nativeVectorBatch = nativeVectorBatch;
        int vectorCount = getVectorCountNative(nativeVectorBatch);
        if (vectorCount == 0) {
            throw new IllegalArgumentException("There is no vector in the vec batch.");
        }
        vectors = new Vec[vectorCount];
        for (int idx = 0; idx < vectorCount; idx++) {
            Vec vector;
            long nativeVector = getVectorNative(nativeVectorBatch, idx);
            VecType type = new VecType(getTypeNative(nativeVector));
            if (OMNI_VEC_TYPE_INT.equals(type)) {
                vector = new IntVec(nativeVector);
            }
            else if (OMNI_VEC_TYPE_LONG.equals(type)) {
                vector = new LongVec(nativeVector);
            }
            else if (OMNI_VEC_TYPE_DOUBLE.equals(type)) {
                vector = new DoubleVec(nativeVector);
            }
            else if (OMNI_VEC_TYPE_SHORT.equals(type)) {
                vector = new ShortVec(nativeVector);
            }
            else if (OMNI_VEC_TYPE_CONTAINER.equals(type)) {
                vector = new ContainerVec(nativeVector);
            }
            else if (OMNI_VEC_TYPE_VARCHAR.equals(type)) {
                vector = new VarcharVec(nativeVector);
            }
            else if (OMNI_VEC_TYPE_128_DECIMAL.equals(type)) {
                vector = new Decimal128Vec(nativeVector);
            }
            else if (OMNI_VEC_TYPE_256_DECIMAL.equals(type)) {
                vector = new Decimal256Vec(nativeVector);
            }
            else {
                throw new IllegalArgumentException(String.format("Not Support Vec Type %s", type));
            }
            vectors[idx] = vector;
        }
        this.rowCount = vectors[0].getSize();
    }

    /**
     * row count in the vecBatch
     *
     * @return row count
     */
    public int getRowCount() {
        return rowCount;
    }

    /**
     * vector count in the vecBatch
     *
     * @return vector count
     */
    public int getVectorCount() {
        return vectors.length;
    }

    public Vec[] getVectors() {
        return vectors;
    }

    public long getNativeVectorBatch() {
        return nativeVectorBatch;
    }

    @Override
    public void close() {
        for (Vec vector : vectors) {
            vector.close();
        }
        freeVectorBatchNative(nativeVectorBatch);
    }

    /**
     * create vector batch based on the number of vectors
     *
     * @param vectorCount vector count
     * @return vecotr batch address
     */
    public static native long newVectorBatchNative(int vectorCount);

    /**
     * release vector batch
     *
     * @param nativeVectorBatch vector batch address
     */
    public static native void freeVectorBatchNative(long nativeVectorBatch);

    /**
     * get vector count
     *
     * @param nativeVectorBatch vector batch address
     * @return vector count
     */
    public static native int getVectorCountNative(long nativeVectorBatch);

    /**
     * set vector
     *
     * @param nativeVectorBatch vector batch address
     * @param index vector offset in vector batch
     * @param nativeVector vector address
     */
    public static native void setVectorNative(long nativeVectorBatch, int index, long nativeVector);

    /**
     * get vector
     *
     * @param nativeVectorBatch vector batch address
     * @param index vector offset in vector batch
     * @return vector address
     */
    public static native long getVectorNative(long nativeVectorBatch, int index);
}
