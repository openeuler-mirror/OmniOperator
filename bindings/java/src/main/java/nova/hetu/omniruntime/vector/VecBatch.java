/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

package nova.hetu.omniruntime.vector;

import nova.hetu.omniruntime.type.VecType;
import nova.hetu.omniruntime.type.VecTypeSerializer;

import java.io.Closeable;
import java.util.Arrays;
import java.util.List;

import static nova.hetu.omniruntime.vector.Vec.getTypeNative;

/**
 * vec batch
 *
 * @since 2021-07-17
 */
public class VecBatch implements Closeable {
    private final Vec[] vectors;

    private final int rowCount;

    private final long nativeVectorBatch;

    public VecBatch(Vec[] vectors, int rowCount) {
        this.vectors = vectors;
        this.rowCount = rowCount;
        long[] nativeVectors = Arrays.asList(vectors).stream().mapToLong(vec -> vec.getNativeVector()).toArray();
        this.nativeVectorBatch = newVectorBatchNative(nativeVectors, rowCount);
    }

    public VecBatch(Vec[] vectors) {
        this(vectors, vectors[0].getSize());
    }

    public VecBatch(List<Vec> vectors, int rowCount) {
        this(vectors.toArray(new Vec[vectors.size()]), rowCount);
    }

    public VecBatch(List<Vec> vectors) {
        this(vectors.toArray(new Vec[vectors.size()]));
    }

    /**
     * This constructor is for native to call
     *
     * @param nativeVecBatch
     */
    public VecBatch(long nativeVecBatch, long[] nativeVectors, int rowCount) {
        int vecCount = nativeVectors.length;
        Vec[] newVectors = new Vec[vecCount];
        for (int idx = 0; idx < vecCount; idx++) {
            long nativeVector = nativeVectors[idx];
            VecType vecType = VecTypeSerializer.deserializeSingle(getTypeNative(nativeVector));
            newVectors[idx] = VecFactory.create(nativeVector, vecType);
        }
        this.rowCount = rowCount;
        this.nativeVectorBatch = nativeVecBatch;
        this.vectors = newVectors;
    }

    /**
     * create vector batch based on the number of vectors
     *
     * @param nativeVectors native vector array.
     * @param rowCount      the row count of vector batch
     * @return vector batch address
     */
    public static native long newVectorBatchNative(long[] nativeVectors, int rowCount);

    /**
     * release vector batch
     *
     * @param nativeVectorBatch vector batch address
     */
    public static native void freeVectorBatchNative(long nativeVectorBatch);

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

    /**
     * release all vectors resource of vector batch.
     */
    public void releaseAllVectors() {
        for (Vec vector : vectors) {
            vector.close();
        }
    }

    @Override
    public void close() {
        freeVectorBatchNative(nativeVectorBatch);
    }
}
