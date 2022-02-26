/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.omniruntime.vector;

import nova.hetu.omniruntime.type.DataType;

/**
 * Lazy vector
 */
public class LazyVec extends FixedWidthVec {
    private LazyVecLoader loader;

    public LazyVec(VecAllocator allocator, int size, LazyVecLoader loader) {
        super(allocator, 0, size, VecEncoding.OMNI_VEC_ENCODING_LAZY, DataType.NONE);
        this.loader = loader;
        setLazyLoaderNative(getNativeVector(), loader);
    }

    @Override
    public Vec slice(int start, int length) {
        throw new UnsupportedOperationException("unsupported slice operator for lazy vector.");
    }

    @Override
    public Vec copy() {
        throw new UnsupportedOperationException("unsupported copy operator for lazy vector.");
    }

    @Override
    public Vec copyPositions(int[] positions, int offset, int length) {
        throw new UnsupportedOperationException("unsupported copy positions operator for lazy vector.");
    }

    @Override
    public Vec copyRegion(int start, int length) {
        throw new UnsupportedOperationException("unsupported copy region operator for lazy vector.");
    }

    /**
     * Lazy vector loader
     */
    public interface LazyVecLoader {
        Vec load();
    }

    private static native void setLazyLoaderNative(long nativeVector, Object loader);

    /**
     * For native call, support load data from data source.
     *
     * @param loader loader
     * @return the address of loaded native vector.
     */
    public static long load(Object loader) {
        Vec vec = ((LazyVecLoader) loader).load();
        return vec.getNativeVector();
    }

    @Override
    public int getRealValueBufCapacityInBytes() {
        return 0;
    }

    @Override
    public VecEncoding getEncoding() {
        return VecEncoding.OMNI_VEC_ENCODING_LAZY;
    }
}
