/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
package nova.hetu.omniruntime.vector;

/**
 * vec allocator
 *
 * @since 2021-07-17
 */
public class VecAllocator
        implements AutoCloseable {
    /**
     * global vector allocator
     */
    public static final VecAllocator GLOBAL_VECTOR_ALLOCATOR = new VecAllocator(getGlobalAllocatorNative());

    private long nativeAllocator;

    public VecAllocator(String scope) {
        nativeAllocator = newAllocatorNative(scope);
    }

    public VecAllocator(long nativeAllocator) {
        this.nativeAllocator = nativeAllocator;
    }

    public long getNativeAllocator() {
        return nativeAllocator;
    }

    @Override
    public void close() {
        freeAllocatorNative(nativeAllocator);
    }

    private static native long newAllocatorNative(String scope);

    private static native long freeAllocatorNative(long nativeAllocator);

    private static native long getGlobalAllocatorNative();
}
