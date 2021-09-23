/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

package nova.hetu.omniruntime.vector;

import nova.hetu.omniruntime.OmniLibs;

/**
 * vec allocator
 *
 * @since 2021-07-17
 */
public class VecAllocator implements AutoCloseable {
    /**
     * global vector allocator
     */
    public static final VecAllocator GLOBAL_VECTOR_ALLOCATOR;

    private static String GLOBAL_SCOPE = "___GLOBAL_SCOPE___";

    private long nativeAllocator;

    static {
        OmniLibs.load();
        GLOBAL_VECTOR_ALLOCATOR = new VecAllocator(newAllocatorNative(GLOBAL_SCOPE));
    }

    public VecAllocator(String scope) {
        nativeAllocator = newAllocatorNative(scope);
    }

    public VecAllocator(long nativeAllocator) {
        this.nativeAllocator = nativeAllocator;
    }

    private static native long newAllocatorNative(String scope);

    private static native long freeAllocatorNative(long nativeAllocator);

    public long getNativeAllocator() {
        return nativeAllocator;
    }

    @Override
    public void close() {
        freeAllocatorNative(nativeAllocator);
    }
}
