/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

package nova.hetu.omniruntime.vector;

import nova.hetu.omniruntime.OmniLibs;
import sun.misc.VM;

/**
 * vec allocator.
 *
 * @since 2021-07-17
 */
public class VecAllocator implements AutoCloseable {
    /**
     * global vector allocator.
     */
    public static final VecAllocator GLOBAL_VECTOR_ALLOCATOR;

    /**
     * -1 means no memory limit
     */
    public static final long UNLIMIT = -1;

    private final long nativeAllocator;

    static {
        OmniLibs.load();
        GLOBAL_VECTOR_ALLOCATOR = new VecAllocator(getGlobalVectorAllocator());
        //get the off heap memory from director, set rootAllocator limit
        setRootAllocatorLimit(VM.maxDirectMemory());
        registerDestroyAllocator();
    }

    /**
     * new a vec allocator from native allocator
     *
     * @param nativeAllocator the allocator address
     */
    public VecAllocator(long nativeAllocator) {
        this.nativeAllocator = nativeAllocator;
    }

    /**
     * set the size of the memory that can be used by the root allocator,
     * this can limit memory usage at the process level
     *
     * @param limit the size of limited memory
     */
    public static void setRootAllocatorLimit(long limit)
    {
        setRootAllocatorLimitNative(limit);
    }

    /**
     * get allocator address
     *
     * @return the address of current allocator
     */
    public long getNativeAllocator() {
        return nativeAllocator;
    }

    /**
     * create its child allocator based on the current allocator
     *
     * @param scope the scope of new allocator
     * @param limit the size of limited memory
     * @param reservation the size of reservation memory
     * @return a new child allocator
     */
    public VecAllocator newChildAllocator(String scope, long limit, long reservation) {
        return new VecAllocator(newChildAllocatorNative(nativeAllocator, scope, limit, reservation));
    }

    /**
     * set current allocator size of limited memory
     * @param limit the size of limited memory in bytes
     */
    public void setLimit(long limit)
    {
        setLimitNative(nativeAllocator, limit);
    }

    /**
     * get the memory limit of current allocator, -1 means no limit
     *
     * @return limit of allocator
     */
    public long getLimit()
    {
        return getLimitNative(nativeAllocator);
    }

    /**
     * get scope of current allocator
     *
     * @return the scope of allocator
     */
    public String getScope()
    {
        return getScopeNative(nativeAllocator);
    }

    /**
     * get allocated memory of current allocator
     *
     * @return allocated memory in bytes
     */
    public long getAllocatedMemory()
    {
        return getAllocatedMemoryNative(nativeAllocator);
    }

    public long getPeakAllocated()
    {
        return getPeakAllocatedNative(nativeAllocator);
    }

    /**
     * get current allocator parent
     *
     * @return allocator
     */
    public VecAllocator getParentAllocator()
    {
        return new VecAllocator(getParentAllocator(nativeAllocator));
    }

    /**
     * get current allocator child allocators
     *
     * @return child allocators
     */
    public VecAllocator[] getChildAllocators()
    {
        long[] nativeAllocators = getChildAllocatorsNative(nativeAllocator);
        VecAllocator[] vecAllocators = new VecAllocator[nativeAllocators.length];
        for (int i = 0; i < nativeAllocators.length; i++) {
            vecAllocators[i] = new VecAllocator(nativeAllocators[i]);
        }
        return vecAllocators;
    }

    @Override
    public void close() {
        freeAllocatorNative(nativeAllocator);
    }

    private static  void registerDestroyAllocator()
    {
        Runtime.getRuntime().addShutdownHook(new Thread(GLOBAL_VECTOR_ALLOCATOR::close));
    }

    private static native long freeAllocatorNative(long nativeAllocator);

    private static native long newChildAllocatorNative(long parentNative, String scope, long limit, long reservation);

    private static native void setLimitNative(long nativeAllocator, long limit);

    private static native long getLimitNative(long nativeAllocator);

    private static native String getScopeNative(long nativeAllocator);

    private static native void setRootAllocatorLimitNative(long limit);

    private static native long getAllocatedMemoryNative(long nativeAllocator);

    private static native long getParentAllocator(long nativeAllocator);

    private static native long[] getChildAllocatorsNative(long nativeAllocator);

    private static native long getGlobalVectorAllocator();

    private static native long getPeakAllocatedNative(long nativeAllocator);
}
