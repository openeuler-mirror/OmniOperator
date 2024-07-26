/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2024. All rights reserved.
 */

package nova.hetu.omniruntime.memory;

import nova.hetu.omniruntime.OmniLibs;
import nova.hetu.omniruntime.utils.ParseUtil;
import sun.misc.VM;

/**
 * memory manager.
 *
 * @since 2023-01-17
 */
public class MemoryManager implements AutoCloseable {
    /**
     * -1 means no memory limit
     */
    public static final long UNLIMITED = -1;

    static {
        OmniLibs.load();
    }

    /**
     * set the size of the memory that can be used by the root allocator,
     * this can limit memory usage at the process level
     */
    public static void setGlobalMemoryLimit() {
        // parse environment variable OMNI_OFFHEAP_MEMORY_SIZE
        String memorySize = System.getenv("OMNI_OFFHEAP_MEMORY_SIZE");
        long rootLimit = memorySize == null ? VM.maxDirectMemory() : ParseUtil.parserMemoryParameters(memorySize);
        // the off heap memory from director or environment variable, set global memory limit
        setGlobalMemoryLimit(rootLimit);
    }

    /**
     * set global memory limit about off-heap
     *
     * @param limit the number of global memory limit about off-heap
     * */
    public static void setGlobalMemoryLimit(long limit) {
        setGlobalMemoryLimitNative(limit);
    }

    /**
     * get allocated memory of current allocator
     *
     * @return allocated memory in bytes
     */
    public long getAllocatedMemory() {
        return getAllocatedMemoryNative();
    }

    /**
     * clear memory of current task and current executor
     * */
    public static void clearMemory() {
        memoryClearNative();
    }

    /**
     * Reclaim memory of current task if memory leak exists
     * */
    public static void reclaimMemory() {
        memoryReclamationNative();
    }

    @Override
    public void close() {}

    private static native void setGlobalMemoryLimitNative(long limit);

    private static native long getAllocatedMemoryNative();

    private static native long memoryClearNative();

    private static native long memoryReclamationNative();
}
