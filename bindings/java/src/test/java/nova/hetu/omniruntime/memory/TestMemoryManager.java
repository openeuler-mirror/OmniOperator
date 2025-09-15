/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2024. All rights reserved.
 */

package nova.hetu.omniruntime.memory;

import static nova.hetu.omniruntime.memory.MemoryManager.clearMemory;
import static nova.hetu.omniruntime.memory.MemoryManager.setGlobalMemoryLimit;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import nova.hetu.omniruntime.utils.OmniRuntimeException;
import nova.hetu.omniruntime.vector.IntVec;
import nova.hetu.omniruntime.vector.LongVec;

import org.testng.annotations.Test;

/**
 * test memory manager
 *
 * @since 2023-01-17
 */
public class TestMemoryManager {
    @Test
    public void testAllocatorBasic() {
        long limit = -1;
        clearMemory();
        setGlobalMemoryLimit(limit);

        MemoryManager memoryManager = new MemoryManager();
        int size = 1024 * 1024;
        IntVec intVec = new IntVec(size);
        // 4325384 = values(size * 4) + nulls(size) + other(CreateFlatVector_ptr(64))
        assertTrue(memoryManager.getAllocatedMemory() >= 4325384);
        LongVec longVec = new LongVec(size);
        // 12845072 = 5242944 + values(size * 8) + nulls(size) + other(CreateFlatVector_ptr(64))
        assertTrue(memoryManager.getAllocatedMemory() >= 12845072);
        intVec.close();
        // 8519688 = 12845072 - 4325384
        assertTrue(memoryManager.getAllocatedMemory() >= 8519688);
        longVec.close();
        assertEquals(memoryManager.getAllocatedMemory(), 0);
    }

    @Test(expectedExceptions = OmniRuntimeException.class, expectedExceptionsMessageRegExp = "memory cap exceeded")
    public void testMemoryManagerBeyondLimit() {
        long limit = 1024L * 1024L;
        int size = 1024 * 1024 + 1;
        setGlobalMemoryLimit(limit);

        LongVec longVec = null;
        try {
            longVec = new LongVec(size);
        } catch (OmniRuntimeException e) {
            throw new OmniRuntimeException("memory cap exceeded");
        } finally {
            if (longVec != null) {
                longVec.close();
            }
            clearMemory();
        }
    }
}
