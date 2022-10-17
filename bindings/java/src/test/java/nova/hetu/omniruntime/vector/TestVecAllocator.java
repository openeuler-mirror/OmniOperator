/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

package nova.hetu.omniruntime.vector;

import static org.testng.Assert.assertEquals;

import nova.hetu.omniruntime.utils.OmniRuntimeException;

import org.testng.annotations.Test;

/**
 * test vec allocator
 *
 * @since 2022-04-02
 */
public class TestVecAllocator {
    @Test
    public void testAllocatorBasic() {
        long limit = 4096L;
        long subLimit = 2048L;

        VecAllocator vecAllocator = VecAllocator.GLOBAL_VECTOR_ALLOCATOR.newChildAllocator("parent", limit, 0);
        vecAllocator.setLimit(limit);

        assertEquals(vecAllocator.getScope(), "parent");
        assertEquals(vecAllocator.getLimit(), limit);

        VecAllocator subVecAllocator1 = vecAllocator.newChildAllocator("operator", subLimit, 0);
        VecAllocator subVecAllocator2 = vecAllocator.newChildAllocator("operator", subLimit, 0);
        for (VecAllocator subVecAllocator : vecAllocator.getChildAllocators()) {
            assertEquals(subVecAllocator.getParentAllocator().getNativeAllocator(), vecAllocator.getNativeAllocator());
            assertEquals(subVecAllocator.getLimit(), subLimit);
            assertEquals(subVecAllocator.getScope(), "operator");
        }

        int size = 8;
        IntVec intVec = new IntVec(subVecAllocator2, size);
        // the value capacity and the null value capacity are added:size * 4 + size
        assertEquals(subVecAllocator2.getAllocatedMemory(), 40);
        LongVec longVec = new LongVec(subVecAllocator1, size);
        // the value capacity and the null value capacity are added:size * 8 + size
        assertEquals(subVecAllocator1.getAllocatedMemory(), 72);
        // add the two capacities 40 + 72
        assertEquals(vecAllocator.getAllocatedMemory(), 0); // subVecAllocator size just unTracked
        assertEquals(vecAllocator.getPeakAllocated(), 0);

        intVec.close();
        assertEquals(subVecAllocator2.getAllocatedMemory(), 0);
        longVec.close();
        assertEquals(subVecAllocator1.getAllocatedMemory(), 0);
        assertEquals(vecAllocator.getAllocatedMemory(), 0);
        subVecAllocator1.close();
        subVecAllocator2.close();
        vecAllocator.close();
    }

    @Test(expectedExceptions = OmniRuntimeException.class, expectedExceptionsMessageRegExp = "memory cap exceeded")
    public void testVecAllocatorBeyondLimit() {
        long limit = 2048L;
        long subLimit = 1024L;
        int size = 200;

        VecAllocator vecAllocator = VecAllocator.GLOBAL_VECTOR_ALLOCATOR.newChildAllocator("parent", limit, 0);
        vecAllocator.setLimit(limit);
        VecAllocator subVecAllocator = vecAllocator.newChildAllocator("operator", subLimit, 0);

        LongVec longVec = null;
        try {
            longVec = new LongVec(subVecAllocator, size);
        } catch (OmniRuntimeException e) {
            throw new OmniRuntimeException("memory cap exceeded");
        } finally {
            if (longVec != null) {
                longVec.close();
            }
            subVecAllocator.close();
            vecAllocator.close();
        }
    }
}
