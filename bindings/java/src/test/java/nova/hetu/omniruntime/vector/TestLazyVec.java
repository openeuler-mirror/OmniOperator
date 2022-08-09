/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 */

package nova.hetu.omniruntime.vector;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

/**
 * test lazy vec
 *
 * @since 2021-7-12
 */
public class TestLazyVec {
    /**
     * test null flag
     */
    @Test
    public void testNullFlag() {
        int size = 10;
        LazyVec vec = new LazyVec(VecAllocator.GLOBAL_VECTOR_ALLOCATOR, size, () -> {
            IntVec loaded = new IntVec(VecAllocator.GLOBAL_VECTOR_ALLOCATOR, size);
            loaded.setNulls(0, new byte[] {1, 0, 1, 0, 1, 0, 0, 0, 0, 0}, 0, size);
            return loaded;
        });

        assertTrue(vec.mayHaveNull());
        assertEquals(vec.getNullCount(), 3);
        vec.close();
    }
}
