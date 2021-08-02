package nova.hetu.omniruntime.vector;

import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import static nova.hetu.omniruntime.constants.VecType.OMNI_VEC_TYPE_LONG;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * test long vec
 */
public class TestLongVec {
    /**
     * tear down
     */
    @AfterClass
    public void tearDown() {
        VecAllocator.GLOBAL_VECTOR_ALLOCATOR.close();
    }

    /**
     * test new vector
     */
    @Test
    public void testNewVector() {
        LongVec vec = new LongVec(256);
        assertEquals(vec.getSize(), 256);
        assertEquals(vec.getOffset(), 0);
        assertEquals(vec.getCapacityInBytes(), 2048);
        assertEquals(vec.getType(), OMNI_VEC_TYPE_LONG);
        vec.close();
    }

    /**
     * test slice
     */
    @Test
    public void testSlice() {
        LongVec originalVec = new LongVec(10);
        for (int i = 0; i < originalVec.getSize(); i++) {
            originalVec.set(i, i);
        }
        int offset = 3;
        LongVec slice1 = originalVec.slice(offset, 7);
        assertEquals(slice1.getSize(), 4);
        for (int i = 0; i < slice1.getSize(); i++) {
            assertEquals(slice1.get(i), originalVec.get(i + offset), "Error item value at: " + i);
        }

        LongVec slice2 = slice1.slice(1, 3);

        for (int i = 0; i < slice2.getSize(); i++) {
            assertEquals(slice2.get(i), originalVec.get(i + offset + 1), "Error item value at: " + i);
        }

        originalVec.close();
        slice1.close();
        slice2.close();
    }

    /**
     * test set and get value
     */
    @Test
    public void testSetAndGetValue() {
        LongVec vec = new LongVec(256);
        for (int i = 0; i < vec.getSize(); i++) {
            vec.set(i, i * 2);
        }

        for (int i = 0; i < vec.getSize(); i++) {
            assertEquals(vec.get(i), i * 2);
        }
        vec.close();
    }

    /**
     * test put value
     */
    @Test
    public void testPutValues() {
        long[] values = {1, 3, 4, 6, 7};
        LongVec vec1 = new LongVec(values.length);
        vec1.put(values, 0, 0, values.length);
        for (int i = 0; i < values.length; i++) {
            assertEquals(vec1.get(i), values[i]);
        }

        LongVec vec2 = new LongVec(values.length);
        vec2.put(values, 1, 2, 3);
        for (int i = 0; i < 3; i++) {
            assertEquals(vec2.get(i + 1), values[i + 2]);
        }

        vec1.close();
        vec2.close();
    }

    /**
     * test value null
     */
    @Test
    public void testValueNull() {
        LongVec longVec = new LongVec(256);
        for (int i = 0; i < longVec.getSize(); i++) {
            if (i % 5 == 0) {
                longVec.setNull(i);
            }
            else {
                longVec.set(i, i);
            }
        }
        for (int i = 0; i < longVec.getSize(); i++) {
            if (i % 5 == 0) {
                assertTrue(longVec.isNull(i));
            }
            else {
                assertFalse(longVec.isNull(i));
            }
        }

        longVec.close();
    }

    /**
     * test copy postion
     */
    @Test
    public void testCopyPositions() {
        LongVec originalVector = new LongVec(4);
        for (int i = 0; i < originalVector.getSize(); i++) {
            originalVector.set(i, i);
        }

        int[] positions = {1, 3};
        LongVec copyPositionVector = originalVector.copyPositions(positions, 0, 2);
        assertEquals(copyPositionVector.getCapacityInBytes(), 16);
        for (int i = 0; i < copyPositionVector.getSize(); i++) {
            assertEquals(copyPositionVector.get(i), originalVector.get(positions[i]));
        }

        originalVector.close();
        copyPositionVector.close();
    }

    /**
     * test copy region
     */
    @Test
    public void testCopyRegion() {
        LongVec originalVector = new LongVec(4);
        for (int i = 0; i < 4; i++) {
            originalVector.set(i, i * 2);
        }

        LongVec copyRegionVector = originalVector.copyRegion(2, 2);
        assertEquals(copyRegionVector.getCapacityInBytes(), 16);
        for (int i = 0; i < copyRegionVector.getSize(); i++) {
            assertEquals(copyRegionVector.get(i), originalVector.get(i + 2));
        }

        originalVector.close();
        copyRegionVector.close();
    }

    /**
     * test zero sized allocate
     */
    @Test
    public void testZeroSizeAllocate() {
        LongVec v1 = new LongVec(0);
        long[] values = new long[0];
        v1.put(values, 0, 0, values.length);
        v1.close();
    }
}
