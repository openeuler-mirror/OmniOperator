package nova.hetu.omniruntime.vector;

import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;
import java.math.BigDecimal;

import static nova.hetu.omniruntime.constants.VecType.OMNI_VEC_TYPE_DOUBLE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * test double vec
 */
public class TestDoubleVec {
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
        DoubleVec vec = new DoubleVec(256);
        assertEquals(vec.getSize(), 256);
        assertEquals(vec.getOffset(), 0);
        assertEquals(vec.getCapacityInBytes(), 2048);
        assertEquals(vec.getType(), OMNI_VEC_TYPE_DOUBLE);
        vec.close();
    }

    /**
     * test slice
     */
    @Test
    public void testSlice() {
        DoubleVec originalVec = new DoubleVec(10);
        for (int i = 0; i < originalVec.getSize(); i++) {
            originalVec.set(i, (double) i / 3);
        }
        int offset = 3;
        DoubleVec slice1 = originalVec.slice(offset, 7);
        assertEquals(slice1.getSize(), 4);
        for (int i = 0; i < slice1.getSize(); i++) {
            assertEquals(slice1.get(i), originalVec.get(i + offset), "Error item value at: " + i);
        }

        DoubleVec slice2 = slice1.slice(1, 3);

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
        DoubleVec vec = new DoubleVec(256);
        for (int i = 0; i < vec.getSize(); i++) {
            vec.set(i, (double) i / 3);
        }

        for (int i = 0; i < vec.getSize(); i++) {
            assertEquals(vec.get(i), (double) i / 3);
        }
        vec.close();
    }

    /**
     * test put value
     */
    @Test
    public void testPutValues() {
        double[] values = {1.13, 3.33, 4.44, 6.66, 7.81};
        DoubleVec doubleVec1 = new DoubleVec(values.length);
        doubleVec1.put(values, 0, 0, values.length);
        for (int i = 0; i < values.length; i++) {
            assertEquals(doubleVec1.get(i), values[i]);
        }

        DoubleVec doubleVec2 = new DoubleVec(values.length);
        doubleVec2.put(values, 1, 2, 3);
        for (int i = 0; i < 3; i++) {
            assertEquals(doubleVec2.get(i + 1), values[i + 2]);
        }

        doubleVec1.close();
        doubleVec2.close();
    }

    /**
     * test value null
     */
    @Test
    public void testValueNull() {
        DoubleVec doubleVec = new DoubleVec(256);
        for (int i = 0; i < doubleVec.getSize(); i++) {
            if (i % 5 == 0) {
                doubleVec.setNull(i);
            }
            else {
                doubleVec.set(i, (double) i / 3);
            }
        }
        for (int i = 0; i < doubleVec.getSize(); i++) {
            if (i % 5 == 0) {
                assertTrue(doubleVec.isNull(i));
            }
            else {
                assertFalse(doubleVec.isNull(i));
            }
        }

        doubleVec.close();
    }

    /**
     * test copy positions
     */
    @Test
    public void testCopyPositions() {
        DoubleVec originalVector = new DoubleVec(4);
        for (int i = 0; i < originalVector.getSize(); i++) {
            originalVector.set(i, i);
        }

        int[] positions = {1, 3};
        DoubleVec copyPositionVector = originalVector.copyPositions(positions, 0, 2);
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
        DoubleVec originalVector = new DoubleVec(4);
        for (int i = 0; i < 4; i++) {
            BigDecimal bd1 = new BigDecimal(i);
            BigDecimal bd2 = new BigDecimal("3.3");
            originalVector.set(i, bd1.multiply(bd2).doubleValue());
        }

        DoubleVec copyRegionVector = originalVector.copyRegion(2, 2);
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
        DoubleVec v1 = new DoubleVec(0);
        double[] values = new double[0];
        v1.put(values, 0, 0, values.length);
        v1.close();
    }
}
