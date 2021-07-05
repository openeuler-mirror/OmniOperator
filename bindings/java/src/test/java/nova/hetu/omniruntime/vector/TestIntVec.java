package nova.hetu.omniruntime.vector;

import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import static nova.hetu.omniruntime.constants.VecType.OMNI_VEC_TYPE_INT;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestIntVec
{
    @AfterClass
    public void tearDown()
    {
        VecAllocator.GLOBAL_VECTOR_ALLOCATOR.close();
    }

    @Test
    public void testNewVector()
    {
        IntVec vec = new IntVec(256);
        assertEquals(vec.getSize(), 256);
        assertEquals(vec.getOffset(), 0);
        assertEquals(vec.getCapacityInBytes(), 1024);
        assertEquals(vec.getType(), OMNI_VEC_TYPE_INT);
        vec.close();
    }

    @Test
    public void testSlice()
    {
        IntVec oritinalVec = new IntVec(10);
        for (int i = 0; i < oritinalVec.getSize(); i++) {
            oritinalVec.set(i, i);
        }
        int offset = 3;
        IntVec slice1 = oritinalVec.slice(offset, 7);
        assertEquals(slice1.getSize(), 4);
        for (int i = 0; i < slice1.getSize(); i++) {
            assertEquals(slice1.get(i), oritinalVec.get(i + offset), "Error item value at: " + i);
        }

        IntVec slice2 = slice1.slice(1, 3);

        for (int i = 0; i < slice2.getSize(); i++) {
            assertEquals(slice2.get(i), oritinalVec.get(i + offset + 1), "Error item value at: " + i);
        }

        oritinalVec.close();
        slice1.close();
        slice2.close();
    }

    @Test
    public void testSetAndGetValue()
    {
        IntVec vec = new IntVec(256);
        for (int i = 0; i < vec.getSize(); i++) {
            vec.set(i, i * 2);
        }

        for (int i = 0; i < vec.getSize(); i++) {
            assertEquals(vec.get(i), i * 2);
        }
        vec.close();
    }

    @Test
    public void testPutValues()
    {
        int[] values = {1, 3, 4, 6, 7};
        IntVec vec1 = new IntVec(values.length);
        vec1.put(values, 0, 0, values.length);
        for (int i = 0; i < values.length; i++) {
            assertEquals(vec1.get(i), values[i]);
        }

        IntVec vec2 = new IntVec(values.length);
        vec2.put(values, 1, 2, 3);
        for (int i = 0; i < 3; i++) {
            assertEquals(vec2.get(i + 1), values[i + 2]);
        }

        vec1.close();
        vec2.close();
    }

    @Test
    public void testValueNull()
    {
        IntVec vec = new IntVec(256);
        for (int i = 0; i < vec.getSize(); i++) {
            if (i % 5 == 0) {
                vec.setNull(i);
            }
            else {
                vec.set(i, i);
            }
        }
        for (int i = 0; i < vec.getSize(); i++) {
            if (i % 5 == 0) {
                assertTrue(vec.isNull(i));
            }
            else {
                assertFalse(vec.isNull(i));
            }
        }

        vec.close();
    }

    @Test
    public void testCopyPositions()
    {
        IntVec originalVector = new IntVec(4);
        for (int i = 0; i < originalVector.getSize(); i++) {
            originalVector.set(i, i);
        }

        int[] positions = {1, 3};
        IntVec copyPositionVector = originalVector.copyPositions(positions, 0, 2);
        assertEquals(copyPositionVector.getCapacityInBytes(), 8);
        for (int i = 0; i < copyPositionVector.getSize(); i++) {
            assertEquals(copyPositionVector.get(i), originalVector.get(positions[i]));
        }

        originalVector.close();
        copyPositionVector.close();
    }

    @Test
    public void testCopyRegion()
    {
        IntVec originalVector = new IntVec(4);
        for (int i = 0; i < 4; i++) {
            originalVector.set(i, i * 2);
        }

        IntVec copyRegionVector = originalVector.copyRegion(2, 2);
        assertEquals(copyRegionVector.getCapacityInBytes(), 8);
        for (int i = 0; i < copyRegionVector.getSize(); i++) {
            assertEquals(copyRegionVector.get(i), originalVector.get(i + 2));
        }

        originalVector.close();
        copyRegionVector.close();
    }

    @Test
    public void testZeroSizeAllocate()
    {
        IntVec v1 = new IntVec(0);
        int[] values = new int[0];
        v1.put(values, 0, 0, values.length);
        v1.close();
    }
}
