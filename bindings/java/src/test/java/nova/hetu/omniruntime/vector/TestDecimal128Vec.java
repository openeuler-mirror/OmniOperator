/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 */

package nova.hetu.omniruntime.vector;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.type.Decimal128DataType;

import org.testng.annotations.Test;

/**
 * test decimal 128-bit vec
 *
 * @since 2021-6-23
 */
public class TestDecimal128Vec {
    /**
     * test new vector
     */
    @Test
    public void testNewVector() {
        Decimal128Vec vec = new Decimal128Vec(256);
        assertEquals(vec.getSize(), 256);
        assertEquals(vec.getOffset(), 0);
        assertEquals(vec.getCapacityInBytes(), 4096);
        assertEquals(vec.getType().getId(), DataType.DataTypeId.OMNI_DECIMAL128);
        assertEquals(((Decimal128DataType) (vec.getType())).getPrecision(), 38);
        assertEquals(((Decimal128DataType) (vec.getType())).getScale(), 0);
        vec.close();
    }

    /**
     * test slice
     */
    @Test
    public void testSlice() {
        final int size = 10;
        Decimal128Vec vec1 = new Decimal128Vec(size);
        for (int i = 0; i < size; i++) {
            long[] value = {1 << (i + 1), 3};
            vec1.set(i, value);
        }
        Decimal128Vec slice1 = vec1.slice(3, 5);
        Decimal128Vec slice2 = vec1.slice(0, vec1.getSize());
        for (int i = 0; i < slice1.getSize(); i++) {
            assertEquals(vec1.get(i + 3)[0], slice1.get(i)[0], "Error item value at: " + i);
            assertEquals(vec1.get(i + 3)[1], slice1.get(i)[1], "Error item value at: " + i);
        }
        for (int i = 0; i < slice2.getSize(); i++) {
            assertEquals(vec1.get(i)[0], slice2.get(i)[0], "Error item value at: " + i);
            assertEquals(vec1.get(i)[1], slice2.get(i)[1], "Error item value at: " + i);
        }
        vec1.close();
        slice1.close();
        slice2.close();
    }

    /**
     * test set and get value
     */
    @Test
    public void testSetAndGetValue() {
        final int size = 1024;
        Decimal128Vec vec1 = new Decimal128Vec(size);
        long[] values = new long[size * 2];
        for (int i = 0; i < size; i++) {
            long[] value = {i, 3};
            vec1.set(i, value);
            values[i * 2] = i;
            values[i * 2 + 1] = 3;
        }

        for (int i = 0; i < size; i++) {
            assertEquals(values[i * 2], vec1.get(i)[0]);
            assertEquals(values[i * 2 + 1], vec1.get(i)[1]);
        }
        vec1.close();
    }

    /**
     * test put value
     */
    @Test
    public void testPutValues() {
        long[] values = {1, 3, 4, 6, 7, 8};
        Decimal128Vec vec1 = new Decimal128Vec(values.length / 2);
        vec1.put(values, 0, 0, values.length);
        for (int i = 0; i < values.length / 2; i++) {
            assertEquals(vec1.get(i)[0], values[i * 2]);
            assertEquals(vec1.get(i)[1], values[i * 2 + 1]);
        }

        Decimal128Vec vec2 = new Decimal128Vec(values.length / 2);
        vec2.put(values, 1, 2, 4);
        for (int i = 0; i < 2; i++) {
            assertEquals(vec2.get(i + 1)[0], values[i * 2 + 2]);
            assertEquals(vec2.get(i + 1)[1], values[i * 2 + 3]);
        }

        vec1.close();
        vec2.close();
    }

    /**
     * test value null
     */
    @Test
    public void testValueNull() {
        final int size = 10;
        Decimal128Vec vec = new Decimal128Vec(size);
        for (int i = 0; i < size; i++) {
            long[] value = {i, 3};
            vec.set(i, value);
        }

        for (int i = 0; i < size; i++) {
            vec.setNull(i);
        }

        for (int i = 0; i < size; i++) {
            assertTrue(vec.isNull(i));
        }
    }

    /**
     * test copy postion
     */
    @Test
    public void testCopyPositions() {
        Decimal128Vec originalVector = new Decimal128Vec(4);
        for (int i = 0; i < originalVector.getSize(); i++) {
            long[] value = {0, i};
            originalVector.set(i, value);
        }

        int[] positions = {1, 3};
        Decimal128Vec copyPositionVector = originalVector.copyPositions(positions, 0, 2);
        assertEquals(copyPositionVector.getCapacityInBytes(), 32);
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
        Decimal128Vec originalVector = new Decimal128Vec(4);
        for (int i = 0; i < 4; i++) {
            long[] value = {0, i * 2};
            originalVector.set(i, value);
        }

        Decimal128Vec copyRegionVector = originalVector.copyRegion(2, 2);
        assertEquals(copyRegionVector.getCapacityInBytes(), 32);
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
        Decimal128Vec v1 = new Decimal128Vec(0);
        long[] values = new long[0];
        v1.put(values, 0, 0, values.length);
        v1.close();
    }
}
