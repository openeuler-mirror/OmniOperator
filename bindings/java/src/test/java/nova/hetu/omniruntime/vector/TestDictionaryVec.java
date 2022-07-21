/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 */

package nova.hetu.omniruntime.vector;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import nova.hetu.omniruntime.util.TestUtils;

import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;

/**
 * test dictionary vec
 *
 * @since 2021-9-8
 */
public class TestDictionaryVec {
    /**
     * test slice
     */
    @Test
    public void testSlice() {
        LongVec originalVec = new LongVec(100);
        for (int i = 0; i < originalVec.getSize(); i++) {
            originalVec.set(i, i);
        }

        int[] ids = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        DictionaryVec dictionaryVec = new DictionaryVec(originalVec, ids);
        int offset1 = 3;
        DictionaryVec slice1 = dictionaryVec.slice(offset1, 7);
        assertEquals(slice1.getSize(), 4);
        for (int i = 0; i < slice1.getSize(); i++) {
            long value = slice1.getLong(i);
            assertEquals(value, originalVec.get(i + offset1), "Error item value from slice1 at: " + i);
        }

        int offset2 = 1;
        DictionaryVec slice2 = slice1.slice(offset2, 3);
        assertEquals(slice2.getSize(), 2);
        for (int i = 0; i < slice2.getSize(); i++) {
            long value = slice2.getLong(i);
            assertEquals(value, slice1.getLong(i + offset2), "Error item value from slice2 at: " + i);
            assertEquals(value, originalVec.get(i + offset2 + offset1), "Error item value from slice2 at: " + i);
        }
        originalVec.close();
        slice2.close();
        slice1.close();
        dictionaryVec.close();
    }

    @Test
    public void testNestedDictionaryVecSlice() {
        LongVec originalVec = new LongVec(100);
        for (int i = 0; i < originalVec.getSize(); i++) {
            originalVec.set(i, i);
        }

        int[] ids = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        DictionaryVec dictionaryVec = new DictionaryVec(originalVec, ids);
        int[] nestedIds = {1, 1, 2, 2, 4, 4, 7, 7};
        DictionaryVec nestedDictionaryVec = new DictionaryVec(dictionaryVec, nestedIds);

        originalVec.close();
        dictionaryVec.close();

        int offset = 3;
        DictionaryVec slice = nestedDictionaryVec.slice(offset, 7);
        assertEquals(slice.getSize(), 4);
        long value = slice.getLong(0);
        assertEquals(value, 2);
        value = slice.getLong(1);
        assertEquals(value, 4);
        value = slice.getLong(2);
        assertEquals(value, 4);
        value = slice.getLong(3);
        assertEquals(value, 7);

        nestedDictionaryVec.close();

        int[] copyIds = {0, 1, 2, 3};
        DictionaryVec copy = slice.copyPositions(copyIds, 0, 4);
        assertEquals(copy.getSize(), 4);
        value = copy.getLong(0);
        assertEquals(value, 2);
        value = copy.getLong(1);
        assertEquals(value, 4);
        value = copy.getLong(2);
        assertEquals(value, 4);
        value = copy.getLong(3);
        assertEquals(value, 7);

        slice.close();
        copy.close();
    }

    @Test
    public void testGetLong() {
        LongVec originalVec = new LongVec(10);
        for (int i = 0; i < 10; i++) {
            originalVec.set(i, i);
        }

        int[] ids = {6, 8, 9};
        DictionaryVec dictionaryVec = new DictionaryVec(originalVec, ids);
        assertEquals(dictionaryVec.getLong(0), originalVec.get(6));
        assertEquals(dictionaryVec.getLong(1), originalVec.get(8));
        assertEquals(dictionaryVec.getLong(2), originalVec.get(9));

        int[] nestedIds = {1, 2};
        DictionaryVec nested = new DictionaryVec(dictionaryVec, nestedIds);
        assertEquals(nested.getLong(0), originalVec.get(8));
        assertEquals(nested.getLong(1), originalVec.get(9));

        originalVec.close();
        dictionaryVec.close();
        nested.close();
    }

    @Test
    public void testGetInt() {
        IntVec originalVec = new IntVec(10);
        for (int i = 0; i < 10; i++) {
            originalVec.set(i, i);
        }

        int[] ids = {6, 8, 9};
        DictionaryVec dictionaryVec = new DictionaryVec(originalVec, ids);
        assertEquals(dictionaryVec.getInt(0), originalVec.get(6));
        assertEquals(dictionaryVec.getInt(1), originalVec.get(8));
        assertEquals(dictionaryVec.getInt(2), originalVec.get(9));

        int[] nestedIds = {1, 2};
        DictionaryVec nested = new DictionaryVec(dictionaryVec, nestedIds);
        assertEquals(nested.getInt(0), originalVec.get(8));
        assertEquals(nested.getInt(1), originalVec.get(9));

        originalVec.close();
        dictionaryVec.close();
        nested.close();
    }

    @Test
    public void testGetBoolean() {
        BooleanVec originalVec = new BooleanVec(10);
        for (int i = 0; i < 10; i++) {
            originalVec.set(i, i % 2 == 0);
        }

        int[] ids = {6, 8, 9};
        DictionaryVec dictionaryVec = new DictionaryVec(originalVec, ids);
        assertEquals(dictionaryVec.getBoolean(0), originalVec.get(6));
        assertEquals(dictionaryVec.getBoolean(1), originalVec.get(8));
        assertEquals(dictionaryVec.getBoolean(2), originalVec.get(9));

        int[] nestedIds = {1, 2};
        DictionaryVec nested = new DictionaryVec(dictionaryVec, nestedIds);
        assertEquals(nested.getBoolean(0), originalVec.get(8));
        assertEquals(nested.getBoolean(1), originalVec.get(9));

        originalVec.close();
        dictionaryVec.close();
        nested.close();
    }

    @Test
    public void testGetDouble() {
        DoubleVec originalVec = new DoubleVec(10);
        for (int i = 0; i < 10; i++) {
            originalVec.set(i, 2.3d * i);
        }

        int[] ids = {6, 8, 9};
        DictionaryVec dictionaryVec = new DictionaryVec(originalVec, ids);
        assertEquals(Double.compare(dictionaryVec.getDouble(0), originalVec.get(6)), 0);
        assertEquals(Double.compare(dictionaryVec.getDouble(0), originalVec.get(6)), 0);
        assertEquals(Double.compare(dictionaryVec.getDouble(1), originalVec.get(8)), 0);
        assertEquals(Double.compare(dictionaryVec.getDouble(2), originalVec.get(9)), 0);

        int[] nestedIds = {1, 2};
        DictionaryVec nested = new DictionaryVec(dictionaryVec, nestedIds);
        assertEquals(Double.compare(nested.getDouble(0), originalVec.get(8)), 0);
        assertEquals(Double.compare(nested.getDouble(1), originalVec.get(9)), 0);

        originalVec.close();
        dictionaryVec.close();
        nested.close();
    }

    @Test
    public void testGetBytes() {
        VarcharVec originalVec = new VarcharVec(1024, 10);
        for (int i = 0; i < 10; i++) {
            originalVec.set(i, String.valueOf(i).getBytes(StandardCharsets.UTF_8));
        }

        int[] ids = {6, 8, 9};
        DictionaryVec dictionaryVec = new DictionaryVec(originalVec, ids);
        assertEquals(dictionaryVec.getBytes(0), originalVec.get(6));
        assertEquals(dictionaryVec.getBytes(1), originalVec.get(8));
        assertEquals(dictionaryVec.getBytes(2), originalVec.get(9));

        int[] nestedIds = {1, 2};
        DictionaryVec nested = new DictionaryVec(dictionaryVec, nestedIds);
        assertEquals(nested.getBytes(0), originalVec.get(8));
        assertEquals(nested.getBytes(1), originalVec.get(9));

        originalVec.close();
        dictionaryVec.close();
        nested.close();
    }

    @Test
    public void testGetDecimal128() {
        Decimal128Vec originalVec = new Decimal128Vec(10);
        for (int i = 0; i < 10; i++) {
            long[] value = {i, i * 2};
            originalVec.set(i, value);
        }

        int[] ids = {6, 8, 9};
        DictionaryVec dictionaryVec = new DictionaryVec(originalVec, ids);
        // means decimal1={6, 12},decimal2={8, 16}, decimal3={9, 18};
        Object[] expected = {6L, 12L, 8L, 16L, 9L, 18L};
        TestUtils.assertDictionaryVecEquals(dictionaryVec, expected);

        int[] nestedIds = {1, 2};
        DictionaryVec nested = new DictionaryVec(dictionaryVec, nestedIds);
        assertEquals(nested.getDecimal128(0), originalVec.get(8));
        assertEquals(nested.getDecimal128(1), originalVec.get(9));

        originalVec.close();
        dictionaryVec.close();
        nested.close();
    }

    /**
     * test copy position
     */
    @Test
    public void testCopyPositions() {
        LongVec originalVector = new LongVec(10);
        for (int i = 0; i < originalVector.getSize(); i++) {
            originalVector.set(i, i);
        }

        int[] ids = {2, 3, 4, 5, 6, 8, 9};
        DictionaryVec dictionaryVec = new DictionaryVec(originalVector, ids);
        int[] positions = {1, 3, 5, 6};
        DictionaryVec copyPositions = dictionaryVec.copyPositions(positions, 1, 3);
        assertEquals(copyPositions.getLong(0), originalVector.get(5));
        assertEquals(copyPositions.getLong(1), originalVector.get(8));
        assertEquals(copyPositions.getLong(2), originalVector.get(9));

        originalVector.close();
        dictionaryVec.close();
        copyPositions.close();

        // dictionary data compress
        originalVector = new LongVec(2);
        originalVector.set(0, 100);
        originalVector.set(1, 200);
        int[] ids1 = {0, 0, 0, 1, 1, 1};
        dictionaryVec = new DictionaryVec(originalVector, ids1);
        int[] positions1 = {1, 2, 3, 5};
        copyPositions = dictionaryVec.copyPositions(positions1, 0, 4);
        assertEquals(copyPositions.getLong(0), originalVector.get(0));
        assertEquals(copyPositions.getLong(1), originalVector.get(0));
        assertEquals(copyPositions.getLong(2), originalVector.get(1));
        assertEquals(copyPositions.getLong(3), originalVector.get(1));

        originalVector.close();
        dictionaryVec.close();
        copyPositions.close();
    }

    /**
     * test copy region
     */
    @Test
    public void testCopyRegion() {
        LongVec originalVector = new LongVec(10);
        for (int i = 0; i < 10; i++) {
            originalVector.set(i, i);
        }

        int[] ids = {6, 8, 9};
        DictionaryVec dictionaryVec = new DictionaryVec(originalVector, ids);
        DictionaryVec copyRegion = dictionaryVec.copyRegion(1, 2);

        assertEquals(copyRegion.getLong(0), originalVector.get(8));
        assertEquals(copyRegion.getLong(1), originalVector.get(9));

        originalVector.close();
        dictionaryVec.close();
        copyRegion.close();
    }

    @Test
    public void testNullFlag() {
        LongVec originalVector = new LongVec(10);
        for (int i = 0; i < 10; i++) {
            if (i % 2 == 0) {
                originalVector.setNull(i);
            } else {
                originalVector.set(i, i);
            }
        }

        int[] ids = {6, 8, 9};
        DictionaryVec dictionaryVec = new DictionaryVec(originalVector, ids);
        originalVector.close();
        assertTrue(dictionaryVec.mayHaveNull());
        assertEquals(dictionaryVec.getNullCount(), 2);

        DictionaryVec copyRegion = dictionaryVec.copyRegion(1, 2);
        assertTrue(copyRegion.mayHaveNull());
        assertEquals(copyRegion.getNullCount(), 1);
        copyRegion.close();

        DictionaryVec slice = dictionaryVec.slice(2, 3);
        assertTrue(slice.mayHaveNull());
        assertEquals(slice.getNullCount(), 0);
        slice.close();

        int[] positions = {0, 2};
        DictionaryVec copyPosition = dictionaryVec.copyPositions(positions, 0, 2);
        assertTrue(copyPosition.mayHaveNull());
        assertEquals(copyPosition.getNullCount(), 1);
        copyPosition.close();

        dictionaryVec.close();
    }
}
