/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package nova.hetu.omniruntime.vector;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import nova.hetu.omniruntime.type.ArrayDataType;
import nova.hetu.omniruntime.type.VarcharDataType;

import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;

/**
 * test array vec
 *
 * @since 2025-09-11
 */
public class TestArrayVec {
    /**
     * test new vector
     */
    @Test
    public void testNewVector() {
        VarcharDataType elementType = new VarcharDataType(256);
        ArrayDataType arrayDataType = new ArrayDataType(elementType);
        ArrayVec vec = new ArrayVec(arrayDataType, 256);
        assertEquals(vec.getSize(), 256);
        assertEquals(vec.getType(), ArrayDataType.ARRAY);
        vec.close();
    }

    /**
     * test slice
     */
    @Test
    public void testSlice() {
        // 2, 3, 5
        int elementSize = 10;
        int rowSize = 3;
        ArrayDataType arrayDataType = new ArrayDataType(new VarcharDataType(16));
        ArrayVec arrayVec = new ArrayVec(arrayDataType, rowSize, true);

        VarcharVec elementVec = new VarcharVec(elementSize);
        String tmpValStr = "testvarcharVal";
        for (int i = 0; i < elementSize; i++) {
            String str = tmpValStr.substring(0, i) + i;
            elementVec.set(i, str.getBytes(StandardCharsets.UTF_8));
        }

        int[] offsets = new int[]{0, 2, 5, 10};
        arrayVec.addElements(elementVec);
        arrayVec.addOffsets(offsets);

        int offset = 1;
        ArrayVec slicedArrayVec1 = arrayVec.slice(offset, 2);
        VarcharVec slicedVec1 = (VarcharVec) (slicedArrayVec1.getElementVec());
        assertEquals(slicedVec1.getSize(), 8);

        for (int i = 0; i < slicedVec1.getSize(); i++) {
            byte[] actualValue = slicedVec1.get(i);
            byte[] expectedValue = elementVec.get(i + offsets[offset]);
            assertEquals(actualValue, expectedValue);
        }


        slicedArrayVec1.close();
        arrayVec.close();
    }

    /**
     * test value null
     */
    @Test
    public void testValueNull() {
        // 2, null, 8
        int elementSize = 10;
        int rowSize = 3;
        ArrayDataType arrayDataType = new ArrayDataType(new VarcharDataType(16));
        ArrayVec arrayVec = new ArrayVec(arrayDataType, rowSize, true);

        VarcharVec elementVec = new VarcharVec(elementSize);
        String tmpValStr = "testvarcharVal";
        for (int i = 0; i < elementSize; i++) {
            String str = tmpValStr.substring(0, i) + i;
            elementVec.set(i, str.getBytes(StandardCharsets.UTF_8));
        }

        arrayVec.setNull(1);

        int[] offsets = new int[]{0, 2, 2, 10};
        arrayVec.addElements(elementVec);
        arrayVec.addOffsets(offsets);

        int offset = 1;
        ArrayVec slicedArrayVec1 = arrayVec.slice(offset, 2);
        assertTrue(slicedArrayVec1.isNull(0));

        VarcharVec slicedVec1 = (VarcharVec) (slicedArrayVec1.getElementVec());
        assertEquals(slicedVec1.getSize(), 8);

        for (int i = 0; i < slicedVec1.getSize(); i++) {
            byte[] actualValue = slicedVec1.get(i);
            byte[] expectedValue = elementVec.get(i + offsets[offset]);
            assertEquals(actualValue, expectedValue);
        }

        slicedArrayVec1.close();
        arrayVec.close();
    }
}
