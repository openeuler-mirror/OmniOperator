/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package nova.hetu.omniruntime.vector;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.type.LongDataType;

import nova.hetu.omniruntime.type.MapDataType;
import nova.hetu.omniruntime.type.StructDataType;
import nova.hetu.omniruntime.type.VarcharDataType;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * test map vec
 *
 * @since 2025-8-30
 */
public class TestMapVec {
    /**
     * test new vector
     */
    @Test
    public void testNewVector() {
        VarcharDataType keyType = new VarcharDataType(16);
        VarcharDataType valueType = new VarcharDataType(256);
        MapDataType mapDataType = new MapDataType(keyType, valueType);
        MapVec vec = new MapVec(mapDataType, 256);
        assertEquals(vec.getSize(), 256);
        assertEquals(vec.getType(), MapDataType.MAP);
        vec.close();
    }

    /**
     * test slice
     */
    @Test
    public void testSlice() {
        // 2,3,5
        int kvSize = 10;
        int rowSize = 3;
        MapDataType mapDataType = new MapDataType(new VarcharDataType(16), new VarcharDataType(16));
        MapVec mapVec = new MapVec(mapDataType, rowSize,true);
        VarcharVec originalVec = new VarcharVec(kvSize);
        String tmpStr = "testvarchar";
        for (int i = 0; i < kvSize; i++) {
            String str = tmpStr.substring(0, i) + i;
            originalVec.set(i, str.getBytes(StandardCharsets.UTF_8));
        }
        //assertEquals(originalVec.getRealValueBufCapacityInBytes(), 55);

        VarcharVec valueVec = new VarcharVec(kvSize);
        String tmpValStr = "testvarcharVal";
        for (int i = 0; i < kvSize; i++) {
            String str = tmpValStr.substring(0, i) + i;
            valueVec.set(i, str.getBytes(StandardCharsets.UTF_8));
        }

        int[] offsets = new int[]{0, 2, 5, 10};
        mapVec.AddKeys(originalVec);
        mapVec.AddValues(valueVec);
        mapVec.AddOffsets(offsets);

        int offset = 1;
        MapVec sliceMapVec1 = mapVec.slice(offset, 2);
        VarcharVec sliceVec1 = (VarcharVec) (sliceMapVec1.getKeyVec());
        assertEquals(sliceVec1.getSize(), 8);
        // assertEquals(sliceVec1.getRealValueBufCapacityInBytes(), 22);

        for (int i = 0; i < sliceVec1.getSize(); i++) {
            byte[] actualValue = sliceVec1.get(i);
            byte[] expectedValue = originalVec.get(i + offsets[offset]);
            assertEquals(actualValue, expectedValue);
        }


        sliceMapVec1.close();
        mapVec.close();
    }

    /**
     * test value null
     */
    @Test
    public void testValueNull() {
        // 2,null,8
        int kvSize = 10;
        int rowSize = 3;
        MapDataType mapDataType = new MapDataType(new VarcharDataType(16), new VarcharDataType(16));
        MapVec mapVec = new MapVec(mapDataType, rowSize,true);
        VarcharVec originalVec = new VarcharVec(kvSize);
        String tmpStr = "testvarchar";
        for (int i = 0; i < kvSize; i++) {
            String str = tmpStr.substring(0, i) + i;
            originalVec.set(i, str.getBytes(StandardCharsets.UTF_8));
        }

        VarcharVec valueVec = new VarcharVec(kvSize);
        String tmpValStr = "testvarcharVal";
        for (int i = 0; i < kvSize; i++) {
            String str = tmpValStr.substring(0, i) + i;
            valueVec.set(i, str.getBytes(StandardCharsets.UTF_8));
        }
        mapVec.setNull(1);

        int[] offsets = new int[]{0, 2, 2, 10};
        mapVec.AddKeys(originalVec);
        mapVec.AddValues(valueVec);
        mapVec.AddOffsets(offsets);

        int offset = 1;
        MapVec sliceMapVec1 = mapVec.slice(offset, 2);
        assertTrue(sliceMapVec1.isNull(0));

        VarcharVec sliceVec1 = (VarcharVec) (sliceMapVec1.getKeyVec());
        assertEquals(sliceVec1.getSize(), 8);

        for (int i = 0; i < sliceVec1.getSize(); i++) {
            byte[] actualValue = sliceVec1.get(i);
            byte[] expectedValue = originalVec.get(i + offsets[offset]);
            assertEquals(actualValue, expectedValue);
        }


        sliceMapVec1.close();
        mapVec.close();
    }
}
