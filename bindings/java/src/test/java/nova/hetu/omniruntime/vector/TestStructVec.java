/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package nova.hetu.omniruntime.vector;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import nova.hetu.omniruntime.type.BooleanDataType;
import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.type.IntDataType;
import nova.hetu.omniruntime.type.LongDataType;
import nova.hetu.omniruntime.type.MapDataType;
import nova.hetu.omniruntime.type.StructDataType;
import nova.hetu.omniruntime.type.VarcharDataType;

import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;

/**
 * test struct vec
 *
 * @since 2025-8-30
 */
public class TestStructVec {
    /**
     * test new vector
     */
    @Test
    public void testNewVector() {
        DataType[] fieldTypes = new DataType[3];
        fieldTypes[0] = new StructDataType(new DataType[]{new VarcharDataType(16), new IntDataType(), new LongDataType(), new BooleanDataType()});
        fieldTypes[1] = new StructDataType(new DataType[]{new VarcharDataType(16), new BooleanDataType(), new IntDataType()});
        fieldTypes[2] = new StructDataType(new DataType[]{new VarcharDataType(16), new MapDataType(new VarcharDataType(16), new VarcharDataType(16)), new IntDataType()});

        StructDataType structDataType = new StructDataType(fieldTypes);
        StructVec vec = new StructVec(structDataType, 256);
        assertEquals(vec.getSize(), 256);
        assertEquals(vec.getType(), StructDataType.STRUCT);
        vec.close();
    }

    /**
     * test slice
     */
    @Test
    public void testSlice() {
        DataType[] fieldTypes = new DataType[]{new VarcharDataType(16)};

        int size = 10;
        StructDataType structDataType = new StructDataType(fieldTypes);
        StructVec structVec = new StructVec(structDataType, size);
        VarcharVec originalVec = (VarcharVec) (structVec.getChild(0));
        String tmpStr = "testvarchar";
        for (int i = 0; i < size; i++) {
            String str = tmpStr.substring(0, i) + i;
            originalVec.set(i, str.getBytes(StandardCharsets.UTF_8));
        }
        assertEquals(originalVec.getRealValueBufCapacityInBytes(), 55);

        int offset = 3;
        StructVec sliceStructVec1 = structVec.slice(offset, 4);
        VarcharVec sliceVec1 = (VarcharVec) (sliceStructVec1.getChild(0));
        assertEquals(sliceVec1.getSize(), 4);
        assertEquals(sliceVec1.getRealValueBufCapacityInBytes(), 22);

        for (int i = 0; i < sliceVec1.getSize(); i++) {
            byte[] actualValue = sliceVec1.get(i);
            byte[] expectedValue = originalVec.get(i + offset);
            assertEquals(actualValue, expectedValue);
        }

        StructVec sliceStructVec2 = sliceStructVec1.slice(1, 2);
        VarcharVec sliceVec2 = (VarcharVec) (sliceStructVec2.getChild(0));
        assertEquals(sliceVec2.getSize(), 2);

        for (int i = 0; i < sliceVec2.getSize(); i++) {
            byte[] actualValue = sliceVec2.get(i);
            byte[] expectedValue = originalVec.get(i + offset + 1);
            assertEquals(actualValue, expectedValue);
        }
        sliceStructVec2.close();
        sliceStructVec1.close();
        structVec.close();
    }

    /**
     * test value null
     */
    @Test
    public void testValueNull() {
        DataType[] fieldTypes = new DataType[]{new VarcharDataType(16)};

        // 10 row
        // index: 0,null,2,null,4,null,6,null,8,null
        int size = 10;
        StructDataType structDataType = new StructDataType(fieldTypes);
        StructVec structVec = new StructVec(structDataType, size, true);
        VarcharVec originalVec = new VarcharVec(10);
        String tmpStr = "testvarchar";
        for (int i = 0; i < size; i++) {
            if (i % 2 == 0) {
                String str = tmpStr.substring(0, i) + i;
                originalVec.set(i, str.getBytes(StandardCharsets.UTF_8));
            } else {
                originalVec.setNull(i);
                structVec.setNull(i);
            }
        }
        structVec.append(originalVec);

        int offset = 4;
        StructVec sliceStructVec1 = structVec.slice(offset, 4);
        VarcharVec sliceVec1 = (VarcharVec) (sliceStructVec1.getChild(0));
        assertEquals(sliceVec1.getSize(), 4);

        for (int i = 0; i < sliceVec1.getSize(); i++) {
            byte[] actualValue = sliceVec1.get(i);
            byte[] expectedValue = originalVec.get(i + offset);
            assertEquals(actualValue, expectedValue);
            if (i % 2 != 0) {
                assertTrue(sliceStructVec1.isNull(i));
            }
        }

        sliceStructVec1.close();
        structVec.close();
    }
}
