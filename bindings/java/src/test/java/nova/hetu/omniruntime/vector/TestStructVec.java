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

import java.util.Arrays;

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
}
