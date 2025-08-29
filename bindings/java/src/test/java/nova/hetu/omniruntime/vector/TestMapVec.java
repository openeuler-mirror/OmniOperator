/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package nova.hetu.omniruntime.vector;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import nova.hetu.omniruntime.type.LongDataType;

import nova.hetu.omniruntime.type.MapDataType;
import nova.hetu.omniruntime.type.VarcharDataType;
import org.testng.annotations.Test;

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
}
