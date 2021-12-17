/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */
package nova.hetu.omniruntime.type;

import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;

public class TestVecTypeSerializer {
    @Test
    public void testAllTypes()
    {
        VecType[] types = new VecType[] {IntVecType.INTEGER, LongVecType.LONG, DoubleVecType.DOUBLE,
                BooleanVecType.BOOLEAN, ShortVecType.SHORT, Decimal64VecType.DECIMAL64, Decimal128VecType.DECIMAL128,
                Date32VecType.DATE32, Date64VecType.DATE64, VarcharVecType.VARCHAR, CharVecType.CHAR,
                DictionaryVecType.DICTIONARY, ContainerVecType.CONTAINER, VecType.INVALID};
        String[] serializeds = new String[types.length];
        for (int i = 0; i < types.length; i++) {
            serializeds[i] = VecTypeSerializer.serializeSingle(types[i]);
        }
        for (int i = 0; i < types.length; i++) {
            assertEquals(VecTypeSerializer.deserializeSingle(serializeds[i]), types[i]);
        }
    }
}
