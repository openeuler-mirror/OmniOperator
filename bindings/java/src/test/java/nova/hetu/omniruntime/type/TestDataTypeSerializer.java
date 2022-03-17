/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */
package nova.hetu.omniruntime.type;

import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;

public class TestDataTypeSerializer {
    @Test
    public void testAllTypes()
    {
        DataType[] types = new DataType[] {IntDataType.INTEGER, LongDataType.LONG, DoubleDataType.DOUBLE,
                BooleanDataType.BOOLEAN, ShortDataType.SHORT, Decimal64DataType.DECIMAL64, Decimal128DataType.DECIMAL128,
                Date32DataType.DATE32, Date64DataType.DATE64, VarcharDataType.VARCHAR, CharDataType.CHAR,
                ContainerDataType.CONTAINER, DataType.INVALID};
        String[] serializeds = new String[types.length];
        for (int i = 0; i < types.length; i++) {
            serializeds[i] = DataTypeSerializer.serializeSingle(types[i]);
        }
        for (int i = 0; i < types.length; i++) {
            assertEquals(DataTypeSerializer.deserializeSingle(serializeds[i]), types[i]);
        }
    }
}
