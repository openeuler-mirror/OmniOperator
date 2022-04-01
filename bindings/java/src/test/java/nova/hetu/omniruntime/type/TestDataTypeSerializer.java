/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

package nova.hetu.omniruntime.type;

import static org.testng.AssertJUnit.assertEquals;

import org.testng.annotations.Test;

/**
 * Data type serializer test
 *
 * @since 2022-2-17
 */
public class TestDataTypeSerializer {
    @Test
    public void testAllTypes() {
        DataType[] types = new DataType[]{IntDataType.INTEGER, LongDataType.LONG, DoubleDataType.DOUBLE,
                BooleanDataType.BOOLEAN, ShortDataType.SHORT, Decimal64DataType.DECIMAL64,
                Decimal128DataType.DECIMAL128, Date32DataType.DATE32, Date64DataType.DATE64, VarcharDataType.VARCHAR,
                CharDataType.CHAR, ContainerDataType.CONTAINER, DataType.INVALID};
        String[] serializeds = new String[types.length];
        for (int i = 0; i < types.length; i++) {
            serializeds[i] = DataTypeSerializer.serializeSingle(types[i]);
        }
        for (int i = 0; i < types.length; i++) {
            assertEquals(DataTypeSerializer.deserializeSingle(serializeds[i]), types[i]);
        }
    }
}
