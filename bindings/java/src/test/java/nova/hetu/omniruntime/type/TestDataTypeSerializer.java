/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

package nova.hetu.omniruntime.type;

import static org.testng.AssertJUnit.assertEquals;

import nova.hetu.omniruntime.vector.ContainerVec;
import nova.hetu.omniruntime.vector.Decimal128Vec;
import nova.hetu.omniruntime.vector.DoubleVec;
import nova.hetu.omniruntime.vector.IntVec;
import nova.hetu.omniruntime.vector.VarcharVec;

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
                CharDataType.CHAR, new ContainerDataType(new DataType[]{IntDataType.INTEGER, CharDataType.CHAR}),
                InvalidDataType.INVALID, NoneDataType.NONE};
        String[] serializeds = new String[types.length];
        for (int i = 0; i < types.length; i++) {
            serializeds[i] = DataTypeSerializer.serializeSingle(types[i]);
        }
        for (int i = 0; i < types.length; i++) {
            assertEquals(DataTypeSerializer.deserializeSingle(serializeds[i]), types[i]);
        }
    }

    @Test
    public void TestContainerVecSerialize() {
        int rows = 3;
        DataType[] dataTypes = new DataType[]{IntDataType.INTEGER, DoubleDataType.DOUBLE,
                Decimal128DataType.DECIMAL128, VarcharDataType.VARCHAR,
                new ContainerDataType(new DataType[]{IntDataType.INTEGER, DoubleDataType.DOUBLE})};

        IntVec intVec = new IntVec(rows);
        DoubleVec doubleVec = new DoubleVec(rows);
        Decimal128Vec decimal128Vec = new Decimal128Vec(rows);
        VarcharVec varcharVec = new VarcharVec(20, 10);

        long[] subAddr = new long[]{intVec.getNativeVector(), doubleVec.getNativeVector()};
        DataType[] subTypes = new DataType[]{IntDataType.INTEGER, DoubleDataType.DOUBLE};
        ContainerVec subContainerVec = new ContainerVec(2, rows, subAddr, subTypes);

        long[] addr = new long[]{intVec.getNativeVector(), doubleVec.getNativeVector(), decimal128Vec.getNativeVector(),
                varcharVec.getNativeVector(), subContainerVec.getNativeVector()};

        ContainerVec vec = new ContainerVec(dataTypes.length, rows, addr, dataTypes);
        ContainerVec vecFromNative = new ContainerVec(vec.getNativeVector());
        DataType[] dataTypesFromNative = vecFromNative.getDataTypes();

        for (int i = 0; i < dataTypes.length; i++) {
            assertEquals(dataTypes[i], dataTypesFromNative[i]);
        }
    }
}
