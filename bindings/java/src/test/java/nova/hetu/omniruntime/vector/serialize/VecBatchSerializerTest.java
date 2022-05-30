/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

package nova.hetu.omniruntime.vector.serialize;

import static nova.hetu.omniruntime.type.CharDataType.CHAR;
import static nova.hetu.omniruntime.type.DataType.INVALID;
import static nova.hetu.omniruntime.type.Date32DataType.DATE32;
import static nova.hetu.omniruntime.type.Date64DataType.DATE64;
import static nova.hetu.omniruntime.type.Decimal64DataType.DECIMAL64;
import static nova.hetu.omniruntime.util.TestUtils.assertVecBatchEquals;
import static nova.hetu.omniruntime.util.TestUtils.assertVecEquals;
import static nova.hetu.omniruntime.util.TestUtils.freeVecBatch;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import nova.hetu.omniruntime.type.ContainerDataType;
import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.type.Decimal128DataType;
import nova.hetu.omniruntime.type.IntDataType;
import nova.hetu.omniruntime.type.LongDataType;
import nova.hetu.omniruntime.type.VarcharDataType;
import nova.hetu.omniruntime.utils.OmniRuntimeException;
import nova.hetu.omniruntime.vector.BooleanVec;
import nova.hetu.omniruntime.vector.ContainerVec;
import nova.hetu.omniruntime.vector.Decimal128Vec;
import nova.hetu.omniruntime.vector.DictionaryVec;
import nova.hetu.omniruntime.vector.DoubleVec;
import nova.hetu.omniruntime.vector.IntVec;
import nova.hetu.omniruntime.vector.LongVec;
import nova.hetu.omniruntime.vector.VarcharVec;
import nova.hetu.omniruntime.vector.Vec;
import nova.hetu.omniruntime.vector.VecAllocator;
import nova.hetu.omniruntime.vector.VecBatch;
import nova.hetu.omniruntime.vector.VecUtil;

import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;

/**
 * Vec batch serializer test
 *
 * @since 2021-9-14
 */
public class VecBatchSerializerTest {
    private static final int ROW_COUNT = 1024;

    @Test
    public void testSerializeCommonTypes() {
        // prepare vector batch
        LongVec longVec = new LongVec(ROW_COUNT);
        IntVec intVec = new IntVec(ROW_COUNT);
        VarcharVec varCharVec = new VarcharVec(2 * ROW_COUNT * 20, ROW_COUNT);
        Decimal128Vec decimal128Vec = new Decimal128Vec(ROW_COUNT);
        for (int i = 0; i < ROW_COUNT; i++) {
            longVec.set(i, i);
            intVec.set(i, i);
            varCharVec.set(i, ("test" + i).getBytes(StandardCharsets.UTF_8));
            decimal128Vec.set(i, new long[]{i, i + 1});
        }
        Vec[] vecs = {longVec, intVec, varCharVec, decimal128Vec};
        VecBatch vecBatch = new VecBatch(vecs);

        // serialize
        VecBatchSerializer serializer = VecBatchSerializerFactory.create();
        byte[] str = serializer.serialize(vecBatch);

        // deserialize
        VecBatch checkVecBatch = serializer.deserialize(str);

        // check result
        LongVec checkLongVec = (LongVec) checkVecBatch.getVectors()[0];
        IntVec checkIntVec = (IntVec) checkVecBatch.getVectors()[1];
        VarcharVec checkVarCharVec = (VarcharVec) checkVecBatch.getVectors()[2];
        Decimal128Vec checkDecimal128Vec = (Decimal128Vec) checkVecBatch.getVectors()[3];
        for (int i = 0; i < ROW_COUNT; i++) {
            assertEquals(i, checkLongVec.get(i));
            assertEquals(i, checkIntVec.get(i));
            assertEquals("test" + i, new String(checkVarCharVec.get(i)));
            assertEquals(i, checkDecimal128Vec.get(i)[0]);
            assertEquals(i + 1, checkDecimal128Vec.get(i)[1]);
        }
        freeVecBatch(vecBatch);
        freeVecBatch(checkVecBatch);
    }

    @Test
    public void testSerializeDirectoryVecContainsLongVec() {
        // prepare vector batch
        LongVec dictionary = new LongVec(ROW_COUNT);
        for (int i = 0; i < ROW_COUNT; i++) {
            dictionary.set(i, i);
        }
        DictionaryVec dictionaryVec = new DictionaryVec(dictionary, new int[]{1, 2, 1000});
        dictionary.close();
        VecBatch vecBatch = new VecBatch(new Vec[]{dictionaryVec});

        // serialize
        VecBatchSerializer serializer = VecBatchSerializerFactory.create();
        byte[] str = serializer.serialize(vecBatch);

        // deserialize
        VecBatch checkVecBatch = serializer.deserialize(str);

        // check result
        LongVec checkLongVec = (LongVec) checkVecBatch.getVectors()[0];
        assertEquals(3, checkLongVec.getSize());
        assertEquals(1, checkLongVec.get(0));
        assertEquals(2, checkLongVec.get(1));
        assertEquals(1000, checkLongVec.get(2));

        freeVecBatch(vecBatch);
        freeVecBatch(checkVecBatch);
    }

    @Test
    public void testSerializeDirectoryVecContainsVarcharVec() {
        // prepare vector batch
        VarcharVec dictionary = new VarcharVec(ROW_COUNT * 20, ROW_COUNT);
        for (int i = 0; i < ROW_COUNT; i++) {
            dictionary.set(i, ("test" + i).getBytes(StandardCharsets.UTF_8));
        }
        DictionaryVec dictionaryVec = new DictionaryVec(dictionary, new int[]{1, 2, 1000});
        dictionary.close();
        VecBatch vecBatch = new VecBatch(new Vec[]{dictionaryVec});

        // serialize
        VecBatchSerializer serializer = VecBatchSerializerFactory.create();
        byte[] str = serializer.serialize(vecBatch);

        // deserialize
        VecBatch checkVecBatch = serializer.deserialize(str);

        // check result
        VarcharVec checkLongVec = (VarcharVec) checkVecBatch.getVectors()[0];
        assertEquals(3, checkLongVec.getSize());
        assertEquals("test1", new String(checkLongVec.get(0)));
        assertEquals("test2", new String(checkLongVec.get(1)));
        assertEquals("test1000", new String(checkLongVec.get(2)));

        freeVecBatch(vecBatch);
        freeVecBatch(checkVecBatch);
    }

    @Test
    public void testSerializeNestedDirectoryVec() {
        // prepare vector batch
        VarcharVec dictionary = new VarcharVec(ROW_COUNT * 20, ROW_COUNT);
        for (int i = 0; i < ROW_COUNT; i++) {
            dictionary.set(i, ("test" + i).getBytes(StandardCharsets.UTF_8));
        }
        DictionaryVec dictionaryVec = new DictionaryVec(dictionary, new int[]{1, 2, 3, 4, 5, 6, 7, 1000});
        dictionary.close();
        DictionaryVec nestedDictionaryVec = new DictionaryVec(dictionaryVec, new int[]{1, 2, 7});
        dictionaryVec.close();
        VecBatch vecBatch = new VecBatch(new Vec[]{nestedDictionaryVec});

        // serialize
        VecBatchSerializer serializer = VecBatchSerializerFactory.create();
        byte[] str = serializer.serialize(vecBatch);

        // deserialize
        VecBatch checkVecBatch = serializer.deserialize(str);

        // check result
        VarcharVec checkLongVec = (VarcharVec) checkVecBatch.getVectors()[0];
        assertEquals(3, checkLongVec.getSize());
        assertEquals("test2", new String(checkLongVec.get(0)));
        assertEquals("test3", new String(checkLongVec.get(1)));
        assertEquals("test1000", new String(checkLongVec.get(2)));

        freeVecBatch(vecBatch);
        freeVecBatch(checkVecBatch);
    }

    @Test
    public void testSerializeContainerVec() {
        // prepare vector batch
        LongVec longVec = new LongVec(ROW_COUNT);
        IntVec intVec = new IntVec(ROW_COUNT);
        VarcharVec varCharVec = new VarcharVec(ROW_COUNT * 20, ROW_COUNT);
        Decimal128Vec decimal128Vec = new Decimal128Vec(ROW_COUNT);
        for (int i = 0; i < ROW_COUNT; i++) {
            longVec.set(i, i);
            intVec.set(i, i);
            varCharVec.set(i, ("test" + i).getBytes(StandardCharsets.UTF_8));
            decimal128Vec.set(i, new long[]{i, i + 1});
        }
        long[] vecAddresses = new long[]{longVec.getNativeVector(), intVec.getNativeVector(),
                varCharVec.getNativeVector(), decimal128Vec.getNativeVector()};
        ContainerVec containerVec = new ContainerVec(vecAddresses.length, ROW_COUNT, vecAddresses, new DataType[]{
                new LongDataType(), new IntDataType(), new VarcharDataType(20), new Decimal128DataType(10, 1)});
        VecBatch vecBatch = new VecBatch(new Vec[]{containerVec});

        // serialize
        VecBatchSerializer serializer = VecBatchSerializerFactory.create();
        byte[] str = serializer.serialize(vecBatch);

        // deserialize
        VecBatch checkVecBatch = serializer.deserialize(str);

        // check result
        ContainerVec checkContainerVec = (ContainerVec) checkVecBatch.getVectors()[0];
        assertEquals(1024, checkContainerVec.getSize());
        LongVec checkLongVec = new LongVec(checkContainerVec.getVector(0));
        IntVec checkIntVec = new IntVec(checkContainerVec.getVector(1));
        VarcharVec checkVarCharVec = new VarcharVec(checkContainerVec.getVector(2));
        Decimal128Vec checkDecimal128Vec = new Decimal128Vec(checkContainerVec.getVector(3));
        for (int i = 0; i < ROW_COUNT; i++) {
            assertEquals(i, checkLongVec.get(i));
            assertEquals(i, checkIntVec.get(i));
            assertEquals("test" + i, new String(checkVarCharVec.get(i)));
            assertEquals(i, checkDecimal128Vec.get(i)[0]);
            assertEquals(i + 1, checkDecimal128Vec.get(i)[1]);
        }

        freeVecBatch(vecBatch);
        freeVecBatch(checkVecBatch);
    }

    @Test(enabled = false)
    public void testSerializeNestedContainerVec() {
        // prepare vector batch
        LongVec longVec = new LongVec(ROW_COUNT);
        IntVec intVec = new IntVec(ROW_COUNT);
        VarcharVec varCharVec = new VarcharVec(ROW_COUNT * 20, ROW_COUNT);
        Decimal128Vec decimal128Vec = new Decimal128Vec(ROW_COUNT);
        for (int i = 0; i < ROW_COUNT; i++) {
            longVec.set(i, i);
            intVec.set(i, i);
            varCharVec.set(i, ("test" + i).getBytes(StandardCharsets.UTF_8));
            decimal128Vec.set(i, new long[]{i, i + 1});
        }
        long[] vecAddresses = new long[]{longVec.getNativeVector(), intVec.getNativeVector(),
                varCharVec.getNativeVector(), decimal128Vec.getNativeVector()};
        ContainerVec containerVec = new ContainerVec(vecAddresses.length, ROW_COUNT, vecAddresses, new DataType[]{
                new LongDataType(), new IntDataType(), new VarcharDataType(20), new Decimal128DataType(10, 1)});
        ContainerVec nestedContainerVec = new ContainerVec(1, ROW_COUNT, new long[]{containerVec.getNativeVector()},
                new DataType[]{new ContainerDataType()});
        VecBatch vecBatch = new VecBatch(new Vec[]{nestedContainerVec});

        // serialize
        VecBatchSerializer serializer = VecBatchSerializerFactory.create();
        byte[] str = serializer.serialize(vecBatch);

        // deserialize
        VecBatch checkVecBatch = serializer.deserialize(str);

        // check result
        ContainerVec checkContainerVec = (ContainerVec) checkVecBatch.getVectors()[0];
        assertEquals(4, checkContainerVec.getSize());
        LongVec checkLongVec = new LongVec(checkContainerVec.getVector(0));
        IntVec checkIntVec = new IntVec(checkContainerVec.getVector(1));
        VarcharVec checkVarCharVec = new VarcharVec(checkContainerVec.getVector(2));
        Decimal128Vec checkDecimal128Vec = new Decimal128Vec(checkContainerVec.getVector(3));
        for (int i = 0; i < ROW_COUNT; i++) {
            assertEquals(i, checkLongVec.get(i));
            assertEquals(i, checkIntVec.get(i));
            assertEquals("test" + i, new String(checkVarCharVec.get(i)));
            assertEquals(i, checkDecimal128Vec.get(i)[0]);
            assertEquals(i + 1, checkDecimal128Vec.get(i)[1]);
        }

        freeVecBatch(vecBatch);
    }

    @Test
    public void testSerializeSameVectorMultipleTimes() {
        int size = 10;
        LongVec col1 = new LongVec(size);
        for (int i = 0; i < size; i++) {
            col1.set(i, i);
        }

        Vec[] vecs = new Vec[1];
        for (int count = 0; count < 2; count++) {
            vecs[0] = col1;
            VecBatch vecBatch = new VecBatch(vecs, size);
            // serialize
            VecBatchSerializer serializer = VecBatchSerializerFactory.create();
            byte[] str = serializer.serialize(vecBatch);
            // deserialize
            VecBatch resultVecBatch = serializer.deserialize(str);
            Object[][] expectedDatas = {{0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L}};
            assertVecBatchEquals(resultVecBatch, expectedDatas);
            vecBatch.close();
            freeVecBatch(resultVecBatch);
        }
        col1.close();
    }

    @Test
    public void testSerializeCharVec() {
        // prepare vector batch
        VarcharVec dictionary = new VarcharVec(ROW_COUNT * 20, ROW_COUNT);
        for (int i = 0; i < ROW_COUNT; i++) {
            dictionary.set(i, ("test" + i).getBytes(StandardCharsets.UTF_8));
        }
        DictionaryVec dictionaryVec = new DictionaryVec(dictionary, new int[]{1, 2, 1000});
        dictionary.close();
        VecBatch vecBatch = new VecBatch(new Vec[]{dictionaryVec});

        // serialize
        VecBatchSerializer serializer = VecBatchSerializerFactory.create();
        byte[] str = serializer.serialize(vecBatch);

        // deserialize
        VecBatch checkVecBatch = serializer.deserialize(str);

        // check result
        VarcharVec checkResultVec = (VarcharVec) checkVecBatch.getVectors()[0];
        assertEquals(3, checkResultVec.getSize());
        assertEquals("test1", new String(checkResultVec.get(0)));
        assertEquals("test2", new String(checkResultVec.get(1)));
        assertEquals("test1000", new String(checkResultVec.get(2)));

        freeVecBatch(vecBatch);
        freeVecBatch(checkVecBatch);
    }

    @Test
    public void testSerializeVectorSizeReset() {
        int size = 1000;
        LongVec col1 = new LongVec(size);
        for (int i = 0; i < size; i++) {
            col1.set(i, i);
        }
        col1.setSize(5);
        Vec[] vecs = new Vec[]{col1};
        VecBatch vecBatch = new VecBatch(vecs, size);
        // serialize
        VecBatchSerializer serializer = VecBatchSerializerFactory.create();
        byte[] str = serializer.serialize(vecBatch);
        // deserialize
        VecBatch resultVecBatch = serializer.deserialize(str);
        Object[][] expectedDatas = {{0L, 1L, 2L, 3L, 4L}};
        assertVecBatchEquals(resultVecBatch, expectedDatas);

        freeVecBatch(vecBatch);
        freeVecBatch(resultVecBatch);
    }

    @Test
    public void testSerializeVarcharVecWithNull() {
        // prepare vector batch
        int row = 10;
        VarcharVec vec = new VarcharVec(row * 20, row);
        for (int i = 0; i < row; i++) {
            if (i % 2 == 0) {
                vec.set(i, ("test" + i).getBytes(StandardCharsets.UTF_8));
            } else {
                vec.setNull(i);
            }
        }
        VecBatch vecBatch = new VecBatch(new Vec[]{vec});

        // serialize
        VecBatchSerializer serializer = VecBatchSerializerFactory.create();
        byte[] str = serializer.serialize(vecBatch);

        // deserialize
        VecBatch checkVecBatch = serializer.deserialize(str);

        // check result
        VarcharVec checkResultVec = (VarcharVec) checkVecBatch.getVector(0);
        assertEquals(row, checkResultVec.getSize());
        for (int i = 0; i < row; i++) {
            if (i % 2 == 0) {
                assertEquals("test" + i, new String(checkResultVec.get(i)));
            } else {
                assertTrue(checkResultVec.isNull(i));
            }
        }

        freeVecBatch(vecBatch);
        freeVecBatch(checkVecBatch);
    }

    @Test
    public void testSerializeWithSetDataType() {
        int row = 5;
        IntVec data32 = new IntVec(row);
        VecUtil.setDataType(data32, DATE32);
        data32.put(new int[]{1, 2, 3, 4, 5}, 0, 0, row);
        LongVec data64 = new LongVec(row);
        VecUtil.setDataType(data64, DATE64);
        data64.put(new long[]{1, 2, 3, 4, 5}, 0, 0, row);
        LongVec decimal64 = new LongVec(row);
        VecUtil.setDataType(decimal64, DECIMAL64);
        decimal64.put(new long[]{1, 2, 3, 4, 5}, 0, 0, row);
        VarcharVec charVec = new VarcharVec(1024, row);
        VecUtil.setDataType(charVec, CHAR);
        charVec.put(0, "12345".getBytes(StandardCharsets.UTF_8), 0, new int[]{0, 1, 2, 3, 4, 5}, 0, row);

        DoubleVec doubleVec = new DoubleVec(row);
        doubleVec.put(new double[]{1.1, 2.2, 3.3, 4.4, 5.5}, 0, 0, row);
        BooleanVec booleanVec = new BooleanVec(row);
        booleanVec.put(new boolean[]{true, false, true, false, true}, 0, 0, row);

        VecBatch vecBatch = new VecBatch(new Vec[]{data32, data64, decimal64, charVec, doubleVec, booleanVec});

        // serialize
        VecBatchSerializer serializer = VecBatchSerializerFactory.create();
        byte[] serialized = serializer.serialize(vecBatch);

        // deserialize
        VecBatch checkVecBatch = serializer.deserialize(serialized);

        // check result
        Object[][] expectedDatas = {{1, 2, 3, 4, 5}, {1L, 2L, 3L, 4L, 5L}, {1L, 2L, 3L, 4L, 5L},
                {"1", "2", "3", "4", "5"}, {1.1D, 2.2D, 3.3D, 4.4D, 5.5D}, {true, false, true, false, true}};
        assertVecBatchEquals(checkVecBatch, expectedDatas);

        freeVecBatch(vecBatch);
        freeVecBatch(checkVecBatch);
    }

    @Test(expectedExceptions = IllegalStateException.class,
        expectedExceptionsMessageRegExp = "Unexpected data type: OMNI_INVALID")
    public void testSerializeInvalidType() {
        int row = 5;
        IntVec invalidType = new IntVec(row);
        VecUtil.setDataType(invalidType, INVALID);
        VecBatch vecBatch = new VecBatch(new Vec[]{invalidType});
        VecBatchSerializer serializer = VecBatchSerializerFactory.create();
        try {
            serializer.serialize(vecBatch);
        } finally {
            freeVecBatch(vecBatch);
        }
    }

    @Test(expectedExceptions = OmniRuntimeException.class, expectedExceptionsMessageRegExp = "deserialize failed.null")
    public void deserializeInvalid() {
        VecBatchSerializer serializer = VecBatchSerializerFactory.create();
        serializer.deserialize("invalid".getBytes(StandardCharsets.UTF_8));
    }

    @Test(expectedExceptions = OmniRuntimeException.class, expectedExceptionsMessageRegExp = "deserialize failed.null")
    public void deserializeInvalidWithAllocator() {
        VecBatchSerializer serializer = VecBatchSerializerFactory.create();
        serializer.deserialize(VecAllocator.GLOBAL_VECTOR_ALLOCATOR, "invalid".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void testDeserializeWithAllocator() {
        int row = 5;
        IntVec intVec = new IntVec(row);
        int[] values = new int[]{1, 2, 3, 4, 5};
        intVec.put(values, 0, 0, row);
        VecBatch vecBatch = new VecBatch(new Vec[]{intVec});
        VecBatchSerializer serializer = VecBatchSerializerFactory.create();
        byte[] serialized = serializer.serialize(vecBatch);
        VecBatch checkVecBatch = serializer.deserialize(VecAllocator.GLOBAL_VECTOR_ALLOCATOR, serialized);
        IntVec checkResultVec = (IntVec) checkVecBatch.getVector(0);
        assertVecEquals(checkResultVec, new Object[]{1, 2, 3, 4, 5});

        freeVecBatch(vecBatch);
        freeVecBatch(checkVecBatch);
    }

    @Test
    public void testSerializeDecimal128Vec() {
        int row = 8;
        VecAllocator vecAllocator = VecAllocator.GLOBAL_VECTOR_ALLOCATOR
                .newChildAllocator("VecBatchSerializerTest_testSerializeDecimal128Vec", VecAllocator.UNLIMIT, 0);
        IntVec intVec = new IntVec(vecAllocator, row);
        VarcharVec varcharVec = new VarcharVec(vecAllocator, row * 20, row);
        Decimal128Vec decimal128Vec = new Decimal128Vec(vecAllocator, row);
        for (int i = 0; i < row; i++) {
            intVec.set(i, i);
            varcharVec.set(i, ("test" + i).getBytes(StandardCharsets.UTF_8));
            decimal128Vec.set(i, new long[]{i, i + 1});
        }
        Vec[] vecArray = new Vec[]{intVec, varcharVec, decimal128Vec};

        long[] nativeVectors = new long[vecArray.length];
        long[] nativeVectorValueBufAddresses = new long[vecArray.length];
        long[] nativeVectorNullBufAddresses = new long[vecArray.length];
        long[] nativeVectorAllocators = new long[vecArray.length];
        int[] capacityInBytes = new int[vecArray.length];
        int[] offsets = new int[vecArray.length];
        int[] encodings = new int[vecArray.length];
        int[] dataTypeIds = new int[vecArray.length];
        for (int i = 0; i < vecArray.length; i++) {
            nativeVectors[i] = vecArray[i].getNativeVector();
            nativeVectorValueBufAddresses[i] = vecArray[i].getValuesBuf().getAddress();
            nativeVectorNullBufAddresses[i] = vecArray[i].getValueNullsBuf().getAddress();
            nativeVectorAllocators[i] = vecArray[i].getAllocator().getNativeAllocator();
            capacityInBytes[i] = vecArray[i].getCapacityInBytes();
            offsets[i] = vecArray[i].getOffset();
            encodings[i] = vecArray[i].getEncoding().ordinal();
            dataTypeIds[i] = vecArray[i].getType().getId().ordinal();
        }
        long[] nativeVectorOffsetBufAddresses = new long[]{0, varcharVec.getOffsetsBuf().getAddress(), 0};
        VecBatch vecBatch = new VecBatch(vecArray, row);
        VecBatch vecBatchFromNative = new VecBatch(vecBatch.getNativeVectorBatch(), nativeVectors,
                nativeVectorValueBufAddresses, nativeVectorNullBufAddresses, nativeVectorOffsetBufAddresses,
                nativeVectorAllocators, capacityInBytes, offsets, encodings, dataTypeIds, row);
        VecBatchSerializer serializer = VecBatchSerializerFactory.create();
        byte[] vecBatchSerialized = serializer.serialize(vecBatchFromNative);
        VecBatch vecBatchDeserialized = serializer.deserialize(vecBatchSerialized);
        assertVecBatchEquals(vecBatchDeserialized, vecBatchFromNative);
        assertVecBatchEquals(vecBatch, vecBatchFromNative);

        freeVecBatch(vecBatch);
        freeVecBatch(vecBatchDeserialized);
        vecAllocator.close();
    }
}
