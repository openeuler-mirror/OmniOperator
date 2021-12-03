package nova.hetu.omniruntime.vector.serialize;

import static nova.hetu.omniruntime.util.TestUtils.assertVecBatchEquals;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import nova.hetu.omniruntime.type.ContainerVecType;
import nova.hetu.omniruntime.type.Decimal128VecType;
import nova.hetu.omniruntime.type.IntVecType;
import nova.hetu.omniruntime.type.LongVecType;
import nova.hetu.omniruntime.type.VarcharVecType;
import nova.hetu.omniruntime.type.VecType;
import nova.hetu.omniruntime.vector.ContainerVec;
import nova.hetu.omniruntime.vector.Decimal128Vec;
import nova.hetu.omniruntime.vector.DictionaryVec;
import nova.hetu.omniruntime.vector.IntVec;
import nova.hetu.omniruntime.vector.LongVec;
import nova.hetu.omniruntime.vector.VarcharVec;
import nova.hetu.omniruntime.vector.Vec;
import nova.hetu.omniruntime.vector.VecBatch;

import org.testng.annotations.Test;

public class VecBatchSerializerTest {

    @Test
    public void should_return_right_result_when_serialize_common_types()
    {
        // prepare vector batch
        int ROW_COUNT = 1024;
        LongVec longVec = new LongVec(ROW_COUNT);
        IntVec intVec = new IntVec(ROW_COUNT);
        VarcharVec varCharVec = new VarcharVec(2 * ROW_COUNT * 20, ROW_COUNT);
        Decimal128Vec decimal128Vec = new Decimal128Vec(ROW_COUNT);
        for (int i = 0; i < ROW_COUNT; i++) {
            longVec.set(i, i);
            intVec.set(i, i);
            varCharVec.set(i, ("test" + i).getBytes());
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
        LongVec checkLongVec = (LongVec)checkVecBatch.getVectors()[0];
        IntVec checkIntVec = (IntVec)checkVecBatch.getVectors()[1];
        VarcharVec checkVarCharVec = (VarcharVec)checkVecBatch.getVectors()[2];
        Decimal128Vec checkDecimal128Vec = (Decimal128Vec)checkVecBatch.getVectors()[3];
        for (int i = 0; i < ROW_COUNT; i++) {
            assertEquals(i, checkLongVec.get(i));
            assertEquals(i, checkIntVec.get(i));
            assertEquals("test" + i, new String(checkVarCharVec.get(i)));
            assertEquals(i, checkDecimal128Vec.get(i)[0]);
            assertEquals(i + 1, checkDecimal128Vec.get(i)[1]);
        }
        vecBatch.releaseAllVectors();
        vecBatch.close();
    }

    @Test
    public void should_return_right_result_when_serialize_directory_vec_contains_long_vec()
    {
        // prepare vector batch
        int ROW_COUNT = 1024;
        LongVec longVec = new LongVec(ROW_COUNT);
        for (int i = 0; i < ROW_COUNT; i++) {
            longVec.set(i, i);
        }
        DictionaryVec dictionaryVec = new DictionaryVec(longVec, new int[] {1, 2, 1000});
        VecBatch vecBatch = new VecBatch(new Vec[]{dictionaryVec});

        // serialize
        VecBatchSerializer serializer = VecBatchSerializerFactory.create();
        byte[] str = serializer.serialize(vecBatch);

        // deserialize
        VecBatch checkVecBatch = serializer.deserialize(str);

        // check result
        LongVec checkLongVec = (LongVec)checkVecBatch.getVectors()[0];
        assertEquals(3, checkLongVec.getSize());
        assertEquals(1, checkLongVec.get(0));
        assertEquals(2, checkLongVec.get(1));
        assertEquals(1000, checkLongVec.get(2));

        vecBatch.releaseAllVectors();
        vecBatch.close();
    }

    @Test
    public void should_return_right_result_when_serialize_directory_vec_contains_varchar_vec()
    {
        // prepare vector batch
        int ROW_COUNT = 1024;
        VarcharVec varCharVec = new VarcharVec(ROW_COUNT * 20, ROW_COUNT);
        for (int i = 0; i < ROW_COUNT; i++) {
            varCharVec.set(i, ("test" + i).getBytes());
        }
        DictionaryVec dictionaryVec = new DictionaryVec(varCharVec, new int[] {1, 2, 1000});
        VecBatch vecBatch = new VecBatch(new Vec[]{dictionaryVec});

        // serialize
        VecBatchSerializer serializer = VecBatchSerializerFactory.create();
        byte[] str = serializer.serialize(vecBatch);

        // deserialize
        VecBatch checkVecBatch = serializer.deserialize(str);

        // check result
        VarcharVec checkLongVec = (VarcharVec)checkVecBatch.getVectors()[0];
        assertEquals(3, checkLongVec.getSize());
        assertEquals("test1", new String(checkLongVec.get(0)));
        assertEquals("test2", new String(checkLongVec.get(1)));
        assertEquals("test1000", new String(checkLongVec.get(2)));

        vecBatch.releaseAllVectors();
        vecBatch.close();
    }

    @Test(enabled = false)
    public void should_return_right_result_when_serialize_nested_directory_vec()
    {
        // prepare vector batch
        int ROW_COUNT = 1024;
        VarcharVec varCharVec = new VarcharVec(ROW_COUNT * 20, ROW_COUNT);
        for (int i = 0; i < ROW_COUNT; i++) {
            varCharVec.set(i, ("test" + i).getBytes());
        }
        DictionaryVec dictionaryVec = new DictionaryVec(varCharVec, new int[] {1, 2, 3, 4, 5, 6, 7, 1000});
        DictionaryVec nestedDictionaryVec = new DictionaryVec(dictionaryVec, new int[] {1, 2, 7});
        VecBatch vecBatch = new VecBatch(new Vec[]{nestedDictionaryVec});

        // serialize
        VecBatchSerializer serializer = VecBatchSerializerFactory.create();
        byte[] str = serializer.serialize(vecBatch);

        // deserialize
        VecBatch checkVecBatch = serializer.deserialize(str);

        // check result
        VarcharVec checkLongVec = (VarcharVec)checkVecBatch.getVectors()[0];
        assertEquals(3, checkLongVec.getSize());
        assertEquals("test1", new String(checkLongVec.get(0)));
        assertEquals("test2", new String(checkLongVec.get(1)));
        assertEquals("test1000", new String(checkLongVec.get(2)));

        vecBatch.releaseAllVectors();
        vecBatch.close();
    }

    @Test
    public void should_return_right_result_when_serialize_container_vec()
    {
        // prepare vector batch
        int ROW_COUNT = 1024;
        LongVec longVec = new LongVec(ROW_COUNT);
        IntVec intVec = new IntVec(ROW_COUNT);
        VarcharVec varCharVec = new VarcharVec(ROW_COUNT * 20, ROW_COUNT);
        Decimal128Vec decimal128Vec = new Decimal128Vec(ROW_COUNT);
        for (int i = 0; i < ROW_COUNT; i++) {
            longVec.set(i, i);
            intVec.set(i, i);
            varCharVec.set(i, ("test" + i).getBytes());
            decimal128Vec.set(i, new long[]{i, i + 1});
        }
        long[] vecAddresses = new long[]{longVec.getNativeVector(), intVec.getNativeVector(), varCharVec.getNativeVector(), decimal128Vec.getNativeVector()};
        ContainerVec containerVec = new ContainerVec(vecAddresses.length, ROW_COUNT, vecAddresses, new VecType[]{
            new LongVecType(), new IntVecType(), new VarcharVecType(20), new Decimal128VecType(10, 1)
        });
        VecBatch vecBatch = new VecBatch(new Vec[]{containerVec});

        // serialize
        VecBatchSerializer serializer = VecBatchSerializerFactory.create();
        byte[] str = serializer.serialize(vecBatch);

        // deserialize
        VecBatch checkVecBatch = serializer.deserialize(str);

        // check result
        ContainerVec checkContainerVec = (ContainerVec)checkVecBatch.getVectors()[0];
        assertEquals(4, checkContainerVec.getSize());
        LongVec checkLongVec = new LongVec(checkContainerVec.getVector(0));
        IntVec checkIntVec = new IntVec(checkContainerVec.getVector(1));
        VarcharVec checkVarCharVec = new VarcharVec(checkContainerVec.getVector(2));
        Decimal128Vec checkDecimal128Vec = new Decimal128Vec(checkContainerVec.getVector(3), checkContainerVec.getVecTypes()[3]);
        for (int i = 0; i < ROW_COUNT; i++) {
            assertEquals(i, checkLongVec.get(i));
            assertEquals(i, checkIntVec.get(i));
            assertEquals("test" + i, new String(checkVarCharVec.get(i)));
            assertEquals(i, checkDecimal128Vec.get(i)[0]);
            assertEquals(i + 1, checkDecimal128Vec.get(i)[1]);
        }

        vecBatch.releaseAllVectors();
        vecBatch.close();
    }

    @Test(enabled = false)
    public void should_return_right_result_when_serialize_nested_container_vec()
    {
        // prepare vector batch
        int ROW_COUNT = 1024;
        LongVec longVec = new LongVec(ROW_COUNT);
        IntVec intVec = new IntVec(ROW_COUNT);
        VarcharVec varCharVec = new VarcharVec(ROW_COUNT * 20, ROW_COUNT);
        Decimal128Vec decimal128Vec = new Decimal128Vec(ROW_COUNT);
        for (int i = 0; i < ROW_COUNT; i++) {
            longVec.set(i, i);
            intVec.set(i, i);
            varCharVec.set(i, ("test" + i).getBytes());
            decimal128Vec.set(i, new long[]{i, i + 1});
        }
        long[] vecAddresses = new long[]{longVec.getNativeVector(), intVec.getNativeVector(), varCharVec.getNativeVector(), decimal128Vec.getNativeVector()};
        ContainerVec containerVec = new ContainerVec(vecAddresses.length, ROW_COUNT, vecAddresses, new VecType[]{
            new LongVecType(), new IntVecType(), new VarcharVecType(20), new Decimal128VecType(10, 1)
        });
        ContainerVec nestedContainerVec = new ContainerVec(1, ROW_COUNT, new long[] {containerVec.getNativeVector()}, new VecType[]{
            new ContainerVecType()
        });
        VecBatch vecBatch = new VecBatch(new Vec[]{nestedContainerVec});

        // serialize
        VecBatchSerializer serializer = VecBatchSerializerFactory.create();
        byte[] str = serializer.serialize(vecBatch);

        // deserialize
        VecBatch checkVecBatch = serializer.deserialize(str);

        // check result
        ContainerVec checkContainerVec = (ContainerVec)checkVecBatch.getVectors()[0];
        assertEquals(4, checkContainerVec.getSize());
        LongVec checkLongVec = new LongVec(checkContainerVec.getVector(0));
        IntVec checkIntVec = new IntVec(checkContainerVec.getVector(1));
        VarcharVec checkVarCharVec = new VarcharVec(checkContainerVec.getVector(2));
        Decimal128Vec checkDecimal128Vec = new Decimal128Vec(checkContainerVec.getVector(3), checkContainerVec.getVecTypes()[3]);
        for (int i = 0; i < ROW_COUNT; i++) {
            assertEquals(i, checkLongVec.get(i));
            assertEquals(i, checkIntVec.get(i));
            assertEquals("test" + i, new String(checkVarCharVec.get(i)));
            assertEquals(i, checkDecimal128Vec.get(i)[0]);
            assertEquals(i + 1, checkDecimal128Vec.get(i)[1]);
        }

        vecBatch.releaseAllVectors();
        vecBatch.close();
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
            resultVecBatch.close();
        }
        col1.close();
    }

    @Test
    public void testSerializeCharVec()
    {
        // prepare vector batch
        int ROW_COUNT = 1024;
        VarcharVec charVec = new VarcharVec(ROW_COUNT * 20, ROW_COUNT);
        for (int i = 0; i < ROW_COUNT; i++) {
            charVec.set(i, ("test" + i).getBytes());
        }
        DictionaryVec dictionaryVec = new DictionaryVec(charVec, new int[] {1, 2, 1000});
        VecBatch vecBatch = new VecBatch(new Vec[]{dictionaryVec});

        // serialize
        VecBatchSerializer serializer = VecBatchSerializerFactory.create();
        byte[] str = serializer.serialize(vecBatch);

        // deserialize
        VecBatch checkVecBatch = serializer.deserialize(str);

        // check result
        VarcharVec checkResultVec = (VarcharVec)checkVecBatch.getVectors()[0];
        assertEquals(3, checkResultVec.getSize());
        assertEquals("test1", new String(checkResultVec.get(0)));
        assertEquals("test2", new String(checkResultVec.get(1)));
        assertEquals("test1000", new String(checkResultVec.get(2)));

        vecBatch.releaseAllVectors();
        vecBatch.close();
        checkVecBatch.releaseAllVectors();
        checkVecBatch.close();
    }

    @Test
    public void testSerializeVectorSizeReset() {
        int size = 1000;
        LongVec col1 = new LongVec(size);
        for (int i = 0; i < size; i++) {
            col1.set(i, i);
        }
        col1.setSize(5);
        Vec[] vecs = new Vec []{col1};
        VecBatch vecBatch = new VecBatch(vecs, size);
        // serialize
        VecBatchSerializer serializer = VecBatchSerializerFactory.create();
        byte[] str = serializer.serialize(vecBatch);
        // deserialize
        VecBatch resultVecBatch = serializer.deserialize(str);
        Object[][] expectedDatas = {{0L, 1L, 2L, 3L, 4L}};
        assertVecBatchEquals(resultVecBatch, expectedDatas);
        vecBatch.close();
        resultVecBatch.close();
        col1.close();
    }

    @Test
    public void testSerializeVarcharVecWithNull()
    {
        // prepare vector batch
        int row = 10;
        VarcharVec vec = new VarcharVec(row * 20, row);
        for (int i = 0; i < row; i++) {
            if (i % 2 == 0) {
                vec.set(i, ("test" + i).getBytes());
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

        vecBatch.releaseAllVectors();
        vecBatch.close();
        checkVecBatch.releaseAllVectors();
        checkVecBatch.close();
    }
}
