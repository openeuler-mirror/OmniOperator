package nova.hetu.omniruntime.vector.serialize;

import static org.testng.AssertJUnit.assertEquals;

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
}
