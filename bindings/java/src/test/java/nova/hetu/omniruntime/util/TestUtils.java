package nova.hetu.omniruntime.util;

import nova.hetu.omniruntime.type.VecType;
import nova.hetu.omniruntime.vector.DictionaryVec;
import nova.hetu.omniruntime.vector.DoubleVec;
import nova.hetu.omniruntime.vector.IntVec;
import nova.hetu.omniruntime.vector.LongVec;
import nova.hetu.omniruntime.vector.VarcharVec;
import nova.hetu.omniruntime.vector.Vec;
import nova.hetu.omniruntime.vector.VecBatch;

import java.nio.charset.StandardCharsets;

import static nova.hetu.omniruntime.type.VecType.VecTypeId.OMNI_VEC_TYPE_DICTIONARY;
import static nova.hetu.omniruntime.type.VecType.VecTypeId.OMNI_VEC_TYPE_DOUBLE;
import static nova.hetu.omniruntime.type.VecType.VecTypeId.OMNI_VEC_TYPE_INT;
import static nova.hetu.omniruntime.type.VecType.VecTypeId.OMNI_VEC_TYPE_LONG;
import static nova.hetu.omniruntime.type.VecType.VecTypeId.OMNI_VEC_TYPE_VARCHAR;
import static org.testng.Assert.assertEquals;

public class TestUtils {
    public static VecBatch createVecBatch(VecType[] types, Object[][] datas) {
        Vec[] vecs = new Vec[types.length];
        for (int i = 0; i < types.length; i++) {
            VecType.VecTypeId typeId = types[i].getId();
            if (typeId.equals(OMNI_VEC_TYPE_INT)) {
                vecs[i] = new IntVec(datas[i].length);
                for (int j = 0; j < datas[i].length; j++) {
                    ((IntVec) vecs[i]).set(j, (int) datas[i][j]);
                }
            }
            else if (typeId.equals(OMNI_VEC_TYPE_LONG)) {
                vecs[i] = new LongVec(datas[i].length);
                for (int j = 0; j < datas[i].length; j++) {
                    ((LongVec) vecs[i]).set(j, (long) datas[i][j]);
                }
            }
            else if (typeId.equals(OMNI_VEC_TYPE_DOUBLE)) {
                vecs[i] = new DoubleVec(datas[i].length);
                for (int j = 0; j < datas[i].length; j++) {
                    ((DoubleVec) vecs[i]).set(j, (double) datas[i][j]);
                }
            }
            else if (typeId.equals(OMNI_VEC_TYPE_VARCHAR)) {
                vecs[i] = new VarcharVec(1024, datas[i].length);
                for (int j = 0; j < datas[i].length; j++) {
                    ((VarcharVec) vecs[i]).set(j, ((String) datas[i][j]).getBytes(StandardCharsets.UTF_8));
                }
            }
            else {
                vecs[i] = null;
            }
        }

        VecBatch vecBatch = new VecBatch(vecs);
        return vecBatch;
    }

    public static void assertVecBatchEquals(VecBatch vecBatch, Object[][] expectedDatas) {
        int vectorCount = vecBatch.getVectorCount();
        assertEquals(vectorCount, expectedDatas.length);

        Vec[] vecs = vecBatch.getVectors();
        for (int i = 0; i < vectorCount; i++) {
            Vec vec = vecs[i];
            VecType.VecTypeId typeId = vec.getType().getId();
            if (typeId.equals(OMNI_VEC_TYPE_DICTIONARY)) {
                assertDictionaryVecEquals((DictionaryVec) vec, expectedDatas[i]);
                continue;
            }

            int len = vec.getSize();
            for (int j = 0; j < len; j++) {
                if (vec.isNull(j)) {
                    assertEquals(null, expectedDatas[i][j]);
                    continue;
                }
                if (typeId.equals(OMNI_VEC_TYPE_INT)) {
                    assertEquals(((IntVec) vec).get(j), expectedDatas[i][j]);
                }
                else if (typeId.equals(OMNI_VEC_TYPE_LONG)) {
                    assertEquals(((LongVec) vec).get(j), expectedDatas[i][j]);
                }
                else if (typeId.equals(OMNI_VEC_TYPE_DOUBLE)) {
                    assertEquals(((DoubleVec) vec).get(j), expectedDatas[i][j]);
                }
                else if (typeId.equals(OMNI_VEC_TYPE_VARCHAR)) {
                    assertEquals(new String(((VarcharVec) vec).get(j)), expectedDatas[i][j]);
                }
            }
        }
    }

    private static void assertDictionaryVecEquals(DictionaryVec vec, Object[] expectedData) {
        VecType.VecTypeId typeId = vec.getDictionary().getType().getId();
        for (int i = 0; i < vec.getSize(); i++) {
            if (typeId.equals(OMNI_VEC_TYPE_INT)) {
                assertEquals(vec.getInt(i), expectedData[i]);
            }
            else if (typeId.equals(OMNI_VEC_TYPE_LONG)) {
                assertEquals(vec.getLong(i), expectedData[i]);
            }
        }
    }
}
