package nova.hetu.omniruntime.util;

import static nova.hetu.omniruntime.type.VecType.VecTypeId.OMNI_VEC_TYPE_DICTIONARY;
import static nova.hetu.omniruntime.type.VecType.VecTypeId.OMNI_VEC_TYPE_DOUBLE;
import static nova.hetu.omniruntime.type.VecType.VecTypeId.OMNI_VEC_TYPE_INT;
import static nova.hetu.omniruntime.type.VecType.VecTypeId.OMNI_VEC_TYPE_LONG;
import static nova.hetu.omniruntime.type.VecType.VecTypeId.OMNI_VEC_TYPE_VARCHAR;

import static org.testng.Assert.assertEquals;

import nova.hetu.omniruntime.type.VarcharVecType;
import nova.hetu.omniruntime.type.VecType;
import nova.hetu.omniruntime.vector.DictionaryVec;
import nova.hetu.omniruntime.vector.DoubleVec;
import nova.hetu.omniruntime.vector.IntVec;
import nova.hetu.omniruntime.vector.LongVec;
import nova.hetu.omniruntime.vector.VarcharVec;
import nova.hetu.omniruntime.vector.Vec;
import nova.hetu.omniruntime.vector.VecBatch;

import java.nio.charset.StandardCharsets;

public class TestUtils {
    public static VecBatch createVecBatch(VecType[] types, Object[][] datas) {
        Vec[] vecs = new Vec[types.length];
        for (int i = 0; i < types.length; i++) {
            VecType.VecTypeId typeId = types[i].getId();
            switch (typeId) {
                case OMNI_VEC_TYPE_INT:
                    vecs[i] = createIntVec(datas[i]);
                    break;
                case OMNI_VEC_TYPE_LONG:
                    vecs[i] = createLongVec(datas[i]);
                    break;
                case OMNI_VEC_TYPE_DOUBLE:
                    vecs[i] = createDoubleVec(datas[i]);
                    break;
                case OMNI_VEC_TYPE_VARCHAR:
                    vecs[i] = createVarcharVec((VarcharVecType) types[i], datas[i]);
                    break;
                default:
                    vecs[i] = null;
                    break;
            }
        }
        VecBatch vecBatch = new VecBatch(vecs);
        return vecBatch;
    }

    private static IntVec createIntVec(Object[] data) {
        IntVec result = new IntVec(data.length);
        for (int j = 0; j < data.length; j++) {
            if (data[j] == null) {
                result.setNull(j);
            } else {
                result.set(j, (int) data[j]);
            }
        }
        return result;
    }

    private static LongVec createLongVec(Object[] data) {
        LongVec result = new LongVec(data.length);
        for (int j = 0; j < data.length; j++) {
            if (data[j] == null) {
                result.setNull(j);
            } else {
                result.set(j, (long) data[j]);
            }
        }
        return result;
    }

    private static DoubleVec createDoubleVec(Object[] data) {
        DoubleVec result = new DoubleVec(data.length);
        for (int j = 0; j < data.length; j++) {
            if (data[j] == null) {
                result.setNull(j);
            } else {
                result.set(j, (double) data[j]);
            }
        }
        return result;
    }

    private static VarcharVec createVarcharVec(VarcharVecType varcharVecType, Object[] data) {
        VarcharVec result = new VarcharVec(varcharVecType.getWidth() * data.length, data.length);
        for (int j = 0; j < data.length; j++) {
            if (data[j] == null) {
                result.setNull(j);
            } else {
                result.set(j, ((String) data[j]).getBytes(StandardCharsets.UTF_8));
            }
        }
        return result;
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
                switch (typeId) {
                    case OMNI_VEC_TYPE_INT:
                        assertEquals(((IntVec) vec).get(j), expectedDatas[i][j]);
                        break;
                    case OMNI_VEC_TYPE_LONG:
                        assertEquals(((LongVec) vec).get(j), expectedDatas[i][j]);
                        break;
                    case OMNI_VEC_TYPE_DOUBLE:
                        assertEquals(((DoubleVec) vec).get(j), expectedDatas[i][j]);
                        break;
                    case OMNI_VEC_TYPE_VARCHAR:
                        assertEquals(new String(((VarcharVec) vec).get(j)), expectedDatas[i][j]);
                        break;
                }
            }
        }
    }

    private static void assertDictionaryVecEquals(DictionaryVec vec, Object[] expectedData) {
        VecType.VecTypeId typeId = vec.getDictionary().getType().getId();
        for (int i = 0; i < vec.getSize(); i++) {
            switch (typeId) {
                case OMNI_VEC_TYPE_INT:
                    assertEquals(vec.getInt(i), expectedData[i]);
                    break;
                case OMNI_VEC_TYPE_LONG:
                    assertEquals(vec.getLong(i), expectedData[i]);
                    break;
            }
        }
    }
}
