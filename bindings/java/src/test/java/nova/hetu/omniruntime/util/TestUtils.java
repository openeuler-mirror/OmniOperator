package nova.hetu.omniruntime.util;

import static nova.hetu.omniruntime.type.VecType.VecTypeId.OMNI_VEC_TYPE_DICTIONARY;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import nova.hetu.omniruntime.type.VarcharVecType;
import nova.hetu.omniruntime.type.VecType;
import nova.hetu.omniruntime.vector.BooleanVec;
import nova.hetu.omniruntime.vector.Decimal128Vec;
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
            vecs[i] = createVec(types[i], datas[i]);
        }
        VecBatch vecBatch = new VecBatch(vecs);
        return vecBatch;
    }

    public static Vec createVec(VecType type, Object[] data) {
        switch (type.getId()) {
            case OMNI_VEC_TYPE_INT:
            case OMNI_VEC_TYPE_DATE32:
                return createIntVec(data);
            case OMNI_VEC_TYPE_LONG:
            case OMNI_VEC_TYPE_DECIMAL64:
                return createLongVec(data);
            case OMNI_VEC_TYPE_DOUBLE:
                return createDoubleVec(data);
            case OMNI_VEC_TYPE_BOOLEAN:
                return createBooleanVec(data);
            case OMNI_VEC_TYPE_VARCHAR:
                return createVarcharVec((VarcharVecType) type, data);
            default:
                throw new UnsupportedOperationException("Unsupported type : " + type.getId());
        }
    }

    public static Vec createVec(VecType type, Object[][] data) {
        switch (type.getId()) {
            case OMNI_VEC_TYPE_DECIMAL128:
                return createDecimal128Vec(data);
            default:
                throw new UnsupportedOperationException("Unsupported type : " + type.getId());
        }
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

    private static BooleanVec createBooleanVec(Object[] data) {
        BooleanVec result = new BooleanVec(data.length);
        for (int i = 0; i < data.length; i++) {
            if (data[i] == null) {
                result.setNull(i);
            } else {
                result.set(i, (boolean) data[i]);
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

    private static Decimal128Vec createDecimal128Vec(Object[][] data) {
        Decimal128Vec result = new Decimal128Vec(data.length);
        for (int i = 0; i < data.length; i++) {
            if (data[i] == null) {
                result.setNull(i);
            } else {
                result.set(i, new long[] {(long) data[i][0], (long) data[i][1]});
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
            assertEquals(vec.getSize(), expectedDatas[i].length);
            VecType.VecTypeId typeId = vec.getType().getId();
            if (typeId.equals(OMNI_VEC_TYPE_DICTIONARY)) {
                assertDictionaryVecEquals((DictionaryVec) vec, expectedDatas[i]);
                continue;
            }
            assertVecEquals(vec, expectedDatas[i]);
        }
    }

    public static void assertVecEquals(Vec vec, Object[] expectedData) {
        for (int i = 0; i < vec.getSize(); i++) {
            if (vec.isNull(i)) {
                assertEquals(null, expectedData[i]);
                continue;
            }
            switch (vec.getType().getId()) {
                case OMNI_VEC_TYPE_INT:
                case OMNI_VEC_TYPE_DATE32:
                    assertEquals(((IntVec) vec).get(i), expectedData[i]);
                    break;
                case OMNI_VEC_TYPE_LONG:
                case OMNI_VEC_TYPE_DECIMAL64:
                    assertEquals(((LongVec) vec).get(i), expectedData[i]);
                    break;
                case OMNI_VEC_TYPE_DOUBLE:
                    assertTrue(Double.compare(((DoubleVec) vec).get(i), (Double) expectedData[i]) == 0);
                    break;
                case OMNI_VEC_TYPE_BOOLEAN:
                    assertEquals(((BooleanVec) vec).get(i), expectedData[i]);
                    break;
                case OMNI_VEC_TYPE_VARCHAR:
                    assertEquals(new String(((VarcharVec) vec).get(i)), expectedData[i]);
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported type : " + vec.getType().getId());
            }
        }
    }

    public static void assertVecEquals(Vec vec, Object[][] expectedData) {
        for (int i = 0; i < vec.getSize(); i++) {
            if (vec.isNull(i)) {
                assertEquals(null, expectedData[i]);
                continue;
            }
            switch (vec.getType().getId()) {
                case OMNI_VEC_TYPE_DECIMAL128:
                    assertEquals(
                            ((Decimal128Vec) vec).get(i),
                            new long[] {(long) expectedData[i][0], (long) expectedData[i][1]});
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported type : " + vec.getType().getId());
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

    public static void freeVecBatch(VecBatch vecBatch) {
        vecBatch.releaseAllVectors();
        vecBatch.close();
    }
}
