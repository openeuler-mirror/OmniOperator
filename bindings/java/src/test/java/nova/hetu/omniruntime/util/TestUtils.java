
package nova.hetu.omniruntime.util;


import static nova.hetu.omniruntime.vector.VecEncoding.OMNI_VEC_ENCODING_DICTIONARY;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import nova.hetu.omniruntime.type.VarcharDataType;
import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.vector.BooleanVec;
import nova.hetu.omniruntime.vector.Decimal128Vec;
import nova.hetu.omniruntime.vector.DictionaryVec;
import nova.hetu.omniruntime.vector.DoubleVec;
import nova.hetu.omniruntime.vector.IntVec;
import nova.hetu.omniruntime.vector.LongVec;
import nova.hetu.omniruntime.vector.VarcharVec;
import nova.hetu.omniruntime.vector.Vec;
import nova.hetu.omniruntime.vector.VecBatch;
import nova.hetu.omniruntime.vector.VecEncoding;

import java.nio.charset.StandardCharsets;

public class TestUtils {

    public static VecBatch createBlankVecBatch(DataType[] types) {
        Object[] data = {};
        Vec[] vecs = new Vec[types.length];
        for (int i = 0; i < types.length; i++) {
            vecs[i] = createVec(types[i], data);
        }
        VecBatch vecBatch = new VecBatch(vecs, 0);
        return vecBatch;
    }

    public static VecBatch createVecBatch(DataType[] types, Object[][] datas) {
        Vec[] vecs = new Vec[types.length];
        for (int i = 0; i < types.length; i++) {
            vecs[i] = createVec(types[i], datas[i]);
        }
        VecBatch vecBatch = new VecBatch(vecs);
        return vecBatch;
    }

    public static Vec createVec(DataType type, Object[] data) {
        switch (type.getId()) {
            case OMNI_INT:
            case OMNI_DATE32:
                return createIntVec(data);
            case OMNI_LONG:
            case OMNI_DECIMAL64:
                return createLongVec(data);
            case OMNI_DOUBLE:
                return createDoubleVec(data);
            case OMNI_BOOLEAN:
                return createBooleanVec(data);
            case OMNI_VARCHAR:
            case OMNI_CHAR:
                return createVarcharVec((VarcharDataType) type, data);
            default :
                throw new UnsupportedOperationException("Unsupported type : " + type.getId());
        }
    }

    public static Vec createVec(DataType type, Object[][] data) {
        switch (type.getId()) {
            case OMNI_DECIMAL128:
                return createDecimal128Vec(data);
            default :
                throw new UnsupportedOperationException("Unsupported type : " + type.getId());
        }
    }

    public static IntVec createIntVec(Object[] data) {
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

    public static LongVec createLongVec(Object[] data) {
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

    public static DoubleVec createDoubleVec(Object[] data) {
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

    public static BooleanVec createBooleanVec(Object[] data) {
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

    public static VarcharVec createVarcharVec(VarcharDataType varcharVecType, Object[] data) {
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

    public static Decimal128Vec createDecimal128Vec(Object[][] data) {
        Decimal128Vec result = new Decimal128Vec(data.length);
        for (int i = 0; i < data.length; i++) {
            if (data[i] == null) {
                result.setNull(i);
            } else {
                result.set(i, new long[]{(long) data[i][0], (long) data[i][1]});
            }
        }
        return result;
    }

    public static DictionaryVec createDictionaryVec(DataType dataType, Object[] data, int[] ids) {
        Vec dictionary = createVec(dataType, data);
        DictionaryVec dictionaryVec = new DictionaryVec(dictionary, ids);
        dictionary.close();
        return dictionaryVec;
    }

    public static void assertVecBatchEquals(VecBatch vecBatch, Object[][] expectedDatas) {
        int vectorCount = vecBatch.getVectorCount();
        assertEquals(vectorCount, expectedDatas.length);

        Vec[] vecs = vecBatch.getVectors();
        for (int i = 0; i < vectorCount; i++) {
            Vec vec = vecs[i];
            assertEquals(vec.getSize(), expectedDatas[i].length);
            VecEncoding vecEncoding = vec.getEncoding();
            if (vecEncoding.equals(OMNI_VEC_ENCODING_DICTIONARY)) {
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
                case OMNI_INT:
                case OMNI_DATE32:
                    assertEquals(((IntVec) vec).get(i), expectedData[i]);
                    break;
                case OMNI_LONG:
                case OMNI_DECIMAL64:
                    assertEquals(((LongVec) vec).get(i), expectedData[i]);
                    break;
                case OMNI_DOUBLE:
                    assertTrue(Double.compare(((DoubleVec) vec).get(i), (Double) expectedData[i]) == 0);
                    break;
                case OMNI_BOOLEAN:
                    assertEquals(((BooleanVec) vec).get(i), expectedData[i]);
                    break;
                case OMNI_VARCHAR:
                case OMNI_CHAR:
                    assertEquals(new String(((VarcharVec) vec).get(i)), expectedData[i]);
                    break;
                default :
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
                case OMNI_DECIMAL128:
                    assertEquals(((Decimal128Vec) vec).get(i),
                            new long[]{(long) expectedData[i][0], (long) expectedData[i][1]});
                    break;
                default :
                    throw new UnsupportedOperationException("Unsupported type : " + vec.getType().getId());
            }
        }
    }

    public static void assertDictionaryVecEquals(DictionaryVec vec, Object[] expectedData) {
        Vec dictionary = vec.getDictionary();
        while (dictionary.getEncoding() == OMNI_VEC_ENCODING_DICTIONARY) {
            dictionary = ((DictionaryVec) dictionary).getDictionary();
        }
        DataType.DataTypeId typeId = dictionary.getType().getId();

        for (int i = 0; i < vec.getSize(); i++) {
            if (vec.isNull(i)) {
                assertEquals(null, expectedData[i]);
                continue;
            }
            switch (typeId) {
                case OMNI_INT:
                case OMNI_DATE32:
                    assertEquals(vec.getInt(i), expectedData[i]);
                    break;
                case OMNI_LONG:
                case OMNI_DECIMAL64:
                    assertEquals(vec.getLong(i), expectedData[i]);
                    break;
                case OMNI_BOOLEAN:
                    assertEquals(vec.getBoolean(i), expectedData[i]);
                    break;
                case OMNI_DOUBLE:
                    assertEquals(Double.compare(vec.getDouble(i), (Double) expectedData[i]), 0);
                    break;
                case OMNI_VARCHAR:
                case OMNI_CHAR:
                    assertEquals(vec.getBytes(i), ((String) (expectedData[i])).getBytes(StandardCharsets.UTF_8));
                    break;
                case OMNI_DECIMAL128:
                    assertEquals(vec.getDecimal128(i),
                            new long[]{(long) expectedData[i * 2], (long) expectedData[i * 2 + 1]});
                    break;
                default :
                    throw new UnsupportedOperationException("Unsupported type : " + typeId);
            }
        }
    }

    public static void freeVecBatch(VecBatch vecBatch) {
        vecBatch.releaseAllVectors();
        vecBatch.close();
    }
}
