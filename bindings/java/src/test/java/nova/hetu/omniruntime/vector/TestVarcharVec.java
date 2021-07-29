package nova.hetu.omniruntime.vector;

import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static nova.hetu.omniruntime.constants.VecType.OMNI_VEC_TYPE_VARCHAR;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * test varchar vec
 */
public class TestVarcharVec {
    /**
     * teardown
     */
    @AfterClass
    public void tearDown() {
        VecAllocator.GLOBAL_VECTOR_ALLOCATOR.close();
    }

    /**
     * test new vector
     */
    @Test
    public void testNewVector() {
        VarcharVec vec = new VarcharVec(1024, 256);
        assertEquals(vec.getSize(), 256);
        assertEquals(vec.getOffset(), 0);
        assertEquals(vec.getCapacityInBytes(), 1024);
        assertEquals(vec.getType(), OMNI_VEC_TYPE_VARCHAR);
        vec.close();
    }

    /**
     * test slice
     */
    @Test
    public void testSlice() {
        int size = 10;
        VarcharVec originalVec = new VarcharVec(1024, size);
        String tmpStr = "testvarchar";
        for (int i = 0; i < size; i++) {
            String str = tmpStr.substring(0, i) + i;
            originalVec.setValue(i, str.getBytes(StandardCharsets.UTF_8));
        }

        int offset = 3;
        VarcharVec sliceVec1 = originalVec.slice(offset, 7);
        assertEquals(sliceVec1.getOffset(), offset);
        assertEquals(sliceVec1.getSize(), 4);

        for (int i = 0; i < sliceVec1.getSize(); i++) {
            byte[] actualValue = sliceVec1.getValue(i);
            byte[] expectedValue = originalVec.getValue(i + offset);
            assertEquals(actualValue, expectedValue);
        }

        VarcharVec sliceVec2 = sliceVec1.slice(1, 3);
        assertEquals(sliceVec2.getOffset(), offset + 1);
        assertEquals(sliceVec2.getSize(), 2);

        for (int i = 0; i < sliceVec2.getSize(); i++) {
            byte[] actualValue = sliceVec2.getValue(i);
            byte[] expectedValue = originalVec.getValue(i + offset + 1);
            assertEquals(actualValue, expectedValue);
        }
        sliceVec2.close();
        sliceVec1.close();
        originalVec.close();
    }

    /**
     * test set and get value
     */
    @Test
    public void testSetAndGetValue() {
        int size = 4;
        VarcharVec varcharVec = new VarcharVec(1024, size);
        String tmpStr = "test";
        for (int i = 0; i < 4; i++) {
            String str = tmpStr.substring(0, i) + i;
            varcharVec.setValue(i, str.getBytes(StandardCharsets.UTF_8));
        }

        for (int i = 0; i < 4; i++) {
            String str = tmpStr.substring(0, i) + i;
            byte[] actualValue = varcharVec.getValue(i);
            assertEquals(actualValue, str.getBytes(StandardCharsets.UTF_8));
        }

        varcharVec.close();
    }

    @Test
    public void testPutValues()
    {
        int size = 100;
        int[] offsets = new int[size + 1];
        StringBuilder data = new StringBuilder();
        for (int i = 0; i < size; i++) {
            String str = "test" + i;
            offsets[i + 1] = str.length() + offsets[i];
            data.append(str);
        }
        VarcharVec values = new VarcharVec(data.toString().length(), size);
        values.put(0, data.toString().getBytes(StandardCharsets.UTF_8), 0, offsets, 0, size);
        ByteBuffer buffer = ByteBuffer.wrap(data.toString().getBytes(StandardCharsets.UTF_8));
        for (int i = 0; i < size; i++) {
            assertEquals(values.getValue(i), getDataFromBuffer(buffer, offsets[i], offsets[i + 1] - offsets[i]));
        }

        assertEquals(offsets, values.getRawValueOffset());
        values.close();
    }

    private byte[] getDataFromBuffer(ByteBuffer buffer, int offsetInBytes, int length)
    {
        byte[] data = new byte[length];
        buffer.position(offsetInBytes);
        buffer.get(data, 0, length);
        return data;
    }

    /**
     * test value null
     */
    @Test
    public void testValueNull() {
        VarcharVec varcharVec = new VarcharVec(1024, 256);
        for (int i = 0; i < varcharVec.getSize(); i++) {
            if (i % 5 == 0) {
                varcharVec.setNull(i);
            }
            else {
                varcharVec.setValue(i, "test".getBytes(StandardCharsets.UTF_8));
            }
        }
        for (int i = 0; i < varcharVec.getSize(); i++) {
            if (i % 5 == 0) {
                assertTrue(varcharVec.isNull(i));
            }
            else {
                assertFalse(varcharVec.isNull(i));
            }
        }

        varcharVec.close();
    }

    @Test
    public void testBatchSetValueNull()
    {
        int size = 256;
        boolean[] isNulls = new boolean[size];
        for (int i = 0; i < size; i++) {
            isNulls[i] = i % 2 == 0;
        }
        VarcharVec varcharVec = new VarcharVec(1024, size);
        varcharVec.setNulls(0, isNulls, 0, isNulls.length);
        assertTrue(varcharVec.hasNullValue());
        assertEquals(isNulls, varcharVec.getRawValueNulls());
        varcharVec.close();
    }

    /**
     * test copy postion
     */
    @Test
    public void testCopyPositions() {
        VarcharVec originalVector = new VarcharVec(1024, 4);
        String tmpStr = "test";
        for (int i = 0; i < 4; i++) {
            String str = tmpStr.substring(0, i) + i;
            originalVector.setValue(i, str.getBytes(StandardCharsets.UTF_8));
        }

        int[] positions = {1, 3};
        VarcharVec copyPostionVector = originalVector.copyPositions(positions, 0, 2);

        for (int i = 0; i < copyPostionVector.getSize(); i++) {
            byte[] expectedValue = originalVector.getValue(positions[i]);
            byte[] actualValue = copyPostionVector.getValue(i);
            assertEquals(actualValue, expectedValue);
        }

        originalVector.close();
        copyPostionVector.close();
    }

    /**
     * test copy region
     */
    @Test
    public void testCopyRegion() {
        VarcharVec originalVector = new VarcharVec(1024, 4);
        String tmpStr = "test";
        for (int i = 0; i < 4; i++) {
            String str = tmpStr.substring(0, i) + i;
            originalVector.setValue(i, str.getBytes(StandardCharsets.UTF_8));
        }

        VarcharVec copyRegionVector = originalVector.copyRegion(2, 2);

        for (int i = 0; i < copyRegionVector.getSize(); i++) {
            byte[] expectedValue = originalVector.getValue(i + 2);
            byte[] actualValue = copyRegionVector.getValue(i);
            assertEquals(actualValue, expectedValue);
        }

        originalVector.close();
        copyRegionVector.close();
    }
}
