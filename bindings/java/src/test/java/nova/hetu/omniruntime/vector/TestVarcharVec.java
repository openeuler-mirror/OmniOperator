package nova.hetu.omniruntime.vector;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static nova.hetu.omniruntime.type.VecType.VecTypeId.OMNI_VEC_TYPE_VARCHAR;
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
        assertEquals(vec.getType().getId(), OMNI_VEC_TYPE_VARCHAR);
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
            originalVec.set(i, str.getBytes(StandardCharsets.UTF_8));
        }

        int offset = 3;
        VarcharVec sliceVec1 = originalVec.slice(offset, 7);
        assertEquals(sliceVec1.getOffset(), offset);
        assertEquals(sliceVec1.getSize(), 4);

        for (int i = 0; i < sliceVec1.getSize(); i++) {
            byte[] actualValue = sliceVec1.get(i);
            byte[] expectedValue = originalVec.get(i + offset);
            assertEquals(actualValue, expectedValue);
        }

        VarcharVec sliceVec2 = sliceVec1.slice(1, 3);
        assertEquals(sliceVec2.getOffset(), offset + 1);
        assertEquals(sliceVec2.getSize(), 2);

        for (int i = 0; i < sliceVec2.getSize(); i++) {
            byte[] actualValue = sliceVec2.get(i);
            byte[] expectedValue = originalVec.get(i + offset + 1);
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
            varcharVec.set(i, str.getBytes(StandardCharsets.UTF_8));
        }

        for (int i = 0; i < 4; i++) {
            String str = tmpStr.substring(0, i) + i;
            byte[] actualValue = varcharVec.get(i);
            assertEquals(actualValue, str.getBytes(StandardCharsets.UTF_8));
        }

        varcharVec.close();
    }

    @Test
    public void testPutValues() {
        int size = 100;
        int[] offsets = new int[size * 2 + 1];
        StringBuilder data = new StringBuilder();
        for (int i = 0; i < size; i++) {
            String str = "test" + i;
            offsets[i + 1] = str.length() + offsets[i];
            data.append(str);
        }

        for (int i = 0; i < size; i++) {
            String str = i + "put";
            offsets[size + i + 1] = str.length() + offsets[size + i];
            data.append(str);
        }

        VarcharVec values = new VarcharVec(data.toString().length(), size * 2);
        values.put(0, data.toString().getBytes(StandardCharsets.UTF_8), 0, offsets, 0, size);
        values.put(size, data.toString().getBytes(StandardCharsets.UTF_8),  0, offsets, size, size);
        ByteBuffer buffer = ByteBuffer.wrap(data.toString().getBytes(StandardCharsets.UTF_8));
        for (int i = 0; i < size * 2; i++) {
            assertEquals(new String(values.get(i)), new String(getDataFromBuffer(buffer, offsets[i], offsets[i + 1] - offsets[i])));
        }

        assertEquals(offsets, values.getRawValueOffset());
        values.close();
    }

    private byte[] getDataFromBuffer(ByteBuffer buffer, int offsetInBytes, int length) {
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
                varcharVec.set(i, ("test" + i).getBytes(StandardCharsets.UTF_8));
            }
        }
        for (int i = 0; i < varcharVec.getSize(); i++) {
            if (i % 5 == 0) {
                assertTrue(varcharVec.isNull(i));
            }
            else {
                assertEquals("test" + i, new String (varcharVec.get(i), StandardCharsets.UTF_8));
            }
        }

        varcharVec.close();
    }

    @Test
    public void testBatchSetValueNull() {
        int size = 256;
        boolean[] isNulls = new boolean[size];
        for (int i = 0; i < size; i++) {
            isNulls[i] = i % 2 == 0;
        }
        VarcharVec varcharVec = new VarcharVec(1024, size);
        varcharVec.setNulls(0, isNulls, 0, isNulls.length);
        assertTrue(varcharVec.hasNullValue());
        byte[] result = varcharVec.getRawValueNulls();
        assertEquals(isNulls, varcharVec.transformByteToBoolean(result, 0, result.length));
        assertEquals(varcharVec.getValuesNulls(0, size) ,isNulls);
        int offset = 3;
        boolean[] acutal = varcharVec.getValuesNulls(offset, size / 2);
        for (int i = 0; i < size / 2; i++) {
            assertEquals(acutal[i], isNulls[i + offset]);
        }
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
            originalVector.set(i, str.getBytes(StandardCharsets.UTF_8));
        }

        int[] positions = {1, 3};
        VarcharVec copyPostionVector = originalVector.copyPositions(positions, 0, 2);

        for (int i = 0; i < copyPostionVector.getSize(); i++) {
            byte[] expectedValue = originalVector.get(positions[i]);
            byte[] actualValue = copyPostionVector.get(i);
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
            originalVector.set(i, str.getBytes(StandardCharsets.UTF_8));
        }

        VarcharVec copyRegionVector = originalVector.copyRegion(2, 2);

        for (int i = 0; i < copyRegionVector.getSize(); i++) {
            byte[] expectedValue = originalVector.get(i + 2);
            byte[] actualValue = copyRegionVector.get(i);
            assertEquals(actualValue, expectedValue);
        }

        originalVector.close();
        copyRegionVector.close();
    }

    @Test
    public void testGetValues() {
        int size = 10;
        StringBuilder getData = new StringBuilder();
        VarcharVec getVec = new VarcharVec(1024 * 1024, size);
        for (int i = 0; i < size; i++) {
            String str = "gets" + i;
            getVec.set(i, str.getBytes(StandardCharsets.UTF_8));
            getData.append(str);
        }

        byte[] actual = getVec.get(0, size / 2);
        ByteBuffer buffer = ByteBuffer.wrap(getData.toString().getBytes(StandardCharsets.UTF_8));
        int total = 0;
        for (int i = 0; i < size / 2; i++) {
            total += getVec.getDataLength(i);
        }
        byte[] expected = getDataFromBuffer(buffer, getVec.getValueOffset(0), total);
        assertEquals(getString(actual), getString(expected));

        int getLen = 5;
        int offset = 2;
        byte[] acutal1 = getVec.get(offset, getLen);
        ByteBuffer buffer1 = ByteBuffer.wrap(acutal1);
        int[] offsets = getVec.getValueOffset(offset, getLen);
        for (int i = 0; i < getLen; i++) {
            assertEquals(getString(getDataFromBuffer(buffer1, offsets[i], offsets[i + 1] - offsets[i])), getString(getVec.get(i + offset)));
        }
        getVec.close();
    }

    @Test
    public void testEmptyString() {
        String[] data = new String[] {"a", "ef", "", "ef", "", ""};
        int[] offsets = new int[] {0, 1, 3, 3, 5, 5, 5};
        String[] expected = new String[] {"a", "ef", "", "ef", "", ""};
        int size = 6;
        VarcharVec varcharVec = new VarcharVec(1024, size);
        for (int i = 0; i < size; i++) {
            varcharVec.set(i, data[i].getBytes(StandardCharsets.UTF_8));
        }

        String[] result = new String[size];
        for (int i = 0; i < size; i++) {
            result[i] = getString(varcharVec.get(i));
        }

        Assert.assertEquals(result, expected);

        VarcharVec vec2 = new VarcharVec(1024, size);
        StringBuilder sb = new StringBuilder();
        for (String str : data) {
            sb.append(str);
        }
        vec2.put(0, sb.toString().getBytes(StandardCharsets.UTF_8), 0, offsets, 0, size);

        String[] result1 = new String[size];
        for (int i = 0; i < size; i++) {
            result1[i] = getString(vec2.get(i));
        }

        Assert.assertEquals(result1, expected);

        // slice
        VarcharVec sliceEmpty = varcharVec.slice(2, 3);
        String emptyString = "";
        Assert.assertEquals(getString(sliceEmpty.get(0)), emptyString);

        // getRegion
        VarcharVec copyRegionEmpty = varcharVec.copyRegion(2, 1);
        Assert.assertEquals(getString(copyRegionEmpty.get(0)), emptyString);

        // copyPosition
        int[] positions = new int[] {2, 4, 5};
        VarcharVec copyPosition = varcharVec.copyPositions(positions, 0, 3);
        for (int i = 0; i< copyPosition.size; i++) {
            Assert.assertEquals(getString(copyPosition.get(i)), emptyString);
        }

        varcharVec.close();
        sliceEmpty.close();
        copyRegionEmpty.close();
        copyPosition.close();
    }

    private String getString(byte[] strInBytes)
    {
        return new String(strInBytes, StandardCharsets.UTF_8);
    }
}
