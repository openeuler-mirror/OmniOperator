package nova.hetu.omniruntime.vector;

import java.math.BigDecimal;
import java.math.BigInteger;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * test decimal 128-bit vec
 */
public class TestDecimal128Vec {
    /**
     * test set and get decimal
     */
    public void testSetAndGetDecimal() {
        final int size = 1024;
        Decimal128Vec vec1 = new Decimal128Vec(size, 10, 3);
        BigDecimal[] values = new BigDecimal[size];
        for (int i = 0; i < size; i++) {
            BigDecimal decimal = new BigDecimal(BigInteger.valueOf(i), 3);
            vec1.set(i, decimal);
            values[i] = decimal;
        }

        for (int i = 0; i < size; i++) {
            assertEquals(values[i], vec1.get(i));
        }
        vec1.close();
    }

    /**
     * test slice
     */
    public void testSlice() {
        final int size = 10;
        Decimal128Vec vec1 = new Decimal128Vec(size, 10, 3);
        for (int i = 0; i < size; i++) {
            BigDecimal decimal = new BigDecimal(BigInteger.valueOf(1 << i + 1), 3);
            vec1.set(i, decimal);
        }
        Decimal128Vec slice1 = vec1.slice(3, 5);
        Decimal128Vec slice2 = vec1.slice(0, vec1.getSize());
        for (int i = 0; i < slice1.getSize(); i++) {
            assertEquals(vec1.get(i + 3), slice1.get(i), "Error item value at: " + i);
        }
        for (int i = 0; i < slice2.getSize(); i++) {
            assertEquals(vec1.get(i), slice2.get(i), "Error item value at: " + i);
        }
        vec1.close();
        slice1.close();
        slice2.close();
    }

    /**
     * set null value
     */
    public void setNullValue() {
        final int size = 10;
        Decimal128Vec vec = new Decimal128Vec(size, 10, 3);
        for (int i = 0; i < size; i++) {
            vec.set(i, new BigDecimal(BigInteger.valueOf(i), 3));
        }

        for (int i = 0; i < size; i++) {
            vec.setNull(i);
        }

        for (int i = 0; i < size; i++) {
            assertTrue(vec.isNull(i));
        }
    }
}
