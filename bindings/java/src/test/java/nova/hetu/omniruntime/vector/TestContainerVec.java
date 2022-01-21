package nova.hetu.omniruntime.vector;

import nova.hetu.omniruntime.type.VecType;
import org.testng.annotations.Test;

import static nova.hetu.omniruntime.type.DoubleVecType.DOUBLE;
import static nova.hetu.omniruntime.type.LongVecType.LONG;
import static org.testng.Assert.assertEquals;

public class TestContainerVec
{
    @Test
    public void testSlice()
    {
        int rows = 10;
        DoubleVec field1 = new DoubleVec(rows);
        double[] data1 = new double[] {0, 1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7, 8.8, 9.9};
        field1.put(data1, 0, 0, rows);
        LongVec field2 = new LongVec(rows);
        long[] data2 = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        field2.put(data2, 0, 0, rows);
        ContainerVec originalVec = new ContainerVec(2, rows,
                new long[] {field1.getNativeVector(), field2.getNativeVector()},
                new VecType[] {DOUBLE, LONG});

        int offset = 1;
        ContainerVec sliced = originalVec.slice(offset, 5);
        DoubleVec result1 = new DoubleVec(sliced.get(0));
        LongVec result2 = new LongVec(sliced.get(1));
        for (int i = 0; i < 5; i++) {
            assertEquals(result1.get(i), data1[offset + i]);
            assertEquals(result2.get(i), data2[offset + i]);
        }
        originalVec.close();
        sliced.close();
    }

    @Test
    public void testCopyPositions()
    {
        int rows = 10;
        DoubleVec field1 = new DoubleVec(rows);
        double[] data1 = new double[] {0, 1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7, 8.8, 9.9};
        field1.put(data1, 0, 0, rows);
        LongVec field2 = new LongVec(rows);
        long[] data2 = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        field2.put(data2, 0, 0, rows);
        ContainerVec originalVec = new ContainerVec(2, rows,
                new long[] {field1.getNativeVector(), field2.getNativeVector()},
                new VecType[] {DOUBLE, LONG});

        int[] positions = new int[] {1, 3, 5, 7, 9};
        ContainerVec copyPositionsed = originalVec.copyPositions(positions, 0, 5);
        DoubleVec result1 = new DoubleVec(copyPositionsed.get(0));
        LongVec result2 = new LongVec(copyPositionsed.get(1));
        for (int i = 0; i < 5; i++) {
            assertEquals(result1.get(i), data1[positions[i]]);
            assertEquals(result2.get(i), data2[positions[i]]);
        }
        originalVec.close();
        copyPositionsed.close();
    }

    @Test
    public void testCopyRegion()
    {
        int rows = 10;
        DoubleVec field1 = new DoubleVec(rows);
        double[] data1 = new double[] {0, 1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7, 8.8, 9.9};
        field1.put(data1, 0, 0, rows);
        LongVec field2 = new LongVec(rows);
        long[] data2 = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        field2.put(data2, 0, 0, rows);
        ContainerVec originalVec = new ContainerVec(2, rows,
                new long[] {field1.getNativeVector(), field2.getNativeVector()},
                new VecType[] {DOUBLE, LONG});
        int offset = 1;
        ContainerVec copyRegioned = originalVec.copyRegion(offset, 5);
        DoubleVec result1 = new DoubleVec(copyRegioned.get(0));
        LongVec result2 = new LongVec(copyRegioned.get(1));
        for (int i = 0; i < 5; i++) {
            assertEquals(result1.get(i), data1[offset + i]);
            assertEquals(result2.get(i), data2[offset + i]);
        }
        originalVec.close();
        copyRegioned.close();
    }

    @Test
    public void testAppend()
    {
        int rows = 5;
        DoubleVec field1 = new DoubleVec(rows);
        double[] doubles = new double[] {1.1, 2.2, 3.3, 4.4, 5.5};
        field1.put(doubles,0, 0, rows);
        LongVec field2 = new LongVec(rows);
        long[] longs = new long[] {1, 2, 3, 4, 5};
        field2.put(longs, 0, 0, rows);
        ContainerVec originalVec = new ContainerVec(2, rows,
                new long[] {field1.getNativeVector(), field2.getNativeVector()},
                new VecType[] {DOUBLE, LONG});

        DoubleVec field11 = new DoubleVec(rows);
        double[] doubles1 = new double[] {6.6, 7.7, 8.8, 9.9, 10.1};
        field11.put(doubles1,0, 0, rows);
        LongVec field22 = new LongVec(rows);
        long[] longs1 = new long[] {6, 7, 8, 9, 10};
        field22.put(longs1, 0, 0, rows);
        ContainerVec originalVec1 = new ContainerVec(2, rows,
                new long[] {field11.getNativeVector(), field22.getNativeVector()},
                new VecType[] {DOUBLE, LONG});

        DoubleVec appendedDouble = new DoubleVec(rows * 2);
        LongVec appendedLong = new LongVec(rows * 2);
        ContainerVec appended = new ContainerVec(2, rows * 2,
                new long[] {appendedDouble.getNativeVector(), appendedLong.getNativeVector()},
                new VecType[] {DOUBLE, LONG});

        appended.append(originalVec, 0 , 5);
        appended.append(originalVec1, 5, 5);

        double[] expected1 = new double[] {1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7, 8.8, 9.9, 10.1};
        DoubleVec result1 = new DoubleVec(appended.get(0));
        for (int i = 0; i < result1.getSize(); i++) {
            assertEquals(result1.get(i), expected1[i]);
        }

        long[] expected2 = new long[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        LongVec result2 = new LongVec(appended.get(1));
        for (int i = 0; i < result2.getSize(); i++) {
            assertEquals(result2.get(i), expected2[i]);
        }

        originalVec.close();
        originalVec1.close();
        appended.close();
    }
}
