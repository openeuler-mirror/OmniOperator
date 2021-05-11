package nova.hetu.omniruntime.vector;

import org.testng.Assert;
import org.testng.annotations.Test;

public class LongVecTest
{
    @Test
    public void testSingle()
            throws Exception
    {
        LongVec vec1 = new LongVec(1024);
        Assert.assertEquals(vec1.size(), 1024, "Size is expected to be 1024");
        for (int i = 0; i < vec1.size(); i++) {
            vec1.set(i, (long) i);
        }

        for (int i = 0; i < vec1.size(); i++) {
            Assert.assertEquals(i, vec1.get(i));
        }

        Assert.assertEquals(true, vec1.close());

        LongVec vec2 = new LongVec(1024);
        Assert.assertEquals(vec2.size(), 1024, "Size is expected to be 1024");
        for (int i = 0; i < vec2.size(); i++) {
            vec2.set(i, (long) i * 2);
        }

        for (int i = 0; i < vec2.size(); i++) {
            Assert.assertEquals(i * 2, vec2.get(i));
        }

        Assert.assertEquals(true, vec2.close());
    }

    @Test
    public void testSlice()
    {
        LongVec vec1 = new LongVec(10);
        for (int i = 0; i < vec1.size(); i++) {
            vec1.set(i, (long) i / 2);
        }
        LongVec slice1 = vec1.slice(3, 5);
        LongVec slice2 = vec1.slice(0, vec1.size());
        for (int i = 0; i < slice1.size(); i++) {
            Assert.assertEquals(vec1.get(i + 3), slice1.get(i), "Error item value at: " + i);
        }
        for (int i = 0; i < slice2.size(); i++) {
            Assert.assertEquals(vec1.get(i), slice2.get(i), "Error item value at: " + i);
        }
    }

    @Test
    public void testFree()
            throws Exception
    {
        for (int j = 0; j < 10; j++) {
            for (int i = 0; i < 1000; i++) {
                //ByteBuffer.allocateDirect(8192);
                Assert.assertEquals(true, new LongVec(1024).close());
            }
            System.gc();
            System.out.println("finish round: " + j);
        }
    }

    //    @Test
    //TODO open when move to C++
    public void testCopy()
    {
        int rowNum = 3000;
        int[] selectedPositions = new int[rowNum/2];
        for (int i = 0; i < rowNum/2; i++) {
            selectedPositions[i] = 2 * i;
        }

        LongVec originalVec = new LongVec(rowNum);
        for (int i = 0; i < rowNum; i++) {
            originalVec.set(i, i);
        }

        for (int i = 0; i < 10; i++) {
            long start = System.nanoTime();
            LongVec newVec = new LongVec(rowNum / 2);

            originalVec.copy(newVec, selectedPositions, 0, rowNum / 2, 0);

            long total = System.nanoTime() - start;
            System.out.println("total time: " + total);
        }
    }

    @Test
    public void testZeroSizeAllocate() {
        LongVec v1 = new LongVec(0);
        long[] values = new long[0];
        v1.getData().asLongBuffer().put(values, 0 , values.length);
        v1.close();
    }
}
