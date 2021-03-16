package nova.hetu.omnicache.vector;

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

        vec1.close();

        LongVec vec2 = new LongVec(1024);
        Assert.assertEquals(vec2.size(), 1024, "Size is expected to be 1024");
        for (int i = 0; i < vec2.size(); i++) {
            vec2.set(i, (long) i * 2);
        }

        for (int i = 0; i < vec2.size(); i++) {
            Assert.assertEquals(i * 2, vec2.get(i));
        }

        vec2.close();
    }

    @Test
    public void testMul()
            throws Exception
    {
        LongVec vec1 = new LongVec(1024);
        LongVec vec2 = new LongVec(1024);

        for (int i = 0; i < vec1.size; i++) {
            Assert.assertEquals(vec1.get(i), 0, "invalid initialize value at item: " + i);
            vec1.set(i, (long) i);
            vec2.set(i, (long) i * 2);
        }

        vec1.mul(10);
        vec2.mul(20);

        for (int i = 0; i < 10; i++) {
            Assert.assertEquals(vec1.get(i), i * 10);
            Assert.assertEquals(vec2.get(i), i * 2 * 20);
        }

        vec1.close();
        vec2.close();
    }

    @Test
    public void testMul_BigVector()
            throws Exception
    {
        LongVec vec1 = new LongVec(1024 * 1024 * 128);
        LongVec vec2 = new LongVec(1024 * 1024 * 128);

        for (int i = 0; i < vec1.size; i++) {
            Assert.assertEquals(vec1.get(i), 0, "invalid initialize value at item: " + i);
            vec1.set(i, (long) i);
            vec2.set(i, (long) i * 2);
        }

        vec1.mul(10);
        vec2.mul(20);

        for (int i = 0; i < 10; i++) {
            Assert.assertEquals(vec1.get(i), i * 10);
            Assert.assertEquals(vec2.get(i), i * 2 * 20);
        }

        vec1.close();
        vec2.close();
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
    public void testConcat()
            throws Exception
    {
        LongVec vec1 = new LongVec(1024);
        LongVec vec2 = new LongVec(1024);
        for (int i = 0; i < vec1.size; i++) {
            Assert.assertEquals(vec1.get(i), 0, "invalid initialize value at item: " + i);
            vec1.set(i, (long) i);
            vec2.set(i, (long) i * 2);
        }
        LongVec newVec = (LongVec) vec1.concat(vec2);
        Assert.assertEquals(newVec.size(), 2048L, "Error length after concat");

        long[] comp = new long[2048];
        for (int i = 0; i < 1024; ++i) {
            comp[i] = i;
        }
        for (int i = 0; i < 1024; ++i) {
            comp[i + 1024] = i * 2;
        }
        for (int i = 0; i < newVec.size; i++) {
            Assert.assertEquals(newVec.get(i), comp[i], "Error item value at: " + i);
        }
    }

    @Test
    public void testFree()
            throws Exception
    {
        for (int j = 0; j < 10; j++) {
            for (int i = 0; i < 1000; i++) {
                //ByteBuffer.allocateDirect(8192);
                new LongVec(1024).close();
            }
            System.gc();
            System.out.println("finish round: " + j);
        }
    }
}
