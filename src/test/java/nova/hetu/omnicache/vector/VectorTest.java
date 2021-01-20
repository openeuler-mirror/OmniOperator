package nova.hetu.omnicache.vector;

import org.testng.Assert;
import org.testng.annotations.Test;

public class VectorTest
{

    @Test
    public void testIntMul()
    {
        IntVec vec1 = new IntVec(1024);
        for (int i=0; i<vec1.size(); i++) {
            vec1.set(i, i);
        }

        vec1.mul(10);

        for (int i=0; i<vec1.size(); i++) {
            Assert.assertEquals(vec1.get(i).intValue(), i * 10);
        }
    }

    @Test
    public void testDoubleMul()
    {
        DoubleVec vec1 = new DoubleVec(1024);
        for (int i=0; i<vec1.size(); i++) {
            vec1.set(i, (double)i/10);
            Assert.assertEquals(vec1.get(i), i/10.0);
        }

        vec1.mul(10.0);

        for (int i=0; i<vec1.size(); i++) {
            Assert.assertEquals(vec1.get(i).doubleValue(), (double)i);
        }
    }

    @Test
    public void testLongMul()
            throws Exception
    {
        LongVec vec1 = new LongVec(1024);
        LongVec vec2 = new LongVec(1024);

        for (int i=0; i<vec1.size; i++) {
            Assert.assertEquals(vec1.get(i).longValue(), 0, "invalid initialize value at item: " + i);
            vec1.set(i, (long)i);
            vec2.set(i, (long)i * 2);
        }

        vec1.mul((long)10);
        vec2.mul((long)20);

        for (int i=0; i<10; i++) {
            Assert.assertEquals(vec1.get(i).longValue(), i * 10);
            Assert.assertEquals(vec2.get(i).longValue(), i * 2 * 20);
        }

        vec1.close();
        vec2.close();
    }

    @Test
    public void testLongMul_BigVector()
            throws Exception
    {
        LongVec vec1 = new LongVec(1024 * 1024 * 128);
        LongVec vec2 = new LongVec(1024 * 1024 * 128);

        for (int i=0; i<vec1.size; i++) {
            Assert.assertEquals(vec1.get(i).longValue(), 0, "invalid initialize value at item: " + i);
            vec1.set(i, (long)i);
            vec2.set(i, (long)i * 2);
        }

        vec1.mul((long)10);
        vec2.mul((long)20);

        for (int i=0; i<10; i++) {
            Assert.assertEquals(vec1.get(i).longValue(), i * 10);
            Assert.assertEquals(vec2.get(i).longValue(), i * 2 * 20);
        }

        vec1.close();
        vec2.close();
    }

    @Test
    public void testSingleInt()
            throws Exception
    {
        IntVec vec1 = new IntVec(1024);
        for (int i=0; i<vec1.size(); i++) {
            vec1.set(i, i);
        }

        for (int i=0; i<vec1.size(); i++) {
            Assert.assertEquals(i, vec1.get(i).intValue());
        }
    }

    @Test
    public void testSingleLong()
            throws Exception
    {
        LongVec vec1 = new LongVec(1024);
        Assert.assertEquals(vec1.size(), 1024, "Size is expected to be 1024");
        for (int i=0; i<vec1.size(); i++) {
            vec1.set(i, (long)i);
        }

        for (int i=0; i<vec1.size(); i++) {
            Assert.assertEquals(i, vec1.get(i).longValue());
        }

        vec1.close();

        LongVec vec2 = new LongVec(1024);
        Assert.assertEquals(vec2.size(), 1024, "Size is expected to be 1024");
        for (int i=0; i<vec2.size(); i++) {
            vec2.set(i, (long)i * 2);
        }

        for (int i=0; i<vec2.size(); i++) {
            Assert.assertEquals(i * 2, vec2.get(i).longValue());
        }

        vec2.close();
    }

    @Test
    public void testFree()
            throws Exception
    {
        for (int j=0; j<10; j++) {
            for (int i = 0; i < 1000000; i++) {
                //ByteBuffer.allocateDirect(8192);
                new LongVec(1024).close();
            }
            System.gc();
            System.out.println("finish round: " + j);
        }
    }

}