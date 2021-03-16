package nova.hetu.omnicache.vector;

import org.testng.Assert;
import org.testng.annotations.Test;

public class DoubleVecTest {
    @Test
    public void testSingle()
            throws Exception
    {
        DoubleVec vec1 = new DoubleVec(1024);
        for (int i = 0; i < vec1.size(); i++) {
            vec1.set(i, (double) i/3);
        }

        for (int i = 0; i < vec1.size(); i++) {
            Assert.assertEquals((double) i/3, vec1.get(i));
        }
        vec1.close();
    }

    @Test
    public void testMul()
    {
        DoubleVec vec1 = new DoubleVec(1024);
        for (int i = 0; i < vec1.size(); i++) {
            vec1.set(i, (double) i / 10);
            Assert.assertEquals(vec1.get(i), i / 10.0);
        }

        vec1.mul(10);

        for (int i = 0; i < vec1.size(); i++) {
            Assert.assertEquals(vec1.get(i), (double) i);
        }
    }

    @Test
    public void testSlice()
    {
        DoubleVec vec1 = new DoubleVec(10);
        for (int i = 0; i < vec1.size(); i++) {
            vec1.set(i, (double) i / 2);
        }
        DoubleVec slice1 = vec1.slice(3, 5);
        for (int i = 0; i < slice1.size(); i++) {
            Assert.assertEquals(vec1.get(i+3), slice1.get(i));
        }

        DoubleVec slice2 = vec1.slice(0, vec1.size());
        for (int i = 0; i < slice2.size(); i++) {
            Assert.assertEquals(vec1.get(i), slice2.get(i));
        }
    }

    @Test
    public void testConcat()
    {
        DoubleVec vec1 = new DoubleVec(10);
        DoubleVec vec2 = new DoubleVec(10);
        for (int i = 0; i < vec1.size; i++) {
            Assert.assertEquals(vec1.get(i), (double) 0, "invalid initialize value at item: " + i);
            vec1.set(i, (double) i / 3);
            vec2.set(i, (double) i * 2 / 3);
        }
        DoubleVec newVec = (DoubleVec) vec1.concat(vec2);
        Assert.assertEquals(newVec.size(), 20L, "Error length after concat");

        double[] comp = new double[20];
        for (int i = 0; i < 20; ++i) {
            comp[i] = (double) i / 3;
        }
        for (int i = 0; i < 10; ++i) {
            comp[i + 10] = (double) i * 2 / 3;
        }
        for (int i = 0; i < newVec.size; i++) {
            Assert.assertEquals(newVec.get(i), comp[i], "Error item value at: " + i);
        }
    }
}