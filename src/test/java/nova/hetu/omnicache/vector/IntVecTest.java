package nova.hetu.omnicache.vector;

import org.testng.Assert;
import org.testng.annotations.Test;

public class IntVecTest
{
    @Test
    public void testSingle()
            throws Exception
    {
        IntVec vec1 = new IntVec(1024);
        for (int i = 0; i < vec1.size(); i++) {
            vec1.set(i, i);
        }

        for (int i = 0; i < vec1.size(); i++) {
            Assert.assertEquals(i, vec1.get(i));
        }
        vec1.close();
    }

    @Test
    public void testMul()
    {
        IntVec vec1 = new IntVec(1024);
        for (int i = 0; i < vec1.size(); i++) {
            vec1.set(i, i);
        }

        vec1.mul(10);

        for (int i = 0; i < vec1.size(); i++) {
            Assert.assertEquals(vec1.get(i), i * 10);
        }
    }

    @Test
    public void testSlice()
    {
        IntVec vec1 = new IntVec(10);
        for (int i = 0; i < vec1.size(); i++) {
            vec1.set(i, i);
        }
        IntVec slice1 = vec1.slice(3, 5);
        for (int i = 0; i < slice1.size(); i++) {
            Assert.assertEquals(i + 3, slice1.get(i));
        }
        IntVec slice2 = vec1.slice(0, vec1.size());
        for (int i = 0; i < slice2.size(); i++) {
            Assert.assertEquals(i, slice2.get(i));
        }
    }

    @Test
    public void testConcat()
    {
        IntVec vec1 = new IntVec(10);
        IntVec vec2 = new IntVec(10);
        for (int i = 0; i < vec1.size; i++) {
            Assert.assertEquals(vec1.get(i), 0, "invalid initialize value at item: " + i);
            vec1.set(i, i);
            vec2.set(i, i * 2);
        }
        IntVec newVec = (IntVec) vec1.concat(vec2);
        Assert.assertEquals(newVec.size(), 20L, "Error length after concat");

        int[] comp = new int[20];
        for (int i = 0; i < 20; ++i) {
            comp[i] = i;
        }
        for (int i = 0; i < 10; ++i) {
            comp[i + 10] = i * 2;
        }
        for (int i = 0; i < newVec.size; i++) {
            Assert.assertEquals(newVec.get(i), comp[i], "Error item value at: " + i);
        }
    }
}
