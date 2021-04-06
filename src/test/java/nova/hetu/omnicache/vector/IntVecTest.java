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
        Assert.assertEquals(true, vec1.close());
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
}
