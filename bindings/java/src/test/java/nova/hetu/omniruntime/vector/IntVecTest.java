package nova.hetu.omniruntime.vector;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class IntVecTest
{
    @Test
    public void testSingle()
            throws Exception
    {
        IntVec vec1 = new IntVec(1024);
        for (int i = 0; i < vec1.getSize(); i++) {
            vec1.set(i, i);
        }

        for (int i = 0; i < vec1.getSize(); i++) {
            assertEquals(i, vec1.get(i));
        }
    }

    @Test
    public void testSlice()
    {
        IntVec vec1 = new IntVec(10);
        for (int i = 0; i < vec1.getSize(); i++) {
            vec1.set(i, i);
        }
        IntVec slice1 = vec1.slice(3, 5);
        for (int i = 0; i < slice1.getSize(); i++) {
            assertEquals(i + 3, slice1.get(i));
        }
        IntVec slice2 = vec1.slice(0, vec1.getSize());
        for (int i = 0; i < slice2.getSize(); i++) {
            assertEquals(i, slice2.get(i));
        }
        vec1.close();
        slice1.close();
        slice2.close();
    }

    @Test
    public void testZeroSizeAllocate()
    {
        IntVec v1 = new IntVec(0);
        int[] values = new int[0];
        v1.getValues().asIntBuffer().put(values, 0, values.length);
        v1.close();
    }
}
