package nova.hetu.omnicache.vector;

import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Random;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class MemoryTest
{
    static int count = 1000000;
    static int size = 1024;
    static Random random = new Random();

    @Test
    void testDoubleAllocation()
            throws InterruptedException
    {
        ArrayList<DoubleVec> list = new ArrayList();
        for (int i = 0; i < count; i++) {
            list.add(new DoubleVec(size));
        }

        for (int i = 0; i < count; i++) {
            for (int j = 0; j < size; j++) {
                int v = random.nextInt();
                list.get(j).set(j, v);
            }
        }

        for (DoubleVec vec : list) {
            vec.close();
        }
    }

    @Test
    void testIntAllocation()
            throws InterruptedException
    {
        ArrayList<IntVec> list = new ArrayList();
        for (int i = 0; i < count; i++) {
            list.add(new IntVec(size));
        }

        for (int i = 0; i < count; i++) {
            for (int j = 0; j < size; j++) {
                int v = random.nextInt();
                list.get(j).set(j, v);
            }
        }

        for (IntVec vec : list) {
            vec.close();
        }
    }

    @Test
    void testLongAllocation()
            throws InterruptedException
    {
        ArrayList<LongVec> list = new ArrayList();
        for (int i = 0; i < count; i++) {
            list.add(new LongVec(size));
        }

        for (int i = 0; i < count; i++) {
            for (int j = 0; j < size; j++) {
                int v = random.nextInt();
                list.get(j).set(j, v);
            }
        }

        for (LongVec vec : list) {
            vec.close();
        }
    }

    @Test
    public void testMemMultiFree() {
        LongVec v1 = new LongVec(1024);
        v1.set(0, 1000);
        long address = v1.getAddress();
        int count = 10;
        for (int i = 0;i < count;i++) {
           OMVectorBase.release(address);
        }
    }

    @Test
    void testSetClosable()
    {
        final int vecSize = 1024;
        Vec vec = new LongVec(vecSize);
        assertTrue(vec.close());

        vec = new LongVec(vecSize);
        vec.setClosable(true);
        assertTrue(vec.close());

        vec = new LongVec(vecSize);
        vec.setClosable(false);
        assertFalse(vec.close());
        assertEquals(vec.size(), vecSize);
    }

    @AfterClass
    void tearDown()
            throws InterruptedException
    {
        Thread.sleep(100);
    }
}
