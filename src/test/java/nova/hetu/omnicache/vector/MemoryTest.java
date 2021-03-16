package nova.hetu.omnicache.vector;

import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Random;

public class MemoryTest {
    static int count = 1000000;
    static int size = 1024;
    static Random random = new Random();

    @Test
    void testDoubleAllocation() throws InterruptedException {
        ArrayList<DoubleVec> list = new ArrayList();
        for (int i=0; i<count; i++) {
            list.add(new DoubleVec(size));
        }

        for (int i=0; i<count; i++) {
            for(int j=0;j<size;j++){
                int v = random.nextInt();
                list.get(j).set(j, v);
            }
        }

        for (DoubleVec vec : list){
            vec.close();
        }
    }

    @Test
    void testIntAllocation() throws InterruptedException {
        ArrayList<IntVec> list = new ArrayList();
        for (int i=0; i<count; i++) {
            list.add(new IntVec(size));
        }

        for (int i=0; i<count; i++) {
            for(int j=0;j<size;j++){
                int v = random.nextInt();
                list.get(j).set(j, v);
            }
        }

        for (IntVec vec : list){
            vec.close();
        }
    }

    @Test
    void testLongAllocation() throws InterruptedException {
        ArrayList<LongVec> list = new ArrayList();
        for (int i=0; i<count; i++) {
            list.add(new LongVec(size));
        }

        for (int i=0; i<count; i++) {
            for(int j=0;j<size;j++){
                int v = random.nextInt();
                list.get(j).set(j, v);
            }
        }

        for (LongVec vec : list){
            vec.close();
        }
    }

    @AfterClass
    void tearDown() throws InterruptedException {
        Thread.sleep(100);
    }
}
