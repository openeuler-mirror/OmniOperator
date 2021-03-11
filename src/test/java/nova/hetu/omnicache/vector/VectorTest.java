package nova.hetu.omnicache.vector;

import org.testng.annotations.Test;

public class VectorTest
{
    @Test
    public void testClose()
    {
        int rowSize = 1000;
        int allocSize = rowSize * 10;;
        for (int j = 0; j < 10; j++) {
            for (int i = 0; i < 10000; i++) {
                Vec vec = new TestVec(rowSize, allocSize);
                vec.close();
            }
            System.gc();
            System.out.println("finish round: " + j);
        }
    }
}