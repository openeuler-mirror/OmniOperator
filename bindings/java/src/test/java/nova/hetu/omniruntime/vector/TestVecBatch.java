package nova.hetu.omniruntime.vector;

import org.testng.annotations.Test;

/**
 * test vec batch
 */
public class TestVecBatch {
    /**
     * test new vec batch
     */
    @Test
    public void testNewVecBatch() {
        int vecCount = 10;
        int rowCount = 1024;
        Vec[] vecArray = new Vec[vecCount];
        for (int i = 0; i < vecCount; i++) {
            vecArray[i] = new LongVec(rowCount);
        }
        VecBatch vecBatch = new VecBatch(vecArray);
        vecBatch.close();
    }
}
