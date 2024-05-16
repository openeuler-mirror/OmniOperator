package nova.hetu.omniruntime.vector;

import org.testng.annotations.Test;

import static nova.hetu.omniruntime.vector.RowBatch.transFromVectorBatch;

public class TestOmniRow {
    @Test
    public void testNewRowBatch() {
        int vecCount = 10;
        int rowCount = 1024;
        Vec[] vecArray = new Vec[vecCount];
        for (int i = 0; i < vecCount; i++) {
            vecArray[i] = new LongVec(rowCount);
        }
        VecBatch vecBatch = new VecBatch(vecArray, rowCount);

        RowBatch rowBatch = new RowBatch(vecBatch);

        rowBatch.close();

        vecBatch.releaseAllVectors();
        vecBatch.close();
    }
}
