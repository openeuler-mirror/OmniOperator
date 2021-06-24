package nova.hetu.omniruntime.operator;

import com.google.common.collect.ImmutableList;
import nova.hetu.omniruntime.constants.VecType;
import nova.hetu.omniruntime.operator.sort.OmniSortOperatorFactory;
import nova.hetu.omniruntime.vector.IntVec;
import nova.hetu.omniruntime.vector.LongVec;
import nova.hetu.omniruntime.vector.Vec;
import nova.hetu.omniruntime.vector.VecBatch;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static nova.hetu.omniruntime.constants.VecType.OMNI_VEC_TYPE_INT;
import static nova.hetu.omniruntime.constants.VecType.OMNI_VEC_TYPE_LONG;
import static org.testng.Assert.assertEquals;

public class OmniSortOperatorTest
{
    int totalPageCount = 20;
    int pageDistinctCount = 4;
    int pageDistinctValueRepeatCount = 25000;

    @Test
    public void testOrderByTwoColumn()
    {
        int[] data1 = {5, 3, 2, 6, 1, 4, 7, 8};
        int[] data2 = {5, 3, 2, 6, 1, 4, 7, 8};
        IntVec vec1 = new IntVec(8);
        IntVec vec2 = new IntVec(8);
        for (int i = 0; i < 8; i++) {
            vec1.set(i, data1[i]);
            vec2.set(i, data2[i]);
        }

        VecBatch vecBatch = new VecBatch(new Vec[] {vec1, vec2});

        VecType[] sourceTypes = {OMNI_VEC_TYPE_INT, OMNI_VEC_TYPE_INT};
        int[] outputCols = {0, 1};
        int[] sortCols = {0, 1};
        int[] ascendings = {1, 1};
        int[] nullFirsts = {0, 0};

        OmniSortOperatorFactory sortOperatorFactory = new OmniSortOperatorFactory(
                sourceTypes, outputCols, sortCols, ascendings, nullFirsts);
        OmniOperator sortOperator = sortOperatorFactory.createOperator();
        sortOperator.addInput(vecBatch);
        Iterator<VecBatch> results = sortOperator.getOutput();

        results.hasNext();
        VecBatch resultVecBatch = results.next();
        ByteBuffer output0 = resultVecBatch.getVectors()[0].getValues();
        ByteBuffer output1 = resultVecBatch.getVectors()[1].getValues();
        int len = resultVecBatch.getRowCount();

        int[] actual0 = new int[len];
        int[] actual1 = new int[len];
        output0.order(ByteOrder.LITTLE_ENDIAN);
        output1.order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < len; i++) {
            actual0[i] = output0.getInt(i * Integer.BYTES);
            actual1[i] = output1.getInt(i * Integer.BYTES);
        }
        int[] expected0 = {1, 2, 3, 4, 5, 6, 7, 8};
        int[] expected1 = {1, 2, 3, 4, 5, 6, 7, 8};
        assertEquals(actual0, expected0);
        assertEquals(actual1, expected1);
    }

    @Test
    public void testOrderByPerformance()
    {
        long start = System.currentTimeMillis();
        ImmutableList<VecBatch> vecs = buildVecs();
        long elapsed = System.currentTimeMillis() - start;
        System.out.println("buildVecs elapsed time : " + elapsed + " ms");

        VecType[] sourceTypes = {OMNI_VEC_TYPE_INT, OMNI_VEC_TYPE_INT};
        int[] outputCols = {0, 1};
        int[] sortCols = {0, 1};
        int[] ascendings = {1, 1};
        int[] nullFirsts = {0, 0};

        OmniSortOperatorFactory sortOperatorFactory = new OmniSortOperatorFactory(
                sourceTypes, outputCols, sortCols, ascendings, nullFirsts);

        start = System.currentTimeMillis();
        OmniOperator sortOperator = sortOperatorFactory.createOperator();
        for (VecBatch vec : vecs) {
            sortOperator.addInput(vec);
        }
        sortOperator.getOutput();
        elapsed = System.currentTimeMillis() - start;
        System.out.println("getResult elapsed time : " + elapsed + " ms");
    }

    @Test
    public void testOrderByMultiPerformance()
    {
        ImmutableList<VecBatch> vecs = buildVecs();

        VecType[] sourceTypes = {OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_LONG};
        int[] outputCols = {0, 1};
        int[] sortCols = {0, 1};
        int[] ascendings = {1, 1};
        int[] nullFirsts = {0, 0};
        OmniSortOperatorFactory sortOperatorFactory = new OmniSortOperatorFactory(
                sourceTypes, outputCols, sortCols, ascendings, nullFirsts);

        int threadNum = 4;
        CountDownLatch countDownLatch = new CountDownLatch(threadNum);
        for (int i = 0; i < threadNum; i++) {
            Thread thread = new Thread(() -> {
                try {
                    OmniOperator sortOperator = sortOperatorFactory.createOperator();
                    for (VecBatch vec : vecs) {
                        sortOperator.addInput(vec);
                    }
                    sortOperator.getOutput();
                }
                finally {
                    countDownLatch.countDown();
                }
            });
            thread.start();
        }
        try {
            countDownLatch.await();
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private ImmutableList<VecBatch> buildVecs()
    {
        ImmutableList.Builder<VecBatch> vecBatchList = ImmutableList.builder();
        int positionCount = pageDistinctCount * pageDistinctValueRepeatCount;
        List<Vec> vecs = new ArrayList<>();
        for (int i = 0; i < totalPageCount; i++) {
            LongVec longVec1 = new LongVec(positionCount);
            LongVec longVec2 = new LongVec(positionCount);
            int idx = 0;
            for (int j = 0; j < pageDistinctCount; j++) {
                for (int k = 0; k < pageDistinctValueRepeatCount; k++) {
                    longVec1.set(idx, j);
                    longVec2.set(idx, j);
                    idx++;
                }
            }
            vecs.add(longVec1);
            vecs.add(longVec2);
            VecBatch vecBatch = new VecBatch(new Vec[] {longVec1, longVec2});
            vecBatchList.add(vecBatch);
        }
        return vecBatchList.build();
    }
}
