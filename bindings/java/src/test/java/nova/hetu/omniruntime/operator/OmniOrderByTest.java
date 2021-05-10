package nova.hetu.omniruntime.operator;

import nova.hetu.omniruntime.vector.IntVec;
import nova.hetu.omniruntime.vector.LongVec;
import nova.hetu.omniruntime.vector.Vec;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class OmniOrderByTest
{
    OmniOrderBy orderBy;
    List<Vec> dataVecs;
    List<Vec> dataNulls;
    int totalPageCount = 20;
    int pageDistinctCount = 4;
    int pageDistinctValueRepeatCount = 25000;

    @BeforeClass
    public void setUp()
    {
        orderBy = new OmniOrderBy();
    }

    @Test
    public void testOrderByTwoColumn()
    {
        IntVec vec1 = new IntVec(8);
        vec1.set(0, 5);
        vec1.set(1, 3);
        vec1.set(2, 2);
        vec1.set(3, 6);
        vec1.set(4, 1);
        vec1.set(5, 4);
        vec1.set(6, 7);
        vec1.set(7, 8);

        IntVec vec2 = new IntVec(8);
        vec2.set(0, 5);
        vec2.set(1, 3);
        vec2.set(2, 2);
        vec2.set(3, 6);
        vec2.set(4, 1);
        vec2.set(5, 4);
        vec2.set(6, 7);
        vec2.set(7, 8);

        List<Vec> datas = new ArrayList<>();
        datas.add(vec1);
        datas.add(vec2);

        int[] sourceTypes = {1, 1};
        int[] outputCols = {0, 1};
        int[] sortCols = {0, 1};
        int[] ascendings = {1, 1};
        int[] nullFirsts = {0, 0};

        long factoryAddress = 0;
        long operatorAddress = orderBy.createOperator(factoryAddress);
        orderBy.addInput(operatorAddress, datas, 1, 2);
        OMResult result = orderBy.getOutput(operatorAddress);

        ByteBuffer[] output = result.getBuffers();
        int len = result.getLength();
        int[] actual0 = new int[len];
        int[] actual1 = new int[len];
        output[0].order(ByteOrder.LITTLE_ENDIAN);
        output[1].order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < len; i++) {
            actual0[i] = output[0].getInt(i * Integer.BYTES);
            actual1[i] = output[1].getInt(i * Integer.BYTES);
        }
        int[] expected0 = {1, 2, 3, 4, 5, 6, 7, 8};
        int[] expected1 = {1, 2, 3, 4, 5, 6, 7, 8};
        Assert.assertEquals(actual0, expected0);
        Assert.assertEquals(actual1, expected1);

        factoryAddress = orderBy.createOperatorFactory(sourceTypes, 2, outputCols, 2, sortCols, ascendings, nullFirsts, 2);
        operatorAddress = orderBy.createOperator(factoryAddress);
        orderBy.addInput(operatorAddress, datas, 1, 2);
        result = orderBy.getOutput(operatorAddress);

        output = result.getBuffers();
        len = result.getLength();
        actual0 = new int[len];
        actual1 = new int[len];
        output[0].order(ByteOrder.LITTLE_ENDIAN);
        output[1].order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < len; i++) {
            actual0[i] = output[0].getInt(i * Integer.BYTES);
            actual1[i] = output[1].getInt(i * Integer.BYTES);
        }
        Assert.assertEquals(actual0, expected0);
        Assert.assertEquals(actual1, expected1);
    }

    @Test
    public void testOrderByPerformance()
    {
        long start = System.currentTimeMillis();
        List<Vec> vecs = buildVecs();
        long elapsed = System.currentTimeMillis() - start;
        System.out.println("buildVecs elapsed time : " + elapsed + " ms");

        int[] sourceTypes = {1, 1};
        int[] outputCols = {0, 1};
        int[] sortCols = {0, 1};
        int[] ascendings = {1, 1};
        int[] nullFirsts = {0, 0};

        long factoryAddress = orderBy.createOperatorFactory(sourceTypes, 2, outputCols, 2, sortCols, ascendings, nullFirsts, 2);

        start = System.currentTimeMillis();
        long operatorAddress = orderBy.createOperator(factoryAddress);

        orderBy.addInput(operatorAddress, vecs, 10, 2);

        OMResult result = orderBy.getOutput(operatorAddress);
        elapsed = System.currentTimeMillis() - start;
        System.out.println("getResult elapsed time : " + elapsed + " ms");

        ByteBuffer[] output = result.getBuffers();
    }

    @Test
    public void testOrderByMultiPerformance()
    {
        dataVecs = buildVecs();

        int[] sourceTypes = {2, 2};
        int[] outputCols = {0, 1};
        int[] sortCols = {0, 1};
        int[] ascendings = {1, 1};
        int[] nullFirsts = {0, 0};
        long factoryAddress = orderBy.createOperatorFactory(sourceTypes, 2, outputCols, 2, sortCols, ascendings, nullFirsts, 2);

        int threadNum = 4;
        CountDownLatch countDownLatch = new CountDownLatch(threadNum);
        for (int i = 0; i < threadNum; i++) {
            Thread thread = new Thread(() -> {
                try {
                    long operatorAddress = orderBy.createOperator(factoryAddress);
                    orderBy.addInput(operatorAddress, dataVecs, totalPageCount, 2);
                    orderBy.getOutput(operatorAddress);
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

    private List<Vec> buildVecs()
    {
        List<Vec> vecs = new ArrayList<>();
        for (int i = 0; i < totalPageCount; i++) {
            LongVec longVec1 = new LongVec(pageDistinctCount * pageDistinctValueRepeatCount);
            LongVec longVec2 = new LongVec(pageDistinctCount * pageDistinctValueRepeatCount);
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
        }
        return vecs;
    }
}
