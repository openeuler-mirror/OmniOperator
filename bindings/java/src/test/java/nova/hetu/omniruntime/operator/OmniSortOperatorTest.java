package nova.hetu.omniruntime.operator;

import static nova.hetu.omniruntime.util.TestUtils.assertVecBatchEquals;
import static nova.hetu.omniruntime.util.TestUtils.assertVecEquals;
import static nova.hetu.omniruntime.util.TestUtils.createVecBatch;
import static nova.hetu.omniruntime.util.TestUtils.createVec;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.google.common.collect.ImmutableList;

import nova.hetu.omniruntime.operator.sort.OmniSortOperatorFactory;
import nova.hetu.omniruntime.type.Date32VecType;
import nova.hetu.omniruntime.type.Decimal128VecType;
import nova.hetu.omniruntime.type.Decimal64VecType;
import nova.hetu.omniruntime.type.IntVecType;
import nova.hetu.omniruntime.type.LongVecType;
import nova.hetu.omniruntime.type.VarcharVecType;
import nova.hetu.omniruntime.type.VecType;
import nova.hetu.omniruntime.vector.LongVec;
import nova.hetu.omniruntime.vector.Vec;
import nova.hetu.omniruntime.vector.VecBatch;

import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * The type Omni sort operator test.
 */
public class OmniSortOperatorTest {
    /**
     * The Total page count.
     */
    int totalPageCount = 20;

    /**
     * The Page distinct count.
     */
    int pageDistinctCount = 4;

    /**
     * The Page distinct value repeat count.
     */
    int pageDistinctValueRepeatCount = 25000;

    /**
     * Test order by two column.
     */
    @Test
    public void testOrderByTwoColumn() {
        VecType[] sourceTypes = {IntVecType.INTEGER, IntVecType.INTEGER};
        Object[][] sourceDatas = {{5, 3, 2, 6, 1, 4, 7, 8}, {5, 3, 2, 6, 1, 4, 7, 8}};
        VecBatch vecBatch = createVecBatch(sourceTypes, sourceDatas);

        int[] outputCols = {0, 1};
        int[] sortCols = {0, 1};
        int[] ascendings = {1, 1};
        int[] nullFirsts = {0, 0};
        OmniSortOperatorFactory sortOperatorFactory =
                new OmniSortOperatorFactory(sourceTypes, outputCols, sortCols, ascendings, nullFirsts);
        OmniOperator sortOperator = sortOperatorFactory.createOperator();
        sortOperator.addInput(vecBatch);
        Iterator<VecBatch> results = sortOperator.getOutput();

        VecBatch resultVecBatch = results.next();
        int len = resultVecBatch.getRowCount();
        assertEquals(len, sourceDatas[0].length);

        Object[][] expectedDatas = {{1, 2, 3, 4, 5, 6, 7, 8}, {1, 2, 3, 4, 5, 6, 7, 8}};
        assertVecBatchEquals(resultVecBatch, expectedDatas);
        vecBatch.releaseAllVectors();
        vecBatch.close();
        resultVecBatch.releaseAllVectors();
        resultVecBatch.close();
    }

    /**
     * Test order by two varchar column.
     */
    @Test
    public void testOrderByTwoVarcharColumn() {
        VecType[] sourceTypes = {new VarcharVecType(1), LongVecType.LONG, new VarcharVecType(3)};
        Object[][] sourceDatas = {
            {"0", "1", "2", "0", "1", "2"}, {0L, 1L, 2L, 3L, 4L, 5L}, {"6.6", "5.5", "4.4", "3.3", "2.2", "1.1"}
        };
        VecBatch vecBatch = createVecBatch(sourceTypes, sourceDatas);

        int[] outputCols = {1, 2};
        int[] sortCols = {0, 2};
        int[] ascendings = {0, 1};
        int[] nullFirsts = {1, 1};
        OmniSortOperatorFactory sortOperatorFactory =
                new OmniSortOperatorFactory(sourceTypes, outputCols, sortCols, ascendings, nullFirsts);
        OmniOperator sortOperator = sortOperatorFactory.createOperator();
        sortOperator.addInput(vecBatch);
        Iterator<VecBatch> results = sortOperator.getOutput();

        VecBatch resultVecBatch = results.next();
        Object[][] expectedDatas = {{5L, 2L, 4L, 1L, 3L, 0L}, {"1.1", "4.4", "2.2", "5.5", "3.3", "6.6"}};
        assertVecBatchEquals(resultVecBatch, expectedDatas);
        vecBatch.releaseAllVectors();
        vecBatch.close();
        resultVecBatch.releaseAllVectors();
        resultVecBatch.close();
    }

    /**
     * Test order by two date32 column.
     */
    @Test
    public void testOrderByTwoDate32Column() {
        VecType[] sourceTypes = {new Date32VecType(VecType.DateUnit.DAY), LongVecType.LONG, new Date32VecType(VecType.DateUnit.MILLI)};
        Object[][] sourceDatas = {
                {0, 1, 2, 0, 1, 2}, {0L, 1L, 2L, 3L, 4L, 5L}, {66, 55, 44, 33, 22, 11}
        };
        VecBatch vecBatch = createVecBatch(sourceTypes, sourceDatas);

        int[] outputCols = {1, 2};
        int[] sortCols = {0, 2};
        int[] ascendings = {0, 1};
        int[] nullFirsts = {1, 1};
        OmniSortOperatorFactory sortOperatorFactory =
                new OmniSortOperatorFactory(sourceTypes, outputCols, sortCols, ascendings, nullFirsts);
        OmniOperator sortOperator = sortOperatorFactory.createOperator();
        sortOperator.addInput(vecBatch);
        Iterator<VecBatch> results = sortOperator.getOutput();

        VecBatch resultVecBatch = results.next();
        Object[][] expectedDatas = {{5L, 2L, 4L, 1L, 3L, 0L}, {11, 44, 22, 55, 33, 66}};
        assertVecBatchEquals(resultVecBatch, expectedDatas);
        vecBatch.releaseAllVectors();
        vecBatch.close();
        resultVecBatch.releaseAllVectors();
        resultVecBatch.close();
    }

    /**
     * Test order by two decimal64 column.
     */
    @Test
    public void testOrderByTwoDecimal64Column() {
        VecType[] sourceTypes = {new Decimal64VecType(1, 0), LongVecType.LONG, new Decimal64VecType(2, 0)};
        Object[][] sourceDatas = {
                {0L, 1L, 2L, 0L, 1L, 2L}, {0L, 1L, 2L, 3L, 4L, 5L}, {66L, 55L, 44L, 33L, 22L, 11L}
        };
        VecBatch vecBatch = createVecBatch(sourceTypes, sourceDatas);

        int[] outputCols = {1, 2};
        int[] sortCols = {0, 2};
        int[] ascendings = {0, 1};
        int[] nullFirsts = {1, 1};
        OmniSortOperatorFactory sortOperatorFactory =
                new OmniSortOperatorFactory(sourceTypes, outputCols, sortCols, ascendings, nullFirsts);
        OmniOperator sortOperator = sortOperatorFactory.createOperator();
        sortOperator.addInput(vecBatch);
        Iterator<VecBatch> results = sortOperator.getOutput();

        VecBatch resultVecBatch = results.next();
        Object[][] expectedDatas = {{5L, 2L, 4L, 1L, 3L, 0L}, {11L, 44L, 22L, 55L, 33L, 66L}};
        assertVecBatchEquals(resultVecBatch, expectedDatas);
        vecBatch.releaseAllVectors();
        vecBatch.close();
        resultVecBatch.releaseAllVectors();
        resultVecBatch.close();
    }

    /**
     * Test order by two decimal128 column.
     */
    @Test
    public void testOrderByTwoDecimal128Column() {
        VecType[] sourceTypes = {new Decimal128VecType(1, 0), LongVecType.LONG, new Decimal128VecType(2, 0)};
        Vec[] vecs = new Vec[sourceTypes.length];
        vecs[0] = createVec(sourceTypes[0], new Object[][] {{0L, 0L}, {1L, 0L}, {2L, 0L}, {0L, 0L}, {1L, 0L}, {2L, 0L}});
        vecs[1] = createVec(sourceTypes[1], new Object[] {0L, 1L, 2L, 3L, 4L, 5L});
        vecs[2] = createVec(sourceTypes[2], new Object[][] {{66L, 0L}, {55L, 0L}, {44L, 0L}, {33L, 0L}, {22L, 0L}, {11L, 0L}});
        VecBatch vecBatch = new VecBatch(vecs);

        int[] outputCols = {1, 2};
        int[] sortCols = {0, 2};
        int[] ascendings = {0, 1};
        int[] nullFirsts = {1, 1};
        OmniSortOperatorFactory sortOperatorFactory =
                new OmniSortOperatorFactory(sourceTypes, outputCols, sortCols, ascendings, nullFirsts);
        OmniOperator sortOperator = sortOperatorFactory.createOperator();
        sortOperator.addInput(vecBatch);
        Iterator<VecBatch> results = sortOperator.getOutput();

        VecBatch resultVecBatch = results.next();
        assertEquals(resultVecBatch.getVectorCount(), outputCols.length);
        assertVecEquals(resultVecBatch.getVectors()[0], new Object[] {5L, 2L, 4L, 1L, 3L, 0L});
        assertVecEquals(resultVecBatch.getVectors()[1], new Object[][] {{11L, 0L}, {44L, 0L}, {22L, 0L}, {55L, 0L}, {33L, 0L}, {66L, 0L}});

        vecBatch.releaseAllVectors();
        vecBatch.close();
        resultVecBatch.releaseAllVectors();
        resultVecBatch.close();
    }

    @Test
    public void testOrderByWithNullFirst() {
        VecType[] sourceTypes = {IntVecType.INTEGER, LongVecType.LONG};
        Object[][] sourceDatas = {{4, 3, 2, 1, 0, null}, {0L, 1L, 2L, 3L, 4L, null}};
        VecBatch vecBatch = createVecBatch(sourceTypes, sourceDatas);
        vecBatch.getVectors()[0].setNull(5);
        vecBatch.getVectors()[1].setNull(5);

        int[] outputCols = {0, 1};
        int[] sortCols = {1};
        int[] ascendings = {0};
        int[] nullFirsts = {1};
        OmniSortOperatorFactory sortOperatorFactory =
                new OmniSortOperatorFactory(sourceTypes, outputCols, sortCols, ascendings, nullFirsts);
        OmniOperator sortOperator = sortOperatorFactory.createOperator();
        sortOperator.addInput(vecBatch);
        Iterator<VecBatch> results = sortOperator.getOutput();
        VecBatch resultVecBatch = results.next();

        Object[][] expectedDatas = {{null, 0, 1, 2, 3, 4}, {null, 4L, 3L, 2L, 1L, 0L}};
        assertVecBatchEquals(resultVecBatch, expectedDatas);
        vecBatch.releaseAllVectors();
        vecBatch.close();
        resultVecBatch.releaseAllVectors();
        resultVecBatch.close();
    }

    @Test
    public void testOrderByWithNullLast() {
        VecType[] sourceTypes = {IntVecType.INTEGER, LongVecType.LONG};
        Object[][] sourceDatas = {{4, 3, 2, 1, 0, null}, {0L, 1L, 2L, 3L, 4L, null}};
        VecBatch vecBatch = createVecBatch(sourceTypes, sourceDatas);
        vecBatch.getVectors()[0].setNull(5);
        vecBatch.getVectors()[1].setNull(5);

        int[] outputCols = {0, 1};
        int[] sortCols = {1};
        int[] ascendings = {0};
        int[] nullFirsts = {0};
        OmniSortOperatorFactory sortOperatorFactory =
                new OmniSortOperatorFactory(sourceTypes, outputCols, sortCols, ascendings, nullFirsts);
        OmniOperator sortOperator = sortOperatorFactory.createOperator();
        sortOperator.addInput(vecBatch);
        Iterator<VecBatch> results = sortOperator.getOutput();
        VecBatch resultVecBatch = results.next();

        Object[][] expectedDatas = {{0, 1, 2, 3, 4, null}, {4L, 3L, 2L, 1L, 0L, null}};
        assertVecBatchEquals(resultVecBatch, expectedDatas);
        vecBatch.releaseAllVectors();
        vecBatch.close();
        resultVecBatch.releaseAllVectors();
        resultVecBatch.close();
    }

    /**
     * Test order by performance.
     */
    @Test
    public void testOrderByPerformance() {
        ImmutableList<VecBatch> vecs = buildVecs();

        VecType[] sourceTypes = {IntVecType.INTEGER, IntVecType.INTEGER};
        int[] outputCols = {0, 1};
        int[] sortCols = {0, 1};
        int[] ascendings = {1, 1};
        int[] nullFirsts = {0, 0};

        OmniSortOperatorFactory sortOperatorFactory =
                new OmniSortOperatorFactory(sourceTypes, outputCols, sortCols, ascendings, nullFirsts);

        long start = System.currentTimeMillis();
        OmniOperator sortOperator = sortOperatorFactory.createOperator();
        for (VecBatch vec : vecs) {
            sortOperator.addInput(vec);
        }
        Iterator<VecBatch> iterator = sortOperator.getOutput();
        long elapsed = System.currentTimeMillis() - start;
        System.out.println("testOrderByPerformance elapsed time : " + elapsed + "ms");

        vecs.forEach(VecBatch::releaseAllVectors);
        vecs.forEach(VecBatch::close);
        while (iterator.hasNext()) {
            VecBatch result = iterator.next();
            result.releaseAllVectors();
            result.close();
        }
    }

    /**
     * Test order by multi performance.
     */
    @Test
    public void testOrderByMultiPerformance() {
        ImmutableList<VecBatch> vecs = buildVecs();

        VecType[] sourceTypes = {LongVecType.LONG, LongVecType.LONG};
        int[] outputCols = {0, 1};
        int[] sortCols = {0, 1};
        int[] ascendings = {1, 1};
        int[] nullFirsts = {0, 0};
        OmniSortOperatorFactory sortOperatorFactory =
                new OmniSortOperatorFactory(sourceTypes, outputCols, sortCols, ascendings, nullFirsts);

        int threadNum = 4;
        CountDownLatch countDownLatch = new CountDownLatch(threadNum);
        for (int i = 0; i < threadNum; i++) {
            Thread thread =
                    new Thread(
                            () -> {
                                try {
                                    OmniOperator sortOperator = sortOperatorFactory.createOperator();
                                    for (VecBatch vec : vecs) {
                                        sortOperator.addInput(vec);
                                    }
                                    Iterator<VecBatch> iterator = sortOperator.getOutput();
                                    while (iterator.hasNext()) {
                                        VecBatch result = iterator.next();
                                        result.releaseAllVectors();
                                        result.close();
                                    }
                                } finally {
                                    countDownLatch.countDown();
                                }
                            });
            thread.setName("thread" + i);
            thread.setUncaughtExceptionHandler(
                    new Thread.UncaughtExceptionHandler() {
                        @Override
                        public void uncaughtException(Thread thread1, Throwable throwable) {
                            assertTrue(false);
                        }
                    });
            thread.start();
        }
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            assertTrue(false);
        }
        vecs.forEach(VecBatch::releaseAllVectors);
        vecs.forEach(VecBatch::close);
    }

    private ImmutableList<VecBatch> buildVecs() {
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
