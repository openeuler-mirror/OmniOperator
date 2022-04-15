/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 */

package nova.hetu.omniruntime.operator;

import static nova.hetu.omniruntime.util.TestUtils.assertVecBatchEquals;
import static nova.hetu.omniruntime.util.TestUtils.assertVecEquals;
import static nova.hetu.omniruntime.util.TestUtils.createVec;
import static nova.hetu.omniruntime.util.TestUtils.createVecBatch;
import static nova.hetu.omniruntime.util.TestUtils.freeVecBatch;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.google.common.collect.ImmutableList;

import nova.hetu.omniruntime.operator.sort.OmniSortOperatorFactory;
import nova.hetu.omniruntime.type.CharDataType;
import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.type.Date32DataType;
import nova.hetu.omniruntime.type.Decimal128DataType;
import nova.hetu.omniruntime.type.Decimal64DataType;
import nova.hetu.omniruntime.type.IntDataType;
import nova.hetu.omniruntime.type.LongDataType;
import nova.hetu.omniruntime.type.VarcharDataType;
import nova.hetu.omniruntime.util.TestUtils;
import nova.hetu.omniruntime.vector.DictionaryVec;
import nova.hetu.omniruntime.vector.LongVec;
import nova.hetu.omniruntime.vector.Vec;
import nova.hetu.omniruntime.vector.VecBatch;

import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * The type Omni sort operator test.
 *
 * @since 2021-5-10
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
     * Test sort two columns.
     */
    @Test
    public void testSortTwoColumns() {
        DataType[] sourceTypes = {IntDataType.INTEGER, IntDataType.INTEGER};
        Object[][] sourceDatas = {{5, 3, 2, 6, 1, 4, 7, 8}, {5, 3, 2, 6, 1, 4, 7, 8}};
        VecBatch vecBatch = createVecBatch(sourceTypes, sourceDatas);

        int[] outputCols = {0, 1};
        String[] sortCols = {"#0", "#1"};
        int[] ascendings = {1, 1};
        int[] nullFirsts = {0, 0};
        OmniSortOperatorFactory sortOperatorFactory = new OmniSortOperatorFactory(sourceTypes, outputCols, sortCols,
                ascendings, nullFirsts);
        OmniOperator sortOperator = sortOperatorFactory.createOperator();
        sortOperator.addInput(vecBatch);
        Iterator<VecBatch> results = sortOperator.getOutput();

        VecBatch resultVecBatch = results.next();
        int len = resultVecBatch.getRowCount();
        assertEquals(len, sourceDatas[0].length);

        Object[][] expectedDatas = {{1, 2, 3, 4, 5, 6, 7, 8}, {1, 2, 3, 4, 5, 6, 7, 8}};
        assertVecBatchEquals(resultVecBatch, expectedDatas);
        freeVecBatch(resultVecBatch);
        sortOperator.close();
        sortOperatorFactory.close();
    }

    /**
     * Test sort two columns with dictionary vector.
     */
    @Test
    public void testSortTwoColumnsWithDict() {
        DataType[] sourceTypes = {IntDataType.INTEGER, IntDataType.INTEGER};
        Object[][] sourceDatas = {{5, 3, 2, 6, 1, 4, 7, 8}, {5, 3, 2, 6, 1, 4, 7, 8}};
        Vec[] vecs = new Vec[2];
        vecs[0] = TestUtils.createIntVec(sourceDatas[0]);
        int[] ids = {0, 1, 2, 3, 4, 5, 6, 7};
        vecs[1] = TestUtils.createDictionaryVec(sourceTypes[1], sourceDatas[1], ids);
        Vec dictionary = vecs[1];
        vecs[1] = new DictionaryVec(dictionary, ids);
        dictionary.close();
        VecBatch vecBatch = new VecBatch(vecs);

        int[] outputCols = {0, 1};
        String[] sortCols = {"#0", "#1"};
        int[] ascendings = {1, 1};
        int[] nullFirsts = {0, 0};
        OmniSortOperatorFactory sortOperatorFactory = new OmniSortOperatorFactory(sourceTypes, outputCols, sortCols,
                ascendings, nullFirsts);
        OmniOperator sortOperator = sortOperatorFactory.createOperator();
        sortOperator.addInput(vecBatch);
        Iterator<VecBatch> results = sortOperator.getOutput();

        VecBatch resultVecBatch = results.next();
        int len = resultVecBatch.getRowCount();
        assertEquals(len, sourceDatas[0].length);

        Object[][] expectedDatas = {{1, 2, 3, 4, 5, 6, 7, 8}, {1, 2, 3, 4, 5, 6, 7, 8}};
        assertVecBatchEquals(resultVecBatch, expectedDatas);
        freeVecBatch(resultVecBatch);
        sortOperator.close();
        sortOperatorFactory.close();
    }

    /**
     * Test sort two varchar columns.
     */
    @Test
    public void testSortTwoVarcharColumns() {
        DataType[] sourceTypes = {new VarcharDataType(1), LongDataType.LONG, new VarcharDataType(3)};
        Object[][] sourceDatas = {{"0", "1", "2", "0", "1", "2"}, {0L, 1L, 2L, 3L, 4L, 5L},
                {"6.6", "5.5", "4.4", "3.3", "2.2", "1.1"}};
        VecBatch vecBatch = createVecBatch(sourceTypes, sourceDatas);

        int[] outputCols = {1, 2};
        String[] sortCols = {"#0", "#2"};
        int[] ascendings = {0, 1};
        int[] nullFirsts = {1, 1};
        OmniSortOperatorFactory sortOperatorFactory = new OmniSortOperatorFactory(sourceTypes, outputCols, sortCols,
                ascendings, nullFirsts);
        OmniOperator sortOperator = sortOperatorFactory.createOperator();
        sortOperator.addInput(vecBatch);
        Iterator<VecBatch> results = sortOperator.getOutput();

        VecBatch resultVecBatch = results.next();
        Object[][] expectedDatas = {{5L, 2L, 4L, 1L, 3L, 0L}, {"1.1", "4.4", "2.2", "5.5", "3.3", "6.6"}};
        assertVecBatchEquals(resultVecBatch, expectedDatas);
        freeVecBatch(resultVecBatch);
        sortOperator.close();
        sortOperatorFactory.close();
    }

    /**
     * Test sort two char columns.
     */
    @Test
    public void testSortTwoCharColumns() {
        DataType[] sourceTypes = {new CharDataType(1), LongDataType.LONG, new CharDataType(3)};
        Object[][] sourceDatas = {{"0", "1", "2", "0", "1", "2"}, {0L, 1L, 2L, 3L, 4L, 5L},
                {"6.6", "5.5", "4.4", "3.3", "2.2", "1.1"}};
        VecBatch vecBatch = createVecBatch(sourceTypes, sourceDatas);

        int[] outputCols = {1, 2};
        String[] sortCols = {"#0", "#2"};
        int[] ascendings = {0, 1};
        int[] nullFirsts = {1, 1};
        OmniSortOperatorFactory sortOperatorFactory = new OmniSortOperatorFactory(sourceTypes, outputCols, sortCols,
                ascendings, nullFirsts);
        OmniOperator sortOperator = sortOperatorFactory.createOperator();
        sortOperator.addInput(vecBatch);
        Iterator<VecBatch> results = sortOperator.getOutput();

        VecBatch resultVecBatch = results.next();
        Object[][] expectedDatas = {{5L, 2L, 4L, 1L, 3L, 0L}, {"1.1", "4.4", "2.2", "5.5", "3.3", "6.6"}};
        assertVecBatchEquals(resultVecBatch, expectedDatas);
        freeVecBatch(resultVecBatch);
        sortOperator.close();
        sortOperatorFactory.close();
    }

    /**
     * Test sort two date32 columns.
     */
    @Test
    public void testSortTwoDate32Columns() {
        DataType[] sourceTypes = {new Date32DataType(DataType.DateUnit.DAY), LongDataType.LONG,
                new Date32DataType(DataType.DateUnit.MILLI)};
        Object[][] sourceDatas = {{0, 1, 2, 0, 1, 2}, {0L, 1L, 2L, 3L, 4L, 5L}, {66, 55, 44, 33, 22, 11}};
        VecBatch vecBatch = createVecBatch(sourceTypes, sourceDatas);

        int[] outputCols = {1, 2};
        String[] sortCols = {"#0", "#2"};
        int[] ascendings = {0, 1};
        int[] nullFirsts = {1, 1};
        OmniSortOperatorFactory sortOperatorFactory = new OmniSortOperatorFactory(sourceTypes, outputCols, sortCols,
                ascendings, nullFirsts);
        OmniOperator sortOperator = sortOperatorFactory.createOperator();
        sortOperator.addInput(vecBatch);
        Iterator<VecBatch> results = sortOperator.getOutput();

        VecBatch resultVecBatch = results.next();
        Object[][] expectedDatas = {{5L, 2L, 4L, 1L, 3L, 0L}, {11, 44, 22, 55, 33, 66}};
        assertVecBatchEquals(resultVecBatch, expectedDatas);
        freeVecBatch(resultVecBatch);
        sortOperator.close();
        sortOperatorFactory.close();
    }

    /**
     * Test sort two decimal64 columns.
     */
    @Test
    public void testSortTwoDecimal64Columns() {
        DataType[] sourceTypes = {new Decimal64DataType(1, 0), LongDataType.LONG, new Decimal64DataType(2, 0)};
        Object[][] sourceDatas = {{0L, 1L, 2L, 0L, 1L, 2L}, {0L, 1L, 2L, 3L, 4L, 5L}, {66L, 55L, 44L, 33L, 22L, 11L}};
        VecBatch vecBatch = createVecBatch(sourceTypes, sourceDatas);

        int[] outputCols = {1, 2};
        String[] sortCols = {"#0", "#2"};
        int[] ascendings = {0, 1};
        int[] nullFirsts = {1, 1};
        OmniSortOperatorFactory sortOperatorFactory = new OmniSortOperatorFactory(sourceTypes, outputCols, sortCols,
                ascendings, nullFirsts);
        OmniOperator sortOperator = sortOperatorFactory.createOperator();
        sortOperator.addInput(vecBatch);
        Iterator<VecBatch> results = sortOperator.getOutput();

        VecBatch resultVecBatch = results.next();
        Object[][] expectedDatas = {{5L, 2L, 4L, 1L, 3L, 0L}, {11L, 44L, 22L, 55L, 33L, 66L}};
        assertVecBatchEquals(resultVecBatch, expectedDatas);
        freeVecBatch(resultVecBatch);
        sortOperator.close();
        sortOperatorFactory.close();
    }

    /**
     * Test sort two decimal128 columns.
     */
    @Test
    public void testSortTwoDecimal128Columns() {
        DataType[] sourceTypes = {new Decimal128DataType(1, 0), LongDataType.LONG, new Decimal128DataType(2, 0)};
        Vec[] vecs = new Vec[sourceTypes.length];
        vecs[0] = createVec(sourceTypes[0], new Object[][]{{0L, 0L}, {1L, 0L}, {2L, 0L}, {0L, 0L}, {1L, 0L}, {2L, 0L}});
        vecs[1] = createVec(sourceTypes[1], new Object[]{0L, 1L, 2L, 3L, 4L, 5L});
        vecs[2] = createVec(sourceTypes[2],
                new Object[][]{{66L, 0L}, {55L, 0L}, {44L, 0L}, {33L, 0L}, {22L, 0L}, {11L, 0L}});
        VecBatch vecBatch = new VecBatch(vecs);

        int[] outputCols = {1, 2};
        String[] sortCols = {"#0", "#2"};
        int[] ascendings = {0, 1};
        int[] nullFirsts = {1, 1};
        OmniSortOperatorFactory sortOperatorFactory = new OmniSortOperatorFactory(sourceTypes, outputCols, sortCols,
                ascendings, nullFirsts);
        OmniOperator sortOperator = sortOperatorFactory.createOperator();
        sortOperator.addInput(vecBatch);
        Iterator<VecBatch> results = sortOperator.getOutput();

        VecBatch resultVecBatch = results.next();
        assertEquals(resultVecBatch.getVectorCount(), outputCols.length);
        assertVecEquals(resultVecBatch.getVectors()[0], new Object[]{5L, 2L, 4L, 1L, 3L, 0L});
        assertVecEquals(resultVecBatch.getVectors()[1],
                new Object[][]{{11L, 0L}, {44L, 0L}, {22L, 0L}, {55L, 0L}, {33L, 0L}, {66L, 0L}});
        freeVecBatch(resultVecBatch);
        sortOperator.close();
        sortOperatorFactory.close();
    }

    /**
     * Test sort with null first.
     */
    @Test
    public void testSortWithNullFirst() {
        DataType[] sourceTypes = {IntDataType.INTEGER, LongDataType.LONG};
        Object[][] sourceDatas = {{4, 3, 2, 1, 0, null}, {0L, 1L, 2L, 3L, 4L, null}};
        VecBatch vecBatch = createVecBatch(sourceTypes, sourceDatas);

        int[] outputCols = {0, 1};
        String[] sortCols = {"#1"};
        int[] ascendings = {0};
        int[] nullFirsts = {1};
        OmniSortOperatorFactory sortOperatorFactory = new OmniSortOperatorFactory(sourceTypes, outputCols, sortCols,
                ascendings, nullFirsts);
        OmniOperator sortOperator = sortOperatorFactory.createOperator();
        sortOperator.addInput(vecBatch);
        Iterator<VecBatch> results = sortOperator.getOutput();
        VecBatch resultVecBatch = results.next();

        Object[][] expectedDatas = {{null, 0, 1, 2, 3, 4}, {null, 4L, 3L, 2L, 1L, 0L}};
        assertVecBatchEquals(resultVecBatch, expectedDatas);
        freeVecBatch(resultVecBatch);
        sortOperator.close();
        sortOperatorFactory.close();
    }

    /**
     * Test sort with null last.
     */
    @Test
    public void testSortWithNullLast() {
        DataType[] sourceTypes = {IntDataType.INTEGER, LongDataType.LONG};
        Object[][] sourceDatas = {{4, 3, 2, 1, 0, null}, {0L, 1L, 2L, 3L, 4L, null}};
        VecBatch vecBatch = createVecBatch(sourceTypes, sourceDatas);

        int[] outputCols = {0, 1};
        String[] sortCols = {"#1"};
        int[] ascendings = {0};
        int[] nullFirsts = {0};
        OmniSortOperatorFactory sortOperatorFactory = new OmniSortOperatorFactory(sourceTypes, outputCols, sortCols,
                ascendings, nullFirsts);
        OmniOperator sortOperator = sortOperatorFactory.createOperator();
        sortOperator.addInput(vecBatch);
        Iterator<VecBatch> results = sortOperator.getOutput();
        VecBatch resultVecBatch = results.next();

        Object[][] expectedDatas = {{0, 1, 2, 3, 4, null}, {4L, 3L, 2L, 1L, 0L, null}};
        assertVecBatchEquals(resultVecBatch, expectedDatas);
        freeVecBatch(resultVecBatch);
        sortOperator.close();
        sortOperatorFactory.close();
    }

    /**
     * Test sort with multi nulls.
     */
    @Test
    public void testSortWithMultiNulls() {
        DataType[] sourceTypes = {IntDataType.INTEGER, LongDataType.LONG};
        Object[][] sourceDatas = {{4, 3, 2, 1, 0, null}, {0L, 1L, null, null, null, null}};
        VecBatch vecBatch = createVecBatch(sourceTypes, sourceDatas);

        int[] outputCols = {0, 1};
        String[] sortCols = {"#1", "#0"};
        int[] ascendings = {0, 0};
        int[] nullFirsts = {1, 1};
        OmniSortOperatorFactory sortOperatorFactory = new OmniSortOperatorFactory(sourceTypes, outputCols, sortCols,
                ascendings, nullFirsts);
        OmniOperator sortOperator = sortOperatorFactory.createOperator();
        sortOperator.addInput(vecBatch);
        Iterator<VecBatch> results = sortOperator.getOutput();
        VecBatch resultVecBatch = results.next();

        Object[][] expectedDatas = {{null, 2, 1, 0, 3, 4}, {null, null, null, null, 1L, 0L}};
        assertVecBatchEquals(resultVecBatch, expectedDatas);
        freeVecBatch(resultVecBatch);
        sortOperator.close();
        sortOperatorFactory.close();
    }

    /**
     * Test sort performance whether with jit or not.
     */
    @Test
    public void testSortComparePref() {
        DataType[] sourceTypes = {LongDataType.LONG, LongDataType.LONG};
        int[] outputCols = {0, 1};
        String[] sortCols = {"#0", "#1"};
        int[] ascendings = {1, 1};
        int[] nullFirsts = {0, 0};

        OmniSortOperatorFactory sortOperatorFactoryWithoutJit = new OmniSortOperatorFactory(sourceTypes, outputCols,
                sortCols, ascendings, nullFirsts, false);
        OmniOperator sortOperatorWithoutJit = sortOperatorFactoryWithoutJit.createOperator();
        ImmutableList<VecBatch> vecsWithoutJit = buildVecs();

        long start = System.currentTimeMillis();
        for (VecBatch vec : vecsWithoutJit) {
            sortOperatorWithoutJit.addInput(vec);
        }
        Iterator<VecBatch> outputWithoutJit = sortOperatorWithoutJit.getOutput();
        long end = System.currentTimeMillis();
        System.out.println("Sort without jit use " + (end - start) + " ms.");

        OmniSortOperatorFactory sortOperatorFactoryWithJit = new OmniSortOperatorFactory(sourceTypes, outputCols,
                sortCols, ascendings, nullFirsts, true);
        OmniOperator sortOperatorWithJit = sortOperatorFactoryWithJit.createOperator();
        ImmutableList<VecBatch> vecsWithJit = buildVecs();

        start = System.currentTimeMillis();
        for (VecBatch vec : vecsWithJit) {
            sortOperatorWithJit.addInput(vec);
        }
        Iterator<VecBatch> outputWithJit = sortOperatorWithJit.getOutput();
        end = System.currentTimeMillis();
        System.out.println("Sort with jit use " + (end - start) + " ms.");

        while (outputWithoutJit.hasNext()) {
            VecBatch resultWithoutJit = outputWithoutJit.next();
            VecBatch resultWithJit = outputWithJit.next();
            assertVecBatchEquals(resultWithoutJit, resultWithJit);
            freeVecBatch(resultWithJit);
        }

        sortOperatorWithoutJit.close();
        sortOperatorWithJit.close();
        sortOperatorFactoryWithoutJit.close();
        sortOperatorFactoryWithJit.close();
    }

    private VecBatch duplicateVecBatch(VecBatch vecBatch) {
        int vecCount = vecBatch.getVectorCount();
        int rowCount = vecBatch.getRowCount();
        Vec[] vecs = new Vec[vecCount];
        for (int i = 0; i < vecCount; i++) {
            vecs[i] = vecBatch.getVector(i).slice(0, rowCount);
        }
        return new VecBatch(vecs);
    }

    /**
     * Test sort performance when multi threads.
     */
    @Test
    public void testSortMultiThreadsPerformance() {
        DataType[] sourceTypes = {LongDataType.LONG, LongDataType.LONG};
        int[] outputCols = {0, 1};
        String[] sortCols = {"#0", "#1"};
        int[] ascendings = {1, 1};
        int[] nullFirsts = {0, 0};
        OmniSortOperatorFactory sortOperatorFactory = new OmniSortOperatorFactory(sourceTypes, outputCols, sortCols,
                ascendings, nullFirsts);

        final int threadNum = 4;
        final int corePoolSize = 10;
        final int maximumPoolSize = 50;
        CountDownLatch countDownLatch = new CountDownLatch(threadNum);
        ThreadPoolExecutor threadPool = new ThreadPoolExecutor(
                corePoolSize, maximumPoolSize, 60L, TimeUnit.SECONDS, new LinkedBlockingQueue<>(threadNum));
        ImmutableList<VecBatch> vecs = buildVecs();
        for (int i = 0; i < threadNum; i++) {
            CompletableFuture.runAsync(() -> {
                try {
                    OmniOperator sortOperator = sortOperatorFactory.createOperator();
                    for (VecBatch vec : vecs) {
                        sortOperator.addInput(duplicateVecBatch(vec));
                    }
                    Iterator<VecBatch> iterator = sortOperator.getOutput();
                    while (iterator.hasNext()) {
                        VecBatch result = iterator.next();
                        freeVecBatch(result);
                    }
                    sortOperator.close();
                } finally {
                    countDownLatch.countDown();
                }
            }, threadPool);
        }

        // This will wait until all future ready.
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            assertTrue(false);
        }

        threadPool.shutdown();
        vecs.forEach(TestUtils::freeVecBatch);
        sortOperatorFactory.close();
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
            VecBatch vecBatch = new VecBatch(new Vec[]{longVec1, longVec2});
            vecBatchList.add(vecBatch);
        }
        return vecBatchList.build();
    }
}
