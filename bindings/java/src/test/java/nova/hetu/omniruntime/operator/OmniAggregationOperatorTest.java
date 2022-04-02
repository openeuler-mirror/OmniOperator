/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.omniruntime.operator;

import static java.lang.String.format;
import static nova.hetu.omniruntime.constants.FunctionType.OMNI_AGGREGATION_TYPE_COUNT_ALL;
import static nova.hetu.omniruntime.constants.FunctionType.OMNI_AGGREGATION_TYPE_COUNT_COLUMN;
import static nova.hetu.omniruntime.constants.FunctionType.OMNI_AGGREGATION_TYPE_SUM;
import static nova.hetu.omniruntime.util.TestUtils.freeVecBatch;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.fail;

import com.google.common.collect.ImmutableList;

import nova.hetu.omniruntime.constants.FunctionType;
import nova.hetu.omniruntime.operator.aggregator.OmniAggregationOperatorFactory;
import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.type.LongDataType;
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
 * The type Omni aggregation operator test.
 *
 * @since 2021-06-09
 */
public class OmniAggregationOperatorTest {
    /**
     * Test execute agg multiple page.
     */
    @Test
    public void testExecuteCountMultiplePage() {
        DataType[] sourceTypes = {LongDataType.LONG};
        FunctionType[] aggFunctionTypes = {OMNI_AGGREGATION_TYPE_COUNT_ALL, OMNI_AGGREGATION_TYPE_COUNT_COLUMN};
        int[] aggInputChannels = {1};
        int[] maskChannels = {-1, -1};
        DataType[] aggOutputTypes = {LongDataType.LONG, LongDataType.LONG};
        OmniAggregationOperatorFactory factory = new OmniAggregationOperatorFactory(sourceTypes, aggFunctionTypes,
                aggInputChannels, maskChannels, aggOutputTypes, true, false);

        ImmutableList.Builder<VecBatch> vecBatchList = ImmutableList.builder();
        int rowNum = 100;
        int pageCount = 10;
        for (int i = 0; i < pageCount; i++) {
            vecBatchList.add(new VecBatch(buildDataForCount(rowNum)));
        }

        OmniOperator omniOperator = factory.createOperator();
        for (VecBatch vecBatch : vecBatchList.build()) {
            omniOperator.addInput(vecBatch);
        }

        Iterator<VecBatch> output = omniOperator.getOutput();
        while (output.hasNext()) {
            VecBatch vecBatch = output.next();
            if (vecBatch.getVectors().length != aggOutputTypes.length) {
                throw new IllegalArgumentException(
                        format("output vec size error: result size: %s, outputTypes size: %s,rows: %s",
                                vecBatch.getVectors().length, aggOutputTypes.length, vecBatch.getRowCount()));
            }

            assertNotNull(vecBatch);
            assertEquals(vecBatch.getVectors().length, 2);
            Vec[] vectors = vecBatch.getVectors();
            assertEquals(((LongVec) vectors[0]).get(0), 1000L);
            assertEquals(((LongVec) vectors[1]).get(0), 500L);

            freeVecBatch(vecBatch);
        }

        omniOperator.close();
        factory.close();
    }

    @Test
    public void testExecuteAggMultiplePage() {
        DataType[] sourceTypes = {LongDataType.LONG, LongDataType.LONG, LongDataType.LONG, LongDataType.LONG};
        FunctionType[] aggFunctionTypes = {OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_SUM,
                OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_SUM};
        int[] aggInputChannels = {0, 1, 2, 3};
        int[] maskChannels = {-1, -1, -1, -1};
        DataType[] aggOutputTypes = {LongDataType.LONG, LongDataType.LONG, LongDataType.LONG, LongDataType.LONG};
        OmniAggregationOperatorFactory factory = new OmniAggregationOperatorFactory(sourceTypes, aggFunctionTypes,
                aggInputChannels, maskChannels, aggOutputTypes, true, false);

        List<Vec> inputData = new ArrayList<>();
        ImmutableList.Builder<VecBatch> vecBatchList = ImmutableList.builder();
        int rowNum = 40000;
        int pageCount = 10;
        for (int i = 0; i < pageCount; i++) {
            inputData.addAll(build4Columns(rowNum));
            vecBatchList.add(new VecBatch(build4Columns(rowNum)));
        }

        OmniOperator omniOperator = factory.createOperator();
        for (VecBatch vecBatch : vecBatchList.build()) {
            omniOperator.addInput(vecBatch);
        }
        // release input data memory
        releaseVecMemory(inputData.toArray(new Vec[0]));

        Iterator<VecBatch> output = omniOperator.getOutput();
        while (output.hasNext()) {
            VecBatch vecBatch = output.next();
            if (vecBatch.getVectors().length != aggOutputTypes.length) {
                throw new IllegalArgumentException(
                        format("output vec size error: result size: %s, outputTypes size: %s,rows: %s",
                                vecBatch.getVectors().length, aggOutputTypes.length, vecBatch.getRowCount()));
            }

            assertNotNull(vecBatch);
            assertEquals(vecBatch.getVectors().length, 4);
            Vec[] vectors = vecBatch.getVectors();
            assertEquals(((LongVec) vectors[0]).get(0), 0);
            assertEquals(((LongVec) vectors[1]).get(0), 0);
            assertEquals(((LongVec) vectors[2]).get(0), rowNum * pageCount);
            assertEquals(((LongVec) vectors[3]).get(0), rowNum * pageCount);

            releaseVecMemory(vecBatch.getVectors());
        }
    }

    /**
     * Test execute agg multiple thread.
     */
    @Test
    public void testExecuteAggMultipleThread() {
        int pageCount = 10;
        int threadCount = 10;
        int rowNum = 100;
        multiThreadExecution(threadCount, rowNum, pageCount);
    }

    private void multiThreadExecution(int threadCount, int rowNum, int pageCount) {
        DataType[] sourceTypes = {LongDataType.LONG, LongDataType.LONG, LongDataType.LONG,
                LongDataType.LONG};
        FunctionType[] aggFunctionTypes = {OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_SUM,
                OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_SUM};
        int[] aggInputChannels = {0, 1, 2, 3};
        int[] maskChannels = {-1, -1, -1, -1};
        DataType[] aggOutputTypes = {LongDataType.LONG, LongDataType.LONG, LongDataType.LONG,
                LongDataType.LONG};
        OmniAggregationOperatorFactory factory = new OmniAggregationOperatorFactory(sourceTypes,
                aggFunctionTypes, aggInputChannels, maskChannels, aggOutputTypes, true, false);

        CountDownLatch downLatch = new CountDownLatch(threadCount);
        final int corePoolSize = 10;
        final int maximumPoolSize = 50;
        ThreadPoolExecutor threadPool = new ThreadPoolExecutor(corePoolSize, maximumPoolSize, 60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(threadCount));

        for (int tIdx = 0; tIdx < threadCount; tIdx++) {
            CompletableFuture.runAsync(() -> {
                try {
                    ImmutableList.Builder<VecBatch> vecBatchList = ImmutableList.builder();
                    for (int i = 0; i < pageCount; i++) {
                        vecBatchList.add(new VecBatch(build4Columns(rowNum)));
                    }

                    OmniOperator omniOperator = factory.createOperator();
                    for (VecBatch vecBatch : vecBatchList.build()) {
                        omniOperator.addInput(vecBatch);
                    }

                    assertResult(rowNum, pageCount, aggOutputTypes, omniOperator);
                    omniOperator.close();
                } finally {
                    downLatch.countDown();
                }
            }, threadPool);
        }

        try {
            downLatch.await();
        } catch (InterruptedException ex) {
            fail();
        }

        threadPool.shutdown();
        factory.close();
    }

    private void assertResult(int rowNum, int pageCount, DataType[] aggOutputTypes, OmniOperator omniOperator) {
        Iterator<VecBatch> output = omniOperator.getOutput();
        while (output.hasNext()) {
            VecBatch vecBatch = output.next();
            if (vecBatch.getVectors().length != aggOutputTypes.length) {
                throw new IllegalArgumentException(
                        format("output vec size error: result size: %s, outputTypes size: %s,rows: %s",
                                vecBatch.getVectors().length, aggOutputTypes.length, vecBatch.getRowCount()));
            }

            assertNotNull(vecBatch);
            assertEquals(vecBatch.getVectors().length, 4);
            Vec[] vectors = vecBatch.getVectors();
            assertEquals(((LongVec) vectors[0]).get(0), 0);
            assertEquals(((LongVec) vectors[1]).get(0), 0);
            assertEquals(((LongVec) vectors[2]).get(0), rowNum * pageCount);
            assertEquals(((LongVec) vectors[3]).get(0), rowNum * pageCount);

            releaseVecMemory(vecBatch.getVectors());
        }
    }

    private List<Vec> buildDataForCount(int rowNum) {
        LongVec c1 = new LongVec(rowNum);
        for (int i = 0; i < rowNum; i++) {
            c1.set(i, 0);
        }

        LongVec c2 = new LongVec(rowNum);
        for (int i = 0; i < rowNum; i++) {
            if (i % 2 == 0) {
                c2.set(i, 1);
            } else {
                c2.setNull(i);
            }
        }

        List<Vec> columns = new ArrayList<>();
        columns.add(c1);
        columns.add(c2);

        return columns;
    }

    private List<Vec> build4Columns(int rowNum) {
        LongVec c1 = new LongVec(rowNum);
        LongVec c2 = new LongVec(rowNum);
        for (int i = 0; i < rowNum; i++) {
            c1.set(i, 0);
            c2.set(i, 0);
        }

        LongVec c3 = new LongVec(rowNum);
        LongVec c4 = new LongVec(rowNum);
        for (int i = 0; i < rowNum; i++) {
            c3.set(i, 1);
            c4.set(i, 1);
        }

        List<Vec> columns = new ArrayList<>();
        columns.add(c1);
        columns.add(c2);
        columns.add(c3);
        columns.add(c4);

        return columns;
    }

    private void releaseVecMemory(Vec[] vecs) {
        for (Vec vec : vecs) {
            vec.close();
        }
    }
}
