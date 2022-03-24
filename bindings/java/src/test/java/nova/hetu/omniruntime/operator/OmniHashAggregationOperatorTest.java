
package nova.hetu.omniruntime.operator;

import static java.lang.String.format;
import static nova.hetu.omniruntime.constants.FunctionType.OMNI_AGGREGATION_TYPE_COUNT_ALL;
import static nova.hetu.omniruntime.constants.FunctionType.OMNI_AGGREGATION_TYPE_COUNT_COLUMN;
import static nova.hetu.omniruntime.constants.FunctionType.OMNI_AGGREGATION_TYPE_SUM;
import static nova.hetu.omniruntime.util.TestUtils.freeVecBatch;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import nova.hetu.omniruntime.constants.FunctionType;
import nova.hetu.omniruntime.operator.aggregator.OmniHashAggregationOperatorFactory;
import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.type.LongDataType;
import nova.hetu.omniruntime.vector.LongVec;
import nova.hetu.omniruntime.vector.Vec;
import nova.hetu.omniruntime.vector.VecBatch;

import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * The type Omni hash aggregation operator test.
 */
public class OmniHashAggregationOperatorTest {
    /**
     * Test execute agg multiple page.
     */
    @Test
    public void testExecuteCountMultiplePage() {
        String[] groupByChannel = {"#0", "#1"};
        DataType[] groupByTypes = {LongDataType.LONG, LongDataType.LONG};
        String[] aggChannels = {"#3"};
        DataType[] aggTypes = {LongDataType.LONG};
        FunctionType[] aggFunctionTypes = {OMNI_AGGREGATION_TYPE_COUNT_COLUMN, OMNI_AGGREGATION_TYPE_COUNT_ALL};
        DataType[] aggOutputTypes = {LongDataType.LONG, LongDataType.LONG, LongDataType.LONG, LongDataType.LONG};

        OmniHashAggregationOperatorFactory factory = new OmniHashAggregationOperatorFactory(groupByChannel,
                groupByTypes, aggChannels, aggTypes, aggFunctionTypes, aggOutputTypes, true, false);
        int rowNum = 100;
        int pageCount = 10;

        OmniOperator omniOperator = factory.createOperator();

        for (int i = 0; i < pageCount; i++) {
            VecBatch vecBatch = new VecBatch(buildDataForCount(rowNum));
            omniOperator.addInput(vecBatch);
        }

        Iterator<VecBatch> output = omniOperator.getOutput();
        VecBatch vecBatch = null;
        while (output.hasNext()) {
            vecBatch = output.next();
            if (vecBatch.getVectors().length != aggOutputTypes.length) {
                throw new IllegalArgumentException(
                        format("output vec size error: result size: %s, outputTypes size: %s,rows: %s",
                                vecBatch.getVectors().length, aggOutputTypes.length, vecBatch.getRowCount()));
            }
            assertNotNull(vecBatch);
            assertEquals(vecBatch.getVectors().length, 4);
            Vec[] vectors = vecBatch.getVectors();
            assertEquals(((LongVec) vectors[0]).get(0), 1);
            assertEquals(((LongVec) vectors[1]).get(0), 1);
            assertEquals(((LongVec) vectors[2]).get(0), 500L);
            assertEquals(((LongVec) vectors[3]).get(0), 1000L);

            freeVecBatch(vecBatch);
        }

        omniOperator.close();
        factory.close();
    }

    @Test
    public void testExecuteAggMultiplePage() {
        String[] groupByChanel = {"#0", "#1"};
        DataType[] groupByTypes = {LongDataType.LONG, LongDataType.LONG};
        String[] aggChannels = {"#2", "#3"};
        DataType[] aggTypes = {LongDataType.LONG, LongDataType.LONG};
        FunctionType[] aggFunctionTypes = {OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_SUM};
        DataType[] aggOutputTypes = {LongDataType.LONG, LongDataType.LONG, LongDataType.LONG, LongDataType.LONG};

        DataType[] inputTypes = {LongDataType.LONG, LongDataType.LONG, LongDataType.LONG, LongDataType.LONG};
        OmniHashAggregationOperatorFactory factory = new OmniHashAggregationOperatorFactory(groupByChanel, groupByTypes,
                aggChannels, aggTypes, aggFunctionTypes, aggOutputTypes, true, false);
        int rowNum = 40000;
        int pageCount = 10;
        int[] rowNums = new int[pageCount];

        OmniOperator omniOperator = factory.createOperator();

        List<Vec> inputData = new ArrayList<>();
        for (int i = 0; i < pageCount; i++) {
            inputData.addAll(build4Columns(rowNum));
            VecBatch vecBatch = new VecBatch(build4Columns(rowNum));
            omniOperator.addInput(vecBatch);
        }

        Iterator<VecBatch> output = omniOperator.getOutput();
        VecBatch vecBatch = null;
        while (output.hasNext()) {
            vecBatch = output.next();
            if (vecBatch.getVectors().length != aggOutputTypes.length) {
                throw new IllegalArgumentException(
                        format("output vec size error: result size: %s, outputTypes size: %s,rows: %s",
                                vecBatch.getVectors().length, aggOutputTypes.length, vecBatch.getRowCount()));
            }
            assertNotNull(vecBatch);
            assertEquals(vecBatch.getVectors().length, 4);
            Vec[] vectors = vecBatch.getVectors();
            assertEquals(((LongVec) vectors[0]).get(0), 1);
            assertEquals(((LongVec) vectors[1]).get(0), 1);
            assertEquals(((LongVec) vectors[2]).get(0), rowNum * pageCount);
            assertEquals(((LongVec) vectors[3]).get(0), rowNum * pageCount);
            releaseVecMemory(vecBatch.getVectors());
        }
    }

    private void releaseVecMemory(Vec[] vecs) {
        for (Vec vec : vecs) {
            vec.close();
        }
    }

    /**
     * Test execute agg multiple thread.
     */
    @Test
    public void testExecuteAggMultipleThread() {
        int pageCount = 10;
        int threadCount = 1;
        int rowNum = 100;
        multiThreadExecution(threadCount, rowNum, pageCount);
    }

    private void multiThreadExecution(int threadCount, int rowNum, int pageCount) {
        CountDownLatch downLatch = new CountDownLatch(threadCount);
        for (int tIdx = 0; tIdx < threadCount; tIdx++) {
            Thread thread = new Thread(() -> {
                try {
                    String[] groupByChanel = {"#0", "#1"};
                    DataType[] groupByTypes = {LongDataType.LONG, LongDataType.LONG};
                    String[] aggChannels = {"#2", "#3"};
                    DataType[] aggTypes = {LongDataType.LONG, LongDataType.LONG};
                    FunctionType[] aggFunctionTypes = {OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_SUM};
                    DataType[] aggOutputTypes = {LongDataType.LONG, LongDataType.LONG, LongDataType.LONG,
                            LongDataType.LONG};
                    DataType[] inputTypes = {LongDataType.LONG, LongDataType.LONG, LongDataType.LONG,
                            LongDataType.LONG};
                    OmniHashAggregationOperatorFactory factory = new OmniHashAggregationOperatorFactory(groupByChanel,
                            groupByTypes, aggChannels, aggTypes, aggFunctionTypes, aggOutputTypes, true, false);

                    OmniOperator omniOperator = factory.createOperator();
                    for (int i = 0; i < pageCount; i++) {
                        omniOperator.addInput(new VecBatch(build4Columns(rowNum)));
                    }

                    assertResult(rowNum, pageCount, aggOutputTypes, omniOperator);
                } finally {
                    downLatch.countDown();
                }
            });
            thread.setName("thread-" + tIdx);
            thread.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
                @Override
                public void uncaughtException(Thread thread1, Throwable throwable) {
                    throwable.printStackTrace();
                }
            });
            thread.start();
        }
        try {
            downLatch.await();
        } catch (InterruptedException ex) {
            assertTrue(false);
        }
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
            assertEquals(((LongVec) vectors[0]).get(0), 1);
            assertEquals(((LongVec) vectors[1]).get(0), 1);
            assertEquals(((LongVec) vectors[2]).get(0), rowNum * pageCount);
            assertEquals(((LongVec) vectors[3]).get(0), rowNum * pageCount);
        }
    }

    private List<Vec> build4Columns(int rowNum) {
        List<Vec> columns = new ArrayList<>();

        LongVec c1 = new LongVec(rowNum);
        LongVec c2 = new LongVec(rowNum);
        for (int i = 0; i < rowNum; i++) {
            c1.set(i, 1);
            c2.set(i, 1);
        }

        LongVec c3 = new LongVec(rowNum);
        LongVec c4 = new LongVec(rowNum);
        for (int i = 0; i < rowNum; i++) {
            c3.set(i, 1);
            c4.set(i, 1);
        }

        columns.add(c1);
        columns.add(c2);
        columns.add(c3);
        columns.add(c4);

        return columns;
    }

    private List<Vec> build2Columns(int rowNum) {
        List<Vec> columns = new ArrayList<>();

        LongVec c1 = new LongVec(rowNum);
        for (int i = 0; i < rowNum; i++) {
            c1.set(i, 0);
        }
        columns.add(c1);

        LongVec c2 = new LongVec(rowNum);
        for (int i = 0; i < rowNum; i++) {
            c2.set(i, 1);
        }
        columns.add(c2);

        return columns;
    }

    private List<Vec> buildDataForCount(int rowNum) {
        List<Vec> columns = new ArrayList<>();

        LongVec c1 = new LongVec(rowNum);
        LongVec c2 = new LongVec(rowNum);
        for (int i = 0; i < rowNum; i++) {
            c1.set(i, 1);
            c2.set(i, 1);
        }

        LongVec c3 = new LongVec(rowNum);
        LongVec c4 = new LongVec(rowNum);
        for (int i = 0; i < rowNum; i++) {
            if (i % 2 == 0) {
                c3.set(i, 1);
                c4.set(i, 1);
            } else {
                c3.setNull(i);
                c4.setNull(i);
            }
        }

        columns.add(c1);
        columns.add(c2);
        columns.add(c3);
        columns.add(c4);

        return columns;
    }
}
