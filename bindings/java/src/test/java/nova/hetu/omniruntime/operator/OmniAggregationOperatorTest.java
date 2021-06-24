package nova.hetu.omniruntime.operator;

import com.google.common.collect.ImmutableList;
import nova.hetu.omniruntime.constants.AggType;
import nova.hetu.omniruntime.constants.VecType;
import nova.hetu.omniruntime.operator.aggregator.OmniAggregationOperatorFactory;
import nova.hetu.omniruntime.vector.LongVec;
import nova.hetu.omniruntime.vector.Vec;
import nova.hetu.omniruntime.vector.VecBatch;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static java.lang.String.format;
import static nova.hetu.omniruntime.constants.AggType.OMNI_AGGREGATION_TYPE_SUM;
import static nova.hetu.omniruntime.constants.VecType.OMNI_VEC_TYPE_LONG;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.fail;

public class OmniAggregationOperatorTest
{
    @Test
    public void testExecuteAggMultiplePage()
    {
        VecType[] aggTypes = {OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_LONG};
        AggType[] aggFunctionTypes = {OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_SUM};
        VecType[] aggOutputTypes = {OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_LONG};
        OmniAggregationOperatorFactory factory = new OmniAggregationOperatorFactory(
                aggTypes, aggFunctionTypes, aggOutputTypes);

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
                throw new IllegalArgumentException(format("output vec size error: result size: %s, outputTypes size: %s,rows: %s", vecBatch.getVectors().length, aggOutputTypes.length, vecBatch.getRowCount()));
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

    @Test
    public void testExecuteAggMultipleThread()
    {
        int pageCount = 10;
        int threadCount = 10;
        int rowNum = 100;
        multiThreadExecution(threadCount, rowNum, pageCount);
    }

    private void multiThreadExecution(int threadCount, int rowNum, int pageCount)
    {
        CountDownLatch downLatch = new CountDownLatch(threadCount);
        for (int tIdx = 0; tIdx < threadCount; tIdx++) {
            Thread thread = new Thread(() -> {
                try {
                    VecType[] aggTypes = {OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_LONG};
                    AggType[] aggFunctionTypes = {OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_SUM};
                    VecType[] aggOutputTypes = {OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_LONG};
                    OmniAggregationOperatorFactory factory = new OmniAggregationOperatorFactory(
                            aggTypes, aggFunctionTypes, aggOutputTypes);

                    List<Vec> inputData = new ArrayList<>();
                    ImmutableList.Builder<VecBatch> vecBatchList = ImmutableList.builder();
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
                            throw new IllegalArgumentException(format("output vec size error: result size: %s, outputTypes size: %s,rows: %s", vecBatch.getVectors().length, aggOutputTypes.length, vecBatch.getRowCount()));
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
                finally {
                    downLatch.countDown();
                }
            });
            thread.setName("thread-" + tIdx);
            thread.start();
        }
        try {
            downLatch.await();
        }
        catch (InterruptedException ex) {
            fail();
        }
    }

    private List<Vec> build4Columns(int rowNum)
    {
        List<Vec> columns = new ArrayList<>();

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

        columns.add(c1);
        columns.add(c2);
        columns.add(c3);
        columns.add(c4);

        return columns;
    }

    private void releaseVecMemory(Vec[] vecs)
    {
        for (Vec vec : vecs) {
            vec.close();
        }
    }
}
