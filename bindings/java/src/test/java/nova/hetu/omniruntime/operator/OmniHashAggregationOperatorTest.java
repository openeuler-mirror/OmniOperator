package nova.hetu.omniruntime.operator;

import com.google.common.collect.ImmutableList;
import nova.hetu.omniruntime.operator.aggregator.OmniHashAggregationOperatorFactory;
import nova.hetu.omniruntime.vector.AggType;
import nova.hetu.omniruntime.vector.LongVec;
import nova.hetu.omniruntime.vector.Vec;
import nova.hetu.omniruntime.vector.VecBatch;
import nova.hetu.omniruntime.vector.VecType;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static java.lang.String.format;

public class OmniHashAggregationOperatorTest
{
    @Test
    public void testExecuteAggMultiplePage()
    {
        int[] groupByChanel = {0, 1};
        VecType[] groupByTypes = {VecType.LONG, VecType.LONG};
        int[] aggChannels = {2, 3};
        VecType[] aggTypes = {VecType.LONG, VecType.LONG};
        AggType[] aggFunctionTypes = {AggType.SUM, AggType.SUM};
        VecType[] aggOutputTypes = {VecType.LONG, VecType.LONG, VecType.LONG, VecType.LONG};

        VecType[] inputTypes = {VecType.LONG, VecType.LONG, VecType.LONG, VecType.LONG};
        OmniHashAggregationOperatorFactory factory = new OmniHashAggregationOperatorFactory(
                groupByChanel, groupByTypes, aggChannels, aggTypes, aggFunctionTypes, aggOutputTypes
        );
        int rowNum = 40000;
        int pageCount = 10;
        int[] rowNums = new int[pageCount];

        ImmutableList.Builder<VecBatch> vecBatchList = ImmutableList.builder();
        List<Vec> inputData = new ArrayList<>();
        for (int i = 0; i < pageCount; i++) {
            inputData.addAll(build4Columns(rowNum));
            VecBatch vecBatch = new VecBatch(build4Columns(rowNum).toArray(new Vec[4]), rowNum);
            vecBatchList.add(vecBatch);
        }

        OmniOperator omniOperator = factory.createOperator();

        omniOperator.addInput(vecBatchList.build());
        VecBatch oneBatch = new VecBatch(build4Columns(rowNum).toArray(new Vec[4]), rowNum);
        omniOperator.addInput(oneBatch);

        // release input data memory
        releaseVecMemory(inputData.toArray(new Vec[0]));

        Iterator<VecBatch> output = omniOperator.getOutput();
        VecBatch vecBatch;
        while (output.hasNext()) {
            vecBatch = output.next();
            if (vecBatch.getVectors().length != aggOutputTypes.length) {
                throw new IllegalArgumentException(format("output vec size error: result size: %s, outputTypes size: %s,rows: %s", vecBatch.getVectors().length, aggOutputTypes.length, vecBatch.getRowCount()));
            }
            Assert.assertNotNull(vecBatch);
            Assert.assertEquals(vecBatch.getVectors().length, 4);
            Vec[] vectors = vecBatch.getVectors();
            Assert.assertEquals(((LongVec) vectors[0]).get(0), 0);
            Assert.assertEquals(((LongVec) vectors[1]).get(0), 0);
            Assert.assertEquals(((LongVec) vectors[2]).get(0), rowNum * (pageCount + 1));
            Assert.assertEquals(((LongVec) vectors[3]).get(0), rowNum * (pageCount + 1));
            releaseVecMemory(vecBatch.getVectors());
        }
    }

    private void releaseVecMemory(Vec[] vecs)
    {
        for (Vec vec : vecs) {
            vec.close();
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
                    int[] groupByChanel = {0, 1};
                    VecType[] groupByTypes = {VecType.LONG, VecType.LONG};
                    int[] aggChannels = {2, 3};
                    VecType[] aggTypes = {VecType.LONG, VecType.LONG};
                    AggType[] aggFunctionTypes = {AggType.SUM, AggType.SUM};
                    VecType[] aggOutputTypes = {VecType.LONG, VecType.LONG, VecType.LONG, VecType.LONG};
                    VecType[] inputTypes = {VecType.LONG, VecType.LONG, VecType.LONG, VecType.LONG};
                    OmniHashAggregationOperatorFactory factory = new OmniHashAggregationOperatorFactory(
                            groupByChanel, groupByTypes, aggChannels, aggTypes, aggFunctionTypes, aggOutputTypes
                    );

                    List<Vec> inputData = new ArrayList<>();
                    ImmutableList.Builder<VecBatch> vecBatchList = ImmutableList.builder();
                    for (int i = 0; i < pageCount; i++) {
                        inputData.addAll(build4Columns(rowNum));
                        vecBatchList.add(new VecBatch(build4Columns(rowNum), rowNum));
                    }

                    OmniOperator omniOperator = factory.createOperator();
                    omniOperator.addInput(vecBatchList.build());
                    // release input data memory
                    releaseVecMemory(inputData.toArray(new Vec[0]));

                    Iterator<VecBatch> output = omniOperator.getOutput();
                    while (output.hasNext()) {
                        VecBatch vecBatch = output.next();
                        if (vecBatch.getVectors().length != aggOutputTypes.length) {
                            throw new IllegalArgumentException(format("output vec size error: result size: %s, outputTypes size: %s,rows: %s", vecBatch.getVectors().length, aggOutputTypes.length, vecBatch.getRowCount()));
                        }

                        Assert.assertNotNull(vecBatch);
                        Assert.assertEquals(vecBatch.getVectors().length, 4);
                        Vec[] vectors = vecBatch.getVectors();
                        Assert.assertEquals(((LongVec) vectors[0]).get(0), 0);
                        Assert.assertEquals(((LongVec) vectors[1]).get(0), 0);
                        Assert.assertEquals(((LongVec) vectors[2]).get(0), rowNum * pageCount);
                        Assert.assertEquals(((LongVec) vectors[3]).get(0), rowNum * pageCount);

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
            Assert.assertTrue(false);
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

    private List<Vec> build2Columns(int rowNum)
    {
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

    private static int[] transformVecType(VecType[] vecTypes)
    {
        int[] vecTypeValue = new int[vecTypes.length];
        for (int idx = 0; idx < vecTypes.length; idx++) {
            vecTypeValue[idx] = vecTypes[idx].getValue();
        }
        return vecTypeValue;
    }
}
