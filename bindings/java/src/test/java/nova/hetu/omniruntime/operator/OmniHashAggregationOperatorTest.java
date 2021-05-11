package nova.hetu.omniruntime.operator;

import nova.hetu.omniruntime.operator.aggregator.JOmniHashAggregationOperator;
import nova.hetu.omniruntime.vector.*;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import static java.lang.String.format;

public class OmniHashAggregationOperatorTest {
    private Vec[][] generateOMVec(OMResult[] result, int[] outputTypes)
    {
        Vec[][] output = new Vec[result.length][outputTypes.length];
        for (int i = 0; i < result.length; ++i) {
            int length = result[i].getLength();

            for (int idx = 0; idx < outputTypes.length; idx++) {
                ByteBuffer vecData = result[i].getBuffers()[idx];
                //TODO: Need Byte Order Configurable
                vecData.order(ByteOrder.LITTLE_ENDIAN);
                switch (outputTypes[idx]) {
                    case 1:
                        output[i][idx] = new IntVec(vecData, length);
                        break;
                    case 2:
                        output[i][idx] = new LongVec(vecData, length);
                        break;
                    case 3:
                        output[i][idx] = new DoubleVec(vecData, length);
                        break;
                    default:
                        throw new IllegalArgumentException(String.format("Not Support Vec Type %s", outputTypes[idx]));
                }
            }
        }
        return output;
    }

    @Test
    public void testExecuteAggOnePage() {
        long operatorId = UUID.randomUUID().getMostSignificantBits() & Long.MAX_VALUE;
        long stageId = UUID.randomUUID().getMostSignificantBits() & Long.MAX_VALUE;
        int totalChannel = 2;
        int[] groupByChanel = {0};
        int[] groupByTypes = {2};
        int[] aggChannels = {1};
        int[] aggTypes = {2};
        int[] aggFunctionTypes = {0};
        int[] aggOutputTypes = {2, 2};
        VecType[] inputTypes = {VecType.LONG, VecType.LONG};
        JOmniHashAggregationOperator.JOmniHashAggregationOperatorFactory factory = JOmniHashAggregationOperator.JOmniHashAggregationOperatorFactory.createJOmniHashAggregationOperatorFactory(
                groupByChanel, groupByTypes,aggChannels,aggTypes,aggFunctionTypes,aggOutputTypes
        );

        JOmniHashAggregationOperator omniOperator = (JOmniHashAggregationOperator)factory.createOmniOperator();

        int rowNum = 10;
        List<Vec> inputData = build2Columns(rowNum);
        omniOperator.addInput(inputData, rowNum, inputTypes);
        // release input data memory
        releaseVecMemory(inputData.toArray(new Vec[0]));

        OMResult[] output = omniOperator.getOutput();
        System.out.println("Native result OMResult number: " + output.length);
        for (int i = 0; i < output.length; ++i) {
            if (output[i].getBuffers().length != aggOutputTypes.length) {
                throw new IllegalArgumentException(format("output vec size error: result size: %s, outputTypes size: %s,rows: %s", output[i].getBuffers().length, aggOutputTypes.length, output[i].getLength()));
            }
        }

        Vec[][] result = generateOMVec(output, aggOutputTypes);
        Assert.assertEquals(result[0].length, 2);
        Assert.assertEquals(((LongVec)result[0][0]).get(0), 0);
        Assert.assertEquals(((LongVec)result[1][0]).get(0), rowNum);
        // release result memory
        for (int i = 0; i < result.length; ++i) {
            releaseVecMemory(result[i]);
        }
    }

    @Test
    public void testExecuteAggMultiplePage() {
        int[] groupByChanel = {0, 1};
        int[] groupByTypes = {2, 2};
        int[] aggChannels = {2, 3};
        int[] aggTypes = {2, 2};
        int[] aggFunctionTypes = {0, 0};
        int[] aggOutputTypes = {2, 2, 2, 2};

        VecType[] inputTypes = {VecType.LONG, VecType.LONG, VecType.LONG, VecType.LONG};
        JOmniHashAggregationOperator.JOmniHashAggregationOperatorFactory factory = JOmniHashAggregationOperator.JOmniHashAggregationOperatorFactory.createJOmniHashAggregationOperatorFactory(
                groupByChanel, groupByTypes,aggChannels,aggTypes,aggFunctionTypes,aggOutputTypes
        );
        int rowNum = 40000;
        int pageCount = 10;

        List<Vec> inputData = new ArrayList<>();
        for (int i = 0; i < pageCount;i++) {
            inputData.addAll(build4Columns(rowNum));
        }

        JOmniHashAggregationOperator omniOperator = (JOmniHashAggregationOperator)factory.createOmniOperator();

        omniOperator.addInput(inputData, rowNum, inputTypes);

        // release input data memory
        releaseVecMemory(inputData.toArray(new Vec[0]));

        OMResult[] output = omniOperator.getOutput();
        System.out.println("Native result OMResult number: " + output.length);
        for (int i = 0; i < output.length; ++i) {
            if (output[i].getBuffers().length != aggOutputTypes.length) {
                throw new IllegalArgumentException(format("output vec size error: result size: %s, outputTypes size: %s,rows: %s", output[i].getBuffers().length, aggOutputTypes.length, output[i].getLength()));
            }
        }
        Vec[][] result = generateOMVec(output, aggOutputTypes);
        Assert.assertEquals(result[0].length, 4);
        Assert.assertEquals(((LongVec)result[0][0]).get(0), 0);
        Assert.assertEquals(((LongVec)result[0][1]).get(0), 0);
        Assert.assertEquals(((LongVec)result[0][2]).get(0), rowNum * pageCount);
        Assert.assertEquals(((LongVec)result[0][3]).get(0), rowNum * pageCount);

        // release result memory
        for (int i = 0; i < result.length; ++i) {
            releaseVecMemory(result[i]);
        }
    }

    private void releaseVecMemory(Vec[] vecs) {
        for (Vec vec: vecs) {
            vec.close();
        }
    }

    @Test
    public void testExecuteAggMultipleThread() {
        int pageCount = 10;
        int threadCount = 10;
        int rowNum = 100;
        multiThreadExecution(threadCount,rowNum, pageCount);
    }

    private void multiThreadExecution( int threadCount, int rowNum, int pageCount)
    {
        CountDownLatch downLatch = new CountDownLatch(threadCount);
        for (int tIdx = 0; tIdx < threadCount; tIdx++) {
            Thread thread = new Thread(() -> {
                try {
                    int[] groupByChanel = {0, 1};
                    int[] groupByTypes = {2, 2};
                    int[] aggChannels = {2, 3};
                    int[] aggTypes = {2, 2};
                    int[] aggFunctionTypes = {0, 0};
                    int[] aggOutputTypes = {2, 2, 2, 2};
                    VecType[] inputTypes = {VecType.LONG, VecType.LONG, VecType.LONG, VecType.LONG};
                    JOmniHashAggregationOperator.JOmniHashAggregationOperatorFactory factory = JOmniHashAggregationOperator.JOmniHashAggregationOperatorFactory.createJOmniHashAggregationOperatorFactory(
                            groupByChanel, groupByTypes,aggChannels,aggTypes,aggFunctionTypes,aggOutputTypes
                    );

                    List<Vec> inputData = new ArrayList<>();
                    for (int i = 0; i < pageCount;i++) {
                        inputData.addAll(build4Columns(rowNum));
                    }

                    JOmniHashAggregationOperator omniOperator = (JOmniHashAggregationOperator)factory.createOmniOperator();
                    omniOperator.addInput(inputData, rowNum, inputTypes);
                    // release input data memory
                    releaseVecMemory(inputData.toArray(new Vec[0]));


                    OMResult[] output = omniOperator.getOutput();
                    System.out.println("Native result OMResult number: " + output.length);
                    for (int i = 0; i < output.length; ++i) {
                        if (output[i].getBuffers().length != aggOutputTypes.length) {
                            throw new IllegalArgumentException(format("output vec size error: result size: %s, outputTypes size: %s,rows: %s", output[i].getBuffers().length, aggOutputTypes.length, output[i].getLength()));
                        }
                    }
                    Vec[][] result = generateOMVec(output, aggOutputTypes);
                    Assert.assertEquals(result[0].length, 4);
                    Assert.assertEquals(((LongVec)result[0][0]).get(0), 0);
                    Assert.assertEquals(((LongVec)result[0][1]).get(0), 0);
                    Assert.assertEquals(((LongVec)result[0][2]).get(0), rowNum * pageCount);
                    Assert.assertEquals(((LongVec)result[0][3]).get(0), rowNum * pageCount);

                    // release result memory
                    for (int i = 0; i < result.length; ++i) {
                        releaseVecMemory(result[i]);
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
            Assert.assertEquals(true,false);
        }
    }

    private List<Vec> build4Columns(int rowNum) {
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
}
