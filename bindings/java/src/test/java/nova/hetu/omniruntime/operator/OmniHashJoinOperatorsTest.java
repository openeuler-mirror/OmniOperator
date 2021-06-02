package nova.hetu.omniruntime.operator;

import com.google.common.collect.ImmutableList;
import nova.hetu.omniruntime.operator.join.OmniHashBuilderOperatorFactory;
import nova.hetu.omniruntime.operator.join.OmniLookupJoinOperatorFactory;
import nova.hetu.omniruntime.vector.LongVec;
import nova.hetu.omniruntime.vector.Vec;
import nova.hetu.omniruntime.vector.VecBatch;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Iterator;

public class OmniHashJoinOperatorsTest
{
    @Test
    public void testInnerHashJoinOneColumn1()
    {
        long[] buildData1 = {1, 2, 1, 2, 3, 4, 5, 6, 7, 1};
        long[] buildData2 = {79, 79, 70, 70, 70, 70, 70, 70, 70, 70};
        LongVec buildVec1 = new LongVec(10);
        LongVec buildVec2 = new LongVec(10);
        for (int i = 0; i < 10; i++) {
            buildVec1.set(i, buildData1[i]);
            buildVec2.set(i, buildData2[i]);
        }
        VecBatch buildVecBatch = new VecBatch(new Vec[] {buildVec1, buildVec2}, 10);

        int[] buildTypes= {2, 2};
        int[] buildOutputCols = {1};
        int[] buildJoinCols = {0};
        int operatorCount = 1;
        OmniHashBuilderOperatorFactory hashBuilderOperatorFactory = new OmniHashBuilderOperatorFactory(buildTypes,
                buildOutputCols, buildJoinCols, operatorCount);
        OmniOperator hashBuilderOperator = hashBuilderOperatorFactory.createOperator();
        hashBuilderOperator.addInput(ImmutableList.of(buildVecBatch));

        long[] probeData1 = {1, 2, 3, 4, 5, 6, 1, 1, 2, 3};
        long[] probeData2 = {78, 78, 78, 78, 78, 78, 78, 82, 82, 65};
        LongVec probeVec1 = new LongVec(10);
        LongVec probeVec2 = new LongVec(10);
        for (int i = 0; i < 10; i++) {
            probeVec1.set(i, probeData1[i]);
            probeVec2.set(i, probeData2[i]);
        }
        VecBatch probeVecBatch = new VecBatch(new Vec[] {probeVec1, probeVec2}, 10);

        int[] probeTypes = {2, 2};
        int[] probeOutputCols = {1};
        int[] probeHashCols = {0};
        int[] buildOutputTypes = {2};
        long nativeHashBuilderOperatorFactory = hashBuilderOperatorFactory.getNativeOperatorFactory();
        OmniLookupJoinOperatorFactory lookupJoinOperatorFactory = new OmniLookupJoinOperatorFactory(
                probeTypes, probeOutputCols, probeHashCols, buildOutputCols, buildOutputTypes, nativeHashBuilderOperatorFactory);
        OmniOperator lookupJoinOperator = lookupJoinOperatorFactory.createOperator();
        lookupJoinOperator.addInput(ImmutableList.of(probeVecBatch));
        Iterator<VecBatch> results = lookupJoinOperator.getOutput();

        results.hasNext();
        VecBatch resultVecBatch = results.next();
        ByteBuffer output0 = resultVecBatch.getVectors()[0].getData();
        ByteBuffer output1 = resultVecBatch.getVectors()[1].getData();
        output0.order(ByteOrder.LITTLE_ENDIAN);
        output1.order(ByteOrder.LITTLE_ENDIAN);

        int len = resultVecBatch.getRowCount();
        Assert.assertEquals(len, 18);

        long[] actual0 = new long[len];
        long[] actual1 = new long[len];
        for (int i = 0; i < len; i++) {
            actual0[i] = output0.getLong(i * Long.BYTES);
            actual1[i] = output1.getLong(i * Long.BYTES);
        }

        long[] expected0 = {78, 78, 78, 78, 78, 78, 78, 78, 78, 78, 78, 78, 82, 82, 82, 82, 82, 65};
        long[] expected1 = {70, 70, 79, 70, 79, 70, 70, 70, 70, 70, 70, 79, 70, 70, 79, 70, 79, 70};
        Assert.assertEquals(actual0, expected0);
        Assert.assertEquals(actual1, expected1);
    }

    @Test
    public void testInnerHashJoinOneColumn2()
    {
        long[] buildData11 = {1, 1, 3, 6, 7, 1};
        long[] buildData12 = {79, 70, 70, 70, 70, 70};
        LongVec buildVec11 = new LongVec(6);
        LongVec buildVec12 = new LongVec(6);
        for (int i = 0; i < 6; i++) {
            buildVec11.set(i, buildData11[i]);
            buildVec12.set(i, buildData12[i]);
        }
        VecBatch buildVecBatch1 = new VecBatch(new Vec[] {buildVec11, buildVec12}, 6);

        long[] buildData21 = {2, 2, 4, 5};
        long[] buildData22 = {79, 70, 70, 70};
        LongVec buildVec21 = new LongVec(4);
        LongVec buildVec22 = new LongVec(4);
        for (int i = 0; i < 6; i++) {
            buildVec21.set(i, buildData21[i]);
            buildVec22.set(i, buildData22[i]);
        }
        VecBatch buildVecBatch2 = new VecBatch(new Vec[] {buildVec21, buildVec22}, 4);

        int[] buildTypes= {2, 2};
        int[] buildOutputCols = {1};
        int[] buildJoinCols = {0};
        int operatorCount = 2;
        OmniHashBuilderOperatorFactory hashBuilderOperatorFactory = new OmniHashBuilderOperatorFactory(buildTypes,
                buildOutputCols, buildJoinCols, operatorCount);
        OmniOperator hashBuilderOperator1 = hashBuilderOperatorFactory.createOperator();
        hashBuilderOperator1.addInput(ImmutableList.of(buildVecBatch1));

        OmniOperator hashBuilderOperator2 = hashBuilderOperatorFactory.createOperator();
        hashBuilderOperator2.addInput(ImmutableList.of(buildVecBatch2));

        long[] probeData1 = {1, 2, 3, 4, 5, 6, 1, 1, 2, 3};
        long[] probeData2 = {78, 78, 78, 78, 78, 78, 78, 82, 82, 65};
        LongVec probeVec1 = new LongVec(10);
        LongVec probeVec2 = new LongVec(10);
        for (int i = 0; i < 10; i++) {
            probeVec1.set(i, probeData1[i]);
            probeVec2.set(i, probeData2[i]);
        }
        VecBatch probeVecBatch = new VecBatch(new Vec[] {probeVec1, probeVec2}, 10);

        int[] probeTypes = {2, 2};
        int[] probeOutputCols = {1};
        int[] probeHashCols = {0};
        int[] buildOutputTypes = {2};
        long nativeHashBuilderOperatorFactory = hashBuilderOperatorFactory.getNativeOperatorFactory();
        OmniLookupJoinOperatorFactory lookupJoinOperatorFactory = new OmniLookupJoinOperatorFactory(
                probeTypes, probeOutputCols, probeHashCols, buildOutputCols, buildOutputTypes, nativeHashBuilderOperatorFactory);
        OmniOperator lookupJoinOperator = lookupJoinOperatorFactory.createOperator();
        lookupJoinOperator.addInput(ImmutableList.of(probeVecBatch));
        Iterator<VecBatch> results = lookupJoinOperator.getOutput();

        results.hasNext();
        VecBatch resultVecBatch = results.next();
        ByteBuffer output0 = resultVecBatch.getVectors()[0].getData();
        ByteBuffer output1 = resultVecBatch.getVectors()[1].getData();
        output0.order(ByteOrder.LITTLE_ENDIAN);
        output1.order(ByteOrder.LITTLE_ENDIAN);

        int len = resultVecBatch.getRowCount();
        Assert.assertEquals(len, 18);

        long[] actual0 = new long[len];
        long[] actual1 = new long[len];
        for (int i = 0; i < len; i++) {
            actual0[i] = output0.getLong(i * Long.BYTES);
            actual1[i] = output1.getLong(i * Long.BYTES);
        }

        long[] expected0 = {78, 78, 78, 78, 78, 78, 78, 78, 78, 78, 78, 78, 82, 82, 82, 82, 82, 65};
        long[] expected1 = {70, 70, 79, 70, 79, 70, 70, 70, 70, 70, 70, 79, 70, 70, 79, 70, 79, 70};
        Assert.assertEquals(actual0, expected0);
        Assert.assertEquals(actual1, expected1);
    }
}
