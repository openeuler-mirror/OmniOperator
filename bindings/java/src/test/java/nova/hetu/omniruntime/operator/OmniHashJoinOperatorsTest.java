package nova.hetu.omniruntime.operator;

import static nova.hetu.omniruntime.constants.VecType.OMNI_VEC_TYPE_DICTIONARY;
import static nova.hetu.omniruntime.constants.VecType.OMNI_VEC_TYPE_LONG;
import static org.testng.Assert.assertEquals;

import nova.hetu.omniruntime.constants.VecType;
import nova.hetu.omniruntime.operator.join.OmniHashBuilderOperatorFactory;
import nova.hetu.omniruntime.operator.join.OmniLookupJoinOperatorFactory;
import nova.hetu.omniruntime.vector.DictionaryVec;
import nova.hetu.omniruntime.vector.LongVec;
import nova.hetu.omniruntime.vector.Vec;
import nova.hetu.omniruntime.vector.VecBatch;

import org.testng.annotations.Test;

import java.util.Iterator;

/**
 * The type Omni hash join operators test.
 */
public class OmniHashJoinOperatorsTest {
    /**
     * Test inner hash join one column 1.
     */
    @Test
    public void testInnerHashJoinOneColumn1() {
        long[] buildData1 = {1, 2, 1, 2, 3, 4, 5, 6, 7, 1};
        long[] buildData2 = {79, 79, 70, 70, 70, 70, 70, 70, 70, 70};
        LongVec buildVec1 = new LongVec(10);
        LongVec buildVec2 = new LongVec(10);
        for (int i = 0; i < 10; i++) {
            buildVec1.set(i, buildData1[i]);
            buildVec2.set(i, buildData2[i]);
        }
        VecBatch buildVecBatch = new VecBatch(new Vec[] {buildVec1, buildVec2});

        VecType[] buildTypes = {OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_LONG};
        int[] buildOutputCols = {1};
        int[] buildJoinCols = {0};
        int operatorCount = 1;
        OmniHashBuilderOperatorFactory hashBuilderOperatorFactory = new OmniHashBuilderOperatorFactory(buildTypes,
            buildOutputCols, buildJoinCols, operatorCount);
        OmniOperator hashBuilderOperator = hashBuilderOperatorFactory.createOperator();
        hashBuilderOperator.addInput(buildVecBatch);
        hashBuilderOperator.getOutput();

        long[] probeData1 = {1, 2, 3, 4, 5, 6, 1, 1, 2, 3};
        long[] probeData2 = {78, 78, 78, 78, 78, 78, 78, 82, 82, 65};
        LongVec probeVec1 = new LongVec(10);
        LongVec probeVec2 = new LongVec(10);
        for (int i = 0; i < 10; i++) {
            probeVec1.set(i, probeData1[i]);
            probeVec2.set(i, probeData2[i]);
        }
        VecBatch probeVecBatch = new VecBatch(new Vec[] {probeVec1, probeVec2});

        assertResult(buildOutputCols, hashBuilderOperatorFactory, probeVecBatch);
    }

    private void assertResult(int[] buildOutputCols, OmniHashBuilderOperatorFactory hashBuilderOperatorFactory,
        VecBatch probeVecBatch) {
        VecType[] probeTypes = {OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_LONG};
        int[] probeOutputCols = {1};
        int[] probeHashCols = {0};
        VecType[] buildOutputTypes = {OMNI_VEC_TYPE_LONG};
        OmniLookupJoinOperatorFactory lookupJoinOperatorFactory = new OmniLookupJoinOperatorFactory(probeTypes,
            probeOutputCols, probeHashCols, buildOutputCols, buildOutputTypes, hashBuilderOperatorFactory);
        OmniOperator lookupJoinOperator = lookupJoinOperatorFactory.createOperator();
        lookupJoinOperator.addInput(probeVecBatch);
        Iterator<VecBatch> results = lookupJoinOperator.getOutput();

        VecBatch resultVecBatch = results.next();
        int len = resultVecBatch.getRowCount();
        assertEquals(len, 18);

        Vec actualVec0 = resultVecBatch.getVectors()[0];
        Vec actualVec1 = resultVecBatch.getVectors()[1];
        long[] actual0 = new long[len];
        long[] actual1 = new long[len];
        for (int i = 0; i < len; i++) {
            if (actualVec0.getType().equals(OMNI_VEC_TYPE_DICTIONARY)) {
                actual0[i] = ((DictionaryVec) actualVec0).getLong(i);
            } else {
                actual0[i] = ((LongVec) actualVec0).get(i);
            }
            actual1[i] = ((LongVec) actualVec1).get(i);
        }

        long[] expected0 = {78, 78, 78, 78, 78, 78, 78, 78, 78, 78, 78, 78, 82, 82, 82, 82, 82, 65};
        long[] expected1 = {70, 70, 79, 70, 79, 70, 70, 70, 70, 70, 70, 79, 70, 70, 79, 70, 79, 70};
        assertEquals(actual0, expected0);
        assertEquals(actual1, expected1);
    }

    /**
     * Test inner hash join one column 2.
     */
    @Test
    public void testInnerHashJoinOneColumn2() {
        long[] buildData11 = {1, 1, 3, 6, 7, 1};
        long[] buildData12 = {79, 70, 70, 70, 70, 70};
        LongVec buildVec11 = new LongVec(6);
        LongVec buildVec12 = new LongVec(6);
        for (int i = 0; i < buildData11.length; i++) {
            buildVec11.set(i, buildData11[i]);
            buildVec12.set(i, buildData12[i]);
        }
        VecBatch buildVecBatch1 = new VecBatch(new Vec[] {buildVec11, buildVec12});

        long[] buildData21 = {2, 2, 4, 5};
        long[] buildData22 = {79, 70, 70, 70};
        LongVec buildVec21 = new LongVec(4);
        LongVec buildVec22 = new LongVec(4);
        for (int i = 0; i < buildData21.length; i++) {
            buildVec21.set(i, buildData21[i]);
            buildVec22.set(i, buildData22[i]);
        }
        VecBatch buildVecBatch2 = new VecBatch(new Vec[] {buildVec21, buildVec22});

        VecType[] buildTypes = {OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_LONG};
        int[] buildOutputCols = {1};
        int[] buildJoinCols = {0};
        int operatorCount = 2;
        OmniHashBuilderOperatorFactory hashBuilderOperatorFactory = new OmniHashBuilderOperatorFactory(buildTypes,
            buildOutputCols, buildJoinCols, operatorCount);
        OmniOperator hashBuilderOperator1 = hashBuilderOperatorFactory.createOperator();
        hashBuilderOperator1.addInput(buildVecBatch1);
        hashBuilderOperator1.getOutput();

        OmniOperator hashBuilderOperator2 = hashBuilderOperatorFactory.createOperator();
        hashBuilderOperator2.addInput(buildVecBatch2);
        hashBuilderOperator2.getOutput();

        long[] probeData1 = {1, 2, 3, 4, 5, 6, 1, 1, 2, 3};
        long[] probeData2 = {78, 78, 78, 78, 78, 78, 78, 82, 82, 65};
        LongVec probeVec1 = new LongVec(10);
        LongVec probeVec2 = new LongVec(10);
        for (int i = 0; i < 10; i++) {
            probeVec1.set(i, probeData1[i]);
            probeVec2.set(i, probeData2[i]);
        }
        VecBatch probeVecBatch = new VecBatch(new Vec[] {probeVec1, probeVec2});

        assertResult(buildOutputCols, hashBuilderOperatorFactory, probeVecBatch);
    }
}
