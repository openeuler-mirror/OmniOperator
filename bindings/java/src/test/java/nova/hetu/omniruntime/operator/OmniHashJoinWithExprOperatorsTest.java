package nova.hetu.omniruntime.operator;

import static nova.hetu.omniruntime.constants.JoinType.OMNI_JOIN_TYPE_INNER;
import static nova.hetu.omniruntime.util.TestUtils.assertVecBatchEquals;
import static nova.hetu.omniruntime.util.TestUtils.createVecBatch;
import static nova.hetu.omniruntime.util.TestUtils.freeVecBatch;

import static org.testng.Assert.assertEquals;

import nova.hetu.omniruntime.operator.join.OmniHashBuilderWithExprOperatorFactory;
import nova.hetu.omniruntime.operator.join.OmniLookupJoinWithExprOperatorFactory;
import nova.hetu.omniruntime.type.LongVecType;
import nova.hetu.omniruntime.type.VecType;
import nova.hetu.omniruntime.util.TestUtils;
import nova.hetu.omniruntime.vector.Vec;
import nova.hetu.omniruntime.vector.VecBatch;

import org.testng.annotations.Test;

import java.util.Iterator;
import java.util.Optional;

public class OmniHashJoinWithExprOperatorsTest {
    /**
     * Test inner hash join one column .
     */
    @Test
    public void testInnerHashJoinOneColumn() {
        VecType[] buildTypes = {LongVecType.LONG, LongVecType.LONG};
        Object[][] buildDatas = {
            {1L, 2L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 1L}, {79L, 79L, 70L, 70L, 70L, 70L, 70L, 70L, 70L, 70L}
        };
        VecBatch buildVecBatch = createVecBatch(buildTypes, buildDatas);

        String[] buildHashKeys = {"ADD:long(#0, 50)"};
        int operatorCount = 1;
        OmniHashBuilderWithExprOperatorFactory hashBuilderOperatorFactory =
                new OmniHashBuilderWithExprOperatorFactory(buildTypes, buildHashKeys, Optional.empty(), operatorCount);
        OmniOperator hashBuilderOperator = hashBuilderOperatorFactory.createOperator();
        hashBuilderOperator.addInput(buildVecBatch);
        hashBuilderOperator.getOutput();

        VecType[] probeTypes = {LongVecType.LONG, LongVecType.LONG};
        Object[][] probeDatas = {
            {1L, 2L, 3L, 4L, 5L, 6L, 1L, 1L, 2L, 3L}, {78L, 78L, 78L, 78L, 78L, 78L, 78L, 82L, 82L, 65L}
        };
        VecBatch probeVecBatch = createVecBatch(probeTypes, probeDatas);

        int[] probeOutputCols = {1};
        String[] probeHashKeys = {"ADD:long(#0, 50)"};
        int[] buildOutputCols = {1};
        VecType[] buildOutputTypes = {LongVecType.LONG};
        OmniLookupJoinWithExprOperatorFactory lookupJoinOperatorFactory =
                new OmniLookupJoinWithExprOperatorFactory(
                        probeTypes,
                        probeOutputCols,
                        probeHashKeys,
                        buildOutputCols,
                        buildOutputTypes,
                        OMNI_JOIN_TYPE_INNER,
                        hashBuilderOperatorFactory);
        OmniOperator lookupJoinOperator = lookupJoinOperatorFactory.createOperator();
        lookupJoinOperator.addInput(probeVecBatch);
        Iterator<VecBatch> results = lookupJoinOperator.getOutput();
        VecBatch resultVecBatch = results.next();

        int len = resultVecBatch.getRowCount();
        assertEquals(len, 18);
        Object[][] expectedDatas = {
            {78L, 78L, 78L, 78L, 78L, 78L, 78L, 78L, 78L, 78L, 78L, 78L, 82L, 82L, 82L, 82L, 82L, 65L},
            {70L, 70L, 79L, 70L, 79L, 70L, 70L, 70L, 70L, 70L, 70L, 79L, 70L, 70L, 79L, 70L, 79L, 70L}
        };
        assertVecBatchEquals(resultVecBatch, expectedDatas);
        freeVecBatch(probeVecBatch);
        freeVecBatch(buildVecBatch);
        freeVecBatch(resultVecBatch);
    }

    /**
     * Test inner hash join one dictionary column .
     */
    @Test
    public void testInnerHashJoinOneDictionaryColumn() {
        VecType[] buildTypes = {LongVecType.LONG, LongVecType.LONG};
        Object[][] buildDatas = {
            {1L, 2L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 1L}, {79L, 79L, 70L, 70L, 70L, 70L, 70L, 70L, 70L, 70L}
        };
        Vec buildVecs[] = new Vec[2];
        int[] ids = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        buildVecs[0] = TestUtils.createDictionaryVec(buildTypes[0], buildDatas[0], ids);
        buildVecs[1] = TestUtils.createDictionaryVec(buildTypes[1], buildDatas[1], ids);
        VecBatch buildVecBatch = new VecBatch(buildVecs);

        String[] buildHashKeys = {"ADD:long(#0, 50)"};
        int operatorCount = 1;
        OmniHashBuilderWithExprOperatorFactory hashBuilderOperatorFactory =
                new OmniHashBuilderWithExprOperatorFactory(buildTypes, buildHashKeys, Optional.empty(), operatorCount);
        OmniOperator hashBuilderOperator = hashBuilderOperatorFactory.createOperator();
        hashBuilderOperator.addInput(buildVecBatch);
        hashBuilderOperator.getOutput();

        VecType[] probeTypes = {LongVecType.LONG, LongVecType.LONG};
        Object[][] probeDatas = {
            {1L, 2L, 3L, 4L, 5L, 6L, 1L, 1L, 2L, 3L}, {78L, 78L, 78L, 78L, 78L, 78L, 78L, 82L, 82L, 65L}
        };
        Vec probeVecs[] = new Vec[2];
        probeVecs[0] = TestUtils.createDictionaryVec(probeTypes[0], probeDatas[0], ids);
        probeVecs[1] = TestUtils.createDictionaryVec(probeTypes[1], probeDatas[1], ids);
        VecBatch probeVecBatch = new VecBatch(probeVecs);

        int[] probeOutputCols = {1};
        String[] probeHashKeys = {"ADD:long(#0, 50)"};
        int[] buildOutputCols = {1};
        VecType[] buildOutputTypes = {LongVecType.LONG};
        OmniLookupJoinWithExprOperatorFactory lookupJoinOperatorFactory =
                new OmniLookupJoinWithExprOperatorFactory(
                        probeTypes,
                        probeOutputCols,
                        probeHashKeys,
                        buildOutputCols,
                        buildOutputTypes,
                        OMNI_JOIN_TYPE_INNER,
                        hashBuilderOperatorFactory);
        OmniOperator lookupJoinOperator = lookupJoinOperatorFactory.createOperator();
        lookupJoinOperator.addInput(probeVecBatch);
        Iterator<VecBatch> results = lookupJoinOperator.getOutput();
        VecBatch resultVecBatch = results.next();

        int len = resultVecBatch.getRowCount();
        assertEquals(len, 18);
        Object[][] expectedDatas = {
            {78L, 78L, 78L, 78L, 78L, 78L, 78L, 78L, 78L, 78L, 78L, 78L, 82L, 82L, 82L, 82L, 82L, 65L},
            {70L, 70L, 79L, 70L, 79L, 70L, 70L, 70L, 70L, 70L, 70L, 79L, 70L, 70L, 79L, 70L, 79L, 70L}
        };
        assertVecBatchEquals(resultVecBatch, expectedDatas);
        freeVecBatch(probeVecBatch);
        freeVecBatch(buildVecBatch);
        freeVecBatch(resultVecBatch);
    }
}
