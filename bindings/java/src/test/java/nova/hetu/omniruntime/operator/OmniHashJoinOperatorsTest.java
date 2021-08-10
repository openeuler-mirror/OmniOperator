package nova.hetu.omniruntime.operator;

import static nova.hetu.omniruntime.constants.JoinType.OMNI_JOIN_TYPE_INNER;
import static nova.hetu.omniruntime.constants.JoinType.OMNI_JOIN_TYPE_LEFT;
import static nova.hetu.omniruntime.util.TestUtils.assertVecBatchEquals;
import static nova.hetu.omniruntime.util.TestUtils.createVecBatch;
import static org.testng.Assert.assertEquals;

import nova.hetu.omniruntime.type.LongVecType;
import nova.hetu.omniruntime.type.VarcharVecType;
import nova.hetu.omniruntime.type.VecType;
import nova.hetu.omniruntime.operator.join.OmniHashBuilderOperatorFactory;
import nova.hetu.omniruntime.operator.join.OmniLookupJoinOperatorFactory;
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
        VecType[] buildTypes = {LongVecType.LONG, LongVecType.LONG};
        Object[][] buildDatas = {{1L, 2L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 1L},
                {79L, 79L, 70L, 70L, 70L, 70L, 70L, 70L, 70L, 70L}};
        VecBatch buildVecBatch = createVecBatch(buildTypes, buildDatas);

        int[] buildOutputCols = {1};
        int[] buildJoinCols = {0};
        int operatorCount = 1;
        OmniHashBuilderOperatorFactory hashBuilderOperatorFactory = new OmniHashBuilderOperatorFactory(buildTypes,
            buildOutputCols, buildJoinCols, operatorCount);
        OmniOperator hashBuilderOperator = hashBuilderOperatorFactory.createOperator();
        hashBuilderOperator.addInput(buildVecBatch);
        hashBuilderOperator.getOutput();

        VecType[] probeTypes = {LongVecType.LONG, LongVecType.LONG};
        Object[][] probeDatas = {{1L, 2L, 3L, 4L, 5L, 6L, 1L, 1L, 2L, 3L},
                {78L, 78L, 78L, 78L, 78L, 78L, 78L, 82L, 82L, 65L}};
        VecBatch probeVecBatch = createVecBatch(probeTypes, probeDatas);

        int[] probeOutputCols = {1};
        int[] probeHashCols = {0};
        VecType[] buildOutputTypes = {LongVecType.LONG};
        OmniLookupJoinOperatorFactory lookupJoinOperatorFactory = new OmniLookupJoinOperatorFactory(probeTypes,
                probeOutputCols, probeHashCols, buildOutputCols, buildOutputTypes, OMNI_JOIN_TYPE_INNER, hashBuilderOperatorFactory);
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
        probeVecBatch.close();
        buildVecBatch.close();
        resultVecBatch.close();
    }

    /**
     * Test inner hash join one column 2.
     */
    @Test
    public void testInnerHashJoinOneColumn2() {
        VecType[] buildTypes = {LongVecType.LONG, LongVecType.LONG};
        Object[][] buildDatas1 = {{1L, 1L, 3L, 6L, 7L, 1L}, {79L, 70L, 70L, 70L, 70L, 70L}};
        VecBatch buildVecBatch1 = createVecBatch(buildTypes, buildDatas1);
        Object[][] buildDatas2 = {{2L, 2L, 4L, 5L}, {79L, 70L, 70L, 70L}};
        VecBatch buildVecBatch2 = createVecBatch(buildTypes, buildDatas2);

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

        VecType[] probeTypes = {LongVecType.LONG, LongVecType.LONG};
        Object[][] probeDatas = {{1L, 2L, 3L, 4L, 5L, 6L, 1L, 1L, 2L, 3L},
                {78L, 78L, 78L, 78L, 78L, 78L, 78L, 82L, 82L, 65L}};
        VecBatch probeVecBatch = createVecBatch(probeTypes, probeDatas);

        int[] probeOutputCols = {1};
        int[] probeHashCols = {0};
        VecType[] buildOutputTypes = {LongVecType.LONG};
        OmniLookupJoinOperatorFactory lookupJoinOperatorFactory = new OmniLookupJoinOperatorFactory(probeTypes,
                probeOutputCols, probeHashCols, buildOutputCols, buildOutputTypes, OMNI_JOIN_TYPE_INNER, hashBuilderOperatorFactory);
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
        probeVecBatch.close();
        buildVecBatch1.close();
        buildVecBatch2.close();
        resultVecBatch.close();
    }

    @Test
    public void testLeftHashEqualityJoin() {
        VecType[] buildTypes = {LongVecType.LONG, LongVecType.LONG};
        Object[][] buildDatas = {{1L, 2L, 3L, 4L}, {111L, 11L, 333L, 33L}};
        VecBatch buildVecBatch = createVecBatch(buildTypes, buildDatas);

        int[] buildOutputCols = {0, 1};
        int[] buildJoinCols = {1};
        int operatorCount = 1;
        OmniHashBuilderOperatorFactory hashBuilderOperatorFactory = new OmniHashBuilderOperatorFactory(buildTypes,
                buildOutputCols, buildJoinCols, operatorCount);
        OmniOperator hashBuilderOperator = hashBuilderOperatorFactory.createOperator();
        hashBuilderOperator.addInput(buildVecBatch);
        hashBuilderOperator.getOutput();

        VecType[] probeTypes = {LongVecType.LONG, LongVecType.LONG};
        Object[][] probeDatas = {{1L, 2L, 3L, 4L}, {11L, 22L, 33L, 44L}};
        VecBatch probeVecBatch = createVecBatch(probeTypes, probeDatas);

        int[] probeOutputCols = {0, 1};
        int[] probeHashCols = {1};
        VecType[] buildOutputTypes = {LongVecType.LONG, LongVecType.LONG};
        OmniLookupJoinOperatorFactory lookupJoinOperatorFactory = new OmniLookupJoinOperatorFactory(
                probeTypes, probeOutputCols, probeHashCols, buildOutputCols, buildOutputTypes, OMNI_JOIN_TYPE_LEFT, hashBuilderOperatorFactory);
        OmniOperator lookupJoinOperator = lookupJoinOperatorFactory.createOperator();
        lookupJoinOperator.addInput(probeVecBatch);
        Iterator<VecBatch> results = lookupJoinOperator.getOutput();

        VecBatch resultVecBatch = results.next();
        int len = resultVecBatch.getRowCount();
        assertEquals(len, 4);
        assertEquals(resultVecBatch.getVectorCount(), 4);
        Object[][] expectedDatas = {
                {1L, 2L, 3L, 4L}, {11L, 22L, 33L, 44L}, {2L, null, 4L, null}, {11L, null, 33L, null}
        };
        assertVecBatchEquals(resultVecBatch, expectedDatas);
        probeVecBatch.close();
        buildVecBatch.close();
        resultVecBatch.close();
    }

    @Test
    public void testLeftHashEqualityJoinVarchar() {
        VecType[] buildTypes = {LongVecType.LONG, new VarcharVecType(3)};
        Object[][] buildDatas = {{1L, 2L, 3L, 4L}, {"aaa", "11", "ccc", "33"}};
        VecBatch buildVecBatch = createVecBatch(buildTypes, buildDatas);

        int[] buildOutputCols = {0, 1};
        int[] buildJoinCols = {1};
        int operatorCount = 1;
        OmniHashBuilderOperatorFactory hashBuilderOperatorFactory = new OmniHashBuilderOperatorFactory(buildTypes,
                buildOutputCols, buildJoinCols, operatorCount);
        OmniOperator hashBuilderOperator = hashBuilderOperatorFactory.createOperator();
        hashBuilderOperator.addInput(buildVecBatch);
        hashBuilderOperator.getOutput();

        VecType[] probeTypes = {LongVecType.LONG, new VarcharVecType(2)};
        Object[][] probeDatas = {{1L, 2L, 3L, 4L}, {"11", "22", "33", "44"}};
        VecBatch probeVecBatch = createVecBatch(probeTypes, probeDatas);

        int[] probeOutputCols = {0, 1};
        int[] probeHashCols = {1};
        VecType[] buildOutputTypes = {LongVecType.LONG, new VarcharVecType(2)};
        OmniLookupJoinOperatorFactory lookupJoinOperatorFactory = new OmniLookupJoinOperatorFactory(
                probeTypes, probeOutputCols, probeHashCols, buildOutputCols, buildOutputTypes, OMNI_JOIN_TYPE_LEFT, hashBuilderOperatorFactory);
        OmniOperator lookupJoinOperator = lookupJoinOperatorFactory.createOperator();
        lookupJoinOperator.addInput(probeVecBatch);
        Iterator<VecBatch> results = lookupJoinOperator.getOutput();

        VecBatch resultVecBatch = results.next();
        int len = resultVecBatch.getRowCount();
        assertEquals(len, 4);
        assertEquals(resultVecBatch.getVectorCount(), 4);
        Object[][] expectedDatas = {
                {1L, 2L, 3L, 4L}, {"11", "22", "33", "44"}, {2L, null, 4L, null}, {"11", null, "33", null}
        };
        assertVecBatchEquals(resultVecBatch, expectedDatas);
        probeVecBatch.close();
        buildVecBatch.close();
        resultVecBatch.close();
    }
}
