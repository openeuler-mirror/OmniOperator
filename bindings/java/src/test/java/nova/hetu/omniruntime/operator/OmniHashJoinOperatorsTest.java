package nova.hetu.omniruntime.operator;

import static nova.hetu.omniruntime.constants.JoinType.OMNI_JOIN_TYPE_INNER;
import static nova.hetu.omniruntime.constants.JoinType.OMNI_JOIN_TYPE_LEFT;
import static nova.hetu.omniruntime.util.TestUtils.assertVecBatchEquals;
import static nova.hetu.omniruntime.util.TestUtils.assertVecEquals;
import static nova.hetu.omniruntime.util.TestUtils.createDictionaryVec;
import static nova.hetu.omniruntime.util.TestUtils.createLongVec;
import static nova.hetu.omniruntime.util.TestUtils.createVec;
import static nova.hetu.omniruntime.util.TestUtils.createVecBatch;
import static nova.hetu.omniruntime.util.TestUtils.freeVecBatch;

import static org.testng.Assert.assertEquals;

import nova.hetu.omniruntime.operator.join.OmniHashBuilderOperatorFactory;
import nova.hetu.omniruntime.operator.join.OmniLookupJoinOperatorFactory;
import nova.hetu.omniruntime.type.Date32VecType;
import nova.hetu.omniruntime.type.Decimal128VecType;
import nova.hetu.omniruntime.type.Decimal64VecType;
import nova.hetu.omniruntime.type.LongVecType;
import nova.hetu.omniruntime.type.VarcharVecType;
import nova.hetu.omniruntime.type.VecType;
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
        VecType[] buildTypes = {LongVecType.LONG, LongVecType.LONG};
        Object[][] buildDatas = {
            {1L, 2L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 1L}, {79L, 79L, 70L, 70L, 70L, 70L, 70L, 70L, 70L, 70L}
        };
        VecBatch buildVecBatch = createVecBatch(buildTypes, buildDatas);

        String[] buildJoinCols = {"#0"};
        int operatorCount = 1;
        OmniHashBuilderOperatorFactory hashBuilderOperatorFactory =
                new OmniHashBuilderOperatorFactory(buildTypes, buildJoinCols, operatorCount);
        OmniOperator hashBuilderOperator = hashBuilderOperatorFactory.createOperator();
        hashBuilderOperator.addInput(buildVecBatch);
        hashBuilderOperator.getOutput();

        VecType[] probeTypes = {LongVecType.LONG, LongVecType.LONG};
        Object[][] probeDatas = {
            {1L, 2L, 3L, 4L, 5L, 6L, 1L, 1L, 2L, 3L}, {78L, 78L, 78L, 78L, 78L, 78L, 78L, 82L, 82L, 65L}
        };
        VecBatch probeVecBatch = createVecBatch(probeTypes, probeDatas);

        int[] probeOutputCols = {1};
        String[] probeHashCols = {"#0"};
        int[] buildOutputCols = {1};
        VecType[] buildOutputTypes = {LongVecType.LONG};
        OmniLookupJoinOperatorFactory lookupJoinOperatorFactory =
                new OmniLookupJoinOperatorFactory(
                        probeTypes,
                        probeOutputCols,
                        probeHashCols,
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
     * Test inner hash join one column 2.
     */
    @Test
    public void testInnerHashJoinOneColumn2() {
        VecType[] buildTypes = {LongVecType.LONG, LongVecType.LONG};
        Object[][] buildDatas1 = {{1L, 1L, 3L, 6L, 7L, 1L}, {79L, 70L, 70L, 70L, 70L, 70L}};
        VecBatch buildVecBatch1 = createVecBatch(buildTypes, buildDatas1);
        Object[][] buildDatas2 = {{2L, 2L, 4L, 5L}, {79L, 70L, 70L, 70L}};
        VecBatch buildVecBatch2 = createVecBatch(buildTypes, buildDatas2);

        String[] buildJoinCols = {"#0"};
        int operatorCount = 2;
        OmniHashBuilderOperatorFactory hashBuilderOperatorFactory =
                new OmniHashBuilderOperatorFactory(buildTypes, buildJoinCols, operatorCount);
        OmniOperator hashBuilderOperator1 = hashBuilderOperatorFactory.createOperator();
        hashBuilderOperator1.addInput(buildVecBatch1);
        hashBuilderOperator1.getOutput();
        OmniOperator hashBuilderOperator2 = hashBuilderOperatorFactory.createOperator();
        hashBuilderOperator2.addInput(buildVecBatch2);
        hashBuilderOperator2.getOutput();

        VecType[] probeTypes = {LongVecType.LONG, LongVecType.LONG};
        Object[][] probeDatas = {
            {1L, 2L, 3L, 4L, 5L, 6L, 1L, 1L, 2L, 3L}, {78L, 78L, 78L, 78L, 78L, 78L, 78L, 82L, 82L, 65L}
        };
        VecBatch probeVecBatch = createVecBatch(probeTypes, probeDatas);

        int[] probeOutputCols = {1};
        String[] probeHashCols = {"#0"};
        int[] buildOutputCols = {1};
        VecType[] buildOutputTypes = {LongVecType.LONG};
        OmniLookupJoinOperatorFactory lookupJoinOperatorFactory =
                new OmniLookupJoinOperatorFactory(
                        probeTypes,
                        probeOutputCols,
                        probeHashCols,
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
        freeVecBatch(buildVecBatch1);
        freeVecBatch(buildVecBatch2);
        freeVecBatch(resultVecBatch);
    }

    /**
     * Test left join
     */
    @Test
    public void testLeftHashEqualityJoin() {
        VecType[] buildTypes = {LongVecType.LONG, LongVecType.LONG};
        Object[][] buildDatas = {{1L, 2L, 3L, 4L}, {111L, 11L, 333L, 33L}};
        VecBatch buildVecBatch = createVecBatch(buildTypes, buildDatas);

        String[] buildJoinCols = {"#1"};
        int operatorCount = 1;
        OmniHashBuilderOperatorFactory hashBuilderOperatorFactory =
                new OmniHashBuilderOperatorFactory(buildTypes, buildJoinCols, operatorCount);
        OmniOperator hashBuilderOperator = hashBuilderOperatorFactory.createOperator();
        hashBuilderOperator.addInput(buildVecBatch);
        hashBuilderOperator.getOutput();

        VecType[] probeTypes = {LongVecType.LONG, LongVecType.LONG};
        Object[][] probeDatas = {{1L, 2L, 3L, 4L}, {11L, 22L, 33L, 44L}};
        VecBatch probeVecBatch = createVecBatch(probeTypes, probeDatas);

        int[] probeOutputCols = {0, 1};
        String[] probeHashCols = {"#1"};
        int[] buildOutputCols = {0, 1};
        VecType[] buildOutputTypes = {LongVecType.LONG, LongVecType.LONG};
        OmniLookupJoinOperatorFactory lookupJoinOperatorFactory =
                new OmniLookupJoinOperatorFactory(
                        probeTypes,
                        probeOutputCols,
                        probeHashCols,
                        buildOutputCols,
                        buildOutputTypes,
                        OMNI_JOIN_TYPE_LEFT,
                        hashBuilderOperatorFactory);
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
        freeVecBatch(probeVecBatch);
        freeVecBatch(buildVecBatch);
        freeVecBatch(resultVecBatch);
    }

    /**
     * Test left join with varchar join key
     */
    @Test
    public void testLeftHashEqualityJoinVarchar() {
        VecType[] buildTypes = {LongVecType.LONG, new VarcharVecType(3)};
        Object[][] buildDatas = {{1L, 2L, 3L, 4L}, {"aaa", "11", "ccc", "33"}};
        VecBatch buildVecBatch = createVecBatch(buildTypes, buildDatas);

        String[] buildJoinCols = {"#1"};
        int operatorCount = 1;
        OmniHashBuilderOperatorFactory hashBuilderOperatorFactory =
                new OmniHashBuilderOperatorFactory(buildTypes, buildJoinCols, operatorCount);
        OmniOperator hashBuilderOperator = hashBuilderOperatorFactory.createOperator();
        hashBuilderOperator.addInput(buildVecBatch);
        hashBuilderOperator.getOutput();

        VecType[] probeTypes = {LongVecType.LONG, new VarcharVecType(2)};
        Object[][] probeDatas = {{1L, 2L, 3L, 4L}, {"11", "22", "33", "44"}};
        VecBatch probeVecBatch = createVecBatch(probeTypes, probeDatas);

        int[] probeOutputCols = {0, 1};
        String[] probeHashCols = {"#1"};
        int[] buildOutputCols = {0, 1};
        VecType[] buildOutputTypes = {LongVecType.LONG, new VarcharVecType(2)};
        OmniLookupJoinOperatorFactory lookupJoinOperatorFactory =
                new OmniLookupJoinOperatorFactory(
                        probeTypes,
                        probeOutputCols,
                        probeHashCols,
                        buildOutputCols,
                        buildOutputTypes,
                        OMNI_JOIN_TYPE_LEFT,
                        hashBuilderOperatorFactory);
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
        freeVecBatch(probeVecBatch);
        freeVecBatch(buildVecBatch);
        freeVecBatch(resultVecBatch);
    }

    /**
     * Test left join with date32 join key
     */
    @Test
    public void testLeftHashEqualityJoinDate32() {
        VecType[] buildTypes = {LongVecType.LONG, new Date32VecType(VecType.DateUnit.DAY)};
        Object[][] buildDatas = {{1L, 2L, 3L, 4L}, {123, 11, 321, 33}};
        VecBatch buildVecBatch = createVecBatch(buildTypes, buildDatas);

        String[] buildJoinCols = {"#1"};
        int operatorCount = 1;
        OmniHashBuilderOperatorFactory hashBuilderOperatorFactory =
                new OmniHashBuilderOperatorFactory(buildTypes, buildJoinCols, operatorCount);
        OmniOperator hashBuilderOperator = hashBuilderOperatorFactory.createOperator();
        hashBuilderOperator.addInput(buildVecBatch);
        hashBuilderOperator.getOutput();

        VecType[] probeTypes = {LongVecType.LONG, new Date32VecType(VecType.DateUnit.DAY)};
        Object[][] probeDatas = {{1L, 2L, 3L, 4L}, {11, 22, 33, 44}};
        VecBatch probeVecBatch = createVecBatch(probeTypes, probeDatas);

        int[] probeOutputCols = {0, 1};
        String[] probeHashCols = {"#1"};
        int[] buildOutputCols = {0, 1};
        VecType[] buildOutputTypes = {LongVecType.LONG, new Date32VecType(VecType.DateUnit.DAY)};
        OmniLookupJoinOperatorFactory lookupJoinOperatorFactory =
                new OmniLookupJoinOperatorFactory(
                        probeTypes,
                        probeOutputCols,
                        probeHashCols,
                        buildOutputCols,
                        buildOutputTypes,
                        OMNI_JOIN_TYPE_LEFT,
                        hashBuilderOperatorFactory);
        OmniOperator lookupJoinOperator = lookupJoinOperatorFactory.createOperator();
        lookupJoinOperator.addInput(probeVecBatch);
        Iterator<VecBatch> results = lookupJoinOperator.getOutput();

        VecBatch resultVecBatch = results.next();
        int len = resultVecBatch.getRowCount();
        assertEquals(len, 4);
        assertEquals(resultVecBatch.getVectorCount(), 4);
        Object[][] expectedDatas = {{1L, 2L, 3L, 4L}, {11, 22, 33, 44}, {2L, null, 4L, null}, {11, null, 33, null}};
        assertVecBatchEquals(resultVecBatch, expectedDatas);
        freeVecBatch(probeVecBatch);
        freeVecBatch(buildVecBatch);
        freeVecBatch(resultVecBatch);
    }

    /**
     * Test left join with decimal64 join key
     */
    @Test
    public void testLeftHashEqualityJoinDecimal64() {
        VecType[] buildTypes = {LongVecType.LONG, new Decimal64VecType(3, 0)};
        Object[][] buildDatas = {{1L, 2L, 3L, 4L}, {123L, 11L, 321L, 33L}};
        VecBatch buildVecBatch = createVecBatch(buildTypes, buildDatas);

        String[] buildJoinCols = {"#1"};
        int operatorCount = 1;
        OmniHashBuilderOperatorFactory hashBuilderOperatorFactory =
                new OmniHashBuilderOperatorFactory(buildTypes, buildJoinCols, operatorCount);
        OmniOperator hashBuilderOperator = hashBuilderOperatorFactory.createOperator();
        hashBuilderOperator.addInput(buildVecBatch);
        hashBuilderOperator.getOutput();

        VecType[] probeTypes = {LongVecType.LONG, new Decimal64VecType(2, 0)};
        Object[][] probeDatas = {{1L, 2L, 3L, 4L}, {11L, 22L, 33L, 44L}};
        VecBatch probeVecBatch = createVecBatch(probeTypes, probeDatas);

        int[] probeOutputCols = {0, 1};
        String[] probeHashCols = {"#1"};
        int[] buildOutputCols = {0, 1};
        VecType[] buildOutputTypes = {LongVecType.LONG, new Decimal64VecType(3, 0)};
        OmniLookupJoinOperatorFactory lookupJoinOperatorFactory =
                new OmniLookupJoinOperatorFactory(
                        probeTypes,
                        probeOutputCols,
                        probeHashCols,
                        buildOutputCols,
                        buildOutputTypes,
                        OMNI_JOIN_TYPE_LEFT,
                        hashBuilderOperatorFactory);
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
        freeVecBatch(probeVecBatch);
        freeVecBatch(buildVecBatch);
        freeVecBatch(resultVecBatch);
    }

    /**
     * Test left join with decimal128 join key
     */
    @Test
    public void testLeftHashEqualityJoinDecimal128() {
        VecType[] buildTypes = {LongVecType.LONG, new Decimal128VecType(3, 0)};
        Vec[] buildVecs = new Vec[buildTypes.length];
        buildVecs[0] = createVec(buildTypes[0], new Object[] {1L, 2L, 3L, 4L});
        buildVecs[1] = createVec(buildTypes[1], new Object[][] {{123L, 0L}, {11L, 0L}, {321L, 0L}, {33L, 0L}});
        VecBatch buildVecBatch = new VecBatch(buildVecs);

        String[] buildJoinCols = {"#1"};
        int operatorCount = 1;
        OmniHashBuilderOperatorFactory hashBuilderOperatorFactory =
                new OmniHashBuilderOperatorFactory(buildTypes, buildJoinCols, operatorCount);
        OmniOperator hashBuilderOperator = hashBuilderOperatorFactory.createOperator();
        hashBuilderOperator.addInput(buildVecBatch);
        hashBuilderOperator.getOutput();

        VecType[] probeTypes = {LongVecType.LONG, new Decimal128VecType(2, 0)};
        Vec[] probeVecs = new Vec[probeTypes.length];
        probeVecs[0] = createVec(probeTypes[0], new Object[] {1L, 2L, 3L, 4L});
        probeVecs[1] = createVec(probeTypes[1], new Object[][] {{11L, 0L}, {22L, 0L}, {33L, 0L}, {44L, 0L}});
        VecBatch probeVecBatch = new VecBatch(probeVecs);

        int[] probeOutputCols = {0, 1};
        String[] probeHashCols = {"#1"};
        int[] buildOutputCols = {0, 1};
        VecType[] buildOutputTypes = {LongVecType.LONG, new Decimal128VecType(3, 0)};
        OmniLookupJoinOperatorFactory lookupJoinOperatorFactory =
                new OmniLookupJoinOperatorFactory(
                        probeTypes,
                        probeOutputCols,
                        probeHashCols,
                        buildOutputCols,
                        buildOutputTypes,
                        OMNI_JOIN_TYPE_LEFT,
                        hashBuilderOperatorFactory);
        OmniOperator lookupJoinOperator = lookupJoinOperatorFactory.createOperator();
        lookupJoinOperator.addInput(probeVecBatch);
        Iterator<VecBatch> results = lookupJoinOperator.getOutput();

        VecBatch resultVecBatch = results.next();
        int len = resultVecBatch.getRowCount();
        assertEquals(len, 4);
        assertEquals(resultVecBatch.getVectorCount(), 4);
        assertVecEquals(resultVecBatch.getVectors()[0], new Object[] {1L, 2L, 3L, 4L});
        assertVecEquals(resultVecBatch.getVectors()[1], new Object[][] {{11L, 0L}, {22L, 0L}, {33L, 0L}, {44L, 0L}});
        assertVecEquals(resultVecBatch.getVectors()[2], new Object[] {2L, null, 4L, null});
        assertVecEquals(resultVecBatch.getVectors()[3], new Object[][] {{11L, 0L}, null, {33L, 0L}, null});
        freeVecBatch(probeVecBatch);
        freeVecBatch(buildVecBatch);
        freeVecBatch(resultVecBatch);
    }

    @Test
    public void testLeftHashEqualityJoinDictionary() {
        VecType[] buildTypes = {LongVecType.LONG, LongVecType.LONG};
        Object[][] buildDatas = {{1L, 2L, 3L, 4L}, {111L, 11L, 333L, 33L}};
        Vec[] vecs = new Vec[2];
        int[] ids = {0, 1, 2, 3};
        vecs[0] = createLongVec(buildDatas[0]);
        vecs[1] = createDictionaryVec(buildTypes[1], buildDatas[1], ids);
        VecBatch buildVecBatch = new VecBatch(vecs);

        String[] buildJoinCols = {"#1"};
        int operatorCount = 1;
        OmniHashBuilderOperatorFactory hashBuilderOperatorFactory =
                new OmniHashBuilderOperatorFactory(buildTypes, buildJoinCols, operatorCount);
        OmniOperator hashBuilderOperator = hashBuilderOperatorFactory.createOperator();
        hashBuilderOperator.addInput(buildVecBatch);
        hashBuilderOperator.getOutput();

        VecType[] probeTypes = {LongVecType.LONG, LongVecType.LONG};
        Object[][] probeDatas = {{1L, 2L, 3L, 4L}, {11L, 22L, 33L, 44L}};
        Vec[] probeVecs = new Vec[2];
        probeVecs[0] = createLongVec(probeDatas[0]);
        probeVecs[1] = createDictionaryVec(probeTypes[1], probeDatas[1], ids);
        VecBatch probeVecBatch = new VecBatch(probeVecs);

        int[] probeOutputCols = {0, 1};
        String[] probeHashCols = {"#1"};
        int[] buildOutputCols = {0, 1};
        VecType[] buildOutputTypes = {LongVecType.LONG, LongVecType.LONG};
        OmniLookupJoinOperatorFactory lookupJoinOperatorFactory =
                new OmniLookupJoinOperatorFactory(
                        probeTypes,
                        probeOutputCols,
                        probeHashCols,
                        buildOutputCols,
                        buildOutputTypes,
                        OMNI_JOIN_TYPE_INNER,
                        hashBuilderOperatorFactory);
        OmniOperator lookupJoinOperator = lookupJoinOperatorFactory.createOperator();
        lookupJoinOperator.addInput(probeVecBatch);
        Iterator<VecBatch> results = lookupJoinOperator.getOutput();

        VecBatch resultVecBatch = results.next();
        int len = resultVecBatch.getRowCount();
        assertEquals(len, 2);
        assertEquals(resultVecBatch.getVectorCount(), 4);
        Object[][] expectedDatas = {
                {1L, 3L}, {11L, 33L}, {2L, 4L}, {11L, 33L}
        };
        assertVecBatchEquals(resultVecBatch, expectedDatas);
        freeVecBatch(probeVecBatch);
        freeVecBatch(buildVecBatch);
        freeVecBatch(resultVecBatch);
    }
}
