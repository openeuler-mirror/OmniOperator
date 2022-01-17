
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
import static org.testng.Assert.assertTrue;

import nova.hetu.omniruntime.operator.join.OmniHashBuilderOperatorFactory;
import nova.hetu.omniruntime.operator.join.OmniLookupJoinOperatorFactory;
import nova.hetu.omniruntime.type.CharVecType;
import nova.hetu.omniruntime.type.Date32VecType;
import nova.hetu.omniruntime.type.Decimal128VecType;
import nova.hetu.omniruntime.type.Decimal64VecType;
import nova.hetu.omniruntime.type.IntVecType;
import nova.hetu.omniruntime.type.LongVecType;
import nova.hetu.omniruntime.type.VarcharVecType;
import nova.hetu.omniruntime.type.VecType;
import nova.hetu.omniruntime.vector.Vec;
import nova.hetu.omniruntime.vector.VecBatch;

import org.testng.annotations.Test;

import java.util.Iterator;
import java.util.Optional;

/**
 * The type Omni hash join operators test.
 */
public class OmniHashJoinOperatorsTest {
    /**
     * Test inner hash join one column 1.
     */
    @Test
    public void testInnerEqualityJoinOneColumn1() {
        VecType[] buildTypes = {LongVecType.LONG, LongVecType.LONG};
        Object[][] buildDatas = {{1L, 2L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 1L},
                {79L, 79L, 70L, 70L, 70L, 70L, 70L, 70L, 70L, 70L}};
        VecBatch buildVecBatch = createVecBatch(buildTypes, buildDatas);

        int[] buildHashCols = {0};
        int operatorCount = 1;
        OmniHashBuilderOperatorFactory hashBuilderOperatorFactory = new OmniHashBuilderOperatorFactory(buildTypes,
                buildHashCols, Optional.empty(), Optional.empty(), null, operatorCount);
        OmniOperator hashBuilderOperator = hashBuilderOperatorFactory.createOperator();
        hashBuilderOperator.addInput(buildVecBatch);
        hashBuilderOperator.getOutput();

        VecType[] probeTypes = {LongVecType.LONG, LongVecType.LONG};
        Object[][] probeDatas = {{1L, 2L, 3L, 4L, 5L, 6L, 1L, 1L, 2L, 3L},
                {78L, 78L, 78L, 78L, 78L, 78L, 78L, 82L, 82L, 65L}};
        VecBatch probeVecBatch = createVecBatch(probeTypes, probeDatas);

        int[] probeOutputCols = {1};
        int[] probeHashCols = {0};
        int[] buildOutputCols = {1};
        VecType[] buildOutputTypes = {LongVecType.LONG};
        OmniLookupJoinOperatorFactory lookupJoinOperatorFactory = new OmniLookupJoinOperatorFactory(probeTypes,
                probeOutputCols, probeHashCols, buildOutputCols, buildOutputTypes, OMNI_JOIN_TYPE_INNER,
                hashBuilderOperatorFactory);
        OmniOperator lookupJoinOperator = lookupJoinOperatorFactory.createOperator();
        lookupJoinOperator.addInput(probeVecBatch);
        Iterator<VecBatch> results = lookupJoinOperator.getOutput();
        VecBatch resultVecBatch = results.next();

        int len = resultVecBatch.getRowCount();
        assertEquals(len, 18);
        Object[][] expectedDatas = {
                {78L, 78L, 78L, 78L, 78L, 78L, 78L, 78L, 78L, 78L, 78L, 78L, 82L, 82L, 82L, 82L, 82L, 65L},
                {70L, 70L, 79L, 70L, 79L, 70L, 70L, 70L, 70L, 70L, 70L, 79L, 70L, 70L, 79L, 70L, 79L, 70L}};
        assertVecBatchEquals(resultVecBatch, expectedDatas);
        freeVecBatch(resultVecBatch);
        lookupJoinOperator.close();
        hashBuilderOperator.close();
        lookupJoinOperatorFactory.close();
        hashBuilderOperatorFactory.close();
    }

    /**
     * Test inner hash join one column 2.
     */
    @Test
    public void testInnerEqualityJoinOneColumn2() {
        VecType[] buildTypes = {LongVecType.LONG, LongVecType.LONG};
        Object[][] buildDatas1 = {{1L, 1L, 3L, 6L, 7L, 1L}, {79L, 70L, 70L, 70L, 70L, 70L}};
        VecBatch buildVecBatch1 = createVecBatch(buildTypes, buildDatas1);
        Object[][] buildDatas2 = {{2L, 2L, 4L, 5L}, {79L, 70L, 70L, 70L}};
        VecBatch buildVecBatch2 = createVecBatch(buildTypes, buildDatas2);

        int[] buildHashCols = {0};
        int operatorCount = 2;
        OmniHashBuilderOperatorFactory hashBuilderOperatorFactory = new OmniHashBuilderOperatorFactory(buildTypes,
                buildHashCols, Optional.empty(), Optional.empty(), null, operatorCount);
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
        int[] buildOutputCols = {1};
        VecType[] buildOutputTypes = {LongVecType.LONG};
        OmniLookupJoinOperatorFactory lookupJoinOperatorFactory = new OmniLookupJoinOperatorFactory(probeTypes,
                probeOutputCols, probeHashCols, buildOutputCols, buildOutputTypes, OMNI_JOIN_TYPE_INNER,
                hashBuilderOperatorFactory);
        OmniOperator lookupJoinOperator = lookupJoinOperatorFactory.createOperator();
        lookupJoinOperator.addInput(probeVecBatch);
        Iterator<VecBatch> results = lookupJoinOperator.getOutput();
        VecBatch resultVecBatch = results.next();

        int len = resultVecBatch.getRowCount();
        assertEquals(len, 18);
        Object[][] expectedDatas = {
                {78L, 78L, 78L, 78L, 78L, 78L, 78L, 78L, 78L, 78L, 78L, 78L, 82L, 82L, 82L, 82L, 82L, 65L},
                {70L, 70L, 79L, 70L, 79L, 70L, 70L, 70L, 70L, 70L, 70L, 79L, 70L, 70L, 79L, 70L, 79L, 70L}};
        assertVecBatchEquals(resultVecBatch, expectedDatas);
        freeVecBatch(resultVecBatch);
        lookupJoinOperator.close();
        hashBuilderOperator1.close();
        hashBuilderOperator2.close();
        lookupJoinOperatorFactory.close();
        hashBuilderOperatorFactory.close();
    }

    /**
     * Test left join
     */
    @Test
    public void testLeftEqualityJoin() {
        VecType[] buildTypes = {LongVecType.LONG, LongVecType.LONG};
        Object[][] buildDatas = {{1L, 2L, 3L, 4L}, {111L, 11L, 333L, 33L}};
        VecBatch buildVecBatch = createVecBatch(buildTypes, buildDatas);

        int[] buildHashCols = {1};
        int operatorCount = 1;
        OmniHashBuilderOperatorFactory hashBuilderOperatorFactory = new OmniHashBuilderOperatorFactory(buildTypes,
                buildHashCols, Optional.empty(), Optional.empty(), null, operatorCount);
        OmniOperator hashBuilderOperator = hashBuilderOperatorFactory.createOperator();
        hashBuilderOperator.addInput(buildVecBatch);
        hashBuilderOperator.getOutput();

        VecType[] probeTypes = {LongVecType.LONG, LongVecType.LONG};
        Object[][] probeDatas = {{1L, 2L, 3L, 4L}, {11L, 22L, 33L, 44L}};
        VecBatch probeVecBatch = createVecBatch(probeTypes, probeDatas);

        int[] probeOutputCols = {0, 1};
        int[] probeHashCols = {1};
        int[] buildOutputCols = {0, 1};
        VecType[] buildOutputTypes = {LongVecType.LONG, LongVecType.LONG};
        OmniLookupJoinOperatorFactory lookupJoinOperatorFactory = new OmniLookupJoinOperatorFactory(probeTypes,
                probeOutputCols, probeHashCols, buildOutputCols, buildOutputTypes, OMNI_JOIN_TYPE_LEFT,
                hashBuilderOperatorFactory);
        OmniOperator lookupJoinOperator = lookupJoinOperatorFactory.createOperator();
        lookupJoinOperator.addInput(probeVecBatch);
        Iterator<VecBatch> results = lookupJoinOperator.getOutput();

        VecBatch resultVecBatch = results.next();
        int len = resultVecBatch.getRowCount();
        assertEquals(len, 4);
        assertEquals(resultVecBatch.getVectorCount(), 4);
        Object[][] expectedDatas = {{1L, 2L, 3L, 4L}, {11L, 22L, 33L, 44L}, {2L, null, 4L, null},
                {11L, null, 33L, null}};
        assertVecBatchEquals(resultVecBatch, expectedDatas);
        freeVecBatch(resultVecBatch);
        lookupJoinOperator.close();
        hashBuilderOperator.close();
        lookupJoinOperatorFactory.close();
        hashBuilderOperatorFactory.close();
    }

    /**
     * Test left join with varchar join key
     */
    @Test
    public void testLeftEqualityJoinVarchar() {
        VecType[] buildTypes = {LongVecType.LONG, new VarcharVecType(3)};
        Object[][] buildDatas = {{1L, 2L, 3L, 4L}, {"aaa", "11", "ccc", "33"}};
        VecBatch buildVecBatch = createVecBatch(buildTypes, buildDatas);

        int[] buildHashCols = {1};
        int operatorCount = 1;
        OmniHashBuilderOperatorFactory hashBuilderOperatorFactory = new OmniHashBuilderOperatorFactory(buildTypes,
                buildHashCols, Optional.empty(), Optional.empty(), null, operatorCount);
        OmniOperator hashBuilderOperator = hashBuilderOperatorFactory.createOperator();
        hashBuilderOperator.addInput(buildVecBatch);
        hashBuilderOperator.getOutput();

        VecType[] probeTypes = {LongVecType.LONG, new VarcharVecType(2)};
        Object[][] probeDatas = {{1L, 2L, 3L, 4L}, {"11", "22", "33", "44"}};
        VecBatch probeVecBatch = createVecBatch(probeTypes, probeDatas);

        int[] probeOutputCols = {0, 1};
        int[] probeHashCols = {1};
        int[] buildOutputCols = {0, 1};
        VecType[] buildOutputTypes = {LongVecType.LONG, new VarcharVecType(2)};
        OmniLookupJoinOperatorFactory lookupJoinOperatorFactory = new OmniLookupJoinOperatorFactory(probeTypes,
                probeOutputCols, probeHashCols, buildOutputCols, buildOutputTypes, OMNI_JOIN_TYPE_LEFT,
                hashBuilderOperatorFactory);
        OmniOperator lookupJoinOperator = lookupJoinOperatorFactory.createOperator();
        lookupJoinOperator.addInput(probeVecBatch);
        Iterator<VecBatch> results = lookupJoinOperator.getOutput();

        VecBatch resultVecBatch = results.next();
        int len = resultVecBatch.getRowCount();
        assertEquals(len, 4);
        assertEquals(resultVecBatch.getVectorCount(), 4);
        Object[][] expectedDatas = {{1L, 2L, 3L, 4L}, {"11", "22", "33", "44"}, {2L, null, 4L, null},
                {"11", null, "33", null}};
        assertVecBatchEquals(resultVecBatch, expectedDatas);
        freeVecBatch(resultVecBatch);
        lookupJoinOperator.close();
        hashBuilderOperator.close();
        lookupJoinOperatorFactory.close();
        hashBuilderOperatorFactory.close();
    }

    /**
     * Test left join with char join key
     */
    @Test
    public void testLeftEqualityJoinChar() {
        VecType[] buildTypes = {LongVecType.LONG, new CharVecType(3)};
        Object[][] buildDatas = {{1L, 2L, 3L, 4L}, {"aaa", "11", "ccc", "33"}};
        VecBatch buildVecBatch = createVecBatch(buildTypes, buildDatas);

        int[] buildJoinCols = {1};
        int operatorCount = 1;
        OmniHashBuilderOperatorFactory hashBuilderOperatorFactory = new OmniHashBuilderOperatorFactory(buildTypes,
                buildJoinCols, Optional.empty(), Optional.empty(), null, operatorCount);
        OmniOperator hashBuilderOperator = hashBuilderOperatorFactory.createOperator();
        hashBuilderOperator.addInput(buildVecBatch);
        hashBuilderOperator.getOutput();

        VecType[] probeTypes = {LongVecType.LONG, new CharVecType(2)};
        Object[][] probeDatas = {{1L, 2L, 3L, 4L}, {"11", "22", "33", "44"}};
        VecBatch probeVecBatch = createVecBatch(probeTypes, probeDatas);

        int[] probeOutputCols = {0, 1};
        int[] probeHashCols = {1};
        int[] buildOutputCols = {0, 1};
        VecType[] buildOutputTypes = {LongVecType.LONG, new CharVecType(2)};
        OmniLookupJoinOperatorFactory lookupJoinOperatorFactory = new OmniLookupJoinOperatorFactory(probeTypes,
                probeOutputCols, probeHashCols, buildOutputCols, buildOutputTypes, OMNI_JOIN_TYPE_LEFT,
                hashBuilderOperatorFactory);
        OmniOperator lookupJoinOperator = lookupJoinOperatorFactory.createOperator();
        lookupJoinOperator.addInput(probeVecBatch);
        Iterator<VecBatch> results = lookupJoinOperator.getOutput();

        VecBatch resultVecBatch = results.next();
        int len = resultVecBatch.getRowCount();
        assertEquals(len, 4);
        assertEquals(resultVecBatch.getVectorCount(), 4);
        Object[][] expectedDatas = {{1L, 2L, 3L, 4L}, {"11", "22", "33", "44"}, {2L, null, 4L, null},
                {"11", null, "33", null}};
        assertVecBatchEquals(resultVecBatch, expectedDatas);
        freeVecBatch(resultVecBatch);
        lookupJoinOperator.close();
        hashBuilderOperator.close();
        lookupJoinOperatorFactory.close();
        hashBuilderOperatorFactory.close();
    }

    /**
     * Test left join with date32 join key
     */
    @Test
    public void testLeftEqualityJoinDate32() {
        VecType[] buildTypes = {LongVecType.LONG, new Date32VecType(VecType.DateUnit.DAY)};
        Object[][] buildDatas = {{1L, 2L, 3L, 4L}, {123, 11, 321, 33}};
        VecBatch buildVecBatch = createVecBatch(buildTypes, buildDatas);

        int[] buildHashCols = {1};
        int operatorCount = 1;
        OmniHashBuilderOperatorFactory hashBuilderOperatorFactory = new OmniHashBuilderOperatorFactory(buildTypes,
                buildHashCols, Optional.empty(), Optional.empty(), null, operatorCount);
        OmniOperator hashBuilderOperator = hashBuilderOperatorFactory.createOperator();
        hashBuilderOperator.addInput(buildVecBatch);
        hashBuilderOperator.getOutput();

        VecType[] probeTypes = {LongVecType.LONG, new Date32VecType(VecType.DateUnit.DAY)};
        Object[][] probeDatas = {{1L, 2L, 3L, 4L}, {11, 22, 33, 44}};
        VecBatch probeVecBatch = createVecBatch(probeTypes, probeDatas);

        int[] probeOutputCols = {0, 1};
        int[] probeHashCols = {1};
        int[] buildOutputCols = {0, 1};
        VecType[] buildOutputTypes = {LongVecType.LONG, new Date32VecType(VecType.DateUnit.DAY)};
        OmniLookupJoinOperatorFactory lookupJoinOperatorFactory = new OmniLookupJoinOperatorFactory(probeTypes,
                probeOutputCols, probeHashCols, buildOutputCols, buildOutputTypes, OMNI_JOIN_TYPE_LEFT,
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
        freeVecBatch(resultVecBatch);
        lookupJoinOperator.close();
        hashBuilderOperator.close();
        lookupJoinOperatorFactory.close();
        hashBuilderOperatorFactory.close();
    }

    /**
     * Test left join with decimal64 join key
     */
    @Test
    public void testLeftEqualityJoinDecimal64() {
        VecType[] buildTypes = {LongVecType.LONG, new Decimal64VecType(3, 0)};
        Object[][] buildDatas = {{1L, 2L, 3L, 4L}, {123L, 11L, 321L, 33L}};
        VecBatch buildVecBatch = createVecBatch(buildTypes, buildDatas);

        int[] buildHashCols = {1};
        int operatorCount = 1;
        OmniHashBuilderOperatorFactory hashBuilderOperatorFactory = new OmniHashBuilderOperatorFactory(buildTypes,
                buildHashCols, Optional.empty(), Optional.empty(), null, operatorCount);
        OmniOperator hashBuilderOperator = hashBuilderOperatorFactory.createOperator();
        hashBuilderOperator.addInput(buildVecBatch);
        hashBuilderOperator.getOutput();

        VecType[] probeTypes = {LongVecType.LONG, new Decimal64VecType(2, 0)};
        Object[][] probeDatas = {{1L, 2L, 3L, 4L}, {11L, 22L, 33L, 44L}};
        VecBatch probeVecBatch = createVecBatch(probeTypes, probeDatas);

        int[] probeOutputCols = {0, 1};
        int[] probeHashCols = {1};
        int[] buildOutputCols = {0, 1};
        VecType[] buildOutputTypes = {LongVecType.LONG, new Decimal64VecType(3, 0)};
        OmniLookupJoinOperatorFactory lookupJoinOperatorFactory = new OmniLookupJoinOperatorFactory(probeTypes,
                probeOutputCols, probeHashCols, buildOutputCols, buildOutputTypes, OMNI_JOIN_TYPE_LEFT,
                hashBuilderOperatorFactory);
        OmniOperator lookupJoinOperator = lookupJoinOperatorFactory.createOperator();
        lookupJoinOperator.addInput(probeVecBatch);
        Iterator<VecBatch> results = lookupJoinOperator.getOutput();

        VecBatch resultVecBatch = results.next();
        int len = resultVecBatch.getRowCount();
        assertEquals(len, 4);
        assertEquals(resultVecBatch.getVectorCount(), 4);
        Object[][] expectedDatas = {{1L, 2L, 3L, 4L}, {11L, 22L, 33L, 44L}, {2L, null, 4L, null},
                {11L, null, 33L, null}};
        assertVecBatchEquals(resultVecBatch, expectedDatas);
        freeVecBatch(resultVecBatch);
        lookupJoinOperator.close();
        hashBuilderOperator.close();
        lookupJoinOperatorFactory.close();
        hashBuilderOperatorFactory.close();
    }

    /**
     * Test left join with decimal128 join key
     */
    @Test
    public void testLeftEqualityJoinDecimal128() {
        VecType[] buildTypes = {LongVecType.LONG, new Decimal128VecType(3, 0)};
        Vec[] buildVecs = new Vec[buildTypes.length];
        buildVecs[0] = createVec(buildTypes[0], new Object[]{1L, 2L, 3L, 4L});
        buildVecs[1] = createVec(buildTypes[1], new Object[][]{{123L, 0L}, {11L, 0L}, {321L, 0L}, {33L, 0L}});
        VecBatch buildVecBatch = new VecBatch(buildVecs);

        int[] buildHashCols = {1};
        int operatorCount = 1;
        OmniHashBuilderOperatorFactory hashBuilderOperatorFactory = new OmniHashBuilderOperatorFactory(buildTypes,
                buildHashCols, Optional.empty(), Optional.empty(), null, operatorCount);
        OmniOperator hashBuilderOperator = hashBuilderOperatorFactory.createOperator();
        hashBuilderOperator.addInput(buildVecBatch);
        hashBuilderOperator.getOutput();

        VecType[] probeTypes = {LongVecType.LONG, new Decimal128VecType(2, 0)};
        Vec[] probeVecs = new Vec[probeTypes.length];
        probeVecs[0] = createVec(probeTypes[0], new Object[]{1L, 2L, 3L, 4L});
        probeVecs[1] = createVec(probeTypes[1], new Object[][]{{11L, 0L}, {22L, 0L}, {33L, 0L}, {44L, 0L}});
        VecBatch probeVecBatch = new VecBatch(probeVecs);

        int[] probeOutputCols = {0, 1};
        int[] probeHashCols = {1};
        int[] buildOutputCols = {0, 1};
        VecType[] buildOutputTypes = {LongVecType.LONG, new Decimal128VecType(3, 0)};
        OmniLookupJoinOperatorFactory lookupJoinOperatorFactory = new OmniLookupJoinOperatorFactory(probeTypes,
                probeOutputCols, probeHashCols, buildOutputCols, buildOutputTypes, OMNI_JOIN_TYPE_LEFT,
                hashBuilderOperatorFactory);
        OmniOperator lookupJoinOperator = lookupJoinOperatorFactory.createOperator();
        lookupJoinOperator.addInput(probeVecBatch);
        Iterator<VecBatch> results = lookupJoinOperator.getOutput();

        VecBatch resultVecBatch = results.next();
        int len = resultVecBatch.getRowCount();
        assertEquals(len, 4);
        assertEquals(resultVecBatch.getVectorCount(), 4);
        assertVecEquals(resultVecBatch.getVectors()[0], new Object[]{1L, 2L, 3L, 4L});
        assertVecEquals(resultVecBatch.getVectors()[1], new Object[][]{{11L, 0L}, {22L, 0L}, {33L, 0L}, {44L, 0L}});
        assertVecEquals(resultVecBatch.getVectors()[2], new Object[]{2L, null, 4L, null});
        assertVecEquals(resultVecBatch.getVectors()[3], new Object[][]{{11L, 0L}, null, {33L, 0L}, null});
        freeVecBatch(resultVecBatch);
        lookupJoinOperator.close();
        hashBuilderOperator.close();
        lookupJoinOperatorFactory.close();
        hashBuilderOperatorFactory.close();
    }

    /**
     * Test inner join with dictionary join key
     */
    @Test
    public void testInnerEqualityJoinDictionary() {
        VecType[] buildTypes = {LongVecType.LONG, LongVecType.LONG};
        Object[][] buildDatas = {{1L, null, 3L, null}, {111L, 11L, 333L, 33L}};
        Vec[] vecs = new Vec[2];
        int[] ids = {0, 1, 2, 3};
        vecs[0] = createLongVec(buildDatas[0]);
        vecs[1] = createDictionaryVec(buildTypes[1], buildDatas[1], ids);
        VecBatch buildVecBatch = new VecBatch(vecs);

        int[] buildHashCols = {1};
        int operatorCount = 1;
        OmniHashBuilderOperatorFactory hashBuilderOperatorFactory = new OmniHashBuilderOperatorFactory(buildTypes,
                buildHashCols, Optional.empty(), Optional.empty(), null, operatorCount);
        OmniOperator hashBuilderOperator = hashBuilderOperatorFactory.createOperator();
        hashBuilderOperator.addInput(buildVecBatch);
        hashBuilderOperator.getOutput();

        VecType[] probeTypes = {LongVecType.LONG, LongVecType.LONG};
        Object[][] probeDatas = {{null, 2L, null, 4L}, {11L, 22L, 33L, 44L}};
        Vec[] probeVecs = new Vec[2];
        probeVecs[0] = createLongVec(probeDatas[0]);
        probeVecs[1] = createDictionaryVec(probeTypes[1], probeDatas[1], ids);
        VecBatch probeVecBatch = new VecBatch(probeVecs);

        int[] probeOutputCols = {0, 1};
        int[] probeHashCols = {1};
        int[] buildOutputCols = {0, 1};
        VecType[] buildOutputTypes = {LongVecType.LONG, LongVecType.LONG};
        OmniLookupJoinOperatorFactory lookupJoinOperatorFactory = new OmniLookupJoinOperatorFactory(probeTypes,
                probeOutputCols, probeHashCols, buildOutputCols, buildOutputTypes, OMNI_JOIN_TYPE_INNER,
                hashBuilderOperatorFactory);
        OmniOperator lookupJoinOperator = lookupJoinOperatorFactory.createOperator();
        lookupJoinOperator.addInput(probeVecBatch);
        Iterator<VecBatch> results = lookupJoinOperator.getOutput();

        VecBatch resultVecBatch = results.next();
        int len = resultVecBatch.getRowCount();
        assertEquals(len, 2);
        assertEquals(resultVecBatch.getVectorCount(), 4);
        Object[][] expectedDatas = {{null, null}, {11L, 33L}, {null, null}, {11L, 33L}};
        assertVecBatchEquals(resultVecBatch, expectedDatas);
        freeVecBatch(resultVecBatch);
        lookupJoinOperator.close();
        hashBuilderOperator.close();
        lookupJoinOperatorFactory.close();
        hashBuilderOperatorFactory.close();
    }

    /**
     * Test inner join with join filter on int column
     */
    @Test
    public void testInnerEqualityJoinWithIntFilter() {
        VecType[] buildTypes = {IntVecType.INTEGER, IntVecType.INTEGER};
        Object[][] buildDatas = {{19, 14, 7, 19, 1, 20, 10, 13, 20, 16},
                {35709, 31904, 35709, 31904, 35709, null, 35709, 31904, null, 31904}};
        VecBatch buildVecBatch = createVecBatch(buildTypes, buildDatas);

        int[] buildHashCols = {0};
        int operatorCount = 1;
        String filterExpression = "$operator$GREATER_THAN:4(#1, #3)";
        OmniHashBuilderOperatorFactory hashBuilderOperatorFactory = new OmniHashBuilderOperatorFactory(buildTypes,
                buildHashCols, Optional.of(filterExpression), Optional.empty(), null, operatorCount);
        OmniOperator hashBuilderOperator = hashBuilderOperatorFactory.createOperator();
        hashBuilderOperator.addInput(buildVecBatch);
        hashBuilderOperator.getOutput();

        VecType[] probeTypes = {IntVecType.INTEGER, IntVecType.INTEGER};
        Object[][] probeDatas = {{20, 16, 13, 4, 20, 4, 22, 19, 8, 7},
                {35709, 35709, 31904, 12477, null, 38721, 90419, 35709, 88371, null}};
        VecBatch probeVecBatch = createVecBatch(probeTypes, probeDatas);

        int[] probeOutputCols = {0, 1};
        int[] probeHashCols = {0};
        int[] buildOutputCols = {0, 1};
        VecType[] buildOutputTypes = {IntVecType.INTEGER, IntVecType.INTEGER};
        OmniLookupJoinOperatorFactory lookupJoinOperatorFactory = new OmniLookupJoinOperatorFactory(probeTypes,
                probeOutputCols, probeHashCols, buildOutputCols, buildOutputTypes, OMNI_JOIN_TYPE_INNER,
                hashBuilderOperatorFactory);
        OmniOperator lookupJoinOperator = lookupJoinOperatorFactory.createOperator();
        lookupJoinOperator.addInput(probeVecBatch);
        Iterator<VecBatch> results = lookupJoinOperator.getOutput();

        VecBatch resultVecBatch = results.next();
        int len = resultVecBatch.getRowCount();
        assertEquals(len, 2);
        assertEquals(resultVecBatch.getVectorCount(), 4);
        Object[][] expectedDatas = {{16, 19}, {35709, 35709}, {16, 19}, {31904, 31904}};
        assertVecBatchEquals(resultVecBatch, expectedDatas);
        freeVecBatch(resultVecBatch);
        lookupJoinOperator.close();
        hashBuilderOperator.close();
        lookupJoinOperatorFactory.close();
        hashBuilderOperatorFactory.close();
    }

    /**
     * Test inner join with join filter on varchar column
     */
    @Test
    public void testInnerEqualityJoinWithCharFilter() {
        VecType[] buildTypes = {IntVecType.INTEGER, new VarcharVecType(5)};
        Object[][] buildDatas = {{19, 14, 7, 19, 1, 20, 10, 13, 20, 16},
                {"35709", "31904", "35709", "31904", "35709", "31904", "35709", "31904", "35709", "31904"}};
        VecBatch buildVecBatch = createVecBatch(buildTypes, buildDatas);

        int[] buildHashCols = {0};
        int operatorCount = 1;
        String filterExpression = "$operator$NOT_EQUAL:4(substr:15(#1, 1:1, 5:1), substr:15(#3, 1:1, 5:1))";
        OmniHashBuilderOperatorFactory hashBuilderOperatorFactory = new OmniHashBuilderOperatorFactory(buildTypes,
                buildHashCols, Optional.of(filterExpression), Optional.empty(), null, operatorCount);
        OmniOperator hashBuilderOperator = hashBuilderOperatorFactory.createOperator();
        hashBuilderOperator.addInput(buildVecBatch);
        hashBuilderOperator.getOutput();

        VecType[] probeTypes = {IntVecType.INTEGER, new VarcharVecType(5)};
        Object[][] probeDatas = {{20, 16, 13, 4, 20, 4, 22, 19, 8, 7},
                {"35709", "35709", "31904", "12477", null, "38721", "90419", "35709", "88371", null}};
        VecBatch probeVecBatch = createVecBatch(probeTypes, probeDatas);

        int[] probeOutputCols = {0, 1};
        int[] probeHashCols = {0};
        int[] buildOutputCols = {0, 1};
        VecType[] buildOutputTypes = {IntVecType.INTEGER, new VarcharVecType(5)};
        OmniLookupJoinOperatorFactory lookupJoinOperatorFactory = new OmniLookupJoinOperatorFactory(probeTypes,
                probeOutputCols, probeHashCols, buildOutputCols, buildOutputTypes, OMNI_JOIN_TYPE_INNER,
                hashBuilderOperatorFactory);
        OmniOperator lookupJoinOperator = lookupJoinOperatorFactory.createOperator();
        lookupJoinOperator.addInput(probeVecBatch);
        Iterator<VecBatch> results = lookupJoinOperator.getOutput();

        VecBatch resultVecBatch = results.next();
        int len = resultVecBatch.getRowCount();
        assertEquals(len, 3);
        assertEquals(resultVecBatch.getVectorCount(), 4);
        Object[][] expectedDatas = {{20, 16, 19}, {"35709", "35709", "35709"}, {20, 16, 19},
                {"31904", "31904", "31904"}};
        assertVecBatchEquals(resultVecBatch, expectedDatas);
        freeVecBatch(resultVecBatch);
        lookupJoinOperator.close();
        hashBuilderOperator.close();
        lookupJoinOperatorFactory.close();
        hashBuilderOperatorFactory.close();
    }

    /**
     * Test inner join without output
     */
    @Test
    public void testInnerEqualityJoinWithNoOutput() {
        VecType[] buildTypes = {LongVecType.LONG, LongVecType.LONG};
        Object[][] buildDatas = {{1L, 2L, 3L, 4L}, {111L, 11L, 333L, 33L}};
        VecBatch buildVecBatch = createVecBatch(buildTypes, buildDatas);

        int[] buildHashCols = {0};
        int operatorCount = 1;
        OmniHashBuilderOperatorFactory hashBuilderOperatorFactory = new OmniHashBuilderOperatorFactory(buildTypes,
                buildHashCols, Optional.empty(), Optional.empty(), null, operatorCount);
        OmniOperator hashBuilderOperator = hashBuilderOperatorFactory.createOperator();
        hashBuilderOperator.addInput(buildVecBatch);
        hashBuilderOperator.getOutput();

        VecType[] probeTypes = {LongVecType.LONG, LongVecType.LONG};
        Object[][] probeDatas = {{0L, 5L, 6L, 7L}, {11L, 22L, 33L, 44L}};
        VecBatch probeVecBatch = createVecBatch(probeTypes, probeDatas);

        int[] probeOutputCols = {0, 1};
        int[] probeHashCols = {0};
        int[] buildOutputCols = {0, 1};
        VecType[] buildOutputTypes = {LongVecType.LONG, LongVecType.LONG};
        OmniLookupJoinOperatorFactory lookupJoinOperatorFactory = new OmniLookupJoinOperatorFactory(probeTypes,
                probeOutputCols, probeHashCols, buildOutputCols, buildOutputTypes, OMNI_JOIN_TYPE_INNER,
                hashBuilderOperatorFactory);
        OmniOperator lookupJoinOperator = lookupJoinOperatorFactory.createOperator();
        lookupJoinOperator.addInput(probeVecBatch);
        Iterator<VecBatch> results = lookupJoinOperator.getOutput();
        assertTrue(results != null);
        assertEquals(results.hasNext(), false);
        lookupJoinOperator.close();
        hashBuilderOperator.close();
        lookupJoinOperatorFactory.close();
        hashBuilderOperatorFactory.close();
    }
}
