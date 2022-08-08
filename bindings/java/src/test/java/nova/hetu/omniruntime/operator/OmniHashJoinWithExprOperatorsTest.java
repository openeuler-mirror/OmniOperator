/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 */

package nova.hetu.omniruntime.operator;

import static nova.hetu.omniruntime.constants.JoinType.OMNI_JOIN_TYPE_INNER;
import static nova.hetu.omniruntime.util.TestUtils.assertVecBatchEquals;
import static nova.hetu.omniruntime.util.TestUtils.createVecBatch;
import static nova.hetu.omniruntime.util.TestUtils.freeVecBatch;
import static nova.hetu.omniruntime.util.TestUtils.getOmniJsonFieldReference;
import static nova.hetu.omniruntime.util.TestUtils.getOmniJsonLiteral;
import static nova.hetu.omniruntime.util.TestUtils.omniFunctionExpr;
import static nova.hetu.omniruntime.util.TestUtils.omniJsonFourArithmeticExpr;
import static nova.hetu.omniruntime.util.TestUtils.omniJsonNotEqualExpr;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

import nova.hetu.omniruntime.operator.config.OperatorConfig;
import nova.hetu.omniruntime.operator.join.OmniHashBuilderWithExprOperatorFactory;
import nova.hetu.omniruntime.operator.join.OmniLookupJoinWithExprOperatorFactory;
import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.type.IntDataType;
import nova.hetu.omniruntime.type.LongDataType;
import nova.hetu.omniruntime.type.VarcharDataType;
import nova.hetu.omniruntime.util.TestUtils;
import nova.hetu.omniruntime.utils.OmniRuntimeException;
import nova.hetu.omniruntime.vector.Vec;
import nova.hetu.omniruntime.vector.VecBatch;

import org.testng.annotations.Test;

import java.util.Iterator;
import java.util.Optional;

/**
 * The type Omni hash join with expression operator test.
 *
 * @since 2021-10-16
 */
public class OmniHashJoinWithExprOperatorsTest {
    /**
     * Test inner hash join one column .
     */
    @Test
    public void testInnerEqualityJoinOneColumn() {
        DataType[] buildTypes = {LongDataType.LONG, LongDataType.LONG};
        Object[][] buildDatas = {{1L, 2L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 1L},
                {79L, 79L, 70L, 70L, 70L, 70L, 70L, 70L, 70L, 70L}};
        VecBatch buildVecBatch = createVecBatch(buildTypes, buildDatas);

        String[] buildHashKeys = {omniJsonFourArithmeticExpr("ADD", 2, getOmniJsonFieldReference(2, 0),
                getOmniJsonLiteral(2, false, 50))};
        int operatorCount = 1;
        OmniHashBuilderWithExprOperatorFactory hashBuilderOperatorFactory = new OmniHashBuilderWithExprOperatorFactory(
                buildTypes, buildHashKeys, Optional.empty(), operatorCount);
        OmniOperator hashBuilderOperator = hashBuilderOperatorFactory.createOperator();
        hashBuilderOperator.addInput(buildVecBatch);
        hashBuilderOperator.getOutput();

        DataType[] probeTypes = {LongDataType.LONG, LongDataType.LONG};
        Object[][] probeDatas = {{1L, 2L, 3L, 4L, 5L, 6L, 1L, 1L, 2L, 3L},
                {78L, 78L, 78L, 78L, 78L, 78L, 78L, 82L, 82L, 65L}};
        VecBatch probeVecBatch = createVecBatch(probeTypes, probeDatas);

        int[] probeOutputCols = {1};
        String[] probeHashKeys = {omniJsonFourArithmeticExpr("ADD", 2, getOmniJsonFieldReference(2, 0),
                getOmniJsonLiteral(2, false, 50))};
        int[] buildOutputCols = {1};
        DataType[] buildOutputTypes = {LongDataType.LONG};
        OmniLookupJoinWithExprOperatorFactory lookupJoinOperatorFactory = new OmniLookupJoinWithExprOperatorFactory(
                probeTypes, probeOutputCols, probeHashKeys, buildOutputCols, buildOutputTypes, OMNI_JOIN_TYPE_INNER,
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
     * Test inner hash join one dictionary column .
     */
    @Test
    public void testInnerEqualityJoinOneDictionaryColumn() {
        DataType[] buildTypes = {LongDataType.LONG, LongDataType.LONG};
        Object[][] buildDatas = {{1L, 2L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 1L},
                {79L, 79L, 70L, 70L, 70L, 70L, 70L, 70L, 70L, 70L}};
        Vec[] buildVecs = new Vec[2];
        int[] ids = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        buildVecs[0] = TestUtils.createDictionaryVec(buildTypes[0], buildDatas[0], ids);
        buildVecs[1] = TestUtils.createDictionaryVec(buildTypes[1], buildDatas[1], ids);
        VecBatch buildVecBatch = new VecBatch(buildVecs);

        String[] buildHashKeys = {omniJsonFourArithmeticExpr("ADD", 2, getOmniJsonFieldReference(2, 0),
                getOmniJsonLiteral(2, false, 50))};
        int operatorCount = 1;
        OmniHashBuilderWithExprOperatorFactory hashBuilderOperatorFactory = new OmniHashBuilderWithExprOperatorFactory(
                buildTypes, buildHashKeys, Optional.empty(), operatorCount);
        OmniOperator hashBuilderOperator = hashBuilderOperatorFactory.createOperator();
        hashBuilderOperator.addInput(buildVecBatch);
        hashBuilderOperator.getOutput();

        DataType[] probeTypes = {LongDataType.LONG, LongDataType.LONG};
        Object[][] probeDatas = {{1L, 2L, 3L, 4L, 5L, 6L, 1L, 1L, 2L, 3L},
                {78L, 78L, 78L, 78L, 78L, 78L, 78L, 82L, 82L, 65L}};
        Vec[] probeVecs = new Vec[2];
        probeVecs[0] = TestUtils.createDictionaryVec(probeTypes[0], probeDatas[0], ids);
        probeVecs[1] = TestUtils.createDictionaryVec(probeTypes[1], probeDatas[1], ids);
        VecBatch probeVecBatch = new VecBatch(probeVecs);

        int[] probeOutputCols = {1};
        String[] probeHashKeys = {omniJsonFourArithmeticExpr("ADD", 2, getOmniJsonFieldReference(2, 0),
                getOmniJsonLiteral(2, false, 50))};
        int[] buildOutputCols = {1};
        DataType[] buildOutputTypes = {LongDataType.LONG};
        OmniLookupJoinWithExprOperatorFactory lookupJoinOperatorFactory = new OmniLookupJoinWithExprOperatorFactory(
                probeTypes, probeOutputCols, probeHashKeys, buildOutputCols, buildOutputTypes, OMNI_JOIN_TYPE_INNER,
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
     * Test inner hash join with join filter expression .
     */
    @Test
    public void testInnerEqualityJoinWithCharFilter() {
        DataType[] buildTypes = {IntDataType.INTEGER, new VarcharDataType(5)};
        Object[][] buildDatas = {{19, 14, 7, 19, 1, 20, 10, 13, 20, 16},
                {"35709", "31904", "35709", "31904", "35709", "31904", "35709", "31904", "35709", "31904"}};
        VecBatch buildVecBatch = createVecBatch(buildTypes, buildDatas);

        String[] buildHashCols = {getOmniJsonFieldReference(1, 0)};
        int operatorCount = 1;
        String filterExpression = omniJsonNotEqualExpr(
                omniFunctionExpr("substr", 15,
                        getOmniJsonFieldReference(15, 1) + "," + getOmniJsonLiteral(1, false, 1) + ","
                                + getOmniJsonLiteral(1, false, 5)),
                omniFunctionExpr("substr", 15, getOmniJsonFieldReference(15, 3) + "," + getOmniJsonLiteral(1, false, 1)
                        + "," + getOmniJsonLiteral(1, false, 5)));
        OmniHashBuilderWithExprOperatorFactory hashBuilderOperatorFactory = new OmniHashBuilderWithExprOperatorFactory(
                buildTypes, buildHashCols, Optional.of(filterExpression), operatorCount);
        OmniOperator hashBuilderOperator = hashBuilderOperatorFactory.createOperator();
        hashBuilderOperator.addInput(buildVecBatch);
        hashBuilderOperator.getOutput();

        DataType[] probeTypes = {IntDataType.INTEGER, new VarcharDataType(5)};
        Object[][] probeDatas = {{20, 16, 13, 4, 20, 4, 22, 19, 8, 7},
                {"35709", "35709", "31904", "12477", null, "38721", "90419", "35709", "88371", null}};
        VecBatch probeVecBatch = createVecBatch(probeTypes, probeDatas);

        int[] probeOutputCols = {0, 1};
        String[] probeHashCols = {getOmniJsonFieldReference(1, 0)};
        int[] buildOutputCols = {0, 1};
        DataType[] buildOutputTypes = {IntDataType.INTEGER, new VarcharDataType(5)};
        OmniLookupJoinWithExprOperatorFactory lookupJoinOperatorFactory = new OmniLookupJoinWithExprOperatorFactory(
                probeTypes, probeOutputCols, probeHashCols, buildOutputCols, buildOutputTypes, OMNI_JOIN_TYPE_INNER,
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

    @Test(expectedExceptions = OmniRuntimeException.class, expectedExceptionsMessageRegExp =
            ".*EXPRESSION_NOT_SUPPORT.*")
    public void testHashBuilderWithInvalidKeys() {
        DataType[] buildTypes = {LongDataType.LONG, LongDataType.LONG};
        int operatorCount = 1;

        // invalid build hash key
        String[] invalidBuildHashKeys = {omniFunctionExpr("abc", 2, getOmniJsonFieldReference(2, 1))};
        OmniHashBuilderWithExprOperatorFactory operatorFactory = new OmniHashBuilderWithExprOperatorFactory(buildTypes,
                invalidBuildHashKeys, Optional.empty(), operatorCount);
    }

    @Test(expectedExceptions = OmniRuntimeException.class, expectedExceptionsMessageRegExp =
            ".*EXPRESSION_NOT_SUPPORT.*")
    public void testLookupJoinWithInvalidKeys() {
        DataType[] buildTypes = {LongDataType.LONG, LongDataType.LONG};
        int operatorCount = 1;
        String[] buildHashCols = {getOmniJsonFieldReference(2, 0)};
        OmniHashBuilderWithExprOperatorFactory hashBuilderOperatorFactory = new OmniHashBuilderWithExprOperatorFactory(
                buildTypes, buildHashCols, Optional.empty(), operatorCount);

        DataType[] probeTypes = {LongDataType.LONG, LongDataType.LONG};
        int[] probeOutputCols = {0, 1};
        String[] invalidProbeHashKeys = {omniFunctionExpr("abc", 2, getOmniJsonFieldReference(2, 1))};
        int[] buildOutputCols = {0, 1};
        DataType[] buildOutputTypes = {LongDataType.LONG, LongDataType.LONG};
        OmniLookupJoinWithExprOperatorFactory lookupJoinOperatorFactory = new OmniLookupJoinWithExprOperatorFactory(
                probeTypes, probeOutputCols, invalidProbeHashKeys, buildOutputCols, buildOutputTypes,
                OMNI_JOIN_TYPE_INNER, hashBuilderOperatorFactory);

        hashBuilderOperatorFactory.close();
    }

    @Test(expectedExceptions = OmniRuntimeException.class, expectedExceptionsMessageRegExp =
            ".*EXPRESSION_NOT_SUPPORT.*")
    public void testInnerEqualityJoinWithInvalidExprs() {
        DataType[] buildTypes = {LongDataType.LONG, LongDataType.LONG};
        int operatorCount = 1;
        String[] buildHashCols = {getOmniJsonFieldReference(2, 0)};
        String filterExpression = omniJsonNotEqualExpr(
                omniFunctionExpr("substring", 15,
                        getOmniJsonFieldReference(2, 1) + "," + getOmniJsonLiteral(1, false, 1) + ","
                                + getOmniJsonLiteral(1, false, 5)),
                omniFunctionExpr("substring", 15, getOmniJsonFieldReference(2, 0) + ","
                        + getOmniJsonLiteral(1, false, 1) + "," + getOmniJsonLiteral(1, false, 5)));
        OmniHashBuilderWithExprOperatorFactory hashBuilderOperatorFactory = new OmniHashBuilderWithExprOperatorFactory(
                buildTypes, buildHashCols, Optional.of(filterExpression), operatorCount);

        DataType[] probeTypes = {LongDataType.LONG, LongDataType.LONG};
        int[] probeOutputCols = {0, 1};
        String[] probeHashCols = {getOmniJsonFieldReference(2, 1)};
        int[] buildOutputCols = {0, 1};
        DataType[] buildOutputTypes = {LongDataType.LONG, LongDataType.LONG};
        OmniLookupJoinWithExprOperatorFactory lookupJoinOperatorFactory = new OmniLookupJoinWithExprOperatorFactory(
                probeTypes, probeOutputCols, probeHashCols, buildOutputCols, buildOutputTypes, OMNI_JOIN_TYPE_INNER,
                hashBuilderOperatorFactory);

        hashBuilderOperatorFactory.close();
    }

    @Test
    public void testFactoryContextEquals() {
        DataType[] buildTypes = {LongDataType.LONG, LongDataType.LONG};
        String[] buildHashKeys = {omniJsonFourArithmeticExpr("ADD", 2, getOmniJsonFieldReference(2, 0),
                getOmniJsonLiteral(2, false, 50))};
        int operatorCount = 1;
        OmniHashBuilderWithExprOperatorFactory.FactoryContext hashBuilderOperatorFactory1 =
                new OmniHashBuilderWithExprOperatorFactory.FactoryContext(
                buildTypes, buildHashKeys, Optional.empty(), operatorCount, new OperatorConfig());
        OmniHashBuilderWithExprOperatorFactory.FactoryContext hashBuilderOperatorFactory2 =
                new OmniHashBuilderWithExprOperatorFactory.FactoryContext(
                buildTypes, buildHashKeys, Optional.empty(), operatorCount, new OperatorConfig());
        OmniHashBuilderWithExprOperatorFactory.FactoryContext hashBuilderOperatorFactory3 = null;
        assertEquals(hashBuilderOperatorFactory2, hashBuilderOperatorFactory1);
        assertEquals(hashBuilderOperatorFactory1, hashBuilderOperatorFactory1);
        assertNotEquals(hashBuilderOperatorFactory3, hashBuilderOperatorFactory1);

        DataType[] probeTypes = {LongDataType.LONG, LongDataType.LONG};
        int[] probeOutputCols = {1};
        String[] probeHashKeys = {omniJsonFourArithmeticExpr("ADD", 2, getOmniJsonFieldReference(2, 0),
                getOmniJsonLiteral(2, false, 50))};
        int[] buildOutputCols = {1};
        DataType[] buildOutputTypes = {LongDataType.LONG};

        OmniHashBuilderWithExprOperatorFactory omniHashBuilderWithExprOperatorFactory =
                new OmniHashBuilderWithExprOperatorFactory(
                buildTypes, buildHashKeys, Optional.empty(), operatorCount, new OperatorConfig());
        OmniLookupJoinWithExprOperatorFactory.FactoryContext lookupJoinOperatorFactory1 =
                new OmniLookupJoinWithExprOperatorFactory.FactoryContext(
                probeTypes, probeOutputCols, probeHashKeys, buildOutputCols, buildOutputTypes, OMNI_JOIN_TYPE_INNER,
                new OperatorConfig(), omniHashBuilderWithExprOperatorFactory);
        OmniLookupJoinWithExprOperatorFactory.FactoryContext lookupJoinOperatorFactory2 =
                new OmniLookupJoinWithExprOperatorFactory.FactoryContext(
                probeTypes, probeOutputCols, probeHashKeys, buildOutputCols, buildOutputTypes, OMNI_JOIN_TYPE_INNER,
                new OperatorConfig(), omniHashBuilderWithExprOperatorFactory);
        OmniLookupJoinWithExprOperatorFactory.FactoryContext lookupJoinOperatorFactory3 = null;

        assertEquals(lookupJoinOperatorFactory2, lookupJoinOperatorFactory1);
        assertEquals(lookupJoinOperatorFactory1, lookupJoinOperatorFactory1);
        assertNotEquals(lookupJoinOperatorFactory3, lookupJoinOperatorFactory1);
    }
}
