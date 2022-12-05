/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

package nova.hetu.omniruntime.operator;

import static nova.hetu.omniruntime.constants.JoinType.OMNI_JOIN_TYPE_INNER;
import static nova.hetu.omniruntime.constants.JoinType.OMNI_JOIN_TYPE_LEFT_SEMI;
import static nova.hetu.omniruntime.util.TestUtils.assertVecBatchEquals;
import static nova.hetu.omniruntime.util.TestUtils.createBlankVecBatch;
import static nova.hetu.omniruntime.util.TestUtils.createVecBatch;
import static nova.hetu.omniruntime.util.TestUtils.freeVecBatch;
import static nova.hetu.omniruntime.util.TestUtils.getOmniJsonFieldReference;
import static nova.hetu.omniruntime.util.TestUtils.getOmniJsonLiteral;
import static nova.hetu.omniruntime.util.TestUtils.omniFunctionExpr;
import static nova.hetu.omniruntime.util.TestUtils.omniJsonFourArithmeticExpr;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

import nova.hetu.omniruntime.operator.config.OperatorConfig;
import nova.hetu.omniruntime.operator.join.OmniSmjBufferedTableWithExprOperatorFactory;
import nova.hetu.omniruntime.operator.join.OmniSmjStreamedTableWithExprOperatorFactory;
import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.type.IntDataType;
import nova.hetu.omniruntime.type.LongDataType;
import nova.hetu.omniruntime.utils.OmniRuntimeException;
import nova.hetu.omniruntime.vector.VecBatch;

import org.testng.annotations.Test;

import java.util.Iterator;
import java.util.Optional;

/**
 * The type Omni sort merge join with expression operators test.
 *
 * @since 2022-1-10
 */
public class OmniSortMergeJoinWithExprOperatorsTest {
    /**
     * Test inner hash join one column 1.
     */
    @Test
    public void testSmjOneTimeEqualCondition() {
        DataType[] streamedTypes = {IntDataType.INTEGER, LongDataType.LONG};

        String[] streamedKeyExps = {
                omniJsonFourArithmeticExpr("ADD", 1, getOmniJsonFieldReference(1, 0), getOmniJsonLiteral(1, false, 5))};
        int[] streamedOutputCols = {1};
        OmniSmjStreamedTableWithExprOperatorFactory streamedBuilderWithExprOperatorFactory =
                new OmniSmjStreamedTableWithExprOperatorFactory(
                streamedTypes, streamedKeyExps, streamedOutputCols, OMNI_JOIN_TYPE_INNER, Optional.empty());

        DataType[] bufferedTypes = {LongDataType.LONG, IntDataType.INTEGER};

        int[] bufferedOutputCols = {0};
        String[] bufferedKeyExps = {
                omniJsonFourArithmeticExpr("ADD", 1, getOmniJsonFieldReference(1, 1), getOmniJsonLiteral(1, false, 5))};
        OmniSmjBufferedTableWithExprOperatorFactory bufferedWithExprOperatorFactory =
                new OmniSmjBufferedTableWithExprOperatorFactory(
                bufferedTypes, bufferedKeyExps, bufferedOutputCols, streamedBuilderWithExprOperatorFactory);
        OmniOperator bufferedTableOperator = bufferedWithExprOperatorFactory.createOperator();

        // start to add input
        Object[][] streamedDatas1 = {{0, 1, 2, 3, 4, 5}, {6600L, 5500L, 4400L, 3300L, 2200L, 1100L}};
        VecBatch streamedVecBatch1 = createVecBatch(streamedTypes, streamedDatas1);
        OmniOperator streamedTableOperator = streamedBuilderWithExprOperatorFactory.createOperator();
        int intputResult = streamedTableOperator.addInput(streamedVecBatch1);
        assertEquals(intputResult, 3);

        Object[][] bufferedDatas1 = {{6006L, 5005L, 4004L, 3003L, 2002L, 1001L}, {0, 1, 2, 3, 4, 5}};
        VecBatch bufferedVecBatch1 = createVecBatch(bufferedTypes, bufferedDatas1);
        intputResult = bufferedTableOperator.addInput(bufferedVecBatch1);
        assertEquals(intputResult, 3);

        VecBatch bufferedVecBatchEof = createBlankVecBatch(bufferedTypes);
        intputResult = bufferedTableOperator.addInput(bufferedVecBatchEof);
        assertEquals(intputResult, 2);

        VecBatch streamedVecBatchEof = createBlankVecBatch(streamedTypes);
        intputResult = streamedTableOperator.addInput(streamedVecBatchEof);
        assertEquals(intputResult, 5);

        Iterator<VecBatch> results = bufferedTableOperator.getOutput();
        VecBatch resultVecBatch = results.next();

        int len = resultVecBatch.getRowCount();
        assertEquals(len, 6);
        Object[][] expectedDatas = {{6600L, 5500L, 4400L, 3300L, 2200L, 1100L},
                {6006L, 5005L, 4004L, 3003L, 2002L, 1001L}};
        assertVecBatchEquals(resultVecBatch, expectedDatas);

        freeVecBatch(resultVecBatch);
        bufferedTableOperator.close();
        bufferedWithExprOperatorFactory.close();
        streamedTableOperator.close();
        streamedBuilderWithExprOperatorFactory.close();
    }

    @Test(expectedExceptions = OmniRuntimeException.class, expectedExceptionsMessageRegExp =
            ".*EXPRESSION_NOT_SUPPORT.*")
    public void testInvalidStreamedKeys() {
        DataType[] streamedTypes = {IntDataType.INTEGER, LongDataType.LONG};
        String[] streamedKeyExps = {omniFunctionExpr("abc", 1, getOmniJsonFieldReference(1, 0))};
        int[] streamedOutputCols = {1};
        OmniSmjStreamedTableWithExprOperatorFactory streamedBuilderWithExprOperatorFactory = new OmniSmjStreamedTableWithExprOperatorFactory(
                streamedTypes, streamedKeyExps, streamedOutputCols, OMNI_JOIN_TYPE_INNER, Optional.empty());
    }

    @Test(expectedExceptions = OmniRuntimeException.class, expectedExceptionsMessageRegExp =
            ".*EXPRESSION_NOT_SUPPORT.*")
    public void testInvalidBufferedKeys() {
        DataType[] streamedTypes = {IntDataType.INTEGER, LongDataType.LONG};
        String[] streamedKeyExps = {
                omniJsonFourArithmeticExpr("ADD", 1, getOmniJsonFieldReference(1, 0), getOmniJsonLiteral(1, false, 5))};
        int[] streamedOutputCols = {1};
        OmniSmjStreamedTableWithExprOperatorFactory streamedBuilderWithExprOperatorFactory =
                new OmniSmjStreamedTableWithExprOperatorFactory(
                streamedTypes, streamedKeyExps, streamedOutputCols, OMNI_JOIN_TYPE_INNER, Optional.empty());

        DataType[] bufferedTypes = {LongDataType.LONG, IntDataType.INTEGER};
        int[] bufferedOutputCols = {0};
        String[] bufferedKeyExps = {omniFunctionExpr("abc", 2, getOmniJsonFieldReference(2, 1))};
        OmniSmjBufferedTableWithExprOperatorFactory bufferedWithExprOperatorFactory =
                new OmniSmjBufferedTableWithExprOperatorFactory(
                bufferedTypes, bufferedKeyExps, bufferedOutputCols, streamedBuilderWithExprOperatorFactory);
    }

    @Test
    public void testFactoryContextEquals() {
        DataType[] streamedTypes = {IntDataType.INTEGER, LongDataType.LONG};

        String[] streamedKeyExps = {
                omniJsonFourArithmeticExpr("ADD", 1, getOmniJsonFieldReference(1, 0), getOmniJsonLiteral(1, false, 5))};
        int[] streamedOutputCols = {1};
        OmniSmjStreamedTableWithExprOperatorFactory.FactoryContext streamedBuilderWithExprOperatorFactory1 =
                new OmniSmjStreamedTableWithExprOperatorFactory.FactoryContext(
                streamedTypes, streamedKeyExps, streamedOutputCols, OMNI_JOIN_TYPE_INNER, Optional.empty(),
                new OperatorConfig());
        OmniSmjStreamedTableWithExprOperatorFactory.FactoryContext streamedBuilderWithExprOperatorFactory2 =
                new OmniSmjStreamedTableWithExprOperatorFactory.FactoryContext(
                streamedTypes, streamedKeyExps, streamedOutputCols, OMNI_JOIN_TYPE_INNER, Optional.empty(),
                new OperatorConfig());
        OmniSmjStreamedTableWithExprOperatorFactory.FactoryContext streamedBuilderWithExprOperatorFactory3 = null;
        assertEquals(streamedBuilderWithExprOperatorFactory2, streamedBuilderWithExprOperatorFactory1);
        assertEquals(streamedBuilderWithExprOperatorFactory1, streamedBuilderWithExprOperatorFactory1);
        assertNotEquals(streamedBuilderWithExprOperatorFactory3, streamedBuilderWithExprOperatorFactory1);

        DataType[] bufferedTypes = {LongDataType.LONG, IntDataType.INTEGER};

        OmniSmjStreamedTableWithExprOperatorFactory streamedBuilderWithExprOperatorFactory =
                new OmniSmjStreamedTableWithExprOperatorFactory(
                streamedTypes, streamedKeyExps, streamedOutputCols, OMNI_JOIN_TYPE_INNER, Optional.empty());

        int[] bufferedOutputCols = {0};
        String[] bufferedKeyExps = {
                omniJsonFourArithmeticExpr("ADD", 1, getOmniJsonFieldReference(1, 0), getOmniJsonLiteral(1, false, 5))};
        OmniSmjBufferedTableWithExprOperatorFactory.FactoryContext bufferedWithExprOperatorFactory1 =
                new OmniSmjBufferedTableWithExprOperatorFactory.FactoryContext(
                bufferedTypes, bufferedKeyExps, bufferedOutputCols, new OperatorConfig(),
                streamedBuilderWithExprOperatorFactory);
        OmniSmjBufferedTableWithExprOperatorFactory.FactoryContext bufferedWithExprOperatorFactory2 =
                new OmniSmjBufferedTableWithExprOperatorFactory.FactoryContext(
                bufferedTypes, bufferedKeyExps, bufferedOutputCols, new OperatorConfig(),
                streamedBuilderWithExprOperatorFactory);
        OmniSmjBufferedTableWithExprOperatorFactory.FactoryContext bufferedWithExprOperatorFactory3 = null;
        assertEquals(bufferedWithExprOperatorFactory2, bufferedWithExprOperatorFactory1);
        assertEquals(bufferedWithExprOperatorFactory1, bufferedWithExprOperatorFactory1);
        assertNotEquals(bufferedWithExprOperatorFactory3, bufferedWithExprOperatorFactory1);
    }

    /**
     * Test left semi join one column 1.
     */
    @Test
    public void testSmjOneTimeEqualConditionForLeftSemiJoin() {
        DataType[] streamedTypes = {IntDataType.INTEGER, LongDataType.LONG};
        String[] streamedKeyExps = {
                omniJsonFourArithmeticExpr("ADD", 1, getOmniJsonFieldReference(1, 0), getOmniJsonLiteral(1, false, 5))};
        int[] streamedOutputCols = {0, 1};
        OmniSmjStreamedTableWithExprOperatorFactory streamedBuilderWithExprOperatorFactory = new OmniSmjStreamedTableWithExprOperatorFactory(
                streamedTypes, streamedKeyExps, streamedOutputCols, OMNI_JOIN_TYPE_LEFT_SEMI, Optional.empty());

        DataType[] bufferedTypes = {LongDataType.LONG, IntDataType.INTEGER};
        int[] bufferedOutputCols = {};
        String[] bufferedKeyExps = {
                omniJsonFourArithmeticExpr("ADD", 1, getOmniJsonFieldReference(1, 1), getOmniJsonLiteral(1, false, 5))};
        OmniSmjBufferedTableWithExprOperatorFactory bufferedWithExprOperatorFactory = new OmniSmjBufferedTableWithExprOperatorFactory(
                bufferedTypes, bufferedKeyExps, bufferedOutputCols, streamedBuilderWithExprOperatorFactory);
        OmniOperator bufferedTableOperator = bufferedWithExprOperatorFactory.createOperator();

        // start to add input
        Object[][] streamedDatas1 = {{0, 1, 2, 2, 2, 3, 4, 5},
                {8800L, 7700L, 6600L, 5500L, 4400L, 3300L, 2200L, 1100L}};
        VecBatch streamedVecBatch1 = createVecBatch(streamedTypes, streamedDatas1);
        streamedVecBatch1.getVector(0).setNull(0);
        streamedVecBatch1.getVector(0).setNull(1); // NULL, NULL, 2, 2, 2, 3, 4, 5
        OmniOperator streamedTableOperator = streamedBuilderWithExprOperatorFactory.createOperator();
        int intputResult = streamedTableOperator.addInput(streamedVecBatch1);
        assertEquals(intputResult, 3);

        Object[][] bufferedDatas1 = {{8008L, 7007L, 6006L, 5005L, 4004L, 3003L, 2002L, 1001L},
                {0, 1, 2, 2, 3, 3, 4, 5}};
        VecBatch bufferedVecBatch1 = createVecBatch(bufferedTypes, bufferedDatas1);
        bufferedVecBatch1.getVector(1).setNull(0); // NULL, 1, 2, 2, 3, 3, 4, 5
        intputResult = bufferedTableOperator.addInput(bufferedVecBatch1);
        assertEquals(intputResult, 3);

        VecBatch bufferedVecBatchEof = createBlankVecBatch(bufferedTypes);
        intputResult = bufferedTableOperator.addInput(bufferedVecBatchEof);
        assertEquals(intputResult, 2);

        VecBatch streamedVecBatchEof = createBlankVecBatch(streamedTypes);
        intputResult = streamedTableOperator.addInput(streamedVecBatchEof);
        assertEquals(intputResult, 5);

        Iterator<VecBatch> results = bufferedTableOperator.getOutput();
        VecBatch resultVecBatch = results.next();

        int len = resultVecBatch.getRowCount();
        assertEquals(len, 6);
        Object[][] expectedDatas = {{2, 2, 2, 3, 4, 5}, {6600L, 5500L, 4400L, 3300L, 2200L, 1100L}};
        assertVecBatchEquals(resultVecBatch, expectedDatas);

        freeVecBatch(resultVecBatch);
        bufferedTableOperator.close();
        bufferedWithExprOperatorFactory.close();
        streamedTableOperator.close();
        streamedBuilderWithExprOperatorFactory.close();
    }
}
