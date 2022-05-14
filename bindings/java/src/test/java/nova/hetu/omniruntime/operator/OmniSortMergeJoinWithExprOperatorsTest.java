/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

package nova.hetu.omniruntime.operator;

import static nova.hetu.omniruntime.constants.JoinType.OMNI_JOIN_TYPE_INNER;
import static nova.hetu.omniruntime.util.TestUtils.assertVecBatchEquals;
import static nova.hetu.omniruntime.util.TestUtils.createBlankVecBatch;
import static nova.hetu.omniruntime.util.TestUtils.createVecBatch;
import static nova.hetu.omniruntime.util.TestUtils.freeVecBatch;
import static nova.hetu.omniruntime.util.TestUtils.getOmniJsonFieldReference;
import static nova.hetu.omniruntime.util.TestUtils.getOmniJsonLiteral;
import static nova.hetu.omniruntime.util.TestUtils.omniFunctionExpr;
import static nova.hetu.omniruntime.util.TestUtils.omniJsonFourArithmeticExpr;
import static nova.hetu.omniruntime.util.TestUtils.omniJsonNotEqualExpr;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
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
        OmniSmjStreamedTableWithExprOperatorFactory streamedBuilderWithExprOperatorFactory = new OmniSmjStreamedTableWithExprOperatorFactory(
                streamedTypes, streamedKeyExps, streamedOutputCols, OMNI_JOIN_TYPE_INNER, Optional.empty());

        DataType[] bufferedTypes = {LongDataType.LONG, IntDataType.INTEGER};

        int[] bufferedOutputCols = {0};
        String[] bufferedKeyExps = {
                omniJsonFourArithmeticExpr("ADD", 1, getOmniJsonFieldReference(1, 1), getOmniJsonLiteral(1, false, 5))};
        OmniSmjBufferedTableWithExprOperatorFactory bufferedWithExprOperatorFactory = new OmniSmjBufferedTableWithExprOperatorFactory(
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

    @Test(expectedExceptions = OmniRuntimeException.class, expectedExceptionsMessageRegExp = ".*EXPRESSION_NOT_SUPPORT.*")
    public void testInvalidStreamedKeys() {
        DataType[] streamedTypes = {IntDataType.INTEGER, LongDataType.LONG};
        String[] streamedKeyExps = {omniFunctionExpr("abc", 1, getOmniJsonFieldReference(1, 0))};
        int[] streamedOutputCols = {1};
        OmniSmjStreamedTableWithExprOperatorFactory streamedBuilderWithExprOperatorFactory = new OmniSmjStreamedTableWithExprOperatorFactory(
                streamedTypes, streamedKeyExps, streamedOutputCols, OMNI_JOIN_TYPE_INNER, Optional.empty());
    }

    @Test(expectedExceptions = OmniRuntimeException.class, expectedExceptionsMessageRegExp = ".*EXPRESSION_NOT_SUPPORT.*")
    public void testInvalidBufferedKeys() {
        DataType[] streamedTypes = {IntDataType.INTEGER, LongDataType.LONG};
        String[] streamedKeyExps = {
                omniJsonFourArithmeticExpr("ADD", 1, getOmniJsonFieldReference(1, 0), getOmniJsonLiteral(1, false, 5))};
        int[] streamedOutputCols = {1};
        OmniSmjStreamedTableWithExprOperatorFactory streamedBuilderWithExprOperatorFactory = new OmniSmjStreamedTableWithExprOperatorFactory(
                streamedTypes, streamedKeyExps, streamedOutputCols, OMNI_JOIN_TYPE_INNER, Optional.empty());

        DataType[] bufferedTypes = {LongDataType.LONG, IntDataType.INTEGER};
        int[] bufferedOutputCols = {0};
        String[] bufferedKeyExps = {omniFunctionExpr("abc", 2, getOmniJsonFieldReference(2, 1))};
        OmniSmjBufferedTableWithExprOperatorFactory bufferedWithExprOperatorFactory = new OmniSmjBufferedTableWithExprOperatorFactory(
                bufferedTypes, bufferedKeyExps, bufferedOutputCols, streamedBuilderWithExprOperatorFactory);
    }

    @Test
    public void testFactoryJitContextEquals() {
        DataType[] streamedTypes = {IntDataType.INTEGER, LongDataType.LONG};

        String[] streamedKeyExps = {
                omniJsonFourArithmeticExpr("ADD", 1, getOmniJsonFieldReference(1, 0), getOmniJsonLiteral(1, false, 5))};
        int[] streamedOutputCols = {1};
        OmniSmjStreamedTableWithExprOperatorFactory.JitContext streamedBuilderWithExprOperatorFactory1 = new OmniSmjStreamedTableWithExprOperatorFactory.JitContext(
                streamedTypes, streamedKeyExps, streamedOutputCols, OMNI_JOIN_TYPE_INNER, Optional.empty(),
                new OperatorConfig());
        OmniSmjStreamedTableWithExprOperatorFactory.JitContext streamedBuilderWithExprOperatorFactory2 = new OmniSmjStreamedTableWithExprOperatorFactory.JitContext(
                streamedTypes, streamedKeyExps, streamedOutputCols, OMNI_JOIN_TYPE_INNER, Optional.empty(),
                new OperatorConfig());
        OmniSmjStreamedTableWithExprOperatorFactory.JitContext streamedBuilderWithExprOperatorFactory3 = null;
        assertTrue(streamedBuilderWithExprOperatorFactory1.equals(streamedBuilderWithExprOperatorFactory2));
        assertTrue(streamedBuilderWithExprOperatorFactory1.equals(streamedBuilderWithExprOperatorFactory1));
        assertFalse(streamedBuilderWithExprOperatorFactory1.equals(streamedBuilderWithExprOperatorFactory3));

        DataType[] bufferedTypes = {LongDataType.LONG, IntDataType.INTEGER};

        int[] bufferedOutputCols = {0};
        String[] bufferedKeyExps = {
                omniJsonFourArithmeticExpr("ADD", 1, getOmniJsonFieldReference(1, 0), getOmniJsonLiteral(1, false, 5))};
        OmniSmjBufferedTableWithExprOperatorFactory.JitContext bufferedWithExprOperatorFactory1 = new OmniSmjBufferedTableWithExprOperatorFactory.JitContext(
                bufferedTypes, bufferedKeyExps, bufferedOutputCols, new OperatorConfig());
        OmniSmjBufferedTableWithExprOperatorFactory.JitContext bufferedWithExprOperatorFactory2 = new OmniSmjBufferedTableWithExprOperatorFactory.JitContext(
                bufferedTypes, bufferedKeyExps, bufferedOutputCols, new OperatorConfig());
        OmniSmjBufferedTableWithExprOperatorFactory.JitContext bufferedWithExprOperatorFactory3 = null;
        assertTrue(bufferedWithExprOperatorFactory1.equals(bufferedWithExprOperatorFactory2));
        assertTrue(bufferedWithExprOperatorFactory1.equals(bufferedWithExprOperatorFactory1));
        assertFalse(bufferedWithExprOperatorFactory1.equals(bufferedWithExprOperatorFactory3));
    }
}
