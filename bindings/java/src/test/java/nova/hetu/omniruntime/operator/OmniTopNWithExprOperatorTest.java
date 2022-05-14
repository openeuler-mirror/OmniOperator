/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 */

package nova.hetu.omniruntime.operator;

import static nova.hetu.omniruntime.util.TestUtils.assertVecBatchEquals;
import static nova.hetu.omniruntime.util.TestUtils.createVecBatch;
import static nova.hetu.omniruntime.util.TestUtils.freeVecBatch;
import static nova.hetu.omniruntime.util.TestUtils.getOmniJsonFieldReference;
import static nova.hetu.omniruntime.util.TestUtils.getOmniJsonLiteral;
import static nova.hetu.omniruntime.util.TestUtils.omniFunctionExpr;
import static nova.hetu.omniruntime.util.TestUtils.omniJsonFourArithmeticExpr;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import nova.hetu.omniruntime.operator.config.OperatorConfig;
import nova.hetu.omniruntime.operator.topn.OmniTopNWithExprOperatorFactory;
import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.type.IntDataType;
import nova.hetu.omniruntime.type.LongDataType;
import nova.hetu.omniruntime.utils.OmniRuntimeException;
import nova.hetu.omniruntime.vector.VecBatch;

import org.testng.annotations.Test;

import java.util.Iterator;

/**
 * The type Omni TopN with expression operator test.
 *
 * @since 2021-11-11
 */
public class OmniTopNWithExprOperatorTest {
    @Test
    public void testTopNWithAllExpr() {
        DataType[] sourceTypes = {IntDataType.INTEGER, LongDataType.LONG, LongDataType.LONG};
        String[] sortKeys = {
                omniJsonFourArithmeticExpr("ADD", 1, getOmniJsonFieldReference(1, 0), getOmniJsonLiteral(1, false, 5)),
                omniJsonFourArithmeticExpr("MODULUS", 2, getOmniJsonFieldReference(2, 2),
                        getOmniJsonLiteral(2, false, 3))};
        int[] sortAsc = {0, 1};
        int[] nullFirst = {0, 0};

        int expectedRowSize = 5;

        OmniTopNWithExprOperatorFactory omniTopNOperatorFactory = new OmniTopNWithExprOperatorFactory(sourceTypes,
                expectedRowSize, sortKeys, sortAsc, nullFirst);
        OmniOperator operator = omniTopNOperatorFactory.createOperator();

        Object[][] sourceDatas = {{5, 8, 8, 6, 8, 4, 13, 15}, {2L, 5L, 3L, 11L, 4L, 3L, 0L, 23L},
                {5L, 3L, 2L, 6L, 1L, 4L, 7L, 8L}};
        VecBatch vecBatch = createVecBatch(sourceTypes, sourceDatas);

        operator.addInput(vecBatch);
        Iterator<VecBatch> output = operator.getOutput();

        assertEquals(output.hasNext(), true);
        VecBatch resultVecBatch = output.next();
        assertEquals(output.hasNext(), false);
        assertEquals(resultVecBatch.getRowCount(), expectedRowSize);
        assertEquals(resultVecBatch.getVectorCount(), sourceTypes.length + sortKeys.length);

        Object[][] expectedDatas = {{15, 13, 8, 8, 8}, {23L, 0L, 5L, 4L, 3L}, {8L, 7L, 3L, 1L, 2L},
                {20, 18, 13, 13, 13}, {2L, 1L, 0L, 1L, 2L}};
        assertVecBatchEquals(resultVecBatch, expectedDatas);

        freeVecBatch(resultVecBatch);
        operator.close();
        omniTopNOperatorFactory.close();
    }

    @Test
    public void testTopNWithPartialExpr() {
        DataType[] sourceTypes = {IntDataType.INTEGER, LongDataType.LONG, LongDataType.LONG};
        String[] sortKeys = {getOmniJsonFieldReference(1, 0), omniJsonFourArithmeticExpr("MODULUS", 2,
                getOmniJsonFieldReference(2, 2), getOmniJsonLiteral(2, false, 3))};
        int[] sortAsc = {0, 1};
        int[] nullFirst = {0, 0};

        int expectedRowSize = 5;

        OmniTopNWithExprOperatorFactory omniTopNOperatorFactory = new OmniTopNWithExprOperatorFactory(sourceTypes,
                expectedRowSize, sortKeys, sortAsc, nullFirst);
        OmniOperator operator = omniTopNOperatorFactory.createOperator();

        Object[][] sourceDatas = {{5, 8, 8, 6, 8, 4, 13, 15}, {2L, 5L, 3L, 11L, 4L, 3L, 0L, 23L},
                {5L, 3L, 2L, 6L, 1L, 4L, 7L, 8L}};
        VecBatch vecBatch = createVecBatch(sourceTypes, sourceDatas);

        operator.addInput(vecBatch);
        Iterator<VecBatch> output = operator.getOutput();

        assertEquals(output.hasNext(), true);
        VecBatch resultVecBatch = output.next();
        assertEquals(output.hasNext(), false);
        assertEquals(resultVecBatch.getRowCount(), expectedRowSize);
        assertEquals(resultVecBatch.getVectorCount(), 4);

        Object[][] expectedDatas = {{15, 13, 8, 8, 8}, {23L, 0L, 5L, 4L, 3L}, {8L, 7L, 3L, 1L, 2L},
                {2L, 1L, 0L, 1L, 2L}};
        assertVecBatchEquals(resultVecBatch, expectedDatas);

        freeVecBatch(resultVecBatch);
        operator.close();
        omniTopNOperatorFactory.close();
    }

    @Test
    public void testTopNWithNoExpr() {
        DataType[] sourceTypes = {IntDataType.INTEGER, LongDataType.LONG, LongDataType.LONG};
        String[] sortKeys = {getOmniJsonFieldReference(1, 0), getOmniJsonFieldReference(2, 2)};
        int[] sortAsc = {0, 1};
        int[] nullFirst = {0, 0};

        int expectedRowSize = 5;

        OmniTopNWithExprOperatorFactory omniTopNOperatorFactory = new OmniTopNWithExprOperatorFactory(sourceTypes,
                expectedRowSize, sortKeys, sortAsc, nullFirst);
        OmniOperator operator = omniTopNOperatorFactory.createOperator();

        Object[][] sourceDatas = {{5, 8, 8, 6, 8, 4, 13, 15}, {2L, 5L, 3L, 11L, 4L, 3L, 0L, 23L},
                {5L, 3L, 2L, 6L, 1L, 4L, 7L, 8L}};
        VecBatch vecBatch = createVecBatch(sourceTypes, sourceDatas);

        operator.addInput(vecBatch);
        Iterator<VecBatch> output = operator.getOutput();

        assertEquals(output.hasNext(), true);
        VecBatch resultVecBatch = output.next();
        assertEquals(output.hasNext(), false);
        assertEquals(resultVecBatch.getRowCount(), expectedRowSize);
        assertEquals(resultVecBatch.getVectorCount(), 3);

        Object[][] expectedDatas = {{15, 13, 8, 8, 8}, {23L, 0L, 4L, 3L, 5L}, {8L, 7L, 1L, 2L, 3L}};
        assertVecBatchEquals(resultVecBatch, expectedDatas);

        freeVecBatch(resultVecBatch);
        operator.close();
        omniTopNOperatorFactory.close();
    }

    @Test(expectedExceptions = OmniRuntimeException.class, expectedExceptionsMessageRegExp = ".*EXPRESSION_NOT_SUPPORT.*")
    public void testTopNWithInvalidKeys() {
        DataType[] sourceTypes = {IntDataType.INTEGER, LongDataType.LONG, LongDataType.LONG};
        String[] sortKeys = {omniFunctionExpr("abc", 2, getOmniJsonFieldReference(2, 1))};
        int[] sortAsc = {0};
        int[] nullFirst = {0};
        int expectedRowSize = 5;

        OmniTopNWithExprOperatorFactory omniTopNOperatorFactory = new OmniTopNWithExprOperatorFactory(sourceTypes,
                expectedRowSize, sortKeys, sortAsc, nullFirst);
    }

    @Test
    public void testFactoryJitContextEquals() {
        DataType[] sourceTypes = {IntDataType.INTEGER, LongDataType.LONG, LongDataType.LONG};
        String[] sortKeys = {getOmniJsonFieldReference(1, 0), getOmniJsonFieldReference(2, 2)};
        int[] sortAsc = {0, 1};
        int[] nullFirst = {0, 0};
        int expectedRowSize = 5;
        OmniTopNWithExprOperatorFactory.JitContext factory1 = new OmniTopNWithExprOperatorFactory.JitContext(
                sourceTypes, expectedRowSize, sortKeys, sortAsc, nullFirst, new OperatorConfig());
        OmniTopNWithExprOperatorFactory.JitContext factory2 = new OmniTopNWithExprOperatorFactory.JitContext(
                sourceTypes, expectedRowSize, sortKeys, sortAsc, nullFirst, new OperatorConfig());
        OmniTopNWithExprOperatorFactory.JitContext factory3 = null;
        assertTrue(factory1.equals(factory2));
        assertTrue(factory1.equals(factory1));
        assertFalse(factory1.equals(factory3));
    }
}
