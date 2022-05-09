/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 */

package nova.hetu.omniruntime.operator;

import static nova.hetu.omniruntime.util.TestUtils.assertVecBatchEquals;
import static nova.hetu.omniruntime.util.TestUtils.createVecBatch;
import static nova.hetu.omniruntime.util.TestUtils.freeVecBatch;
import static nova.hetu.omniruntime.util.TestUtils.getOmniJsonFieldReference;
import static nova.hetu.omniruntime.util.TestUtils.getOmniJsonLiteral;
import static nova.hetu.omniruntime.util.TestUtils.omniJsonFourArithmeticExpr;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import nova.hetu.omniruntime.operator.config.OperatorConfig;
import nova.hetu.omniruntime.operator.config.SparkSpillConfig;
import nova.hetu.omniruntime.operator.sort.OmniSortWithExprOperatorFactory;
import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.type.IntDataType;
import nova.hetu.omniruntime.type.LongDataType;
import nova.hetu.omniruntime.util.TestUtils;
import nova.hetu.omniruntime.utils.OmniRuntimeException;
import nova.hetu.omniruntime.vector.Vec;
import nova.hetu.omniruntime.vector.VecBatch;

import org.testng.annotations.Test;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;

/**
 * The type Omni sort with expression operator test.
 *
 * @since 2021-10-16
 */
public class OmniSortWithExprOperatorTest {
    private String generateSpillPath() {
        Path path = Paths.get("");
        return path.toAbsolutePath() + File.separator + System.currentTimeMillis();
    }

    /**
     * Test Sort by zero columns which one with expression
     */
    @Test
    public void TestSortByZeroColumnWithExpr() {
        DataType[] sourceTypes = {IntDataType.INTEGER, LongDataType.LONG};
        Object[][] sourceDatas = {{5, 3, 2, 6, 1, 4, 7, 8}, {5L, 3L, 2L, 6L, 1L, 4L, 7L, 8L}};
        VecBatch vecBatch = createVecBatch(sourceTypes, sourceDatas);

        int[] outputCols = {0, 1};
        String[] sortKeys = {getOmniJsonFieldReference(1, 0), getOmniJsonFieldReference(2, 1)};
        int[] ascendings = {1, 1};
        int[] nullFirsts = {0, 0};
        OmniSortWithExprOperatorFactory sortWithExprOperatorFactory = new OmniSortWithExprOperatorFactory(sourceTypes,
                outputCols, sortKeys, ascendings, nullFirsts);
        OmniOperator sortWithExprOperator = sortWithExprOperatorFactory.createOperator();
        sortWithExprOperator.addInput(vecBatch);
        Iterator<VecBatch> results = sortWithExprOperator.getOutput();

        VecBatch resultVecBatch = results.next();
        assertEquals(resultVecBatch.getRowCount(), sourceDatas[0].length);
        Object[][] expectedDatas = {{1, 2, 3, 4, 5, 6, 7, 8}, {1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L}};
        assertVecBatchEquals(resultVecBatch, expectedDatas);

        freeVecBatch(resultVecBatch);
        sortWithExprOperator.close();
        sortWithExprOperatorFactory.close();
    }

    /**
     * Test Sort by one columns which one with expression
     */
    @Test
    public void TestSortByOneColumnWithExpr() {
        DataType[] sourceTypes = {IntDataType.INTEGER, LongDataType.LONG};
        Object[][] sourceDatas = {{5, 3, 2, 6, 1, 4, 7, 8}, {5L, 3L, 2L, 6L, 1L, 4L, 7L, 8L}};
        VecBatch vecBatch = createVecBatch(sourceTypes, sourceDatas);

        int[] outputCols = {0, 1};
        String[] sortKeys = {
                omniJsonFourArithmeticExpr("ADD", 1, getOmniJsonFieldReference(1, 0), getOmniJsonLiteral(1, false, 5)),
                getOmniJsonFieldReference(2, 1)};
        int[] ascendings = {1, 1};
        int[] nullFirsts = {0, 0};
        OmniSortWithExprOperatorFactory sortWithExprOperatorFactory = new OmniSortWithExprOperatorFactory(sourceTypes,
                outputCols, sortKeys, ascendings, nullFirsts);
        OmniOperator sortWithExprOperator = sortWithExprOperatorFactory.createOperator();
        sortWithExprOperator.addInput(vecBatch);
        Iterator<VecBatch> results = sortWithExprOperator.getOutput();

        VecBatch resultVecBatch = results.next();
        assertEquals(resultVecBatch.getRowCount(), sourceDatas[0].length);
        Object[][] expectedDatas = {{1, 2, 3, 4, 5, 6, 7, 8}, {1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L}};
        assertVecBatchEquals(resultVecBatch, expectedDatas);

        freeVecBatch(resultVecBatch);
        sortWithExprOperator.close();
        sortWithExprOperatorFactory.close();
    }

    /**
     * Test Sort by two columns with expression
     */
    @Test
    public void TestSortByTwoColumnsWithExpr() {
        DataType[] sourceTypes = {IntDataType.INTEGER, IntDataType.INTEGER};
        Object[][] sourceDatas = {{5, 3, 2, 6, 1, 4, 7, 8}, {5, 3, 2, 6, 1, 4, 7, 8}};
        VecBatch vecBatch = createVecBatch(sourceTypes, sourceDatas);

        int[] outputCols = {0, 1};
        String[] sortKeys = {
                omniJsonFourArithmeticExpr("ADD", 1, getOmniJsonFieldReference(1, 0), getOmniJsonLiteral(1, false, 5)),
                omniJsonFourArithmeticExpr("ADD", 1, getOmniJsonLiteral(1, false, 5), getOmniJsonFieldReference(1, 1))};
        int[] ascendings = {1, 1};
        int[] nullFirsts = {0, 0};
        OmniSortWithExprOperatorFactory sortWithExprOperatorFactory = new OmniSortWithExprOperatorFactory(sourceTypes,
                outputCols, sortKeys, ascendings, nullFirsts);
        OmniOperator sortWithExprOperator = sortWithExprOperatorFactory.createOperator();
        sortWithExprOperator.addInput(vecBatch);
        Iterator<VecBatch> results = sortWithExprOperator.getOutput();

        VecBatch resultVecBatch = results.next();
        assertEquals(resultVecBatch.getRowCount(), sourceDatas[0].length);
        Object[][] expectedDatas = {{1, 2, 3, 4, 5, 6, 7, 8}, {1, 2, 3, 4, 5, 6, 7, 8}};
        assertVecBatchEquals(resultVecBatch, expectedDatas);

        freeVecBatch(resultVecBatch);
        sortWithExprOperator.close();
        sortWithExprOperatorFactory.close();
    }

    /**
     * Test Sort by two dictionary columns with expression
     */
    @Test
    public void TestSortByTwoDictionaryWithExpr() {
        DataType[] sourceTypes = {IntDataType.INTEGER, IntDataType.INTEGER};
        Object[][] sourceDatas = {{5, 3, 2, 6, 1, 4, 7, 8}, {5, 3, 2, 6, 1, 4, 7, 8}};
        Vec[] vecs = new Vec[2];
        int[] ids = {0, 1, 2, 3, 4, 5, 6, 7};
        vecs[0] = TestUtils.createDictionaryVec(sourceTypes[0], sourceDatas[0], ids);
        vecs[1] = TestUtils.createDictionaryVec(sourceTypes[1], sourceDatas[1], ids);
        VecBatch vecBatch = new VecBatch(vecs);

        int[] outputCols = {0, 1};
        String[] sortKeys = {
                omniJsonFourArithmeticExpr("ADD", 1, getOmniJsonFieldReference(1, 0), getOmniJsonLiteral(1, false, 5)),
                omniJsonFourArithmeticExpr("ADD", 1, getOmniJsonLiteral(1, false, 5), getOmniJsonFieldReference(1, 1))};
        int[] ascendings = {1, 1};
        int[] nullFirsts = {0, 0};
        OmniSortWithExprOperatorFactory sortWithExprOperatorFactory = new OmniSortWithExprOperatorFactory(sourceTypes,
                outputCols, sortKeys, ascendings, nullFirsts);
        OmniOperator sortWithExprOperator = sortWithExprOperatorFactory.createOperator();
        sortWithExprOperator.addInput(vecBatch);
        Iterator<VecBatch> results = sortWithExprOperator.getOutput();

        VecBatch resultVecBatch = results.next();
        assertEquals(resultVecBatch.getRowCount(), sourceDatas[0].length);
        Object[][] expectedDatas = {{1, 2, 3, 4, 5, 6, 7, 8}, {1, 2, 3, 4, 5, 6, 7, 8}};
        assertVecBatchEquals(resultVecBatch, expectedDatas);

        freeVecBatch(resultVecBatch);
        sortWithExprOperator.close();
        sortWithExprOperatorFactory.close();
    }

    @Test
    public void testFactoryJitContextEquals() {
        DataType[] sourceTypes = {IntDataType.INTEGER, IntDataType.INTEGER};
        int[] outputCols = {0, 1};
        String[] sortKeys = {
                omniJsonFourArithmeticExpr("ADD", 1, getOmniJsonFieldReference(1, 0), getOmniJsonLiteral(1, false, 5)),
                omniJsonFourArithmeticExpr("ADD", 1, getOmniJsonLiteral(1, false, 5), getOmniJsonFieldReference(1, 1))};
        int[] ascendings = {1, 1};
        int[] nullFirsts = {0, 0};
        OmniSortWithExprOperatorFactory.JitContext factory1 = new OmniSortWithExprOperatorFactory.JitContext(
                sourceTypes, outputCols, sortKeys, ascendings, nullFirsts, new OperatorConfig());
        OmniSortWithExprOperatorFactory.JitContext factory2 = new OmniSortWithExprOperatorFactory.JitContext(
                sourceTypes, outputCols, sortKeys, ascendings, nullFirsts, new OperatorConfig());
        OmniSortWithExprOperatorFactory.JitContext factory3 = null;
        assertTrue(factory1.equals(factory2));
        assertTrue(factory1.equals(factory1));
        assertFalse(factory1.equals(factory3));
    }

    /**
     * Test Sort spill with multi records
     */
    @Test
    public void TestSortSpillWithMultiRecords() {
        DataType[] sourceTypes = {IntDataType.INTEGER, LongDataType.LONG};
        int[] outputCols = {0, 1};
        String[] sortKeys = {getOmniJsonFieldReference(1, 0), getOmniJsonFieldReference(2, 1)};
        int[] ascendings = {1, 1};
        int[] nullFirsts = {0, 0};
        OmniSortWithExprOperatorFactory sortWithExprOperatorFactory = new OmniSortWithExprOperatorFactory(sourceTypes,
                outputCols, sortKeys, ascendings, nullFirsts,
                new OperatorConfig(false, new SparkSpillConfig(true, generateSpillPath(), 1024, 5)));
        OmniOperator sortWithExprOperator = sortWithExprOperatorFactory.createOperator();

        Object[][] sourceDatas1 = {{5, 3, 2, 6, 1}, {5L, 3L, 2L, 6L, 1L}};
        VecBatch vecBatch1 = createVecBatch(sourceTypes, sourceDatas1);
        sortWithExprOperator.addInput(vecBatch1);

        Object[][] sourceDatas2 = {{4}, {4L}};
        VecBatch vecBatch2 = createVecBatch(sourceTypes, sourceDatas2);
        sortWithExprOperator.addInput(vecBatch2);

        Object[][] sourceDatas3 = {{15, 13, 12, 16, 11}, {15L, 13L, 12L, 16L, 11L}};
        VecBatch vecBatch3 = createVecBatch(sourceTypes, sourceDatas3);
        sortWithExprOperator.addInput(vecBatch3);
        Iterator<VecBatch> results = sortWithExprOperator.getOutput();

        VecBatch resultVecBatch = results.next();
        assertEquals(resultVecBatch.getRowCount(),
                sourceDatas1[0].length + sourceDatas2[0].length + sourceDatas3[0].length);
        Object[][] expectedDatas = {{1, 2, 3, 4, 5, 6, 11, 12, 13, 15, 16},
                {1L, 2L, 3L, 4L, 5L, 6L, 11L, 12L, 13L, 15L, 16L}};
        assertVecBatchEquals(resultVecBatch, expectedDatas);

        freeVecBatch(resultVecBatch);
        sortWithExprOperator.close();
        sortWithExprOperatorFactory.close();
    }

    /**
     * Test Sort spill with one record
     */
    @Test
    public void TestSortSpillWithOneRecord() {
        DataType[] sourceTypes = {IntDataType.INTEGER, LongDataType.LONG};
        int[] outputCols = {0, 1};
        String[] sortKeys = {getOmniJsonFieldReference(1, 0), getOmniJsonFieldReference(2, 1)};
        int[] ascendings = {1, 1};
        int[] nullFirsts = {0, 0};
        OmniSortWithExprOperatorFactory sortWithExprOperatorFactory = new OmniSortWithExprOperatorFactory(sourceTypes,
                outputCols, sortKeys, ascendings, nullFirsts,
                new OperatorConfig(false, new SparkSpillConfig(true, generateSpillPath(), 1024, 1)));
        OmniOperator sortWithExprOperator = sortWithExprOperatorFactory.createOperator();

        Object[][] sourceDatas1 = {{5}, {3L}};
        VecBatch vecBatch1 = createVecBatch(sourceTypes, sourceDatas1);
        sortWithExprOperator.addInput(vecBatch1);

        Object[][] sourceDatas2 = {{15}, {13L}};
        VecBatch vecBatch2 = createVecBatch(sourceTypes, sourceDatas2);
        sortWithExprOperator.addInput(vecBatch2);

        Object[][] sourceDatas3 = {{10}, {8L}};
        VecBatch vecBatch3 = createVecBatch(sourceTypes, sourceDatas3);
        sortWithExprOperator.addInput(vecBatch3);

        Iterator<VecBatch> results = sortWithExprOperator.getOutput();

        VecBatch resultVecBatch = results.next();
        assertEquals(resultVecBatch.getRowCount(),
                sourceDatas1[0].length + sourceDatas2[0].length + sourceDatas3[0].length);
        Object[][] expectedDatas = {{5, 10, 15}, {3L, 8L, 13L}};
        assertVecBatchEquals(resultVecBatch, expectedDatas);

        freeVecBatch(resultVecBatch);
        sortWithExprOperator.close();
        sortWithExprOperatorFactory.close();
    }

    @Test(expectedExceptions = OmniRuntimeException.class, expectedExceptionsMessageRegExp = "Enable spill but do not config spill path.")
    public void TestSortSpillWithEmptyPath() {
        DataType[] sourceTypes = {IntDataType.INTEGER, LongDataType.LONG};
        int[] outputCols = {0, 1};
        String[] sortKeys = {getOmniJsonFieldReference(1, 0), getOmniJsonFieldReference(2, 1)};
        int[] ascendings = {1, 1};
        int[] nullFirsts = {0, 0};
        OmniSortWithExprOperatorFactory sortWithExprOperatorFactory1 = new OmniSortWithExprOperatorFactory(sourceTypes,
                outputCols, sortKeys, ascendings, nullFirsts, new OperatorConfig(false, new SparkSpillConfig(null, 1)));

        OmniSortWithExprOperatorFactory sortWithExprOperatorFactory2 = new OmniSortWithExprOperatorFactory(sourceTypes,
                outputCols, sortKeys, ascendings, nullFirsts, new OperatorConfig(false, new SparkSpillConfig("", 1)));
    }

    @Test(expectedExceptions = OmniRuntimeException.class, expectedExceptionsMessageRegExp = ".*PATH_EXIST.*")
    public void TestSortSpillWithExistedPath() {
        DataType[] sourceTypes = {IntDataType.INTEGER, LongDataType.LONG};
        int[] outputCols = {0, 1};
        String[] sortKeys = {getOmniJsonFieldReference(1, 0), getOmniJsonFieldReference(2, 1)};
        int[] ascendings = {1, 1};
        int[] nullFirsts = {0, 0};
        OmniSortWithExprOperatorFactory sortWithExprOperatorFactory = new OmniSortWithExprOperatorFactory(sourceTypes,
                outputCols, sortKeys, ascendings, nullFirsts,
                new OperatorConfig(false, new SparkSpillConfig("/opt", 1)));
    }

    @Test(expectedExceptions = OmniRuntimeException.class, expectedExceptionsMessageRegExp = ".*DISK_SPACE_NOT_ENOUGH.*")
    public void TestSortSpillWithInvalidSpillSize() {
        DataType[] sourceTypes = {IntDataType.INTEGER, LongDataType.LONG};
        int[] outputCols = {0, 1};
        String[] sortKeys = {getOmniJsonFieldReference(1, 0), getOmniJsonFieldReference(2, 1)};
        int[] ascendings = {1, 1};
        int[] nullFirsts = {0, 0};
        OmniSortWithExprOperatorFactory sortWithExprOperatorFactory = new OmniSortWithExprOperatorFactory(sourceTypes,
                outputCols, sortKeys, ascendings, nullFirsts,
                new OperatorConfig(false, new SparkSpillConfig(true, generateSpillPath(), Long.MAX_VALUE, 1)));
    }

    @Test(expectedExceptions = OmniRuntimeException.class, expectedExceptionsMessageRegExp = ".*DISK_STAT_FAILED.*")
    public void TestSortSpillWithInvalidPath() {
        DataType[] sourceTypes = {IntDataType.INTEGER, LongDataType.LONG};
        int[] outputCols = {0, 1};
        String[] sortKeys = {getOmniJsonFieldReference(1, 0), getOmniJsonFieldReference(2, 1)};
        int[] ascendings = {1, 1};
        int[] nullFirsts = {0, 0};
        OmniSortWithExprOperatorFactory sortWithExprOperatorFactory = new OmniSortWithExprOperatorFactory(sourceTypes,
                outputCols, sortKeys, ascendings, nullFirsts,
                new OperatorConfig(false, new SparkSpillConfig("+-ab23", 1)));
    }
}
