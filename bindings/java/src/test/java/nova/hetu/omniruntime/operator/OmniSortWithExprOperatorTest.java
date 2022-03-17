
package nova.hetu.omniruntime.operator;

import static nova.hetu.omniruntime.util.TestUtils.assertVecBatchEquals;
import static nova.hetu.omniruntime.util.TestUtils.createVecBatch;
import static nova.hetu.omniruntime.util.TestUtils.freeVecBatch;

import static org.testng.Assert.assertEquals;

import nova.hetu.omniruntime.operator.sort.OmniSortWithExprOperatorFactory;
import nova.hetu.omniruntime.type.IntDataType;
import nova.hetu.omniruntime.type.LongDataType;
import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.util.TestUtils;
import nova.hetu.omniruntime.vector.Vec;
import nova.hetu.omniruntime.vector.VecBatch;

import org.testng.annotations.Test;

import java.util.Iterator;

public class OmniSortWithExprOperatorTest {
    /**
     * Test Sort by zero columns which one with expression
     */
    @Test
    public void TestSortByZeroColumnWithExpr() {
        DataType[] sourceTypes = {IntDataType.INTEGER, LongDataType.LONG};
        Object[][] sourceDatas = {{5, 3, 2, 6, 1, 4, 7, 8}, {5L, 3L, 2L, 6L, 1L, 4L, 7L, 8L}};
        VecBatch vecBatch = createVecBatch(sourceTypes, sourceDatas);

        int[] outputCols = {0, 1};
        String[] sortKeys = {"#0", "#1"};
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
        String[] sortKeys = {"ADD:1(#0, 5:1)", "#1"};
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
        String[] sortKeys = {"ADD:1(#0, 5:1)", "ADD:1(5:1, #1)"};
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
        Vec vecs[] = new Vec[2];
        int[] ids = {0, 1, 2, 3, 4, 5, 6, 7};
        vecs[0] = TestUtils.createDictionaryVec(sourceTypes[0], sourceDatas[0], ids);
        vecs[1] = TestUtils.createDictionaryVec(sourceTypes[1], sourceDatas[1], ids);
        VecBatch vecBatch = new VecBatch(vecs);

        int[] outputCols = {0, 1};
        String[] sortKeys = {"ADD:1(#0, 5:1)", "ADD:1(5:1, #1)"};
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
}
